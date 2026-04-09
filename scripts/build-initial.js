// scripts/build-initial.js
// 실행: node --env-file=.env scripts/build-initial.js
// 용도: 서비스 첫 배포 전 전체 DB 구축 — 판례 + 법령 + 행정규칙

import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { gzipSync } from 'zlib';

const s3 = new S3Client({
  region: 'auto',
  endpoint: process.env.R2_ENDPOINT,
  credentials: {
    accessKeyId:     process.env.R2_ACCESS_KEY,
    secretAccessKey: process.env.R2_SECRET_KEY,
  },
});

async function uploadGzip(key, payload) {
  const json    = JSON.stringify(payload);
  const gzipped = gzipSync(Buffer.from(json));
  await s3.send(new PutObjectCommand({
    Bucket: process.env.R2_BUCKET, Key: key, Body: gzipped,
    ContentType: 'application/json', ContentEncoding: 'gzip',
    CacheControl: 'public, s-maxage=3600, stale-while-revalidate=600',
  }));
  console.log(`✅ R2 업로드: ${key} (${(gzipped.length/1024).toFixed(1)}KB gzip)`);
}

/* ── 범용 API fetch (3회 재시도) ── */
async function apiFetch(url, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(30000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (e) {
      console.log(`  ⚠ attempt ${attempt}/${retries} failed: ${e.message}`);
      if (attempt === retries) throw e;
      await new Promise(r => setTimeout(r, 5000 * attempt));
    }
  }
}

/* ══════════════════════════════════════
   1. 판례 (prec) 수집
   ══════════════════════════════════════ */
const COURT_MAP = {
  '대법원':0, '헌법재판소':1, '서울고등법원':2, '서울중앙지방법원':3,
  '서울동부지방법원':4, '서울서부지방법원':5, '서울남부지방법원':6,
  '서울북부지방법원':7, '의정부지방법원':8, '인천지방법원':9,
  '수원지방법원':10, '춘천지방법원':11, '청주지방법원':12,
  '대전지방법원':13, '대구지방법원':14, '부산고등법원':15,
  '부산지방법원':16, '울산지방법원':17, '창원지방법원':18,
  '광주고등법원':19, '광주지방법원':20, '전주지방법원':21,
  '제주지방법원':22, '특허법원':23, '수원고등법원':24, '대전고등법원':25,
};
function courtToId(name) {
  if (COURT_MAP[name] !== undefined) return COURT_MAP[name];
  for (const [k, v] of Object.entries(COURT_MAP)) {
    if (name.includes(k) || k.includes(name)) return v;
  }
  return 99;
}

async function collectPrec() {
  console.log('\n═══ 판례 수집 시작 ═══');
  const db = {};
  const ranges = [
    '19480101~19991231', '20000101~20091231',
    '20100101~20151231', '20160101~20191231',
    '20200101~20221231', '20230101~20261231',
  ];

  for (const range of ranges) {
    console.log(`\n[판례] ${range}`);
    let page = 1;
    while (true) {
      const url = `https://www.law.go.kr/DRF/lawSearch.do`
        + `?OC=${process.env.LAW_OC_KEY}&target=prec&type=JSON`
        + `&display=100&page=${page}&prncYd=${range}&sort=date`;
      const data = await apiFetch(url);
      const raw = data?.PrecSearch?.prec;
      const hits = !raw ? [] : Array.isArray(raw) ? raw : [raw];
      if (!hits.length) break;

      for (const p of hits) {
        const key = p.사건번호?.replace(/\s+/g, '');
        if (key) {
          const dateNum = parseInt((p.선고일자 || '').replace(/\./g, ''), 10) || 0;
          db[key] = [dateNum, courtToId(p.법원명 || '')];
        }
      }
      const total = Object.keys(db).length;
      console.log(`  p.${page}: +${hits.length} (누계 ${total.toLocaleString()})`);
      page++;
      await new Promise(r => setTimeout(r, 300));
    }
  }

  const count = Object.keys(db).length;
  console.log(`\n판례 수집 완료: ${count.toLocaleString()}건`);
  return { db, count };
}

/* ══════════════════════════════════════
   2. 법령 (law) 수집 — 법률, 대통령령, 시행규칙 등
   ══════════════════════════════════════ */
const LAW_TYPE_MAP = {
  '헌법':0, '법률':1, '대통령령':2, '총리령':3, '부령':4,
};
function lawTypeToId(name) {
  if (LAW_TYPE_MAP[name] !== undefined) return LAW_TYPE_MAP[name];
  // "행정안전부령" 등은 부령(4)
  if (name.includes('령') && !name.includes('대통령')) return 4;
  return 9;
}

async function collectLaw() {
  console.log('\n═══ 법령 수집 시작 ═══');
  const db = {};
  let page = 1;

  while (true) {
    const url = `https://www.law.go.kr/DRF/lawSearch.do`
      + `?OC=${process.env.LAW_OC_KEY}&target=law&type=JSON`
      + `&display=100&page=${page}`;
    const data = await apiFetch(url);
    const raw = data?.LawSearch?.law;
    const hits = !raw ? [] : Array.isArray(raw) ? raw : [raw];
    if (!hits.length) break;

    for (const l of hits) {
      const name = (l.법령명한글 || '').trim();
      if (!name) continue;
      const dateNum = parseInt((l.시행일자 || '').replace(/[^0-9]/g, ''), 10) || 0;
      const typeId = lawTypeToId(l.법령구분명 || '');
      const mst = parseInt(l.법령일련번호 || '0', 10) || 0;
      // key: 법령명, value: [법령구분코드, 시행일자, 법령일련번호]
      db[name] = [typeId, dateNum, mst];
    }

    const total = Object.keys(db).length;
    console.log(`  p.${page}: +${hits.length} (누계 ${total.toLocaleString()})`);
    page++;
    await new Promise(r => setTimeout(r, 300));
  }

  const count = Object.keys(db).length;
  console.log(`\n법령 수집 완료: ${count.toLocaleString()}건`);
  return { db, count };
}

/* ══════════════════════════════════════
   3. 행정규칙 (admrul) 수집 — 훈령, 예규, 고시, 공고 등
   ══════════════════════════════════════ */
const ADMRUL_TYPE_MAP = {
  '훈령':0, '예규':1, '고시':2, '공고':3, '등기예규':4, '등기선례':5,
};
function admrulTypeToId(name) {
  if (ADMRUL_TYPE_MAP[name] !== undefined) return ADMRUL_TYPE_MAP[name];
  return 9;
}

async function collectAdmrul() {
  console.log('\n═══ 행정규칙 수집 시작 ═══');
  const db = {};
  let page = 1;

  while (true) {
    const url = `https://www.law.go.kr/DRF/lawSearch.do`
      + `?OC=${process.env.LAW_OC_KEY}&target=admrul&type=JSON`
      + `&display=100&page=${page}`;
    const data = await apiFetch(url);
    const raw = data?.AdmRulSearch?.admrul;
    const hits = !raw ? [] : Array.isArray(raw) ? raw : [raw];
    if (!hits.length) break;

    for (const a of hits) {
      const name = (a.행정규칙명 || '').trim();
      if (!name) continue;
      const dateNum = parseInt((a.시행일자 || '').replace(/[^0-9]/g, ''), 10) || 0;
      const typeId = admrulTypeToId(a.행정규칙종류 || '');
      const serial = parseInt(a.행정규칙일련번호 || '0', 10) || 0;
      // key: 행정규칙명, value: [종류코드, 시행일자, 일련번호]
      db[name] = [typeId, dateNum, serial];
    }

    const total = Object.keys(db).length;
    console.log(`  p.${page}: +${hits.length} (누계 ${total.toLocaleString()})`);
    page++;
    await new Promise(r => setTimeout(r, 300));
  }

  const count = Object.keys(db).length;
  console.log(`\n행정규칙 수집 완료: ${count.toLocaleString()}건`);
  return { db, count };
}

/* ══════════════════════════════════════
   메인 — 3종 전체 수집 + R2 업로드
   ══════════════════════════════════════ */
async function main() {
  console.log('[전체 수집 시작]', new Date().toISOString());
  const todayStr = new Date().toISOString().slice(0, 10).replace(/-/g, '');

  // 1. 판례 수집
  const prec = await collectPrec();
  await uploadGzip('prec/v1/db.json', { version: 1, updated: Date.now(), count: prec.count, db: prec.db });
  await uploadGzip('prec/v1/meta.json', { lastUpdated: todayStr, count: prec.count, updatedAt: Date.now(), addedToday: 0 });

  // 2. 법령 수집
  const law = await collectLaw();
  await uploadGzip('law/v1/db.json', { version: 1, updated: Date.now(), count: law.count, db: law.db });
  await uploadGzip('law/v1/meta.json', { lastUpdated: todayStr, count: law.count, updatedAt: Date.now(), addedToday: 0 });

  // 3. 행정규칙 수집
  const admrul = await collectAdmrul();
  await uploadGzip('admrul/v1/db.json', { version: 1, updated: Date.now(), count: admrul.count, db: admrul.db });
  await uploadGzip('admrul/v1/meta.json', { lastUpdated: todayStr, count: admrul.count, updatedAt: Date.now(), addedToday: 0 });

  // 4. 통합 meta 업로드
  await uploadGzip('all/v1/meta.json', {
    lastUpdated: todayStr, updatedAt: Date.now(),
    prec: prec.count, law: law.count, admrul: admrul.count,
    total: prec.count + law.count + admrul.count,
  });

  console.log(`\n🎉 전체 DB 완성`);
  console.log(`  판례:     ${prec.count.toLocaleString()}건`);
  console.log(`  법령:     ${law.count.toLocaleString()}건`);
  console.log(`  행정규칙: ${admrul.count.toLocaleString()}건`);
  console.log(`  총계:     ${(prec.count + law.count + admrul.count).toLocaleString()}건`);
}

main().catch(e => { console.error(e); process.exit(1); });
