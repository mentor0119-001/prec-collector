// scripts/update-incremental.js
// 실행: GitHub Actions cron (매일 KST 00:00)
// 용도: 신규 판례 + 법령 + 행정규칙 증분 수집 → 기존 DB 병합 → R2 덮어쓰기

import { S3Client, PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { gzipSync, gunzipSync } from 'zlib';

const s3 = new S3Client({
  region: 'auto',
  endpoint: process.env.R2_ENDPOINT,
  credentials: {
    accessKeyId:     process.env.R2_ACCESS_KEY,
    secretAccessKey: process.env.R2_SECRET_KEY,
  },
});

/* ── R2 유틸 ── */
async function downloadFromR2(key) {
  const res  = await s3.send(new GetObjectCommand({ Bucket: process.env.R2_BUCKET, Key: key }));
  const buf  = Buffer.from(await res.Body.transformToByteArray());
  const json = gunzipSync(buf).toString('utf-8');
  return JSON.parse(json);
}

async function uploadToR2(key, payload) {
  const gzipped = gzipSync(Buffer.from(JSON.stringify(payload)));
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
      console.log(`  ⚠ attempt ${attempt}/${retries}: ${e.message}`);
      if (attempt === retries) throw e;
      await new Promise(r => setTimeout(r, 5000 * attempt));
    }
  }
}

/* ── 날짜 유틸 ── */
function toApiDate(date) { return date.toISOString().slice(0, 10).replace(/-/g, ''); }
function daysAgo(n) { const d = new Date(); d.setDate(d.getDate() - n); return d; }

/* ══════════════════════════════════════
   1. 판례 증분 수집
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
  for (const [k, v] of Object.entries(COURT_MAP)) { if (name.includes(k) || k.includes(name)) return v; }
  return 99;
}

async function updatePrec(toDate) {
  console.log('\n── 판례 증분 수집 ──');
  let meta;
  try { meta = await downloadFromR2('prec/v1/meta.json'); }
  catch { meta = { lastUpdated: toApiDate(daysAgo(3)), count: 0 }; }
  console.log(`  기존: ${meta.count?.toLocaleString()}건, 마지막: ${meta.lastUpdated}`);

  const newEntries = {};
  let page = 1;
  while (true) {
    const url = `https://www.law.go.kr/DRF/lawSearch.do`
      + `?OC=${process.env.LAW_OC_KEY}&target=prec&type=JSON`
      + `&display=100&page=${page}&prncYd=${meta.lastUpdated}~${toDate}&sort=date`;
    const data = await apiFetch(url);
    const raw = data?.PrecSearch?.prec;
    const hits = !raw ? [] : Array.isArray(raw) ? raw : [raw];
    if (!hits.length) break;
    for (const p of hits) {
      const key = p.사건번호?.replace(/\s+/g, '');
      if (key) newEntries[key] = [parseInt((p.선고일자||'').replace(/\./g,''),10)||0, courtToId(p.법원명||'')];
    }
    console.log(`  p.${page}: +${hits.length}건`);
    page++;
    await new Promise(r => setTimeout(r, 300));
  }

  const newCount = Object.keys(newEntries).length;
  if (newCount === 0) {
    console.log('  신규 판례 없음');
    await uploadToR2('prec/v1/meta.json', { lastUpdated: toDate, count: meta.count, updatedAt: Date.now(), addedToday: 0 });
    return { count: meta.count, added: 0 };
  }

  const existing = await downloadFromR2('prec/v1/db.json');
  const merged = { ...existing.db, ...newEntries };
  const total = Object.keys(merged).length;
  await uploadToR2('prec/v1/db.json', { version: 1, updated: Date.now(), count: total, db: merged });
  await uploadToR2('prec/v1/meta.json', { lastUpdated: toDate, count: total, updatedAt: Date.now(), addedToday: newCount });
  console.log(`  판례: +${newCount} = ${total.toLocaleString()}건`);
  return { count: total, added: newCount };
}

/* ══════════════════════════════════════
   2. 법령 증분 수집 (전체 덮어쓰기 — 법령은 개정으로 내용 변경됨)
   ══════════════════════════════════════ */
const LAW_TYPE_MAP = { '헌법':0, '법률':1, '대통령령':2, '총리령':3, '부령':4 };
function lawTypeToId(name) {
  if (LAW_TYPE_MAP[name] !== undefined) return LAW_TYPE_MAP[name];
  if (name.includes('령') && !name.includes('대통령')) return 4;
  return 9;
}

async function updateLaw(toDate) {
  console.log('\n── 법령 수집 (전체) ──');
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
      const name = (l.법령명한글||'').trim();
      if (!name) continue;
      db[name] = [lawTypeToId(l.법령구분명||''), parseInt((l.시행일자||'').replace(/[^0-9]/g,''),10)||0, parseInt(l.법령일련번호||'0',10)||0];
    }
    if (page % 50 === 0) console.log(`  p.${page}: 누계 ${Object.keys(db).length.toLocaleString()}`);
    page++;
    await new Promise(r => setTimeout(r, 300));
  }

  const count = Object.keys(db).length;
  await uploadToR2('law/v1/db.json', { version: 1, updated: Date.now(), count, db });
  await uploadToR2('law/v1/meta.json', { lastUpdated: toDate, count, updatedAt: Date.now() });
  console.log(`  법령: ${count.toLocaleString()}건`);
  return { count };
}

/* ══════════════════════════════════════
   3. 행정규칙 증분 수집 (전체 덮어쓰기)
   ══════════════════════════════════════ */
const ADMRUL_TYPE_MAP = { '훈령':0, '예규':1, '고시':2, '공고':3, '등기예규':4, '등기선례':5 };
function admrulTypeToId(name) { return ADMRUL_TYPE_MAP[name] ?? 9; }

async function updateAdmrul(toDate) {
  console.log('\n── 행정규칙 수집 (전체) ──');
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
      const name = (a.행정규칙명||'').trim();
      if (!name) continue;
      db[name] = [admrulTypeToId(a.행정규칙종류||''), parseInt((a.시행일자||'').replace(/[^0-9]/g,''),10)||0, parseInt(a.행정규칙일련번호||'0',10)||0];
    }
    if (page % 50 === 0) console.log(`  p.${page}: 누계 ${Object.keys(db).length.toLocaleString()}`);
    page++;
    await new Promise(r => setTimeout(r, 300));
  }

  const count = Object.keys(db).length;
  await uploadToR2('admrul/v1/db.json', { version: 1, updated: Date.now(), count, db });
  await uploadToR2('admrul/v1/meta.json', { lastUpdated: toDate, count, updatedAt: Date.now() });
  console.log(`  행정규칙: ${count.toLocaleString()}건`);
  return { count };
}

/* ══════════════════════════════════════
   메인
   ══════════════════════════════════════ */
async function main() {
  console.log('[증분 수집 시작]', new Date().toISOString());
  const toDate = toApiDate(new Date());

  const prec   = await updatePrec(toDate);
  const law    = await updateLaw(toDate);
  const admrul = await updateAdmrul(toDate);

  // 통합 meta 업데이트
  await uploadToR2('all/v1/meta.json', {
    lastUpdated: toDate, updatedAt: Date.now(),
    prec: prec.count, law: law.count, admrul: admrul.count,
    total: prec.count + law.count + admrul.count,
  });

  console.log(`\n✅ 증분 수집 완료`);
  console.log(`  판례:     ${prec.count.toLocaleString()}건 (+${prec.added})`);
  console.log(`  법령:     ${law.count.toLocaleString()}건`);
  console.log(`  행정규칙: ${admrul.count.toLocaleString()}건`);
  console.log(`  총계:     ${(prec.count + law.count + admrul.count).toLocaleString()}건`);
}

main().catch(e => { console.error(e); process.exit(1); });
