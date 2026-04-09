// scripts/update-incremental.js
// 실행: GitHub Actions cron (매일 KST 00:00)
// 용도: 신규 판례만 증분 수집 → 기존 DB와 병합 → R2 덮어쓰기

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

/* ── R2 파일 다운로드 + gzip 해제 ── */
async function downloadFromR2(key) {
  const res  = await s3.send(new GetObjectCommand({ Bucket: process.env.R2_BUCKET, Key: key }));
  const buf  = Buffer.from(await res.Body.transformToByteArray());
  const json = gunzipSync(buf).toString('utf-8');
  return JSON.parse(json);
}

/* ── R2 업로드 (gzip) ── */
async function uploadToR2(key, payload) {
  const gzipped = gzipSync(Buffer.from(JSON.stringify(payload)));
  await s3.send(new PutObjectCommand({
    Bucket: process.env.R2_BUCKET, Key: key, Body: gzipped,
    ContentType: 'application/json', ContentEncoding: 'gzip',
    CacheControl: 'public, s-maxage=3600, stale-while-revalidate=600',
  }));
  console.log(`✅ R2 업로드: ${key} (${(gzipped.length/1024).toFixed(1)}KB gzip)`);
}

/* ── 법제처 특정 날짜 범위 수집 ── */
async function collectRange(fromDate, toDate) {
  const newEntries = {};
  let page = 1;

  while (true) {
    const url = `https://www.law.go.kr/DRF/lawSearch.do`
      + `?OC=${process.env.LAW_OC_KEY}&target=prec&type=JSON`
      + `&display=100&page=${page}&prncYd=${fromDate}~${toDate}&sort=date`;

    const res  = await fetch(url);
    const data = await res.json();
    const raw  = data?.PrecSearch?.prec;
    const hits = !raw ? [] : Array.isArray(raw) ? raw : [raw];

    if (!hits.length) break;

    for (const p of hits) {
      const key = p.사건번호?.replace(/\s+/g, '');
      if (key) {
        const dateNum = parseInt((p.선고일자 || '').replace(/\./g, ''), 10) || 0;
        const courtId = courtToId(p.법원명 || '');
        newEntries[key] = [dateNum, courtId];
      }
    }

    console.log(`  신규 p.${page}: +${hits.length}건`);
    page++;
    await new Promise(r => setTimeout(r, 300));
  }

  return newEntries;
}

/* ── 날짜 유틸 ── */
function toApiDate(date) {
  return date.toISOString().slice(0, 10).replace(/-/g, '');
}

function daysAgo(n) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d;
}

/* ── 법원명 → 코드 ── */
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

/* ── 메인 ── */
async function main() {
  console.log('[증분 수집 시작]', new Date().toISOString());

  // 1. R2에서 meta.json 로드 → 마지막 수집일 확인
  let meta;
  try {
    meta = await downloadFromR2('prec/v1/meta.json');
    console.log(`마지막 수집일: ${meta.lastUpdated} (${meta.count?.toLocaleString()}건)`);
  } catch {
    console.log('meta.json 없음 → 3일 전부터 수집');
    meta = { lastUpdated: toApiDate(daysAgo(3)), count: 0 };
  }

  // 2. 수집 날짜 범위 결정
  const fromDate = meta.lastUpdated;
  const toDate   = toApiDate(new Date());
  console.log(`수집 범위: ${fromDate} ~ ${toDate}`);

  // 3. 신규 판례 수집
  const newEntries = await collectRange(fromDate, toDate);
  const newCount   = Object.keys(newEntries).length;
  console.log(`신규 수집: ${newCount}건`);

  if (newCount === 0) {
    console.log('신규 판례 없음. meta.json만 갱신.');
    await uploadToR2('prec/v1/meta.json', {
      lastUpdated: toDate,
      count: meta.count,
      updatedAt: Date.now(),
      addedToday: 0,
    });
    return;
  }

  // 4. 기존 DB 다운로드
  console.log('기존 DB 다운로드 중...');
  const existing = await downloadFromR2('prec/v1/db.json');

  // 5. 병합 (신규가 기존을 덮어씀 — 선고일 정정 반영)
  const merged = { ...existing.db, ...newEntries };
  const total  = Object.keys(merged).length;
  console.log(`병합 완료: ${existing.count?.toLocaleString()} + ${newCount} = ${total.toLocaleString()}건`);

  // 6. R2 업로드 (db.json + meta.json)
  await uploadToR2('prec/v1/db.json', {
    version: 1, updated: Date.now(), count: total, db: merged,
  });
  await uploadToR2('prec/v1/meta.json', {
    lastUpdated: toDate,
    count: total,
    updatedAt: Date.now(),
    addedToday: newCount,
  });

  console.log(`✅ 완료: +${newCount}건 추가 (총 ${total.toLocaleString()}건)`);
}

main().catch(e => { console.error(e); process.exit(1); });
