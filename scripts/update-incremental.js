// scripts/update-incremental.js
// 실행: GitHub Actions cron (매일 KST 00:00)
// 용도: 신규 판례 + 법령 + 행정규칙 + 전원합의체 증분 수집 → 기존 DB 병합 → R2 덮어쓰기

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
  const summaryQueue = [];  // [v2.0 M1] 상세 조회 큐 (RAG summary 수집)
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
      if (!key) continue;
      // [M2-fix] db 스키마 3필드 확장: [dateNum, courtId, serialId]
      const serialId = parseInt(p.판례일련번호 || '0', 10) || 0;
      newEntries[key] = [
        parseInt((p.선고일자||'').replace(/\./g,''),10)||0,
        courtToId(p.법원명||''),
        serialId,
      ];
      if (serialId) summaryQueue.push({ key, serialId });
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

  // [v2.0 M1] 신규 판례 상세 조회 + summary 업로드 (DB 병합 전 수행)
  await collectSummaries(summaryQueue);

  const existing = await downloadFromR2('prec/v1/db.json');
  const merged = { ...existing.db, ...newEntries };
  const total = Object.keys(merged).length;
  await uploadToR2('prec/v1/db.json', { version: 1, updated: Date.now(), count: total, db: merged });
  await uploadToR2('prec/v1/meta.json', { lastUpdated: toDate, count: total, updatedAt: Date.now(), addedToday: newCount });
  console.log(`  판례: +${newCount} = ${total.toLocaleString()}건`);
  return { count: total, added: newCount };
}

/* ══════════════════════════════════════
   [v2.0 M1] 판례 summary 수집 — RAG 파이프라인 Phase 1
   Design §5.1 준수 · collectPlenary()의 lawService.do 패턴 재사용
   ══════════════════════════════════════ */
function parseLaws(rawRef) {
  return (rawRef || '')
    .replace(/<br\s*\/?>/gi, '\n')
    .split(/[,、\n]+/)
    .map(s => s.trim())
    .filter(s => s.length > 1)
    .slice(0, 20);
}

function parseCaseNums(rawRef) {
  return (rawRef || '').match(/\b(19|20)\d{2}[가-힣]{1,3}\d+\b/g) || [];
}

function buildSummaryPayload(key, d) {
  return {
    caseNum:  key,
    title:    d.사건명 || '',
    caseType: d.사건종류명 || '',
    issue:    (d.판시사항 || '').slice(0, 2000),
    summary:  (d.판결요지 || '').slice(0, 2000),
    laws:     parseLaws(d.참조조문 || ''),
    refs:     parseCaseNums(d.참조판례 || ''),
    changed:  /변경|파기|종전.*변경|판례.*변경/.test(d.판례내용 || ''),
    date:     (d.선고일자 || '').replace(/\./g, ''),
    court:    d.법원명 || '',
  };
}

async function collectSummaries(queue) {
  if (!queue.length) { console.log('  [summary] 대상 없음'); return; }
  console.log(`  [summary 수집 시작] ${queue.length.toLocaleString()}건`);
  let ok = 0, fail = 0;
  for (const { key, serialId } of queue) {
    try {
      const url = `https://www.law.go.kr/DRF/lawService.do`
        + `?OC=${process.env.LAW_OC_KEY}&target=prec&type=JSON&ID=${serialId}`;
      const detail = await apiFetch(url);
      const d = detail?.PrecService;
      if (!d) { fail++; continue; }

      const payload = buildSummaryPayload(key, d);
      await s3.send(new PutObjectCommand({
        Bucket:       process.env.R2_BUCKET,
        Key:          `prec/summary/${key}.json`,
        Body:         JSON.stringify(payload),
        ContentType:  'application/json',
        CacheControl: 'public, s-maxage=86400, stale-while-revalidate=3600',
      }));
      ok++;
      if (ok % 100 === 0) console.log(`    진행: ${ok}/${queue.length}`);
    } catch (e) {
      fail++;
      if (fail <= 20) console.log(`    실패 ${key}: ${e.message}`);
    }
    await new Promise(r => setTimeout(r, 300));  // rate limit (초당 ~3건)
  }
  console.log(`  [summary 수집 완료] 성공 ${ok.toLocaleString()} / 실패 ${fail.toLocaleString()}`);
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
   4. 전원합의체 판결 증분 수집
   - lastUpdated 있으면: 해당 날짜 이후만 스캔 (일별 증분 — 빠름)
   - lastUpdated 없으면: 스킵 (초기수집은 collect-plenary.yml 워크플로우로 별도 실행)
   ══════════════════════════════════════ */
async function updatePlenary(toDate) {
  console.log('\n── 전원합의체 판결 증분 수집 ──');

  // 기존 plenary.json 로드 (plain JSON, not gzip)
  let existing = { list: [], lastUpdated: null };
  try {
    const res = await s3.send(new GetObjectCommand({ Bucket: process.env.R2_BUCKET, Key: 'prec/plenary.json' }));
    const buf = Buffer.from(await res.Body.transformToByteArray());
    existing = JSON.parse(buf.toString('utf-8'));
  } catch { /* 없으면 빈 목록 */ }
  const existingNums = new Set((existing.list || []).map(p => p.caseNum));
  const lastDate = existing.lastUpdated || null;
  console.log(`  기존: ${existingNums.size}건, lastUpdated: ${lastDate || '없음'}`);

  // lastUpdated 없으면 증분 스캔 불가 — collect-plenary.yml 워크플로우로 초기수집 필요
  if (!lastDate) {
    console.log('  ⚠ lastUpdated 없음 — 초기수집 미완료. collect-plenary.yml 워크플로우를 먼저 실행하세요.');
    return { count: existingNums.size, added: 0 };
  }

  // 날짜 필터: lastUpdated 이후 신규만 스캔
  const dateFilter = `&prncYd=${lastDate}~${toDate}`;
  console.log(`  날짜필터: ${lastDate}~${toDate}`);

  // 법제처 전문검색 — "전원합의체" 포함 대법원 판결
  const ENC_QUERY = encodeURIComponent('전원합의체');
  const ENC_COURT = encodeURIComponent('대법원');
  const newCases = [];
  let page = 1;
  while (true) {
    const url = `https://www.law.go.kr/DRF/lawSearch.do`
      + `?OC=${process.env.LAW_OC_KEY}&target=prec&type=JSON`
      + `&search=2&query=${ENC_QUERY}&display=100&page=${page}&courtNm=${ENC_COURT}${dateFilter}`;
    const data = await apiFetch(url);
    const raw = data?.PrecSearch?.prec;
    const hits = !raw ? [] : Array.isArray(raw) ? raw : [raw];
    if (!hits.length) break;

    for (const p of hits) {
      const caseNum = (p.사건번호 || '').replace(/\s+/g, '');
      if (!caseNum || existingNums.has(caseNum)) continue;

      // 상세 조회 — 실제 전원합의체 판결인지 본문으로 검증
      await new Promise(r => setTimeout(r, 200));
      let detail;
      try {
        detail = await apiFetch(
          `https://www.law.go.kr/DRF/lawService.do?OC=${process.env.LAW_OC_KEY}&target=prec&ID=${p.판례일련번호}&type=JSON`
        );
      } catch (e) { console.log(`  상세조회 실패 ${caseNum}: ${e.message}`); continue; }
      const d = detail?.PrecService;
      if (!d) continue;
      const content = d.판례내용 || '';
      // 인용 판결 제외 — 실제 전원합의체로 선고된 경우만
      const isPlenary = /전원합의체.*선고|선고.*전원합의체|전원합의체[\s가-힣]*(판결|결정)/.test(content.slice(0, 500))
        || (d.선고 || '').includes('전원합의체');
      if (!isPlenary) continue;

      const dateRaw = (d.선고일자 || '').replace(/\./g, '');
      const laws = (d.참조조문 || '').split(/[,、\n]+/).map(s => s.trim()).filter(s => s.length > 1).slice(0, 8);
      const changed = /변경|파기|종전.*변경|판례.*변경/.test(content);

      newCases.push({
        caseNum,
        date: dateRaw,
        court: d.법원명 || '대법원',
        subject: d.사건명 || '',
        overrules: changed,
        oldRule: '', newRule: '',
        keywords: [],
        laws,
        casenoteUrl: `https://casenote.kr/${encodeURIComponent(d.법원명||'대법원')}/${encodeURIComponent(caseNum)}`,
      });
      existingNums.add(caseNum);
      console.log(`  + ${caseNum} (${dateRaw}) ${changed ? '[법리변경]' : ''}`);
    }
    page++;
    if (page % 10 === 0) console.log(`  페이지 ${page} 처리중...`);
    await new Promise(r => setTimeout(r, 400));
  }

  // 신규 없어도 lastUpdated 항상 갱신
  const merged = [...(existing.list || []), ...newCases];
  merged.sort((a, b) => (b.date || '').localeCompare(a.date || ''));

  await s3.send(new PutObjectCommand({
    Bucket: process.env.R2_BUCKET, Key: 'prec/plenary.json',
    Body: JSON.stringify({ list: merged, lastUpdated: toDate, updatedAt: Date.now() }),
    ContentType: 'application/json',
    CacheControl: 'public, s-maxage=3600, stale-while-revalidate=600',
  }));
  console.log(`  전원합의체: +${newCases.length} = ${merged.length}건 (lastUpdated: ${toDate})`);
  return { count: merged.length, added: newCases.length };
}

/* ══════════════════════════════════════
   메인
   --prec-only  : 판례 + 전원합의체 증분만 (빠름 ~3분, API ~5회)
   --full-daily : 판례 + 법령 + 행정규칙 + 전원합의체 (하루 1회, ~30분, API ~300회)
   (인자 없음)  : full-daily와 동일 (하위 호환)
   ══════════════════════════════════════ */
async function main() {
  const isPrecOnly = process.argv.includes('--prec-only');
  const mode = isPrecOnly ? 'prec-only' : 'full-daily';
  console.log(`[증분 수집 시작] mode=${mode}`, new Date().toISOString());
  const toDate = toApiDate(new Date());

  // 판례 증분은 항상 실행
  const prec = await updatePrec(toDate);

  // 전원합의체 증분은 항상 실행 (빠름 — 날짜 필터 적용 시 2 API call)
  const plenary = await updatePlenary(toDate);

  let law    = null;
  let admrul = null;

  if (!isPrecOnly) {
    // 법령 + 행정규칙은 full-daily에서만 (변경 빈도 낮음 — 하루 1회 충분)
    law    = await updateLaw(toDate);
    admrul = await updateAdmrul(toDate);
  } else {
    console.log('\n── 법령·행정규칙 스킵 (prec-only 모드) ──');
  }

  // 통합 meta 업데이트 (prec-only 모드에서는 법령·행정규칙 카운트 기존값 유지)
  let prevMeta = {};
  try { prevMeta = await downloadFromR2('all/v1/meta.json'); } catch {}

  await uploadToR2('all/v1/meta.json', {
    lastUpdated: toDate,
    updatedAt: Date.now(),
    prec:    prec.count,
    law:     law    ? law.count    : (prevMeta.law    || 0),
    admrul:  admrul ? admrul.count : (prevMeta.admrul || 0),
    plenary: plenary.count,
    total:   prec.count + (law ? law.count : (prevMeta.law||0)) + (admrul ? admrul.count : (prevMeta.admrul||0)),
  });

  console.log(`\n✅ 수집 완료 [${mode}]`);
  console.log(`  판례:       ${prec.count.toLocaleString()}건 (+${prec.added})`);
  if (law)    console.log(`  법령:       ${law.count.toLocaleString()}건`);
  if (admrul) console.log(`  행정규칙:   ${admrul.count.toLocaleString()}건`);
  console.log(`  전원합의체: ${plenary.count}건 (+${plenary.added})`);
}

main().catch(e => { console.error(e); process.exit(1); });
