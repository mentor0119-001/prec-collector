// scripts/backfill-summaries.js
// 실행: node scripts/backfill-summaries.js [--chunk=2500]
// 목적: 기존 140K 판례 중 summary 미존재분을 청크 단위로 일괄 수집 → R2 prec/summary/
//
// rate limit: 법제처 OC key 일 ~10K 호출 가능 → 청크당 2,500 (여유 버퍼)
// Design §5.2 + TODO-2 반영:
//   - 기존 HEAD 호출 14만 회 대신 ListObjectsV2 페이지네이션으로 기존 summary Set 구성

import { S3Client, ListObjectsV2Command, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { gunzipSync } from 'zlib';

const CHUNK_SIZE = parseInt(
  process.argv.find(a => a.startsWith('--chunk='))?.split('=')[1] || '2500',
  10
);

const s3 = new S3Client({
  region: 'auto',
  endpoint: process.env.R2_ENDPOINT,
  credentials: {
    accessKeyId:     process.env.R2_ACCESS_KEY,
    secretAccessKey: process.env.R2_SECRET_KEY,
  },
});

/* ── 범용 API fetch (3회 재시도) ── */
async function apiFetch(url, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(30000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (e) {
      if (attempt === retries) throw e;
      await new Promise(r => setTimeout(r, 5000 * attempt));
    }
  }
}

/* ── summary 헬퍼 (collectSummaries와 동일 스키마) ── */
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

async function main() {
  console.log('[backfill-summaries 시작]', new Date().toISOString());
  console.log(`  CHUNK_SIZE = ${CHUNK_SIZE}`);

  // [1/4] 기존 prec/v1/db.json 로드 → 전체 사건번호 목록
  console.log('\n[1/4] prec/v1/db.json 로드...');
  const dbRes = await s3.send(new GetObjectCommand({
    Bucket: process.env.R2_BUCKET, Key: 'prec/v1/db.json',
  }));
  const dbBuf  = Buffer.from(await dbRes.Body.transformToByteArray());
  const dbJson = JSON.parse(gunzipSync(dbBuf).toString('utf-8'));
  const allCaseNums = Object.keys(dbJson.db || {});
  console.log(`  전체 판례: ${allCaseNums.length.toLocaleString()}건`);

  // [2/4] 기존 summary 목록 조회 (ListObjectsV2 페이지네이션) — Design TODO-2
  console.log('\n[2/4] 기존 summary 목록 조회 (ListObjectsV2)...');
  const existing = new Set();
  let token;
  let listPages = 0;
  do {
    const res = await s3.send(new ListObjectsV2Command({
      Bucket: process.env.R2_BUCKET,
      Prefix: 'prec/summary/',
      ContinuationToken: token,
      MaxKeys: 1000,
    }));
    for (const obj of (res.Contents || [])) {
      const key = obj.Key.replace('prec/summary/', '').replace('.json', '');
      if (key) existing.add(key);
    }
    token = res.NextContinuationToken;
    listPages++;
    if (listPages % 10 === 0) {
      console.log(`  페이지 ${listPages}: 누계 ${existing.size.toLocaleString()}`);
    }
  } while (token);
  console.log(`  기존 summary: ${existing.size.toLocaleString()}건 (페이지 ${listPages})`);

  // [3/4] 미존재분 선별 → 청크
  console.log('\n[3/4] 미존재분 선별...');
  const missing = allCaseNums.filter(k => !existing.has(k));
  console.log(`  미존재: ${missing.length.toLocaleString()}건`);

  if (missing.length === 0) {
    console.log('\n[완료] 모든 판례의 summary가 존재합니다. 추가 수집 불필요.');
    return;
  }

  const chunk = missing.slice(0, CHUNK_SIZE);
  console.log(`  이번 청크: ${chunk.length.toLocaleString()}건`);
  console.log(`  남은 예상: ${(missing.length - chunk.length).toLocaleString()}건`);
  console.log(`  전체 완료까지 예상 청크 수: ${Math.ceil(missing.length / CHUNK_SIZE)}`);

  // [4/4] 청크 처리 — 사건번호별 lawSearch → lawService → R2 PUT
  console.log('\n[4/4] summary 수집 실행...');
  let ok = 0, fail = 0, noHit = 0;
  const startMs = Date.now();

  for (const caseNum of chunk) {
    try {
      // 4-a. 사건번호로 lawSearch → 판례일련번호 획득
      const searchUrl = `https://www.law.go.kr/DRF/lawSearch.do`
        + `?OC=${process.env.LAW_OC_KEY}&target=prec&type=JSON`
        + `&search=1&query=${encodeURIComponent(caseNum)}`;
      const search = await apiFetch(searchUrl);
      const raw    = search?.PrecSearch?.prec;
      const hits   = !raw ? [] : Array.isArray(raw) ? raw : [raw];
      const hit    = hits.find(p => p.사건번호?.replace(/\s+/g, '') === caseNum);
      if (!hit?.판례일련번호) { noHit++; continue; }

      // 4-b. lawService.do 상세 조회
      const detailUrl = `https://www.law.go.kr/DRF/lawService.do`
        + `?OC=${process.env.LAW_OC_KEY}&target=prec&type=JSON&ID=${hit.판례일련번호}`;
      const detail = await apiFetch(detailUrl);
      const d = detail?.PrecService;
      if (!d) { fail++; continue; }

      // 4-c. R2 업로드
      const payload = buildSummaryPayload(caseNum, d);
      await s3.send(new PutObjectCommand({
        Bucket:       process.env.R2_BUCKET,
        Key:          `prec/summary/${caseNum}.json`,
        Body:         JSON.stringify(payload),
        ContentType:  'application/json',
        CacheControl: 'public, s-maxage=86400, stale-while-revalidate=3600',
      }));
      ok++;
      if (ok % 100 === 0) {
        const elapsedSec = ((Date.now() - startMs) / 1000).toFixed(0);
        const rate = (ok / (Date.now() - startMs) * 1000).toFixed(2);
        console.log(`  진행: ${ok}/${chunk.length}  (${elapsedSec}s, ${rate}/s)`);
      }
    } catch (e) {
      fail++;
      if (fail <= 20) console.log(`  실패 ${caseNum}: ${e.message}`);
    }
    await new Promise(r => setTimeout(r, 350));  // rate limit 여유 (~2.8/s)
  }

  const totalSec = ((Date.now() - startMs) / 1000).toFixed(0);
  console.log(`\n[backfill 완료] 성공 ${ok.toLocaleString()} / 실패 ${fail.toLocaleString()} / 미조회 ${noHit.toLocaleString()} (${totalSec}s)`);
  console.log(`  다음 실행 시 남은 건수 (추정): ${(missing.length - ok).toLocaleString()}`);
}

main().catch(e => { console.error(e); process.exit(1); });
