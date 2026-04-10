// scripts/collect-plenary-full.js
// 실행: GitHub Actions collect-plenary.yml (수동 트리거)
// 용도: 전원합의체 판결 전체 역사 수집 — 최초 1회 또는 재구축 시
//
// 법제처 API: search=2 (전문검색) + query=전원합의체 + courtNm=대법원
// → 6,818건 검색 → 상세조회로 실제 전원합의체 판결 필터링
// → R2 prec/plenary.json (plain JSON) 업로드

import { S3Client, PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';

const s3 = new S3Client({
  region: 'auto',
  endpoint: process.env.R2_ENDPOINT,
  credentials: {
    accessKeyId:     process.env.R2_ACCESS_KEY,
    secretAccessKey: process.env.R2_SECRET_KEY,
  },
});

/* ── 범용 API fetch (5회 재시도, 긴 대기) ── */
async function apiFetch(url, retries = 5) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(45000) });
      if (!res.ok) {
        if (res.status === 429) {
          // Rate limit — 30초 대기 후 재시도
          console.log(`  ⚠ 429 Rate Limit — 30초 대기 후 재시도`);
          await new Promise(r => setTimeout(r, 30000));
          continue;
        }
        throw new Error(`HTTP ${res.status}`);
      }
      return await res.json();
    } catch (e) {
      console.log(`  ⚠ attempt ${attempt}/${retries}: ${e.message}`);
      if (attempt === retries) throw e;
      await new Promise(r => setTimeout(r, 10000 * attempt));
    }
  }
}

/* ── R2에서 기존 plenary.json 로드 (진행 재개용) ── */
async function loadExisting() {
  try {
    const res = await s3.send(new GetObjectCommand({
      Bucket: process.env.R2_BUCKET, Key: 'prec/plenary.json'
    }));
    const buf = Buffer.from(await res.Body.transformToByteArray());
    const data = JSON.parse(buf.toString('utf-8'));
    console.log(`기존 plenary.json 로드: ${(data.list||[]).length}건`);
    return data;
  } catch {
    console.log('기존 plenary.json 없음 — 처음부터 수집');
    return { list: [] };
  }
}

/* ── R2에 plain JSON 업로드 ── */
async function uploadPlenary(list, lastUpdated) {
  const body = JSON.stringify({ list, lastUpdated, updatedAt: Date.now() });
  await s3.send(new PutObjectCommand({
    Bucket: process.env.R2_BUCKET, Key: 'prec/plenary.json',
    Body: body,
    ContentType: 'application/json',
    CacheControl: 'public, s-maxage=3600, stale-while-revalidate=600',
  }));
  console.log(`✅ R2 업로드: prec/plenary.json — ${list.length}건`);
}

/* ════════════════════════════════════════════
   메인: 전원합의체 전체 수집
   ════════════════════════════════════════════ */
async function main() {
  console.log('[전원합의체 전체 수집 시작]', new Date().toISOString());

  const todayStr = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  const existing = await loadExisting();
  const existingNums = new Set((existing.list || []).map(p => p.caseNum));
  console.log(`기존 ${existingNums.size}건 — 이미 수집된 사건 스킵`);

  const ENC_QUERY = encodeURIComponent('전원합의체');
  const ENC_COURT = encodeURIComponent('대법원');

  const newCases = [];
  let page = 1;
  let totalHits = 0;
  let skipCount = 0;

  console.log('\n[1단계] 검색 목록 수집 + 상세 검증 시작...');

  while (true) {
    const url = `https://www.law.go.kr/DRF/lawSearch.do`
      + `?OC=${process.env.LAW_OC_KEY}&target=prec&type=JSON`
      + `&search=2&query=${ENC_QUERY}&display=100&page=${page}&courtNm=${ENC_COURT}`;

    let data;
    try { data = await apiFetch(url); }
    catch (e) { console.error(`페이지 ${page} 실패:`, e.message); break; }

    const raw = data?.PrecSearch?.prec;
    const hits = !raw ? [] : Array.isArray(raw) ? raw : [raw];
    if (!hits.length) {
      console.log(`페이지 ${page} — 결과 없음 (수집 완료)`);
      break;
    }

    totalHits += hits.length;

    for (const p of hits) {
      const caseNum = (p.사건번호 || '').replace(/\s+/g, '');
      if (!caseNum) continue;
      if (existingNums.has(caseNum)) { skipCount++; continue; }

      // 상세 조회 — 실제 전원합의체 판결 여부 확인
      await new Promise(r => setTimeout(r, 300));
      let detail;
      try {
        detail = await apiFetch(
          `https://www.law.go.kr/DRF/lawService.do?OC=${process.env.LAW_OC_KEY}&target=prec&ID=${p.판례일련번호}&type=JSON`
        );
      } catch (e) {
        console.log(`  상세조회 실패 ${caseNum}: ${e.message}`);
        continue;
      }

      const d = detail?.PrecService;
      if (!d) continue;
      const content = d.판례내용 || '';

      // 실제 전원합의체로 선고된 경우만 (인용 판결 제외)
      const sengo = (d.선고 || '');
      const isPlenary =
        /전원합의체.*선고|선고.*전원합의체/.test(content.slice(0, 800)) ||
        /전원합의체[\s가-힣]*(판결|결정)/.test(content.slice(0, 300)) ||
        sengo.includes('전원합의체');

      if (!isPlenary) continue;

      const dateRaw = (d.선고일자 || '').replace(/\./g, '');
      const laws = (d.참조조문 || '').split(/[,、\n]+/)
        .map(s => s.trim()).filter(s => s.length > 1).slice(0, 8);
      const changed = /변경|파기|종전.*변경|판례.*변경/.test(content);

      const entry = {
        caseNum,
        date: dateRaw,
        court: d.법원명 || '대법원',
        subject: d.사건명 || '',
        overrules: changed,
        oldRule: '',
        newRule: '',
        keywords: [],
        laws,
        casenoteUrl: `https://casenote.kr/${encodeURIComponent(d.법원명 || '대법원')}/${encodeURIComponent(caseNum)}`,
      };
      newCases.push(entry);
      existingNums.add(caseNum);

      const marker = changed ? ' [법리변경]' : '';
      console.log(`  ✓ ${caseNum} (${dateRaw})${marker}  총 ${newCases.length}건`);
    }

    console.log(`페이지 ${page}: ${hits.length}건 처리, 신규확인 ${newCases.length}건, 스킵 ${skipCount}건`);
    page++;

    // 중간 저장 — 50페이지마다 진행상황 R2에 저장 (재시작 시 이어받기 가능)
    if (page % 50 === 0) {
      const merged = [...(existing.list || []), ...newCases];
      merged.sort((a, b) => (b.date || '').localeCompare(a.date || ''));
      await uploadPlenary(merged, null); // lastUpdated는 완료 후 설정
      console.log(`[중간저장] ${merged.length}건 저장 완료`);
    }

    await new Promise(r => setTimeout(r, 500));
  }

  console.log(`\n[수집 완료]`);
  console.log(`  검색결과 총: ${totalHits}건`);
  console.log(`  스킵(기존): ${skipCount}건`);
  console.log(`  신규 확인:  ${newCases.length}건`);

  // 최종 병합 + 정렬 + R2 업로드
  const merged = [...(existing.list || []), ...newCases];
  merged.sort((a, b) => (b.date || '').localeCompare(a.date || ''));
  await uploadPlenary(merged, todayStr);

  console.log(`\n✅ 전원합의체 수집 완료: 총 ${merged.length}건`);
  console.log(`  (기존 ${existingNums.size - newCases.length}건 + 신규 ${newCases.length}건)`);
}

main().catch(e => { console.error(e); process.exit(1); });
