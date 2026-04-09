// scripts/build-initial.js
// 실행: node --env-file=.env scripts/build-initial.js
// 용도: 서비스 첫 배포 전 전체 DB 구축 (1회)

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

async function fetchPage(page, dateRange) {
  const url = `https://www.law.go.kr/DRF/lawSearch.do`
    + `?OC=${process.env.LAW_OC_KEY}&target=prec&type=JSON`
    + `&display=100&page=${page}&prncYd=${dateRange}&sort=date`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  const raw = data?.PrecSearch?.prec;
  return !raw ? [] : Array.isArray(raw) ? raw : [raw];
}

async function main() {
  const db = {};
  let total = 0;

  // 연도 범위 분할 (API 부하 분산)
  const ranges = [
    '19480101~19991231', '20000101~20091231',
    '20100101~20151231', '20160101~20191231',
    '20200101~20221231', '20230101~20261231',
  ];

  for (const range of ranges) {
    console.log(`\n[수집] ${range}`);
    let page = 1;

    while (true) {
      const hits = await fetchPage(page, range);
      if (!hits.length) break;

      for (const p of hits) {
        const key = p.사건번호?.replace(/\s+/g, '');
        if (key) {
          // 선고일을 YYYYMMDD 숫자로, 법원명을 코드로 압축
          const dateNum = parseInt((p.선고일자 || '').replace(/\./g, ''), 10) || 0;
          const courtId = courtToId(p.법원명 || '');
          db[key] = [dateNum, courtId];
        }
      }

      total = Object.keys(db).length;
      console.log(`  p.${page}: +${hits.length} (누계 ${total.toLocaleString()})`);
      page++;
      await new Promise(r => setTimeout(r, 300)); // API 부하 방지
    }
  }

  // db.json 업로드 (앱이 직접 로드하는 파일)
  const todayStr = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  await uploadGzip('prec/v1/db.json', { version: 1, updated: Date.now(), count: total, db });

  // meta.json 업로드 (증분 수집이 참조)
  await uploadGzip('prec/v1/meta.json', {
    lastUpdated: todayStr,
    count: total,
    updatedAt: Date.now(),
    addedToday: 0,
  });

  console.log(`\n🎉 초기 DB 완성: ${total.toLocaleString()}건`);
}

// 법원명 → 코드 변환 (앱의 PREC_COURT와 일치)
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
  // 부분 매칭
  for (const [k, v] of Object.entries(COURT_MAP)) {
    if (name.includes(k) || k.includes(name)) return v;
  }
  return 99; // 기타
}

main().catch(e => { console.error(e); process.exit(1); });
