// Open-model load test: constant-arrival-rate keeps offering load even when the system
// slows down, so queueing and overload show up in the numbers instead of being hidden
// by a closed-model generator that politely backs off.
//
//   STAGES=100,500,1000,2000   arrival rates (RPS), run one after another
//   STAGE_SECONDS=60           duration of each stage
//   MAX_VUS=6000               cap on concurrent virtual users (memory bound!)
//   PREALLOC_SECONDS=4         pre-allocate rate x this many VUs (~ expected latency):
//                              allocating VUs mid-run is slow and shows up as drops
//   MODE=realistic|overhead    label only (mock profile is set on the stack)
//   TARGET=http://nginx:8080

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const TARGET = __ENV.TARGET || 'http://nginx:8080';
const MODE = __ENV.MODE || 'realistic';
const STAGES = (__ENV.STAGES || '100,500,1000,2000').split(',').map(Number);
const STAGE_S = Number(__ENV.STAGE_SECONDS || 60);
const MAX_VUS = Number(__ENV.MAX_VUS || 6000);
const PREALLOC_S = Number(__ENV.PREALLOC_SECONDS || 4);
const GRACE_S = 15; // > the gateway's 10s deadline, so in-flight requests finish

const responses = new Counter('responses');

const scenarios = {};
const thresholds = {
  // Self-validating run: the gateway must always answer (no connection errors / hangs).
  'checks{check:answered}': ['rate>0.999'],
};
const scenarioName = (rate) => `rps_${rate}`;
STAGES.forEach((rate, i) => {
  const name = scenarioName(rate);
  scenarios[name] = {
    executor: 'constant-arrival-rate',
    rate,
    timeUnit: '1s',
    duration: `${STAGE_S}s`,
    preAllocatedVUs: Math.min(MAX_VUS, Math.max(50, Math.ceil(rate * PREALLOC_S))),
    maxVUs: MAX_VUS,
    startTime: `${i * (STAGE_S + GRACE_S)}s`,
    gracefulStop: `${GRACE_S}s`,
    exec: 'process',
  };
  // Sub-metric thresholds make k6 report every stage separately in the summary.
  // `scenario` is a built-in tag, so it is also present on dropped_iterations.
  thresholds[`http_req_duration{scenario:${name}}`] = ['p(99)<11000'];
  thresholds[`http_req_failed{scenario:${name}}`] = ['rate<1'];
  thresholds[`http_reqs{scenario:${name}}`] = ['count>0'];
  thresholds[`dropped_iterations{scenario:${name}}`] = ['count>=0'];
  for (const s of ['200', '429', '502', '503', '504', 'other']) {
    thresholds[`responses{scenario:${name},status:${s}}`] = ['count>=0'];
  }
});

export const options = {
  scenarios,
  thresholds,
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  noConnectionReuse: false,
};

const KNOWN = new Set(['200', '429', '502', '503', '504']);

export function process() {
  const body = JSON.stringify({
    customer_id: `cust-${Math.floor(Math.random() * 1e6)}`,
    amount: Math.floor(Math.random() * 1e6),
    currency: 'IRR',
  });
  const res = http.post(`${TARGET}/v1/process`, body, {
    headers: { 'Content-Type': 'application/json', 'X-API-Key': 'bench' },
    timeout: '20s',
    responseCallback: http.expectedStatuses({ min: 200, max: 299 }),
  });
  const code = String(res.status);
  responses.add(1, { status: KNOWN.has(code) ? code : 'other' });
  check(res, { answered: (r) => r.status !== 0 });
}

function metric(data, name, field) {
  const m = data.metrics[name];
  return m && m.values ? m.values[field] : undefined;
}

export function handleSummary(data) {
  const rows = STAGES.map((rate) => {
    const s = scenarioName(rate);
    const count = metric(data, `http_reqs{scenario:${s}}`, 'count') || 0;
    const byStatus = {};
    for (const st of ['200', '429', '502', '503', '504', 'other']) {
      byStatus[st] = metric(data, `responses{scenario:${s},status:${st}}`, 'count') || 0;
    }
    return {
      offered_rps: rate,
      achieved_rps: +(count / STAGE_S).toFixed(1),
      requests: count,
      dropped_iterations: metric(data, `dropped_iterations{scenario:${s}}`, 'count') || 0,
      avg_ms: +(metric(data, `http_req_duration{scenario:${s}}`, 'avg') || 0).toFixed(1),
      p50_ms: +(metric(data, `http_req_duration{scenario:${s}}`, 'med') || 0).toFixed(1),
      p95_ms: +(metric(data, `http_req_duration{scenario:${s}}`, 'p(95)') || 0).toFixed(1),
      p99_ms: +(metric(data, `http_req_duration{scenario:${s}}`, 'p(99)') || 0).toFixed(1),
      error_rate: +((metric(data, `http_req_failed{scenario:${s}}`, 'rate') || 0) * 100).toFixed(2),
      status: byStatus,
    };
  });

  const header = '| offered | achieved | p50 ms | avg ms | p95 ms | p99 ms | non-2xx % | 200 | 429 | 502 | 503 | 504 | dropped |';
  const sep = '|---|---|---|---|---|---|---|---|---|---|---|---|---|';
  const lines = rows.map((r) => `| ${r.offered_rps} | ${r.achieved_rps} | ${r.p50_ms} | ${r.avg_ms} | ${r.p95_ms} | ${r.p99_ms} | ${r.error_rate} | ${r.status['200']} | ${r.status['429']} | ${r.status['502']} | ${r.status['503']} | ${r.status['504']} | ${r.dropped_iterations} |`);
  const table = [`mode=${MODE} stage=${STAGE_S}s`, header, sep, ...lines].join('\n');
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  return {
    stdout: `\n${table}\n\n`,
    [`/results/${MODE}-${stamp}.json`]: JSON.stringify({ mode: MODE, stage_seconds: STAGE_S, stages: rows }, null, 2),
    [`/results/${MODE}-${stamp}.md`]: `${table}\n`,
  };
}
