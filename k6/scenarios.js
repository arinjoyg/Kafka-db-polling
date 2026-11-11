// Open-model (arrival-rate) load against the requestor's HTTP edge.
// Arrival-rate executors keep sending at the target rate even when the system slows down, so queueing
// shows up in the percentiles instead of being hidden (coordinated omission). If k6 runs out of VUs it
// reports `dropped_iterations` – treat any non-zero value as an invalid run, not a pass.
//
// Usage:
//   k6 run -e SCENARIO=baseline -e RATE=300 -e DURATION=10m k6/scenarios.js
//   k6 run -e SCENARIO=ramp     -e RATE=100 -e MAX_RATE=3000 k6/scenarios.js
//   k6 run -e SCENARIO=spike    -e RATE=300 -e SPIKE_MULT=5 k6/scenarios.js
//   k6 run -e SCENARIO=soak     -e RATE=300 -e DURATION=8h k6/scenarios.js
//   k6 run -e SCENARIO=baseline -e HOT_KEY_FRACTION=0.4 -e HOT_KEYS=3 k6/scenarios.js   # key skew
//   k6 run -e SCENARIO=baseline -e DUP_FRACTION=0.05 k6/scenarios.js                     # duplicates
//   k6 run -e SCENARIO=baseline -e MAX_PADDING=200000 k6/scenarios.js                    # payload variance

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.0.2/index.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const SCENARIO = __ENV.SCENARIO || 'baseline';
const RATE = parseInt(__ENV.RATE || '200', 10);
const MAX_RATE = parseInt(__ENV.MAX_RATE || String(RATE * 10), 10);
const SPIKE_MULT = parseFloat(__ENV.SPIKE_MULT || '5');
const DURATION = __ENV.DURATION || (SCENARIO === 'soak' ? '8h' : '5m');
const PRE_VUS = parseInt(__ENV.PRE_VUS || '200', 10);
const MAX_VUS = parseInt(__ENV.MAX_VUS || '3000', 10);
const HOT_KEY_FRACTION = parseFloat(__ENV.HOT_KEY_FRACTION || '0');
const HOT_KEYS = parseInt(__ENV.HOT_KEYS || '5', 10);
const DUP_FRACTION = parseFloat(__ENV.DUP_FRACTION || '0');
const MAX_PADDING = parseInt(__ENV.MAX_PADDING || '0', 10);
const P99_MS = parseInt(__ENV.P99_MS || '800', 10);
const P999_MS = parseInt(__ENV.P999_MS || '1000', 10);
const MAX_PENDING_RATE = parseFloat(__ENV.MAX_PENDING_RATE || '0.001');

const pending = new Rate('rr_pending');       // 202: no reply inside the budget
const failed = new Rate('rr_failed');         // anything other than 200/202
const duplicates = new Counter('rr_duplicates');

function rampStages() {
  // 8 steps from RATE to MAX_RATE: 30s ramp + 2m plateau each. Read the knee off the plateaus.
  const stages = [];
  for (let i = 1; i <= 8; i++) {
    const target = Math.round(RATE + ((MAX_RATE - RATE) * i) / 8);
    stages.push({ target, duration: '30s' }, { target, duration: '2m' });
  }
  return stages;
}

const executors = {
  baseline: {
    executor: 'constant-arrival-rate', rate: RATE, timeUnit: '1s', duration: DURATION,
    preAllocatedVUs: PRE_VUS, maxVUs: MAX_VUS,
  },
  soak: {
    executor: 'constant-arrival-rate', rate: RATE, timeUnit: '1s', duration: DURATION,
    preAllocatedVUs: PRE_VUS, maxVUs: MAX_VUS,
  },
  ramp: {
    executor: 'ramping-arrival-rate', startRate: RATE, timeUnit: '1s',
    preAllocatedVUs: PRE_VUS, maxVUs: MAX_VUS, stages: rampStages(),
  },
  spike: {
    executor: 'ramping-arrival-rate', startRate: RATE, timeUnit: '1s',
    preAllocatedVUs: PRE_VUS, maxVUs: MAX_VUS,
    stages: [
      { target: RATE, duration: '2m' },                          // steady
      { target: Math.round(RATE * SPIKE_MULT), duration: '10s' }, // spike up
      { target: Math.round(RATE * SPIKE_MULT), duration: '90s' }, // hold
      { target: RATE, duration: '10s' },                          // back down
      { target: RATE, duration: '5m' },                           // recovery window: watch lag drain
    ],
  },
};

if (!executors[SCENARIO]) {
  throw new Error(`Unknown SCENARIO=${SCENARIO}; use one of ${Object.keys(executors).join(', ')}`);
}

// Ramp is a capacity-discovery run: it is EXPECTED to breach the SLA at the top, so no gating thresholds.
const gating = SCENARIO === 'ramp' ? {} : {
  'http_req_duration{expected_response:true}': [`p(99)<${P99_MS}`, `p(99.9)<${P999_MS}`],
  rr_pending: [`rate<${MAX_PENDING_RATE}`],
  rr_failed: ['rate<0.0001'],
  dropped_iterations: ['count<1'],
};

export const options = {
  scenarios: { [SCENARIO]: executors[SCENARIO] },
  thresholds: gating,
  summaryTrendStats: ['avg', 'min', 'med', 'p(95)', 'p(99)', 'p(99.9)', 'max'],
  discardResponseBodies: false,
};

// Per-VU memory of recent commands, used to re-send the exact same requestId (duplicate injection).
const recent = [];

function buildCommand() {
  if (DUP_FRACTION > 0 && recent.length > 0 && Math.random() < DUP_FRACTION) {
    return recent[Math.floor(Math.random() * recent.length)];
  }
  const hot = HOT_KEY_FRACTION > 0 && Math.random() < HOT_KEY_FRACTION;
  const cmd = {
    requestId: uuidv4(),
    paymentId: hot ? `hot-${Math.floor(Math.random() * HOT_KEYS)}` : `pay-${uuidv4()}`,
    amount: (Math.random() * 10000).toFixed(2),
    currency: 'USD',
    memo: MAX_PADDING > 0 ? 'x'.repeat(Math.floor(Math.random() * MAX_PADDING)) : '',
  };
  recent.push(cmd);
  if (recent.length > 50) recent.shift();
  return cmd;
}

export default function () {
  const cmd = buildCommand();
  const res = http.post(`${BASE_URL}/payments`, JSON.stringify(cmd), {
    headers: { 'Content-Type': 'application/json' },
    timeout: '5s',
    tags: { name: 'POST /payments' },
  });

  pending.add(res.status === 202);
  failed.add(res.status !== 200 && res.status !== 202);
  if (res.status === 200) {
    try {
      if (res.json('kind') === 'DUPLICATE') duplicates.add(1);
    } catch (_) { /* ignore */ }
  }
  check(res, { 'status is 200 or 202': (r) => r.status === 200 || r.status === 202 });
}

export function handleSummary(data) {
  const file = `results/${SCENARIO}-${new Date().toISOString().replace(/[:.]/g, '-')}.json`;
  return {
    stdout: textSummary(data, { indent: ' ', enableColors: true }),
    [file]: JSON.stringify(data, null, 2),
  };
}
