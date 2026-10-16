// Load scenario CBL-2.9: PostTransaction via gRPC against the ledger compose stand.
// Commands are given for cmd.exe (from the services/cbl-ledger directory):
//   docker compose -f deploy/docker-compose.yml --profile app up -d --build
//   docker compose -f deploy/docker-compose.yml cp test/load/seed.sql postgres:/tmp/seed.sql
//   docker compose -f deploy/docker-compose.yml exec -T postgres psql -U cbl -d ledger -f /tmp/seed.sql
//   docker run --rm -i -v "%CD%/test/load":/scripts -v "%CD%/../cbl-protos/proto":/proto grafana/k6:2.3.0 run ^
//     -e LEDGER_ADDR=host.docker.internal:9040 -e RATE=100 -e DURATION=30s /scripts/post_transaction.js
// In PowerShell the same commands with $PWD and without the line continuation character.
import grpc from 'k6/net/grpc';
import { check } from 'k6';
import { Rate } from 'k6/metrics';

const client = new grpc.Client();
client.load(['/proto'], 'billing/ledger/v1/ledger.proto');

const address = __ENV.LEDGER_ADDR || 'host.docker.internal:9040';
const rate = Number(__ENV.RATE || 100);
const duration = __ENV.DURATION || '30s';
const pairs = Math.max(1, Math.min(Number(__ENV.PAIRS) || 64, 256));
const preAllocatedVUs = Number(__ENV.PRE_VUS || 50);
const maxVUs = Number(__ENV.MAX_VUS || 300);
// Unique idempotency key prefix per run: a repeated run is not a replay.
// Key length is limited: prefix 14 characters + vu + iter, key limit 128 characters (2.2).
const runID = __ENV.RUN_ID || `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;

// transportFailures records both transport exceptions and responses with a non-OK status:
// check() is not executed when invoke throws, so the success rate is covered by a separate threshold.
const transportFailures = new Rate('transport_failures');

export const options = {
  scenarios: {
    post_transaction: {
      executor: 'constant-arrival-rate',
      rate: rate,
      timeUnit: '1s',
      duration: duration,
      preAllocatedVUs: preAllocatedVUs,
      maxVUs: maxVUs,
    },
  },
  thresholds: {
    grpc_req_duration: ['p(99)<500'],
    checks: ['rate>0.999'],
    transport_failures: ['rate<0.001'],
    dropped_iterations: ['count<500'],
  },
};

// accountID reproduces the deterministic UUIDs from test/load/seed.sql.
function accountID(prefix, index) {
  return prefix + index.toString(16).padStart(12, '0');
}

const DEBIT_PREFIX = '00000000-0000-4000-8000-';
const CREDIT_PREFIX = '00000000-0000-4000-a000-';

export default function () {
  if (__ITER === 0) {
    try {
      client.connect(address, { plaintext: true });
    } catch (connectError) {
      transportFailures.add(true);
      return;
    }
  }
  const pair = (__VU + __ITER) % pairs;
  const key = `${runID}-${__VU}-${__ITER}`;
  const amount = { minor_units: 1, asset: 'USDT', decimals: 2 };
  let response;
  try {
    response = client.invoke('billing.ledger.v1.LedgerService/PostTransaction', {
      idempotency_key: key,
      ref_type: 'load',
      ref_id: key,
      entries: [
        { account_id: accountID(DEBIT_PREFIX, pair), direction: 'DIRECTION_DEBIT', amount: amount },
        { account_id: accountID(CREDIT_PREFIX, pair), direction: 'DIRECTION_CREDIT', amount: amount },
      ],
    });
  } catch (invokeError) {
    transportFailures.add(true);
    return;
  }
  const ok = check(response, {
    'status is OK': (r) => r && r.status === grpc.StatusOK,
  });
  transportFailures.add(!ok);
}
