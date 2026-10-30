// This is an independent load/observability demo, with one network attempt per
// operation. The Go benchmark remains the controlled retry/payload comparison.
import http from 'k6/http';
import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const protocol = __ENV.PROTOCOL || 'rest';
if (!['rest', 'grpc'].includes(protocol)) throw new Error('PROTOCOL must be rest or grpc');
const base = __ENV.BASE_URL || 'http://localhost:8080';
const grpcAddress = __ENV.GRPC_ADDR || 'localhost:9090';
const client = new grpc.Client();
client.load(['../api/image/v1'], 'image.proto');
let connected = false;
const duration = new Trend('operation_duration_ms', true);
const failed = new Rate('operation_failed');

export const options = {
  vus: Number(__ENV.VUS || 16),
  duration: __ENV.DURATION || '60s',
  thresholds: {
    operation_failed: ['rate<0.01'],
    operation_duration_ms: ['p(99)<300'],
    'operation_duration_ms{operation:GetImage}': ['p(99)<100'],
  },
};

export function setup() {
  // Identical REST setup for either variant; only custom operation_* metrics
  // below measure the workload. Setup does not represent gRPC load latency.
  const owner = `k6-${Date.now()}`;
  const images = [];
  for (let i = 0; i < 40; i++) {
    const response = http.post(`${base}/v1/images`, JSON.stringify({
      idempotency_key: `${owner}-seed-${i}`, owner_id: owner,
      caption: 'a'.repeat(80), object_key: 'mock/k6-seed.jpg',
    }), { headers: { 'Content-Type': 'application/json' }, tags: { name: 'setup-create' } });
    if (response.status !== 201) throw new Error(`setup create failed: ${response.status} ${response.body}`);
    const imageID = response.json('image.image_id');
    const complete = http.post(`${base}/v1/images/${imageID}/complete`, '{}', {
      headers: { 'Content-Type': 'application/json' }, tags: { name: 'setup-complete' },
    });
    if (complete.status !== 200) throw new Error(`setup complete failed: ${complete.status}`);
    images.push(imageID);
  }
  return { owner, images };
}

export default function (data) {
  if (protocol === 'grpc' && !connected) {
    client.connect(grpcAddress, { plaintext: true });
    connected = true; // Keep a connection per VU; do not reconnect every request.
  }
  const choice = Math.random();
  let operation, request, path, verb;
  if (choice < 0.90) {
    operation = 'GetImage';
    const id = data.images[Math.floor(Math.random() * data.images.length)];
    request = { image_id: id }; path = `/v1/images/${id}`; verb = 'GET';
  } else if (choice < 0.95) {
    operation = 'ListUserImages';
    request = { owner_id: data.owner, limit: 20 };
    path = `/v1/users/${data.owner}/images?limit=20`; verb = 'GET';
  } else {
    operation = 'CreateImage';
    request = { idempotency_key: `${data.owner}-${__VU}-${__ITER}`, owner_id: data.owner, caption: 'a'.repeat(80) };
    path = '/v1/images'; verb = 'POST';
  }
  const tags = { protocol, operation };
  let ok = false;
  const began = Date.now();
  try {
    if (protocol === 'grpc') {
      const response = client.invoke(`sis.image.v1.ImageService/${operation}`, request, { timeout: '300ms' });
      ok = response.status === grpc.StatusOK;
    } else {
      const response = http.request(verb, `${base}${path}`, verb === 'GET' ? null : JSON.stringify(request), {
        headers: { 'Content-Type': 'application/json', 'X-Request-Timeout-Ms': '300' },
        timeout: '300ms', tags: { name: operation },
      });
      ok = response.status === (operation === 'CreateImage' ? 201 : 200);
    }
  } finally {
    duration.add(Date.now() - began, tags);
    failed.add(!ok, tags);
    check(ok, { 'operation succeeded': (value) => value }, tags);
  }
  sleep(Number(__ENV.PACING || 0.05));
}
