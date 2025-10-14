import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

export const options = {
  scenarios: {
    default: {
      executor: 'per-vu-iterations',
      vus: 50,
      iterations: 2000,
    },
  },
};

// openobserve api
const url = 'http://localhost:5080/api/default/quickstart1/_json';
const params = {
  headers: {
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
        // base64 default login pass, root@example.com Complexpass#123
    'Authorization': 'Basic cm9vdEBleGFtcGxlLmNvbTpDb21wbGV4cGFzcyMxMjM=',
  },
};

function randomString(length) {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let result = '';
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

function generateLogField() {
    let log = ts=`${new Date().toISOString()}`;
    for (let i = 0; i < 24; i++) {
        log += `field${i}=${randomString(10)}`;
    }
    return log;
}

function uuid() {
    return `${randomString(8)}-${randomString(4)}-${randomString(4)}-${randomString(4)}-${randomString(12)}`;
}

export default function () {
  // Generate a unique ID for this iteration to ensure cardinality
  const uniqueId = ((__VU - 1) * exec.scenario.iterations) + __ITER;

  const payload = [];
    for (let i = 0; i < 1000; i++) {
        payload.push({
            "kubernetes.annotations.kubectl.kubernetes.io/default-container": "prometheus",
            "kubernetes.annotations.kubernetes.io/psp": "eks.privileged",
            "kubernetes.container_hash": `quay.io/prometheus/prometheus@sha256:${randomString(64)}`,
            "kubernetes.container_image": "quay.io/prometheus/prometheus:v2.39.1",
            "kubernetes.container_name": "prometheus",
            "kubernetes.docker_id": randomString(64),
            "kubernetes.host": `ip-10-2-50-${Math.floor(Math.random() * 256)}.us-east-2.compute.internal`,
            "kubernetes.labels.app.kubernetes.io/component": "prometheus",
            "kubernetes.labels.app.kubernetes.io/instance": `k8s-${uniqueId}`,
            "kubernetes.labels.app.kubernetes.io/managed-by": "prometheus-operator",
            "kubernetes.labels.app.kubernetes.io/name": "prometheus",
            "kubernetes.labels.app.kubernetes.io/part-of": "kube-prometheus",
            "kubernetes.labels.app.kubernetes.io/version": "2.39.1",
            "kubernetes.labels.controller-revision-hash": `prometheus-k8s-${randomString(10)}`,
            "kubernetes.labels.operator.prometheus.io/name": "k8s",
            "kubernetes.labels.operator.prometheus.io/shard": "0",
            "kubernetes.labels.prometheus": "k8s",
            "kubernetes.labels.statefulset.kubernetes.io/pod-name": `prometheus-k8s-${uniqueId}`,
            "kubernetes.namespace_name": "monitoring",
            "kubernetes.pod_id": `uuid()`,
            "kubernetes.pod_name": `prometheus-k8s-${uniqueId}`,
            "log": `generateLogField()`,
            "stream": "stderr"
        })
    }

  const res = http.post(url, JSON.stringify(payload), params);

  check(res, {
    'status is 200': (r) => r.status === 200,
  });
}
