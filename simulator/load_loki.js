import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

export const options = {
  scenarios: {
    default: {
      executor: 'per-vu-iterations',
      vus: 250,
      iterations: 2000,
    },
  },
};

// victorialogs loki api
const url = 'http://localhost:9428/insert/loki/api/v1/push';
const params = {
  headers: {
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
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
    let log = `{"ts":"${new Date().toISOString()}"`;
    for (let i = 0; i < 24; i++) {
        log += `,"field${i}":"${randomString(10)}"`;
    }
    log += '}';
    return log;
}

function uuid() {
    return `${randomString(8)}-${randomString(4)}-${randomString(4)}-${randomString(4)}-${randomString(12)}`;
}

export default function () {
  // Generate a unique ID for this iteration to ensure cardinality
  const uniqueId = ((__VU - 1) * exec.scenario.iterations) + __ITER;

  const streams = [];
  for (let i = 0; i < 1000; i++) {
    const timestamp = Date.now() * 1000000; // nanoseconds
    streams.push({
      "stream": {
        "kubernetes_annotations_kubectl_kubernetes_io_default_container": "prometheus",
        "kubernetes_annotations_kubernetes_io_psp": "eks.privileged",
        "kubernetes_container_hash": `quay.io/prometheus/prometheus@sha256:${randomString(64)}`,
        "kubernetes_container_image": "quay.io/prometheus/prometheus:v2.39.1",
        "kubernetes_container_name": "prometheus",
        "kubernetes_docker_id": randomString(64),
        "kubernetes_host": `ip-10-2-50-${Math.floor(Math.random() * 256)}.us-east-2.compute.internal`,
        "kubernetes_labels_app_kubernetes_io_component": "prometheus",
        "kubernetes_labels_app_kubernetes_io_instance": `k8s-${uniqueId}`,
        "kubernetes_labels_app_kubernetes_io_managed_by": "prometheus-operator",
        "kubernetes_labels_app_kubernetes_io_name": "prometheus",
        "kubernetes_labels_app_kubernetes_io_part_of": "kube-prometheus",
        "kubernetes_labels_app_kubernetes_io_version": "2.39.1",
        "kubernetes_labels_controller_revision_hash": `prometheus-k8s-${randomString(10)}`,
        "kubernetes_labels_operator_prometheus_io_name": "k8s",
        "kubernetes_labels_operator_prometheus_io_shard": "0",
        "kubernetes_labels_prometheus": "k8s",
        "kubernetes_labels_statefulset_kubernetes_io_pod_name": `prometheus-k8s-${uniqueId}`,
        "kubernetes_namespace_name": "monitoring",
        "kubernetes_pod_id": uuid(),
        "kubernetes_pod_name": `prometheus-k8s-${uniqueId}`,
        "stream": "stderr"
      },
      "values": [
        [`${timestamp}`, generateLogField()]
      ]
    });
  }

  const payload = {
    "streams": streams
  };

  const res = http.post(url, JSON.stringify(payload), params);

  check(res, {
    'status is 204': (r) => r.status === 204,
  });
}
