import { sleep, check } from "k6";
import loki from "k6/x/loki";

export const options = {
  scenarios: {
    default: {
      executor: "per-vu-iterations",
      vus: 4,
      iterations: 50,
    },
  },
};

const TENANT_ID = "0";
const BASE_URL = "http://localhost:9014/ingest";
const timeout = 20000;
const ratio = 0;

const cardinalities = {
  app: 12,
  namespace: 4,
  pod: 8,
  language: 4,
};

const KB = 1024;
const conf = new loki.Config({
  url: BASE_URL,
  tenantID: TENANT_ID,
  timeout,
  protobufRatio: ratio,
  cardinalities,
});
const client = new loki.Client(conf);

export default function () {
  const res = client.pushParameterized(60, 8 * KB, 8 * KB);

  if (res.status !== 200) {
    console.error(`push failed status=${res.status} body=${res.body}`);
  }

  check(res, {
    "status is 200": (r) => r.status === 200,
  });

  sleep(0.2);
}
