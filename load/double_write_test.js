import { sleep, check } from "k6";
import loki from "k6/x/loki";

export const options = {
  scenarios: {
    default: {
      executor: "per-vu-iterations",
      vus: 4,
      iterations: 2000,
    },
  },
};

const TENANT_ID = "42";
const BASE_URL = "http://localhost:9014/insert";
const BASE_URL2 = "http://localhost:9018/insert";
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
const conf2 = new loki.Config({
  url: BASE_URL2,
  tenantID: TENANT_ID,
  timeout,
  protobufRatio: ratio,
  cardinalities,
});
const client = new loki.Client(conf);
const client2 = new loki.Client(conf2);

export default function () {
  const res = client.pushParameterized(60, 2 * KB, 16 * KB);

  if (res.status !== 200) {
    console.error(`push failed status=${res.status} body=${res.body}`);
  }

  check(res, {
    "status is 200": (r) => r.status === 200,
  });

  const res2 = client2.pushParameterized(60, 2 * KB, 8 * KB);

  if (res2.status !== 200) {
    console.error(`push failed status=${res2.status} body=${res2.body}`);
  }

  check(res2, {
    "status is 200": (r) => r.status === 200,
  });

  sleep(1);
}
