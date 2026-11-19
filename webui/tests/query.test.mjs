import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createServer } from "vite";

let server;
let client;

before(async () => {
  server = await createServer({
    configFile: false,
    envDir: false,
    optimizeDeps: { noDiscovery: true, include: [] },
    server: { middlewareMode: true },
  });
  client = await server.ssrLoadModule("/src/client/query.ts");
});
after(async () => {
  await server?.close();
});

test("unexpected failures log diagnostics but expose only a safe message", async (t) => {
  for (const failure of [
    () => new Response("<!DOCTYPE html><pre>Cannot POST /query</pre>", { status: 404 }),
    () => new Response("private server details", { status: 500 }),
    () => new Response("unavailable", { status: 503 }),
    () => {
      throw new TypeError("Unexpected error");
    },
  ]) {
    const fetch = t.mock.method(globalThis, "fetch", async () => failure());
    // log mock to keep the tests output clean
    t.mock.method(console, "error", () => {});

    await assert.rejects(client.queryLogs({ query: "{}" }), { message: "Unexpected error" });

    fetch.mock.restore();
  }
});

test("aborted queries are not logged or replaced with unexpected errors", async (t) => {
  const aborted = new DOMException("Request aborted", "AbortError");
  t.mock.method(globalThis, "fetch", async () => {
    throw aborted;
  });
  const log = t.mock.method(console, "error", () => {});
  await assert.rejects(client.queryLogs({ query: "{}" }), (err) => err === aborted);
  assert.equal(log.mock.callCount(), 0);
});
