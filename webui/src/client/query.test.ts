import { afterEach, expect, test, vi } from "vitest";

import { queryLogs } from "./query";

afterEach(() => {
  vi.restoreAllMocks();
});

test.each([
  () => new Response("<!DOCTYPE html><pre>Cannot POST /query</pre>", { status: 404 }),
  () => new Response("private server details", { status: 500 }),
  () => new Response("unavailable", { status: 503 }),
  () => {
    throw new TypeError("Unexpected error");
  },
])("unexpected failures expose only a safe message (case %#)", async (failure) => {
  vi.spyOn(globalThis, "fetch").mockImplementation(async () => failure());
  vi.spyOn(console, "error").mockImplementation(() => {});

  await expect(queryLogs({ query: "{}" })).rejects.toThrow("Unexpected error");
});

test("aborted queries are not logged or replaced with unexpected errors", async () => {
  const aborted = new DOMException("Request aborted", "AbortError");
  vi.spyOn(globalThis, "fetch").mockRejectedValue(aborted);
  const log = vi.spyOn(console, "error").mockImplementation(() => {});

  await expect(queryLogs({ query: "{}" })).rejects.toBe(aborted);
  expect(log).not.toHaveBeenCalled();
});
