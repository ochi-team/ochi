import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import { setImmediate } from "node:timers/promises";
import vm from "node:vm";
import { transformWithOxc } from "vite";

async function mountQuery({
  stored = [],
  result,
  query = "{service=api}",
  pending = false,
  range = "",
  storageError = false,
} = {}) {
  const source = await readFile(
    new URL("../src/components/widgets/Query.tsx", import.meta.url),
    "utf8",
  );
  const { code } = await transformWithOxc(
    source.replace(/^import .*;$/gm, "").replace("export default function", "function") +
      "\nexports.default = Query;",
    "Query.tsx",
    { jsx: { runtime: "classic", pragma: "jsx" } },
  );
  const exports = {};
  const listeners = new Map();
  const cleanups = [];
  const writes = [];
  const signals = [];
  let runs = 0;
  const QueryHistory = Symbol("QueryHistory");
  const QueryTimePresets = Symbol("QueryTimePresets");
  vm.runInNewContext(code, {
    exports,
    QueryHistory,
    QueryEditor: Symbol("QueryEditor"),
    QueryTimePresets,
    Keycap: Symbol("Keycap"),
    createSignal(value) {
      const state = { value };
      signals.push(state);
      return [
        () => state.value,
        (next) => {
          state.value = next;
        },
      ];
    },
    onSettled(callback) {
      cleanups.push(callback());
    },
    loadQueryHistory() {
      if (storageError) throw new Error("Storage unavailable");
      return stored;
    },
    pushQueryHistory(history, value) {
      if (storageError) throw new Error("Storage unavailable");
      writes.push(value);
      return [value, ...history];
    },
    window: {
      addEventListener(type, listener) {
        listeners.set(type, listener);
      },
      removeEventListener(type, listener) {
        if (listeners.get(type) === listener) listeners.delete(type);
      },
    },
    jsx: (type, props, ...children) => ({ type, props, children }),
  });
  const tree = exports.default({
    query,
    range,
    pending,
    onRun: async () => {
      runs++;
      return result;
    },
  });
  function find(node, type) {
    if (node.type === type) return node;
    for (const child of node.children ?? []) {
      if (child && typeof child === "object") {
        const found = find(child, type);
        if (found) return found;
      }
    }
  }
  return {
    history: () => Array.from(signals[0].value),
    displayedHistory: Array.from(find(tree, QueryHistory).props.history),
    presetRange: find(tree, QueryTimePresets).props.range,
    submit: () => find(tree, "form").props.onSubmit({ preventDefault() {} }),
    key: (modifier) =>
      listeners.get("keydown")?.({ ...modifier, key: "Enter", preventDefault() {} }),
    cleanup: () => cleanups.forEach((cleanup) => cleanup?.()),
    writes,
    listeners,
    runs: () => runs,
  };
}

test("Query derives the active time preset from its text", async () => {
  for (const [query, range, expected] of [
    ["[-5m, now] {}", "-15m", "-5m"],
    ["  [ -1h , now ] {}", "-15m", "-1h"],
    ["[-30m, now] {}", "-15m", "-30m"],
    ["[-1h, -5m] {}", "-15m", ""],
    ["[2026-01-01, now] {}", "-15m", ""],
    ["[-5m,", "-15m", ""],
    ["{}", "-15m", "-15m"],
    ["", "", ""],
  ]) {
    const widget = await mountQuery({ query, range });
    assert.equal(widget.presetRange, expected, query);
  }
});

test("Query loads its own history", async () => {
  for (const stored of [[], ["{}"], ["{service=api}", "{}"]]) {
    const widget = await mountQuery({ stored });
    assert.deepEqual(widget.displayedHistory, stored);
  }
});

test("Query saves only successfully executed queries", async () => {
  for (const result of ["[-15m, now] {service=api}", undefined]) {
    const widget = await mountQuery({ stored: ["{}"], result });
    await widget.submit();
    assert.deepEqual(widget.history(), result ? [result, "{}"] : ["{}"]);
    assert.deepEqual(widget.writes, result ? [result] : []);
  }
});

test("Query runs keyboard shortcuts through its history handler and cleans up", async () => {
  for (const modifier of [{ metaKey: true }, { ctrlKey: true }]) {
    const widget = await mountQuery({ result: "{}" });
    await widget.key(modifier);
    // DOM event listeners do not await async handlers.
    await setImmediate();
    assert.equal(widget.runs(), 1);
    assert.deepEqual(widget.history(), ["{}"]);
    widget.cleanup();
    assert.equal(widget.listeners.size, 0);
  }
});

test("Query ignores empty or pending submissions", async () => {
  for (const options of [{ query: "  " }, { pending: true }]) {
    const widget = await mountQuery(options);
    await widget.submit();
    assert.equal(widget.runs(), 0);
    assert.deepEqual(widget.writes, []);
  }
});

test("Query remains usable when history storage is unavailable", async () => {
  const widget = await mountQuery({ storageError: true, result: "{}" });
  await widget.submit();
  assert.equal(widget.runs(), 1);
  assert.deepEqual(widget.history(), []);
});
