import { fireEvent, render, screen, waitFor, within } from "@solidjs/testing-library";
import { createSignal, flush } from "solid-js";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { pushQueryHistory, loadQueryHistory } from "@/modules/queryHistory";

import Query from "./Query";

function renderQuery({
  query = "{service=api}",
  range = "-15m",
  runQuery = vi.fn(async (): Promise<string | undefined> => undefined),
} = {}) {
  const view = render(() => {
    const [text, setText] = createSignal(query);
    const [timeRange, setTimeRange] = createSignal(range);
    return (
      <Query
        query={text()}
        setQuery={setText}
        range={timeRange()}
        setRange={setTimeRange}
        runQuery={runQuery}
      />
    );
  });
  flush();
  return { ...view, runQuery };
}

function openHistory() {
  const details = screen.getByText("History").closest("details")!;
  if (!details.open) fireEvent.click(screen.getByText("History"));
  flush();
  return within(details);
}

beforeEach(() => localStorage.clear());
afterEach(() => {
  vi.restoreAllMocks();
  localStorage.clear();
});

test.each([
  ["[-5m, now] {}", true],
  ["[-60m, now]", true],
  ["[ -1h , now ] {}", true],
  ["  [ -1h , now ] {}", true],
  ["[-30m, now] {}", true],
  ["[-1h, -5m] {}", true],
  ["[2026-01-01, now] {}", true],
  ["[-5m,", false],
  ["{}", false],
  ["", false],
])("Query enables time presets for supported ranges: %s", (query, enabled) => {
  renderQuery({ query });
  const preset = screen.getByRole("button", { name: "5m" });
  if (enabled) expect(preset).toBeEnabled();
  else expect(preset).toBeDisabled();
});

test("Query time preset updates its selected time preset", () => {
  renderQuery({ query: "[-15m, now] {}" });
  expect(screen.getByRole("button", { name: "15m" })).toHaveAttribute("aria-pressed", "true");
  fireEvent.click(screen.getByRole("button", { name: "5m" }));
  flush();
  expect(screen.getByRole("button", { name: "5m" })).toHaveAttribute("aria-pressed", "true");
  expect(screen.getByRole("button", { name: "15m" })).toHaveAttribute("aria-pressed", "false");
});

test.each([[[]], [["{}"]], [["{service=api}", "{}"]]])(
  "Query loads its own history: %j",
  (stored) => {
    // to reversed because push pushes to the beginning of the history
    stored.toReversed().forEach((it) => pushQueryHistory(it));
    renderQuery();

    const history = openHistory();
    expect(history.queryAllByRole("button").map((button) => button.textContent)).toEqual(stored);
    if (!stored.length) expect(history.getByText("No query history")).toBeVisible();
  },
);

test.each(["[-15m, now] {service=api}", undefined])(
  "Query saves only successfully executed queries: %s",
  async (result) => {
    pushQueryHistory("{}");

    const runQuery = vi.fn(async () => result);
    renderQuery({ runQuery });
    fireEvent.click(screen.getByRole("button", { name: /^Run/ }));
    await waitFor(() => expect(runQuery).toHaveBeenCalledOnce());

    const expected = result ? [result, "{}"] : ["{}"];
    await waitFor(() => expect(loadQueryHistory()).toEqual(expected));
    const history = openHistory();
    await waitFor(() =>
      expect(history.queryAllByRole("button").map((button) => button.textContent)).toEqual(
        expected,
      ),
    );
  },
);

test.each([{ metaKey: true }, { ctrlKey: true }])(
  "Query runs keyboard shortcuts and cleans up: %j",
  async (modifier) => {
    const runQuery = vi.fn(async () => "{}");
    const view = renderQuery({ runQuery });
    fireEvent.keyDown(window, { key: "Enter", ...modifier });
    await waitFor(() => expect(openHistory().getByRole("button", { name: "{}" })).toBeVisible());
    expect(runQuery).toHaveBeenCalledOnce();

    view.unmount();
    fireEvent.keyDown(window, { key: "Enter", ...modifier });
    flush();
    expect(runQuery).toHaveBeenCalledOnce();
  },
);

test.each(["", "  "])("Query disables empty submissions: %j", (query) => {
  const { runQuery } = renderQuery({ query });
  const button = screen.getByRole("button", { name: /^Run/ });
  expect(button).toBeDisabled();

  fireEvent.click(button);
  flush();
  expect(runQuery).not.toHaveBeenCalled();
  expect(loadQueryHistory()).toEqual([]);
});

test("Query prevents duplicate runs while pending and updates history after completion", async () => {
  let complete!: (value: string) => void;
  const runQuery = vi.fn(
    () =>
      new Promise<string>((resolve) => {
        complete = resolve;
      }),
  );
  renderQuery({ runQuery });
  fireEvent.click(screen.getByRole("button", { name: /^Run/ }));
  flush();
  expect(screen.getByRole("button", { name: /^Running/ })).toBeDisabled();

  fireEvent.keyDown(window, { key: "Enter", ctrlKey: true });
  flush();
  expect(runQuery).toHaveBeenCalledOnce();

  complete("{}");
  await waitFor(() => expect(screen.getByRole("button", { name: /^Run / })).toBeEnabled());
  expect(openHistory().getByRole("button", { name: "{}" })).toBeVisible();
});

test("Query remains usable when history storage is unavailable", async () => {
  vi.spyOn(window.localStorage, "getItem").mockImplementation(() => {
    throw new Error("Storage unavailable");
  });
  vi.spyOn(window.localStorage, "setItem").mockImplementation(() => {
    throw new Error("Storage unavailable");
  });
  vi.spyOn(console, "error").mockImplementation(() => {});

  const runQuery = vi.fn(async () => "{}");
  renderQuery({ runQuery });
  fireEvent.click(screen.getByRole("button", { name: /^Run/ }));
  await waitFor(() => expect(runQuery).toHaveBeenCalledOnce());
  await waitFor(() => expect(screen.getByRole("button", { name: /^Run/ })).toBeEnabled());
  expect(openHistory().getByText("No query history")).toBeVisible();
  expect(loadQueryHistory()).toEqual([]);
});
