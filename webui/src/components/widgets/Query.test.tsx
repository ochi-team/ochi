import { fireEvent, render, screen, waitFor, within } from "@solidjs/testing-library";
import { parseHotkey } from "@tanstack/hotkeys";
import { createSignal, flush } from "solid-js";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { browserPlatform, HotkeysProvider, loadCustomHotkeys, saveCustomHotkeys } from "@/modules/hotkeys";
import { appHotkeys } from "@/modules/keymap";
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
      <HotkeysProvider
        scope="global"
        appHotkeys={appHotkeys}
        customHotkeys={loadCustomHotkeys()}
        onCustomHotkeysChange={saveCustomHotkeys}
      >
        <Query
          query={text()}
          setQuery={setText}
          range={timeRange()}
          setRange={setTimeRange}
          runQuery={runQuery}
        />
      </HotkeysProvider>
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

test("Query history closes on outside pointerdown", () => {
  pushQueryHistory("{}");
  renderQuery();
  openHistory();

  const details = screen.getByText("History").closest("details")!;
  expect(details.open).toBe(true);

  fireEvent.pointerDown(details);
  flush();
  expect(details.open).toBe(true);

  fireEvent.pointerDown(document.body);
  flush();
  expect(details.open).toBe(false);
});

test("Query history keyboard navigation, selection and exclusive scope", () => {
  pushQueryHistory("{second}");
  pushQueryHistory("{first}");
  const { runQuery } = renderQuery();

  const key = (binding: (typeof appHotkeys)[keyof typeof appHotkeys]) => {
    if (!binding.hotkey) throw new Error(`Missing hotkey: ${binding.id}`);
    const hotkey = parseHotkey(binding.hotkey, browserPlatform());
    if (Array.isArray(hotkey)) throw new Error(`Expected a chord: ${binding.id}`);

    fireEvent.keyDown(window, {
      key: hotkey.key,
      ctrlKey: hotkey.ctrl,
      metaKey: hotkey.meta,
      altKey: hotkey.alt,
      shiftKey: hotkey.shift,
    });
    flush();
  };

  const details = screen.getByText("History").closest("details")!;
  key(appHotkeys.historyOpen);
  expect(details.open).toBe(true);

  const items = within(details).getAllByRole("option");
  expect(items[0]).toHaveAttribute("aria-selected", "true");
  key(appHotkeys.up);
  expect(items[0]).toHaveAttribute("aria-selected", "true");

  key(appHotkeys.down);
  key(appHotkeys.down);
  expect(items[1]).toHaveAttribute("aria-selected", "true");

  key(appHotkeys.up);
  expect(items[0]).toHaveAttribute("aria-selected", "true");

  key(appHotkeys.queryRun);
  expect(runQuery).not.toHaveBeenCalled();
  key(appHotkeys.queryEdit);
  expect(screen.getByRole("textbox")).not.toHaveFocus();

  key(appHotkeys.select);
  expect(details.open).toBe(false);
  expect(screen.getByRole("textbox")).toHaveValue("{first}");
  expect(screen.getByRole("textbox")).toHaveFocus();

  screen.getByRole("textbox").blur();
  key(appHotkeys.historyOpen);
  key(appHotkeys.close);
  expect(details.open).toBe(false);
});

test("Query history hotkeys do not interrupt typing and handle empty history", () => {
  renderQuery();

  const input = screen.getByRole("textbox");
  input.focus();
  fireEvent.keyDown(input, { key: "h" });
  flush();

  const details = screen.getByText("History").closest("details")!;
  expect(details.open).toBe(false);
  input.blur();

  for (const key of ["h", "j", "k", "Enter", "Escape"]) {
    fireEvent.keyDown(window, { key });
    flush();
  }

  expect(details.open).toBe(false);
});

test.each([
  ["MacIntel", "⌘"],
  ["iPhone", "⌘"],
  ["iPad", "⌘"],
  ["Win32", "Ctrl"],
  ["Linux x86_64", "Ctrl"],
])("Query shows platform-correct shortcut labels on %s", (platform, label) => {
  vi.spyOn(navigator, "userAgent", "get").mockReturnValue(platform);
  renderQuery();

  const button = screen.getByRole("button", { name: /^Run/ });
  expect(button).toHaveTextContent(label);
});

test("Query shortcut sheet displays the app keymap and closes", () => {
  renderQuery();
  fireEvent.click(screen.getByRole("button", { name: "Keyboard shortcuts" }));
  flush();

  const sheet = screen.getByRole("dialog", { name: "Keyboard shortcuts" });
  expect(
    Array.from(sheet.querySelectorAll(":scope > div > span:first-child"), (label) => label.textContent),
  ).toEqual(Object.values(appHotkeys).map((hotkey) => hotkey.meta?.name));

  fireEvent.click(within(sheet).getByRole("button", { name: "Close" }));
  flush();
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
});

test("Query loads persisted custom shortcuts", () => {
  localStorage.setItem("ochi.hotkeys", JSON.stringify({ historyOpen: "Y" }));
  renderQuery();

  fireEvent.keyDown(window, { key: "h" });
  flush();
  expect(screen.getByText("History").closest("details")!.open).toBe(false);

  fireEvent.keyDown(window, { key: "y" });
  flush();
  expect(screen.getByText("History").closest("details")!.open).toBe(true);
});

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
  ["", true],
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
    // reversed because push pushes to the beginning of the history
    stored.slice().reverse().forEach((it) => pushQueryHistory(it));
    renderQuery();

    const history = openHistory();
    expect(history.queryAllByRole("option").map((button) => button.textContent)).toEqual(stored);
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
      expect(history.queryAllByRole("option").map((button) => button.textContent)).toEqual(
        expected,
      ),
    );
  },
);

test.each(["mac", "windows", "linux"] as const)(
  "Query runs keyboard shortcuts and cleans up: %j",
  async (platform) => {
    vi.spyOn(navigator, "userAgent", "get").mockReturnValue(
      platform === "mac" ? "MacIntel" : platform,
    );
    const modifier = platform === "mac" ? { metaKey: true } : { ctrlKey: true };
    const runQuery = vi.fn(async () => "{}");
    const view = renderQuery({ runQuery });
    fireEvent.keyDown(window, { key: "Enter", ...modifier });
    await waitFor(() => expect(openHistory().getByRole("option", { name: "{}" })).toBeVisible());
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
  expect(openHistory().getByRole("option", { name: "{}" })).toBeVisible();
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
