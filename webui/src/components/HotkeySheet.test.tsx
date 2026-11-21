import { fireEvent, render, screen, within } from "@solidjs/testing-library";
import { flush } from "solid-js";
import { afterEach, expect, test, vi } from "vitest";
import { HotkeysProvider } from "@/modules/hotkeys";
import { appHotkeys } from "@/modules/keymap";
import HotkeySheet from "./HotkeySheet";

function renderSheet(customHotkeys = {}) {
  render(() => (
    <HotkeysProvider scope="global" appHotkeys={appHotkeys} customHotkeys={customHotkeys}>
      <HotkeySheet />
    </HotkeysProvider>
  ));
  flush();
  return { trigger: screen.getByRole("button", { name: "Keyboard shortcuts" }) };
}

function openSheet(customHotkeys = {}) {
  const { trigger } = renderSheet(customHotkeys);
  fireEvent.click(trigger);
  flush();
  return { trigger, sheet: screen.getByRole("dialog", { name: "Keyboard shortcuts" }) };
}

afterEach(() => vi.restoreAllMocks());

test("Shortcut popover toggles using the trigger", () => {
  const { trigger } = renderSheet();
  expect(trigger).toHaveAttribute("aria-expanded", "false");
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();

  fireEvent.click(trigger);
  flush();
  const sheet = screen.getByRole("dialog", { name: "Keyboard shortcuts" });
  expect(sheet).toBeVisible();
  expect(trigger).toHaveAttribute("aria-expanded", "true");

  fireEvent.pointerDown(trigger);
  flush();
  expect(sheet).toBeInTheDocument();
  fireEvent.click(trigger);
  flush();
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  expect(trigger).toHaveAttribute("aria-expanded", "false");
});

test.each([
  ["MacIntel", "⌘"],
  ["Win32", "Ctrl"],
  ["Linux x86_64", "Ctrl"],
])(
  "Shortcut popover groups individual bindings with platform labels on %s",
  (platform, modifier) => {
    vi.spyOn(navigator, "userAgent", "get").mockReturnValue(platform);
    const { sheet } = openSheet();
    expect(
      within(sheet)
        .getAllByRole("heading")
        .map((heading) => heading.textContent),
    ).toEqual([
      "Keyboard shortcuts",
      ...new Set(Object.values(appHotkeys).map((binding) => binding.meta?.scope ?? "")),
    ]);

    const bindings = Object.values(appHotkeys);
    expect(
      Array.from(sheet.querySelectorAll("section > div > span"), (label) => label.textContent),
    ).toEqual([...new Set(bindings.map((binding) => binding.meta?.group ?? binding.meta?.name))]);
    expect(sheet.querySelectorAll("kbd")).toHaveLength(bindings.length - 1);
    expect(within(sheet).getByRole("region", { name: "Query" })).toHaveTextContent(modifier);
  },
);

test.each([
  [{}, "K / J"],
  [{ down: "N", up: "P" }, "P / N"],
])("Shortcut popover combines navigation bindings: %j", (customHotkeys, expected) => {
  const { sheet } = openSheet(customHotkeys);
  const row = within(sheet).getByText("Up / Down").parentElement!;
  expect(row.querySelector("kbd")).toHaveTextContent(expected);
  expect(within(sheet).queryByText("Up")).not.toBeInTheDocument();
  expect(within(sheet).queryByText("Down")).not.toBeInTheDocument();
});

test.each(["close", "escape", "outside", "hotkey"])(
  "Shortcut popover dismisses using %s",
  (method) => {
    const { trigger, sheet } = openSheet();
    expect(within(sheet).getByRole("button", { name: "Close" })).toHaveFocus();
    fireEvent.pointerDown(sheet);
    flush();
    expect(sheet).toBeInTheDocument();

    if (method === "close") fireEvent.click(within(sheet).getByRole("button", { name: "Close" }));
    if (method === "escape") fireEvent.keyDown(window, { key: "Escape" });
    if (method === "outside") fireEvent.pointerDown(document.body);
    if (method === "hotkey") fireEvent.keyDown(window, { key: "?", code: "Slash", shiftKey: true });
    flush();

    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(trigger).toHaveAttribute("aria-expanded", "false");
  },
);

test("Shortcut popover opens with the workspace hotkey", () => {
  renderSheet();
  fireEvent.keyDown(window, { key: "?", code: "Slash", shiftKey: true });
  flush();
  expect(screen.getByRole("dialog", { name: "Keyboard shortcuts" })).toBeVisible();
});
