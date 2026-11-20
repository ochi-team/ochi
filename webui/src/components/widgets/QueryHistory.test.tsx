import { fireEvent, render, screen, within } from "@solidjs/testing-library";
import { flush } from "solid-js";
import { expect, test, vi } from "vitest";

import { HotkeysProvider, useHotkey } from "@/modules/hotkeys";
import { appHotkeys } from "@/modules/keymap";
import QueryHistory from "./QueryHistory";

function renderHistory() {
  const setQuery = vi.fn();
  render(() => (
    <HotkeysProvider scope="global" appHotkeys={appHotkeys}>
      <HotkeysProvider scope="query">
        <QueryHistory history={["{first}", "{second}"]} setQuery={setQuery} input={undefined} />
      </HotkeysProvider>
    </HotkeysProvider>
  ));
  flush();
  return setQuery;
}

function key(key: string) {
  fireEvent.keyDown(window, { key });
  flush();
}

test.each(["hotkey", "click"])(
  "history preselects first item and navigates after opening by %s",
  (method) => {
    const setQuery = renderHistory();
    if (method === "hotkey") key("h");
    else {
      fireEvent.click(screen.getByText("History"));
      flush();
    }

    const list = screen.getByRole("listbox");
    const options = within(list).getAllByRole("option");
    expect(options.map((item) => item.getAttribute("aria-selected"))).toEqual(["true", "false"]);

    key("j");
    expect(options.map((item) => item.getAttribute("aria-selected"))).toEqual(["false", "true"]);

    key("k");
    expect(options.map((item) => item.getAttribute("aria-selected"))).toEqual(["true", "false"]);

    key("j");
    key("Enter");
    expect(setQuery.mock.calls).toEqual([["{second}"]]);
    expect(screen.queryByRole("listbox")).not.toBeInTheDocument();
  },
);

test.each(["h", "Escape"])("history closes on %s and can reopen", (closingKey) => {
  renderHistory();

  key("h");
  expect(screen.getByRole("listbox")).toBeVisible();

  key(closingKey);
  expect(screen.queryByRole("listbox")).not.toBeInTheDocument();

  key("h");
  expect(screen.getByRole("option", { name: "{first}" })).toHaveAttribute("aria-selected", "true");
});
