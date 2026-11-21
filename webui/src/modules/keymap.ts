import { defineAppHotkeys } from "@tanstack/hotkeys";

declare module "@tanstack/hotkeys" {
  interface HotkeyMeta {
    scope?: string;
    group?: string;
  }
}

export const appHotkeys = defineAppHotkeys({
  queryEdit: { hotkey: "A", scope: "query", meta: { name: "Focus query", scope: "Query" } },
  queryRun: { hotkey: "Mod+Enter", scope: "query", meta: { name: "Run query", scope: "Query" } },
  queryClose: { hotkey: "Escape", scope: "query", meta: { name: "Unfocus", scope: "Query" } },
  historyOpen: { hotkey: "H", scope: "query", meta: { name: "Query history", scope: "Query" } },

  up: { hotkey: "K", meta: { name: "Up", scope: "Navigation", group: "Up / Down" } },
  down: { hotkey: "J", meta: { name: "Down", scope: "Navigation", group: "Up / Down" } },
  select: { hotkey: "Enter", meta: { name: "Select item", scope: "Navigation" } },
  close: { hotkey: "Escape", meta: { name: "Close", scope: "Navigation" } },

  hotkeysOpen: { hotkey: "?", meta: { name: "Keyboard shortcuts", scope: "" } },
});
