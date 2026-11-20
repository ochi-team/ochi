import { defineAppHotkeys } from "@tanstack/hotkeys";

export const appHotkeys = defineAppHotkeys({
  queryEdit: { hotkey: "A", scope: "query", meta: { name: "Edit query" } },
  queryRun: { hotkey: "Mod+Enter", scope: "query", meta: { name: "Run query" } },
  queryClose: { hotkey: "Escape", scope: "query", meta: { name: "Unfocus" } },
  historyOpen: { hotkey: "H", scope: "query", meta: { name: "Query history" } },

  up: { hotkey: "J", scope: "history", meta: { name: "Up" } },
  down: { hotkey: "K", scope: "history", meta: { name: "Down" } },
  select: { hotkey: "Enter", scope: "history", meta: { name: "Select item" } },
  close: { hotkey: "Escape", scope: "history", meta: { name: "Close" } },
});
