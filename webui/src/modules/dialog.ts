import { onSettled } from "solid-js";
import type { HotkeyCommonOptions } from "@tanstack/hotkeys";

import { useHotkey } from "@/modules/hotkeys";
import { appHotkeys } from "@/modules/keymap";

import { AppHotkey } from "@tanstack/hotkeys";

export function useDismiss(
  root: () => EventTarget | undefined,
  close: () => void,
  toggleKey: AppHotkey | undefined = undefined,
  options: HotkeyCommonOptions = {},
) {
  useHotkey(appHotkeys.close, close, { ignoreInputs: false, ...options });
  if (toggleKey) useHotkey(toggleKey, close, { ignoreInputs: false, ...options });

  // close on click outside
  onSettled(() => {
    const outside = (event: Event) => {
      const r = root();
      if (r && event.target instanceof Node && !event.composedPath().includes(r)) close();
    };
    document.addEventListener("pointerdown", outside);
    return () => {
      document.removeEventListener("pointerdown", outside);
    };
  });
}
