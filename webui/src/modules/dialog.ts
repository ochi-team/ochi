import { onSettled } from "solid-js";

import { useHotkey } from "@/modules/hotkeys";
import { appHotkeys } from "@/modules/keymap";

export function useDismiss(
  root: () => HTMLDetailsElement | undefined,
  open: () => boolean,
  close: () => void,
) {
  // close on esc
  useHotkey(appHotkeys.close, () => close(), { ignoreInputs: false });

  // close on click outside
  onSettled(() => {
    const outside = (event: Event) => {
      const r = root();
      if (r && open() && event.target instanceof Node && !event.composedPath().includes(r)) close();
    };
    document.addEventListener("pointerdown", outside);
    return () => {
      document.removeEventListener("pointerdown", outside);
    };
  });
}
