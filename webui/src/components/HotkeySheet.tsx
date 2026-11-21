import { For, Show, createMemo, createSignal, onSettled } from "solid-js";
import { formatForDisplay, type HotkeyPlatform, type AppHotkey } from "@tanstack/hotkeys";
import { browserPlatform, HotkeysProvider, useAppHotkeyList, useHotkey } from "@/modules/hotkeys";
import { appHotkeys } from "@/modules/keymap";
import { useDismiss } from "@/modules/dialog";

function hotkeysByScope(hostKeyList: AppHotkey[], platform: HotkeyPlatform) {
  const scopes = new Map<string, { name: string; group?: string; label: string }[]>();

  for (const appHotkey of hostKeyList) {
    const scope = appHotkey.meta?.scope ?? "";
    const items = scopes.get(scope) ?? [];
    const name = appHotkey.meta?.group ?? appHotkey.meta?.name ?? "";
    const group = appHotkey.meta?.group;
    const label = appHotkey.hotkey ? formatForDisplay(appHotkey.hotkey, { platform }) : "";

    const grouped = group && items.find((row) => row.group === group);
    if (grouped) {
      // update same group keys view, don't create a new key in the sheet
      grouped.label += ` / ${label}`;
      continue;
    }

    // collect the key view
    items.push({ name, group, label });
    scopes.set(scope, items);
  }

  return Array.from(scopes, ([name, items]) => ({ name, items }));
}

function Sheet(props: { close: () => void; root: () => HTMLElement | undefined }) {
  const list = useAppHotkeyList();
  const platform = browserPlatform();
  const groups = createMemo(() => hotkeysByScope(list(), platform));

  let closeButton: HTMLButtonElement | undefined;
  onSettled(() => closeButton?.focus());

  useDismiss(
    () => props.root(),
    () => props.close(),
    appHotkeys.hotkeysOpen,
  );

  return (
    <section
      id="keyboard-shortcuts"
      role="dialog"
      aria-label="Keyboard shortcuts"
      class="absolute right-0 bottom-[44px] flex max-h-[calc(100dvh-84px)] w-[min(340px,calc(100vw-40px))] flex-col overflow-auto rounded-8 border border-gray-6 bg-gray-2 pb-ds-12 text-gray-12 shadow-[0_12px_32px_#00000040]"
    >
      <header class="flex h-[52px] shrink-0 items-center justify-between border-b border-gray-3 px-ds-16">
        <h2 class="text-14 leading-18 font-semibold">Keyboard shortcuts</h2>
        <button
          ref={(el) => {
            closeButton = el;
          }}
          type="button"
          aria-label="Close"
          class="flex size-6 items-center justify-center rounded-3 text-[18px] leading-22 hover:bg-gray-3"
          onClick={() => props.close()}
        >
          ×
        </button>
      </header>
      <For each={groups()}>
        {(group) => (
          <section aria-label={group.name} class="flex flex-col gap-ds-4 px-ds-16 pt-ds-12 pb-ds-6">
            <h3 class="text-12 leading-20 font-semibold">{group.name}</h3>
            <For each={group.items}>
              {(item) => (
                <div class="flex h-7 shrink-0 items-center justify-between gap-ds-12 text-13 leading-16">
                  <span>{item.name}</span>
                  <kbd class="shrink-0 rounded-3 border border-gray-6 px-ds-6 py-ds-2 font-sans text-12 leading-16">
                    {item.label}
                  </kbd>
                </div>
              )}
            </For>
          </section>
        )}
      </For>
    </section>
  );
}

export default function HotkeySheet() {
  const [open, setOpen] = createSignal(false);
  useHotkey(appHotkeys.hotkeysOpen, () => setOpen((prev) => !prev));

  const close = () => setOpen(false);

  let root: HTMLDivElement | undefined;

  return (
    <div
      ref={(el) => {
        root = el;
      }}
      class="fixed right-ds-20 bottom-ds-20 z-20"
    >
      <button
        type="button"
        aria-label="Keyboard shortcuts"
        aria-haspopup="dialog"
        aria-expanded={open() ? "true" : "false"}
        aria-controls="keyboard-shortcuts"
        onClick={() => setOpen((prev) => !prev)}
        class={[
          "flex size-8 items-center justify-center rounded-99 border text-16 leading-20 font-semibold hover:border-dark-ring hover:bg-dark-accent hover:text-dark-accent-foreground",
          open()
            ? "border-dark-ring bg-dark-accent text-dark-accent-foreground"
            : "border-gray-6 bg-gray-2 text-gray-11",
        ]}
      >
        ?
      </button>
      <Show when={open()}>
        <HotkeysProvider scope="hotkeys" exclusive>
          <Sheet close={close} root={() => root} />
        </HotkeysProvider>
      </Show>
    </div>
  );
}
