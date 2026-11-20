import { For, Show, createSignal, onSettled } from "solid-js";
import { useAppHotkeyList, useHotkeyLabel } from "@/modules/hotkeys";

function Sheet(props: { close: () => void }) {
  const list = useAppHotkeyList();
  let dialog: HTMLDialogElement | undefined;

  onSettled(() => {
    dialog?.showModal();
    return () => dialog?.close();
  });

  return (
    <dialog
      ref={(el) => {
        dialog = el;
      }}
      aria-label="Keyboard shortcuts"
      class="m-auto w-[min(560px,90vw)] rounded-8 border border-gray-6 bg-gray-2 p-ds-16 text-gray-12 backdrop:bg-black/40"
      onCancel={() => props.close()}
      onClick={(event) => {
        if (event.target === dialog) props.close();
      }}
    >
      <header class="mb-ds-12 flex items-center justify-between">
        <h2>Keyboard shortcuts</h2>
        <button type="button" onClick={() => props.close()}>
          Close
        </button>
      </header>
      <For each={list()}>
        {(item) => (
          <div class="flex items-center justify-between gap-ds-12 py-ds-8">
            <span>{item.appHotkey.meta?.name}</span>
            <span
              aria-label={`Rebind ${item.appHotkey.meta?.name}`}
              class="ml-auto rounded-4 border border-gray-6 px-ds-8"
            >
              {useHotkeyLabel(item.appHotkey)()}
            </span>
          </div>
        )}
      </For>
    </dialog>
  );
}

export default function HotkeySheet() {
  const [open, setOpen] = createSignal(false);
  const close = () => setOpen(false);

  return (
    <>
      <button
        type="button"
        aria-label="Keyboard shortcuts"
        onClick={() => setOpen((prev) => !prev)}
        class="px-ds-8 text-gray-11"
      >
        ?
      </button>
      <Show when={open()}>
        <Sheet close={close} />
      </Show>
    </>
  );
}
