import { For, Show, createEffect, createSignal, type Setter } from "solid-js";

import Keycap from "@/components/Keycap";
import { HotkeysProvider, useHotkey, useHotkeyLabel } from "@/modules/hotkeys";
import { appHotkeys } from "@/modules/keymap";
import { useDismiss } from "@/modules/dialog";

interface HistoryItemsProps {
  history: readonly string[];

  selected: number;
  setSelected: Setter<number>;
  select: (query: string) => void;
}

function HistoryItems(props: HistoryItemsProps) {
  useHotkey(
    appHotkeys.down,
    () => {
      const lastIndex = Math.max(0, props.history.length - 1);
      props.setSelected((index) => Math.min(index + 1, lastIndex));
    },
    { ignoreInputs: false },
  );
  useHotkey(
    appHotkeys.up,
    () => {
      props.setSelected((index) => Math.max(0, index - 1));
    },
    { ignoreInputs: false },
  );
  useHotkey(
    appHotkeys.select,
    () => {
      const query = props.history[props.selected];
      if (query) props.select(query);
    },
    { ignoreInputs: false },
  );

  return (
    <div
      role="listbox"
      aria-label="Query history"
      class="absolute top-9 right-0 z-10 max-h-80 w-[min(480px,90vw)] overflow-auto rounded-8 border border-gray-6 bg-gray-2 p-ds-12 shadow-xl"
    >
      <For each={props.history} fallback={<p>No query history</p>}>
        {(query, index) => (
          <button
            type="button"
            role="option"
            aria-selected={props.selected === index() ? "true" : "false"}
            class={[
              "block w-full rounded-4 p-ds-8 text-left font-mono hover:bg-gray-3",
              { "bg-gray-4": props.selected === index() },
            ]}
            onMouseEnter={() => props.setSelected(index())}
            onClick={() => props.select(query)}
          >
            {query}
          </button>
        )}
      </For>
    </div>
  );
}

interface QueryHistoryProps {
  // histoey data to display
  history: readonly string[];
  // set the query from the history to input element
  setQuery: (value: string) => void;
  // input reference itself to focus it right after setting a query
  input: HTMLInputElement | undefined;
}

export default function QueryHistory(props: QueryHistoryProps) {
  let root: HTMLDetailsElement | undefined;
  const [open, setOpen] = createSignal(false);
  const [selected, setSelected] = createSignal(0);
  const label = useHotkeyLabel(appHotkeys.historyOpen);

  const close = () => setOpen(false);
  const show = () => {
    setSelected(0);
    setOpen(true);
  };
  const select = (query: string) => {
    props.setQuery(query);
    close();
    props.input?.focus();
  };

  useHotkey(appHotkeys.historyOpen, show);

  useDismiss(() => root, open, close);

  createEffect(selected, () => {
    root?.querySelector('[aria-selected="true"]')?.scrollIntoView({ block: "nearest" });
  });

  return (
    <details
      class="relative ml-auto"
      ref={(el) => {
        root = el;
      }}
      open={open()}
    >
      <summary
        class="flex cursor-pointer list-none items-center gap-ds-8 px-ds-8 text-16"
        onClick={(event) => {
          event.preventDefault();
          if (open()) close();
          else show();
        }}
      >
        <svg aria-hidden="true" width="14" height="14" class="shrink-0">
          <use href="/icons.svg#history" />
        </svg>
        History
        <Keycap label={label()} />
      </summary>
      <Show when={open()}>
        <HotkeysProvider scope="history" exclusive>
          <HistoryItems
            history={props.history}
            selected={selected()}
            setSelected={setSelected}
            select={select}
          />
        </HotkeysProvider>
      </Show>
    </details>
  );
}
