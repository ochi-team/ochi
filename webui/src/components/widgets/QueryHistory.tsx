import { For } from "solid-js";

import Keycap from "@/components/Keycap";

interface QueryHistoryProps {
  // histoey data to display
  history: readonly string[];
  // set the query from the history to input element
  setQuery: (value: string) => void;
  // input reference itself to focus it right after setting a query
  input: HTMLInputElement | undefined;
}

export default function QueryHistory(props: QueryHistoryProps) {
  let rootRef: HTMLDetailsElement | undefined;

  return (
    <details
      class="relative ml-auto"
      ref={(el) => {
        rootRef = el;
      }}
    >
      <summary class="flex cursor-pointer list-none items-center gap-ds-8 px-ds-8 text-16">
        <svg aria-hidden="true" width="14" height="14" class="shrink-0">
          <use href="/icons.svg#history" />
        </svg>
        History
        <Keycap label="h" />
      </summary>
      <div class="absolute top-9 right-0 z-10 max-h-80 w-[min(480px,90vw)] overflow-auto rounded-8 border border-gray-6 bg-gray-2 p-ds-12 shadow-xl">
        <For each={props.history} fallback={<p>No query history</p>}>
          {(query) => (
            <button
              class="block w-full rounded-4 p-ds-8 text-left font-mono hover:bg-gray-3"
              onClick={() => {
                props.setQuery(query);
                props.input?.focus();
                if (rootRef) rootRef.open = false;
              }}
            >
              {query}
            </button>
          )}
        </For>
      </div>
    </details>
  );
}
