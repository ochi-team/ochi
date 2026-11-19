import { For } from "solid-js";

import Keycap from "@/components/Keycap";


const presets = ["5m", "15m", "1h", "6h", "24h"];

export default function Query(props: {}) {
  return (
    <section
      aria-label="Query controls"
      class="flex h-32 shrink-0 flex-col gap-ds-12 border-b border-gray-6 bg-gray-2 px-ds-16 py-ds-12"
    >
      <form
        class="flex h-[52px] items-center gap-ds-12"
        onSubmit={(event) => {
          event.preventDefault();
          void run();
        }}
      >
        <label class="flex h-full min-w-0 flex-1 items-center gap-ds-16 rounded-6 border border-gray-6 bg-bg px-ds-12 focus-within:border-dark-ring">
          <span>LOQL</span>
          <input
            ref={(el) => {
              input = el;
            }}
            aria-label="LOQL query"
            class="min-w-0 flex-1 bg-transparent font-mono text-13 text-gray-12 outline-none! placeholder:text-gray-11"
            placeholder="Write a query, or use the example below"
            value={draft()}
            onInput={(event) => setDraft(event.currentTarget.value)}
            spellcheck="false"
            autocomplete="off"
          />
          <kbd class="rounded-2 border border-gray-6 px-ds-6 font-sans">a</kbd>
        </label>
        <button
          type="submit"
          disabled={!draft().trim() || pending()}
          class="group flex h-9 w-36 shrink-0 items-center justify-center gap-ds-8 rounded-6 border border-gray-6 bg-dark-primary text-dark-primary-foreground enabled:border-dark-primary enabled:hover:brightness-110 disabled:bg-gray-3 disabled:text-gray-11 disabled:opacity-45"
        >
          <svg aria-hidden="true" width="12" height="12" class="shrink-0">
            <use href="/icons.svg#play" />
          </svg>
          <span class="text-14 font-medium">{pending() ? "Running…" : "Run"}</span>
          <span class="flex gap-ds-3">
            <Keycap label="⌘" />
            <Keycap label="↵" />
          </span>
        </button>
      </form>
      <div class="flex h-7 items-center gap-ds-8">
        <span class="pr-ds-4">Time</span>
        <For each={presets}>
          {(preset) => (
            <button
              class={[
                "h-7 rounded-6 border border-gray-6 px-ds-10 text-16 hover:bg-gray-3",
                {
                  "border-dark-ring bg-dark-accent text-dark-accent-foreground":
                    range() === `-${preset}`,
                },
              ]}
              aria-pressed={range() === `-${preset}` ? "true" : "false"}
              onClick={() => selectRange(`-${preset}`)}
            >
              {preset}
            </button>
          )}
        </For>
        <details class="relative ml-auto">
          <summary class="flex cursor-pointer list-none items-center gap-ds-8 px-ds-8 text-16">
            <svg aria-hidden="true" width="14" height="14" class="shrink-0">
              <use href="/icons.svg#history" />
            </svg>
            History
          </summary>
          <div class="absolute top-9 right-0 z-10 max-h-80 w-[min(480px,90vw)] overflow-auto rounded-8 border border-gray-6 bg-gray-2 p-ds-12 shadow-xl">
            <For each={history()} fallback={<p>No query history</p>}>
              {(query) => (
                <button
                  class="block w-full rounded-4 p-ds-8 text-left font-mono hover:bg-gray-3"
                  onClick={(event) => {
                    setDraft(query);
                    input?.focus();
                    event.currentTarget.closest("details")?.removeAttribute("open");
                  }}
                >
                  {query}
                </button>
              )}
            </For>
          </div>
        </details>
      </div>
    </section>
  );
}
