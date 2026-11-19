import { Title } from "@solidjs/meta";
import { createSignal, For, onSettled, Show } from "solid-js";
import { buildLoqlQuery, queryLogs, type LogEntry } from "../client/query";
import { loadQueryHistory, pushQueryHistory } from "../modules/queryHistory";

import Keycap from "@/components/Keycap";

const example = "[-15m, now] {service=api OR service=worker}";
const presets = ["5m", "15m", "1h", "6h", "24h"];
const control = "rounded-6 border border-gray-6 px-ds-10 h-7 text-16 hover:bg-gray-3";

export default function Home() {
  const [draft, setDraft] = createSignal("");
  const [range, setRange] = createSignal("");
  const [history, setHistory] = createSignal<readonly string[]>([]);
  const [pending, setPending] = createSignal(false);
  const [hasRun, setHasRun] = createSignal(false);
  const [error, setError] = createSignal("");
  const [lines, setLines] = createSignal<LogEntry[]>([]);
  const [collapsed, setCollapsed] = createSignal(false);
  let input: HTMLInputElement | undefined;
  let controller: AbortController | undefined;

  function selectRange(value: string) {
    setRange(value);
    setDraft((valueBefore) => valueBefore.replace(/^\[[^\]]*\]\s*/, `[${value}, now] `));
  }

  function useExample() {
    setDraft(example);
    setRange("-15m");
    input?.focus();
  }

  async function run() {
    const value = draft().trim();
    if (!value || pending()) return;
    const query = value.startsWith("[") ? value : buildLoqlQuery(range() || "-15m", value);
    controller = new AbortController();
    setPending(true);
    setError("");
    try {
      const response = await queryLogs({ query, signal: controller.signal });
      setLines(response.lines);
      setHasRun(true);
      try {
        setHistory(pushQueryHistory(history(), query));
      } catch {
        /* Storage may be disabled by the browser. */
      }
    } catch (err) {
      if (!controller.signal.aborted) setError(err instanceof Error ? err.message : String(err));
    } finally {
      setPending(false);
    }
  }

  onSettled(() => {
    try {
      setHistory(loadQueryHistory());
    } catch {
      /* Storage may be disabled or contain invalid data. */
    }
    const onKey = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
        event.preventDefault();
        void run();
      } else if (
        event.key === "a" &&
        !event.metaKey &&
        !event.ctrlKey &&
        !event.altKey &&
        !(
          event.target instanceof HTMLElement &&
          (event.target.closest("input, textarea, select") || event.target.isContentEditable)
        )
      ) {
        event.preventDefault();
        input?.focus();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("keydown", onKey);
      controller?.abort();
    };
  });

  return (
    <main class="flex h-dvh min-h-[540px] flex-col overflow-hidden bg-bg font-sans text-12 leading-16 tracking-[-0.01em] text-gray-11">
      <Title>Ochi · Query</Title>
      <header class="flex h-12 shrink-0 items-center gap-ds-24 border-b border-gray-6 bg-gray-2 px-ds-16">
        <img src="/ochi.svg" alt="Ochi" class="h-[47px] w-[104px] object-contain" />
        <span class="ml-auto font-mono text-gray-12">{import.meta.env.OCHI_HOST}</span>
        <a
          href="https://ochi.dev/docs/reference/loql/"
          target="_blank"
          rel="noreferrer"
          class="flex items-center gap-ds-6 text-16 hover:text-gray-12"
        >
          Docs{" "}
          <span aria-hidden="true" class="text-14">
            ↗
          </span>
        </a>
      </header>

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
                  control,
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

      <div class="flex min-h-0 flex-1">
        <aside
          class={[
            "flex shrink-0 flex-col border-r border-gray-6 bg-gray-2",
            { "w-60": !collapsed(), "w-12": collapsed() },
          ]}
        >
          <div class="flex h-11 shrink-0 items-center justify-between px-ds-16">
            <Show when={!collapsed()}>
              <h2 class="text-16 font-bold leading-20 text-gray-12">Streams</h2>
            </Show>
            <button
              aria-label={collapsed() ? "Expand sidebar" : "Collapse sidebar"}
              onClick={() => setCollapsed(!collapsed())}
              class="-mx-1 flex size-6 items-center justify-center"
            >
              <svg aria-hidden="true" width="16" height="16" viewBox="0 0 16 16">
                <path
                  d="M2 3h12v10H2zM6 3v10M11 6 9 8l2 2"
                  fill="none"
                  stroke="currentColor"
                  stroke-width="1.2"
                />
              </svg>
            </button>
          </div>
          <Show when={!collapsed()}>
            <p class="px-ds-16 pt-ds-4 pb-ds-24 leading-20">
              Stream labels appear after
              <br />a query has run.
            </p>
            <section class="flex flex-col gap-ds-12 border-t border-gray-6 p-ds-16">
              <h2 class="text-16 font-bold leading-20 text-gray-12">Fields</h2>
              <Show
                when={hasRun()}
                fallback={<p class="leading-20">Run a query to discover fields</p>}
              >
                <For each={[...new Set(lines().flatMap(Object.keys))]}>
                  {(field) => <p class="font-mono">{field}</p>}
                </For>
              </Show>
            </section>
            <p class="mt-auto border-t border-gray-6 px-ds-16 py-ds-12">
              {hasRun() ? `${lines().length} events` : "No query has run"}
            </p>
          </Show>
        </aside>
        <section aria-label="Query results" class="flex min-w-0 flex-1 flex-col">
          <div class="flex h-[88px] shrink-0 flex-col gap-ds-16 border-b border-gray-6 px-ds-16 py-ds-12">
            <h2>Time distribution</h2>
            <p>
              {hasRun()
                ? `${lines().length} events returned`
                : "Run a query to see activity over time"}
            </p>
          </div>
          <div
            class="flex h-9 shrink-0 items-center gap-ds-12 border-b border-gray-6 px-ds-16"
            aria-live="polite"
          >
            <h2 class="font-bold text-gray-12">Results</h2>
            <span>
              {pending() ? "Running…" : hasRun() ? `${lines().length} events` : "Not run"}
            </span>
          </div>
          <Show when={error()}>
            <p
              role="alert"
              class="m-ds-16 rounded-6 border border-status-error-border bg-dark-destructive p-ds-12 text-status-error-text"
            >
              {error()}
            </p>
          </Show>
          <Show
            when={hasRun()}
            fallback={
              <div class="flex min-h-0 flex-1 items-center justify-center overflow-auto bg-query-glow p-ds-24">
                <div class="flex w-full max-w-[640px] flex-col items-start gap-ds-16 pb-ds-60">
                  <h1 class="text-24 font-bold leading-32 text-gray-12">Run a query</h1>
                  <code class="w-full overflow-x-auto whitespace-nowrap rounded-6 border border-gray-6 bg-gray-2 p-ds-12 font-mono text-13 leading-20 text-gray-12">
                    {example}
                  </code>
                  <button
                    onClick={useExample}
                    class="flex h-8 items-center gap-ds-8 rounded-6 border border-chart-bar px-ds-12 text-13 text-dark-ring outline-2 outline-offset-2 outline-dark-ring hover:bg-dark-accent"
                  >
                    Use example <span aria-hidden="true">→</span>
                  </button>
                </div>
              </div>
            }
          >
            <div class="flex-1 overflow-auto" aria-busy={pending() ? "true" : "false"}>
              <For each={lines()} fallback={<p class="p-ds-24">No results</p>}>
                {(line) => (
                  <pre class="border-b border-gray-3 px-ds-16 py-ds-8 font-mono text-13 text-gray-12">
                    {JSON.stringify(line)}
                  </pre>
                )}
              </For>
            </div>
          </Show>
        </section>
      </div>
    </main>
  );
}
