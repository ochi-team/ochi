import { action, createOptimistic, createSignal, onSettled } from "solid-js";

import { loadQueryHistory, pushQueryHistory } from "@/modules/queryHistory";

import Keycap from "@/components/Keycap";
import QueryHistory from "@/components/widgets/QueryHistory";
import QueryTimePresets from "@/components/widgets/QueryTimePresets";
import QueryEditor from "./QueryEditor";

interface QueryProps {
  range: string;
  setRange: (value: string) => void;

  query: string;
  setQuery: (value: string) => void;

  runQuery: () => Promise<string | undefined>;
}

export default function Query(props: QueryProps) {
  const [pending, setPending] = createOptimistic(false);
  const [history, setHistory] = createSignal<readonly string[]>([]);
  const [input, setInput] = createSignal<HTMLInputElement>();

  const run = action(function* () {
    if (!props.query || pending()) return;
    setPending(true);
    const executedQuery: string | undefined = yield props.runQuery();
    if (executedQuery) setHistory(pushQueryHistory(executedQuery));
  });

  onSettled(() => {
    setHistory(loadQueryHistory());

    const onKey = (event: KeyboardEvent) => {
      // cmd + enter => run query
      if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
        event.preventDefault();
        run();
        // a => edit query
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
        input()?.focus();
      }
    };

    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });

  return (
    <section
      aria-label="Query controls"
      class="flex h-32 shrink-0 flex-col gap-ds-12 border-b border-gray-6 bg-gray-2 px-ds-16 py-ds-12"
    >
      <form
        class="flex h-[52px] items-center gap-ds-12"
        onSubmit={(event) => {
          event.preventDefault();
          return run();
        }}
      >
        <QueryEditor query={props.query} setQuery={props.setQuery} setInputRef={setInput} />
        <button
          type="submit"
          disabled={!props.query.trim() || pending()}
          class="group flex h-9 w-36 shrink-0 items-center justify-center gap-ds-8 rounded-6 border border-gray-6 bg-dark-primary text-dark-primary-foreground enabled:border-dark-primary enabled:hover:brightness-110 disabled:bg-gray-3 disabled:text-gray-11 disabled:opacity-45"
        >
          <svg aria-hidden="true" width="12" height="12" class="shrink-0">
            <use href="/icons.svg#play" />
          </svg>
          <span class="text-14 font-medium">{pending() ? "Running" : "Run"}</span>
          <span class="flex gap-ds-3">
            <Keycap label="⌘" />
            <Keycap label="↵" />
          </span>
        </button>
      </form>
      <div class="flex h-7 items-center gap-ds-8">
        <QueryTimePresets range={props.range} setRange={props.setRange} />
        <QueryHistory history={history()} input={input()} setQuery={props.setQuery} />
      </div>
    </section>
  );
}
