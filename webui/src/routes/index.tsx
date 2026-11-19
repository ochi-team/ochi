// import { Title } from "@solidjs/meta";
// import { createSignal, For, onCleanup, Show } from "solid-js";
// import { buildLoqlQuery, queryLogs, type LogEntry } from "../client/query";
// import Query from "../components/widgets/Query";
//
// const example = "[-15m, now] {service=api OR service=worker}";
//
// export default function Home() {
//   const [query, setQuery] = createSignal("");
//   const [range, setRange] = createSignal("");
//   const [pending, setPending] = createSignal(false);
//   const [hasRun, setHasRun] = createSignal(false);
//   const [error, setError] = createSignal("");
//   const [lines, setLines] = createSignal<LogEntry[]>([]);
//   const [collapsed, setCollapsed] = createSignal(false);
//
//   let controller: AbortController | undefined;
//
//   function useExample() {
//     setQuery(example);
//     setRange("-15m");
//   }
//
//   async function run() {
//     const value = query();
//     if (!value || pending()) return;
//     const q = value.startsWith("[") ? value : buildLoqlQuery(range() || "-15m", value);
//     controller = new AbortController();
//     setPending(true);
//     setError("");
//     try {
//       const response = await queryLogs({ query: q, signal: controller.signal });
//       setLines(response.lines);
//       setHasRun(true);
//       return q;
//     } catch (err) {
//       setError(err instanceof Error ? err.message : String(err));
//     } finally {
//       setPending(false);
//     }
//   }
//
//   onCleanup(() => controller?.abort());
//
//   return (
//     <main class="flex h-dvh min-h-[540px] flex-col overflow-hidden bg-bg font-sans text-12 leading-16 tracking-[-0.01em] text-gray-11">
//       <Title>Ochi · Query</Title>
//       <header class="flex h-12 shrink-0 items-center gap-ds-24 border-b border-gray-6 bg-gray-2 px-ds-16">
//         <img src="/ochi.svg" alt="Ochi" class="h-[47px] w-[104px] object-contain" />
//         <span class="ml-auto font-mono text-gray-12">{import.meta.env.OCHI_HOST}</span>
//         <a
//           href="https://ochi.dev/docs/reference/loql/"
//           target="_blank"
//           rel="noreferrer"
//           class="flex items-center gap-ds-6 text-16 hover:text-gray-12"
//         >
//           Docs{" "}
//           <span aria-hidden="true" class="text-14">
//             ↗
//           </span>
//         </a>
//       </header>
//
//       <Query
//         query={query()}
//         setQuery={setQuery}
//         range={range()}
//         setRange={setRange}
//         onRun={run}
//       />
//
//       <div class="flex min-h-0 flex-1">
//         <aside
//           class={[
//             "flex shrink-0 flex-col border-r border-gray-6 bg-gray-2",
//             { "w-60": !collapsed(), "w-12": collapsed() },
//           ]}
//         >
//           <div class="flex h-11 shrink-0 items-center justify-between px-ds-16">
//             <Show when={!collapsed()}>
//               <h2 class="text-16 leading-20 font-bold text-gray-12">Streams</h2>
//             </Show>
//             <button
//               aria-label={collapsed() ? "Expand sidebar" : "Collapse sidebar"}
//               onClick={() => setCollapsed(!collapsed())}
//               class="-mx-1 flex size-6 items-center justify-center"
//             >
//               <svg aria-hidden="true" width="16" height="16" viewBox="0 0 16 16">
//                 <path
//                   d="M2 3h12v10H2zM6 3v10M11 6 9 8l2 2"
//                   fill="none"
//                   stroke="currentColor"
//                   stroke-width="1.2"
//                 />
//               </svg>
//             </button>
//           </div>
//           <Show when={!collapsed()}>
//             <p class="px-ds-16 pt-ds-4 pb-ds-24 leading-20">
//               Stream labels appear after
//               <br />a query has run.
//             </p>
//             <section class="flex flex-col gap-ds-12 border-t border-gray-6 p-ds-16">
//               <h2 class="text-16 leading-20 font-bold text-gray-12">Fields</h2>
//               <Show
//                 when={hasRun()}
//                 fallback={<p class="leading-20">Run a query to discover fields</p>}
//               >
//                 <For each={[...new Set(lines().flatMap(Object.keys))]}>
//                   {(field) => <p class="font-mono">{field}</p>}
//                 </For>
//               </Show>
//             </section>
//             <p class="mt-auto border-t border-gray-6 px-ds-16 py-ds-12">
//               {hasRun() ? `${lines().length} events` : "No query has run"}
//             </p>
//           </Show>
//         </aside>
//         <section aria-label="Query results" class="flex min-w-0 flex-1 flex-col">
//           <div class="flex h-[88px] shrink-0 flex-col gap-ds-16 border-b border-gray-6 px-ds-16 py-ds-12">
//             <h2>Time distribution</h2>
//             <p>
//               {hasRun()
//                 ? `${lines().length} events returned`
//                 : "Run a query to see activity over time"}
//             </p>
//           </div>
//           <div
//             class="flex h-9 shrink-0 items-center gap-ds-12 border-b border-gray-6 px-ds-16"
//             aria-live="polite"
//           >
//             <h2 class="font-bold text-gray-12">Results</h2>
//             <span>
//               {pending() ? "Running…" : hasRun() ? `${lines().length} events` : "Not run"}
//             </span>
//           </div>
//           <Show when={error()}>
//             <p
//               role="alert"
//               class="m-ds-16 rounded-6 border border-status-error-border bg-dark-destructive p-ds-12 text-status-error-text"
//             >
//               {error()}
//             </p>
//           </Show>
//           <Show
//             when={hasRun()}
//             fallback={
//               <div class="flex min-h-0 flex-1 items-center justify-center overflow-auto bg-query-glow p-ds-24">
//                 <div class="flex w-full max-w-[640px] flex-col items-start gap-ds-16 pb-ds-60">
//                   <h1 class="text-24 leading-32 font-bold text-gray-12">Run a query</h1>
//                   <code class="w-full overflow-x-auto rounded-6 border border-gray-6 bg-gray-2 p-ds-12 font-mono text-13 leading-20 whitespace-nowrap text-gray-12">
//                     {example}
//                   </code>
//                   <button
//                     onClick={useExample}
//                     class="flex h-8 items-center gap-ds-8 rounded-6 border border-chart-bar px-ds-12 text-13 text-dark-ring outline-2 outline-offset-2 outline-dark-ring hover:bg-dark-accent"
//                   >
//                     Use example <span aria-hidden="true">→</span>
//                   </button>
//                 </div>
//               </div>
//             }
//           >
//             <div class="flex-1 overflow-auto" aria-busy={pending() ? "true" : "false"}>
//               <For each={lines()} fallback={<p class="p-ds-24">No results</p>}>
//                 {(line) => (
//                   <pre class="border-b border-gray-3 px-ds-16 py-ds-8 font-mono text-13 text-gray-12">
//                     {JSON.stringify(line)}
//                   </pre>
//                 )}
//               </For>
//             </div>
//           </Show>
//         </section>
//       </div>
//     </main>
//   );
// }
import { Title } from "@solidjs/meta";
import { createSignal, onCleanup } from "solid-js";
import { buildLoqlQuery, queryLogs, type LogEntry } from "../client/query";
import Query from "../components/widgets/Query";

export default function Home() {
  const [query, setQuery] = createSignal("");
  const [range, setRange] = createSignal("");

  // sets query and time range
  function setQueryExtractingRange(query: string) {
    setQuery(query);

    if (!query.startsWith("[")) return;
    const range = /^\[\s*(-\d+[a-z]+)\s*,\s*now\s*\]/.exec(query)?.[1] ?? "";
    console.log(range);
    setRange(range);
  }

  function setRangeSubstitutingQuery(value: string) {
    setRange(value);
    setQuery(query().replace(/^\[[^\]]*\]\s*/, `[${value}, now] `));
  }

  let controller: AbortController | undefined;

  async function runQuery() {
    const value = query();
    const q = value.startsWith("[") ? value : buildLoqlQuery(range() || "-15m", value);
    controller = new AbortController();
    try {
      const response = await queryLogs({ query: q, signal: controller.signal });
      return q;
    } catch (err) {}
  }

  onCleanup(() => controller?.abort());

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

      <Query
        query={query()}
        setQuery={setQueryExtractingRange}
        range={range()}
        setRange={setRangeSubstitutingQuery}
        runQuery={runQuery}
      />
    </main>
  );
}
