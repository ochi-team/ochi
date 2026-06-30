import type { Component } from 'solid-js';
import { createMemo, createSignal, onCleanup, onMount } from 'solid-js';

import Lines, { type LinesViewType } from "../widgets/Lines";
import QueryBuilder from "../widgets/QueryBuilder";
import { appendFilterClause, buildFilterClause } from "../widgets/QueryInput";
import { buildLoqlQuery, queryLogs, type JsonValue, type LogEntry } from "../client/query";
import { loadQueryHistory, pushQueryHistory, saveQueryHistory } from "../stores/queryHistory";

const cx = (...classes: Array<string | false | undefined>) => classes.filter(Boolean).join(' ');

const borderStrong = 'border-border';
const titleClass = 'm-0 text-[13px] font-extrabold uppercase leading-[1.2] tracking-[0.08em] text-muted-foreground';

function isPrimitiveAttributeValue(value: JsonValue): value is string | number | boolean | null {
    return value === null || typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean';
}

const LogView: Component = () => {
    const [linesViewType, setLinesViewType] = createSignal<LinesViewType>('message');
    const [query, setQuery] = createSignal('');
    const [timeRangeQueryToken, setTimeRangeQueryToken] = createSignal('15m');
    const [lines, setLines] = createSignal<LogEntry[]>([]);
    const [historicQueries, setHistoricQueries] = createSignal<string[]>(loadQueryHistory());
    const [isQueryLoading, setIsQueryLoading] = createSignal(false);
    const [queryError, setQueryError] = createSignal<string | null>(null);
    let queryController: AbortController | undefined;

    const setQueryToken = (token: string) => {
        setTimeRangeQueryToken(token);
    }

    const querySuggestionKeys = createMemo(() => {
        const seen = new Set<string>();
        for (const line of lines()) {
            for (const key of Object.keys(line)) {
                seen.add(key);
            }
        }
        return [...seen].sort((left, right) => left.localeCompare(right));
    });

    const querySuggestionValues = createMemo(() => {
        const seen = new Set<string>();
        for (const line of lines()) {
            for (const value of Object.values(line)) {
                if (isPrimitiveAttributeValue(value)) {
                    seen.add(String(value));
                }
            }
        }
        return [...seen].sort((left, right) => left.localeCompare(right));
    });

    const rememberCurrentQuery = () => {
        setHistoricQueries((current) => {
            const next = pushQueryHistory(current, query());
            saveQueryHistory(next);
            return next;
        });
    };

    const runQuery = async () => {
        queryController?.abort();
        rememberCurrentQuery();

        const controller = new AbortController();
        queryController = controller;

        setIsQueryLoading(true);
        setQueryError(null);

        try {
            const response = await queryLogs({
                query: buildLoqlQuery(timeRangeQueryToken(), query()),
                signal: controller.signal,
            });
            setLines(response.lines);
        } catch (err) {
            if (controller.signal.aborted) {
                return;
            }
            setQueryError(err instanceof Error ? err.message : 'Query failed');
        } finally {
            if (queryController === controller) {
                queryController = undefined;
                setIsQueryLoading(false);
            }
        }
    };

    onMount(() => {
        void runQuery();
    });

    onCleanup(() => {
        queryController?.abort();
    });

    const applyAttributeAction = (action: 'add' | 'remove', key: string, value: unknown) => {
        const operator = action === 'add' ? '=' : '!=';
        const clause = buildFilterClause(key, operator, value);
        setQuery((current) => appendFilterClause(current, clause));
    };

    return (
        <main class="min-h-screen overflow-hidden bg-background font-sans tracking-normal text-foreground">
            <header
                class={cx(
                    'sticky top-0 z-[5] grid min-h-14 grid-cols-[auto_minmax(0,1fr)_auto] items-center gap-8 border-b bg-background px-4',
                    borderStrong,
                    'max-[900px]:grid-cols-[1fr_auto] max-[900px]:gap-4 max-[900px]:min-h-0 max-[640px]:px-3',
                )}
            >
                <div class='font-["Hanken_Grotesk",var(--font-sans)] text-[22px] font-extrabold text-primary'>
                    <strong>Ochi</strong>
                </div>

                <nav
                    class={cx(
                        'flex min-w-0 items-center gap-0.5 overflow-x-auto',
                        'max-[900px]:order-3 max-[900px]:col-span-full max-[900px]:-mx-4 max-[900px]:border-t max-[900px]:border-border max-[900px]:px-4',
                    )}
                    aria-label="Primary"
                >
                </nav>

            </header>

            <div class="grid min-h-[calc(100vh-57px)] max-[900px]:min-h-0 max-[900px]:grid-cols-1">
                <section
                    class="flex h-[calc(100vh-57px)] min-w-0 flex-col overflow-hidden max-[900px]:h-auto max-[900px]:min-h-[calc(100vh-153px)]"
                    aria-label="Ochi Logs"
                >

                    <QueryBuilder
                        query={query()}
                        setQuery={setQuery}
                        keys={querySuggestionKeys()}
                        values={querySuggestionValues()}
                        historicQueries={historicQueries()}
                        setTimeRangeQueryToken={setQueryToken}
                        entriesCount={lines().length}
                        isLoading={isQueryLoading()}
                        onRunQuery={runQuery}
                    />

                    <section class="flex min-h-0 flex-auto flex-col overflow-hidden" aria-label="Log stream">
                        <div
                            class={cx(
                                'flex items-center justify-between gap-3 border-b bg-popover px-5 py-2.5 max-[640px]:flex-col max-[640px]:items-start max-[640px]:px-3',
                                borderStrong,
                            )}
                        >
                            <div class="flex items-center gap-7 max-[640px]:flex-col max-[640px]:items-start">
                                <h2 class={titleClass}>Log Stream</h2>
                                <div class="flex gap-0 rounded-[2px] bg-muted p-0.5" role="tablist" aria-label="Log display format">
                                    <button
                                        class={cx(
                                            'min-h-7 min-w-[78px] cursor-pointer border-0 px-3 text-[11px] font-extrabold',
                                            linesViewType() === 'message' ? 'bg-primary text-primary-foreground' : 'bg-transparent text-muted-foreground',
                                        )}
                                        role="tab"
                                        aria-selected={linesViewType() === 'message'}
                                        onClick={() => setLinesViewType('message')}
                                    >
                                        MESSAGE
                                    </button>
                                    <button
                                        class={cx(
                                            'min-h-7 min-w-[78px] cursor-pointer border-0 px-3 text-[11px] font-extrabold',
                                            linesViewType() === 'json' ? 'bg-primary text-primary-foreground' : 'bg-transparent text-muted-foreground',
                                        )}
                                        role="tab"
                                        aria-selected={linesViewType() === 'json'}
                                        onClick={() => setLinesViewType('json')}
                                    >
                                        JSON
                                    </button>
                                </div>
                            </div>
                        </div>

                        <Lines
                            viewType={linesViewType()}
                            query={query()}
                            lines={lines()}
                            error={queryError()}
                            isLoading={isQueryLoading()}
                            onAttributeAction={applyAttributeAction}
                        />
                    </section>
                </section>
            </div>
        </main>
    );
};

export default LogView;
