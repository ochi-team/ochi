import type { Component, JSX } from 'solid-js';
import { createMemo, createSignal, For, Show } from 'solid-js';
import { basicFilterSet, hasBasicFilterClause, type QueryFilterOperator } from './QueryInput';

const cx = (...classes: Array<string | false | undefined>) => classes.filter(Boolean).join(' ');

type Level = 'trace' | 'debug' | 'info' | 'warn' | 'error' | 'critical' | 'unknown';
type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };
type LogEntry = Record<string, JsonValue>;
export type LinesViewType = 'message' | 'json';

type LinesProps = {
    viewType: LinesViewType;
    query: string;
    onAttributeAction: (action: AttributeAction, key: string, value: JsonValue) => void;
};

const timestampKey = '_time';
const msgKey = '_msg';
const levelKeys = ['level', 'lvl', 'l', '@l'];
const lineRowGrid = 'grid-cols-[180px_80px_24px_minmax(0,1fr)] max-[640px]:grid-cols-[180px_58px_24px_minmax(180px,1fr)]';

function getTime(e: LogEntry): string {
    return getString(e[timestampKey]);
}

function getMsg(e: LogEntry): string {
    return getString(e[msgKey]);
}

function toLevel(value: JsonValue | undefined): Level {
    if (typeof value !== 'string') {
        return 'unknown';
    }

    switch (value.toLowerCase()) {
        case 'trace':
            return 'trace';
        case 'debug':
            return 'debug';
        case 'info':
            return 'info';
        case 'warn':
        case 'warning':
            return 'warn';
        case 'error':
        case 'err':
            return 'error';
        case 'fatal':
        case 'crit':
        case 'critical':
            return 'critical'
        default:
            return 'unknown';
    }
}
function getLevel(e: LogEntry): Level {
    for (const key of levelKeys) {
        const level = toLevel(e[key]);
        if (level !== 'unknown') {
            return level;
        }
    }

    return 'unknown';
}

function getString(value: JsonValue | undefined): string {
    if (typeof value === 'string') {
        return value;
    }
    if (typeof value === 'number' || typeof value === 'boolean') {
        return String(value);
    }

    return '';
}

function formatEntry(entry: LogEntry): string {
    return formatJsonValue(entry, 0);
}

function formatAttributeValue(value: JsonValue): string {
    if (value === null) {
        return 'null';
    }

    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
        return String(value);
    }

    return JSON.stringify(value);
}

function formatJsonValue(value: JsonValue, depth: number): string {
    const indent = '  '.repeat(depth);
    const childIndent = '  '.repeat(depth + 1);

    if (value === null) {
        return 'null';
    }

    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
        return String(value);
    }

    if (Array.isArray(value)) {
        if (value.length === 0) {
            return '[]';
        }

        const lines = value.map((item, index) => {
            const comma = index === value.length - 1 ? '' : ',';
            return `${childIndent}${formatJsonValue(item, depth + 1)}${comma}`;
        });

        return `[\n${lines.join('\n')}\n${indent}]`;
    }

    const entries = Object.entries(value);
    if (entries.length === 0) {
        return '{}';
    }

    const lines = entries.map(([key, item], index) => {
        const comma = index === entries.length - 1 ? '' : ',';
        return `${childIndent}${key}: ${formatJsonValue(item, depth + 1)}${comma}`;
    });

    return `{\n${lines.join('\n')}\n${indent}}`;
}


const logs: LogEntry[] = [
    {
        _time: '2026-06-30T12:42:12.525Z',
        _msg: 'Socket connection closed unexpectedly by remote peer',
        level: 'debug',
        peer: '10.2.41.87',
        transport: 'tcp',
        retries: 2,
    },
    {
        _time: '2026-06-30T12:42:10.918Z',
        _msg: "Optimization: Database index 'idx_user_email' was used for query",
        lvl: 'info',
        table: 'users',
        index: 'idx_user_email',
        duration_ms: 18,
    },
    {
        _time: '2026-06-30T12:42:08.104Z',
        _msg: 'Rate limit approaching for IP 185.22.41.12',
        l: 'WARN',
        client_ip: '185.22.41.12',
        remaining: 9,
        limit: 100,
    },
    {
        _time: '2026-06-30T12:42:05.711Z',
        _msg: "Health check passed for container 'ochi-api-node-2'",
        '@l': 'INFO',
        id: 'ochi-api-node-2',
        region: 'eu-west-1',
        ready: true,
    },
    {
        _time: '2026-06-30T12:42:02.377Z',
        _msg: 'Failed to load resource: the server responded with a status of 404 (Not Found)',
        level: 'error',
        method: 'GET',
        path: '/assets/app.css',
        status: 404,
    },
    {
        _time: '2026-06-30T12:41:59.880Z',
        _msg: 'Slow query detected: SELECT * FROM audit_logs WHERE user_id = 9912 (1.2s)',
        level: 'warning',
        query: 'SELECT * FROM audit_logs WHERE user_id = 9912',
        duration_ms: 1200,
    },
    {
        _time: '2026-06-30T12:41:56.024Z',
        _msg: 'User session established for sid:492...8a',
        sid: '492...8a',
        user_id: 9912,
        mfa: true,
    },
    {
        _time: '2026-06-30T12:41:53.300Z',
        _msg: 'Worker thread 04 released back to pool',
        level: 'debug',
        thread: 4,
        pool: 'query-exec',
        queued_jobs: 0,
    },
];

const levelStyles: Record<Level, { bar: string; legend: string; badge: string; message: string }> = {
    info: {
        bar: 'bg-chart-1',
        legend: 'before:bg-chart-1',
        badge: 'bg-chart-1 text-foreground',
        message: 'text-foreground',
    },
    error: {
        bar: 'bg-chart-3',
        legend: 'before:bg-chart-3',
        badge: 'bg-chart-3 text-foreground',
        message: 'text-destructive-foreground',
    },
    critical: {
        bar: 'bg-chart-3',
        legend: 'before:bg-chart-3',
        badge: 'bg-chart-3 text-foreground',
        message: 'text-destructive-foreground',
    },
    warn: {
        bar: 'bg-chart-4',
        legend: 'before:bg-chart-4',
        badge: 'bg-chart-4 text-foreground',
        message: 'text-foreground',
    },
    debug: {
        bar: 'bg-chart-2',
        legend: 'before:bg-chart-2',
        badge: 'bg-chart-2 text-foreground',
        message: 'text-foreground',
    },
    trace: {
        bar: 'bg-chart-2',
        legend: 'before:bg-chart-2',
        badge: 'bg-chart-2 text-foreground',
        message: 'text-foreground',
    },
    unknown: {
        bar: 'bg-chart-5',
        legend: 'before:bg-chart-5',
        badge: 'bg-chart-5 text-foreground',
        message: 'text-foreground',
    },
};

const OpenSideWindowIcon: Component = () => (
    <svg class="size-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <rect width="18" height="18" x="3" y="3" rx="2" />
        <path d="M9 3v18" />
        <path d="m14 9 3 3-3 3" />
    </svg>
);

const AddCircleIcon: Component = () => (
    <svg class="size-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <circle cx="12" cy="12" r="10" />
        <path d="M8 12h8" />
        <path d="M12 8v8" />
    </svg>
);

const RemoveCircleIcon: Component = () => (
    <svg class="size-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <circle cx="12" cy="12" r="10" />
        <path d="M8 12h8" />
    </svg>
);

type AttributeAction = 'add' | 'remove';

const attributeActionOperator: Record<AttributeAction, QueryFilterOperator> = {
    add: '=',
    remove: '!=',
};

function stopPropagation(event: Event) {
    event.stopPropagation();
}

type AttributeRowsProps = {
    entry: LogEntry;
    isActionDisabled: (action: AttributeAction, key: string, value: JsonValue) => boolean;
    onAttributeAction: (action: AttributeAction, key: string, value: JsonValue) => void;
};

function finishAttributeAction(
    event: MouseEvent,
    action: AttributeAction,
    key: string,
    value: JsonValue,
    onAttributeAction: (action: AttributeAction, key: string, value: JsonValue) => void,
) {
    stopPropagation(event);
    onAttributeAction(action, key, value);
    (event.currentTarget as HTMLElement | null)?.blur();
}

const AttributeRows: Component<AttributeRowsProps> = (props) => (
    <div class="col-span-full border-t border-border/70 bg-muted/35 py-2 pl-[292px] pr-2 max-[640px]:pl-0">
        <div class="flex flex-col gap-1">
            <For each={Object.entries(props.entry)}>
                {([key, value]) => {
                    const addDisabled = () => props.isActionDisabled('add', key, value);
                    const removeDisabled = () => props.isActionDisabled('remove', key, value);

                    return (
                        <div class="group/attr -mx-1 flex min-h-7 items-center gap-3 rounded-[2px] px-1 font-mono text-[12px] hover:bg-accent max-[640px]:gap-2">
                            <span class="w-32 flex-shrink-0 overflow-hidden text-ellipsis whitespace-nowrap text-muted-foreground">{key}</span>
                            <span class="min-w-0 flex-1 overflow-hidden text-ellipsis whitespace-nowrap font-bold text-foreground">{formatAttributeValue(value)}</span>
                            <div class="flex flex-shrink-0 items-center gap-1 opacity-0 transition-opacity group-hover/attr:opacity-100 group-focus-within/attr:opacity-100">
                                <button
                                    type="button"
                                    disabled={addDisabled()}
                                    class={cx(
                                        'grid size-6 place-items-center rounded-[2px] border-0 bg-transparent p-0 text-muted-foreground',
                                        addDisabled()
                                            ? 'cursor-not-allowed opacity-35'
                                            : 'cursor-pointer hover:bg-primary/20 hover:text-primary',
                                    )}
                                    title={addDisabled() ? `${key} is already included` : `Add ${key}`}
                                    aria-label={`Add ${key}`}
                                    onClick={(event) => finishAttributeAction(event, 'add', key, value, props.onAttributeAction)}
                                >
                                    <AddCircleIcon />
                                </button>
                                <button
                                    type="button"
                                    disabled={removeDisabled()}
                                    class={cx(
                                        'grid size-6 place-items-center rounded-[2px] border-0 bg-transparent p-0 text-muted-foreground',
                                        removeDisabled()
                                            ? 'cursor-not-allowed opacity-35'
                                            : 'cursor-pointer hover:bg-destructive/20 hover:text-destructive',
                                    )}
                                    title={removeDisabled() ? `${key} is already excluded` : `Remove ${key}`}
                                    aria-label={`Remove ${key}`}
                                    onClick={(event) => finishAttributeAction(event, 'remove', key, value, props.onAttributeAction)}
                                >
                                    <RemoveCircleIcon />
                                </button>
                            </div>
                        </div>
                    );
                }}
            </For>
        </div>
    </div>
);

const Lines: Component<LinesProps> = (props) => {
    const [expandedRows, setExpandedRows] = createSignal<Set<number>>(new Set());
    const filters = createMemo(() => basicFilterSet(props.query));

    const isActionDisabled = (action: AttributeAction, key: string, value: JsonValue): boolean =>
        hasBasicFilterClause(filters(), key, attributeActionOperator[action], value);

    function isExpanded(index: number): boolean {
        return expandedRows().has(index);
    }

    function toggleExpanded(index: number) {
        setExpandedRows((current) => {
            const next = new Set(current);
            if (next.has(index)) {
                next.delete(index);
            } else {
                next.add(index);
            }
            return next;
        });
    }

    function onRowKeyDown(event: KeyboardEvent, index: number) {
        if (event.key !== 'Enter' && event.key !== ' ') {
            return;
        }

        event.preventDefault();
        toggleExpanded(index);
    }

    return (
        <div class="grid min-h-0 content-start overflow-auto bg-background">
            <For each={logs}>
                {(entry, rowIndex) => {
                    const index = rowIndex();
                    const level = getLevel(entry);
                    const styles = levelStyles[level];
                    const rowProps: JSX.HTMLAttributes<HTMLElement> = {
                        role: 'button',
                        tabIndex: 0,
                        'aria-expanded': isExpanded(index),
                        onClick: () => toggleExpanded(index),
                        onKeyDown: (event) => onRowKeyDown(event, index),
                    };

                    return (
                        <Show
                            when={props.viewType === 'json'}
                            fallback={
                                <article
                                    {...rowProps}
                                    class={cx(
                                        'grid min-h-[32px] cursor-pointer items-center gap-3 border-b border-border px-5 text-[13px] hover:bg-accent hover:[box-shadow:inset_2px_0_0_var(--primary)] focus-visible:[box-shadow:inset_2px_0_0_var(--primary)] max-[640px]:gap-2 max-[640px]:px-2.5',
                                        lineRowGrid,
                                    )}
                                >
                                    <time class="whitespace-nowrap text-muted-foreground">{getTime(entry)}</time>
                                    <span
                                        class={cx(
                                            'ml-2 inline-flex w-fit rounded-[2px] px-1.5 text-[10px] font-extrabold uppercase leading-4',
                                            styles.badge,
                                        )}
                                    >
                                        {level}
                                    </span>
                                    <button
                                        type="button"
                                        class="grid size-6 cursor-pointer place-items-center border-0 bg-transparent p-0 text-muted-foreground hover:text-foreground"
                                        title="Open in side window"
                                        aria-label="Open in side window"
                                        onClick={stopPropagation}
                                    >
                                        <OpenSideWindowIcon />
                                    </button>
                                    <p class={cx('m-0 overflow-hidden text-ellipsis whitespace-nowrap', styles.message)}>
                                        {getMsg(entry)}
                                    </p>
                                    <Show when={isExpanded(index)}>
                                        <AttributeRows
                                            entry={entry}
                                            isActionDisabled={isActionDisabled}
                                            onAttributeAction={props.onAttributeAction}
                                        />
                                    </Show>
                                </article>
                            }
                        >
                            <article
                                {...rowProps}
                                class={cx(
                                    'grid auto-rows-auto cursor-pointer items-start gap-3 border-b border-border px-5 py-2 text-[13px] hover:bg-accent hover:[box-shadow:inset_2px_0_0_var(--primary)] focus-visible:[box-shadow:inset_2px_0_0_var(--primary)] max-[640px]:gap-2 max-[640px]:px-2.5',
                                    lineRowGrid,
                                )}
                            >
                                <time class="whitespace-nowrap pt-0.5 text-muted-foreground">{getTime(entry)}</time>
                                <span
                                    class={cx(
                                        'ml-2 mt-0.5 inline-flex w-fit rounded-[2px] px-1.5 text-[10px] font-extrabold uppercase leading-4',
                                        styles.badge,
                                    )}
                                >
                                    {level}
                                </span>
                                <button
                                    type="button"
                                    class="grid size-6 cursor-pointer place-items-center border-0 bg-transparent p-0 text-muted-foreground hover:text-foreground"
                                    title="Open in side window"
                                    aria-label="Open in side window"
                                    onClick={stopPropagation}
                                >
                                    <OpenSideWindowIcon />
                                </button>
                                <pre class="m-0 block min-h-0 min-w-0 overflow-x-auto overflow-y-visible whitespace-pre font-mono text-[12px] leading-[1.45] text-foreground">
                                    {formatEntry(entry)}
                                </pre>
                                <Show when={isExpanded(index)}>
                                    <AttributeRows
                                        entry={entry}
                                        isActionDisabled={isActionDisabled}
                                        onAttributeAction={props.onAttributeAction}
                                    />
                                </Show>
                            </article>
                        </Show>
                    );
                }}
            </For>
        </div>
    );
};

export default Lines;
