import type { Component, JSX } from 'solid-js';
import { createMemo, createSignal, For, Show } from 'solid-js';
import { basicFilterSet, hasBasicFilterClause, type QueryFilterOperator } from './QueryInput';
import type { JsonValue, LogEntry } from '../client/query';

const cx = (...classes: Array<string | false | undefined>) => classes.filter(Boolean).join(' ');

type Level = 'trace' | 'debug' | 'info' | 'warn' | 'error' | 'critical' | 'unknown';
export type LinesViewType = 'message' | 'json';

type LinesProps = {
    viewType: LinesViewType;
    query: string;
    lines: LogEntry[];
    isLoading: boolean;
    error: string | null;
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

const CopyIcon: Component = () => (
    <svg class="size-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <rect width="14" height="14" x="8" y="8" rx="2" />
        <path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2" />
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

async function copyAttributeValue(event: MouseEvent, value: JsonValue) {
    stopPropagation(event);

    try {
        await navigator.clipboard.writeText(formatAttributeValue(value));
    } finally {
        (event.currentTarget as HTMLElement | null)?.blur();
    }
}

const AttributeRows: Component<AttributeRowsProps> = (props) => (
    <div class="border-t border-border/70 bg-muted/35 py-2 pl-[292px] pr-2 max-[640px]:pl-0">
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
                                <button
                                    type="button"
                                    class="grid size-6 cursor-pointer place-items-center rounded-[2px] border-0 bg-transparent p-0 text-muted-foreground hover:bg-accent hover:text-foreground"
                                    title={`Copy ${key} value`}
                                    aria-label={`Copy ${key} value`}
                                    onClick={(event) => copyAttributeValue(event, value)}
                                >
                                    <CopyIcon />
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
        <div class="flex min-h-0 flex-col overflow-auto bg-background">
            <Show when={props.error}>
                {(error) => (
                    <div class="border-b border-border bg-destructive/10 px-5 py-3 text-[13px] font-bold text-destructive">
                        {error()}
                    </div>
                )}
            </Show>
            <Show when={!props.isLoading && !props.error && props.lines.length === 0}>
                <div class="border-b border-border px-5 py-3 text-[13px] text-muted-foreground">No entries</div>
            </Show>
            <For each={props.lines}>
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
                                <div class="border-b border-border hover:bg-accent hover:[box-shadow:inset_2px_0_0_var(--primary)] focus-within:[box-shadow:inset_2px_0_0_var(--primary)]">
                                    <article
                                        {...rowProps}
                                        class={cx(
                                            'grid min-h-[32px] cursor-pointer items-center gap-3 px-5 text-[13px] max-[640px]:gap-2 max-[640px]:px-2.5',
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
                                            {/* <OpenSideWindowIcon /> */}
                                        </button>
                                        <p class={cx('m-0 overflow-hidden text-ellipsis whitespace-nowrap', styles.message)}>
                                            {getMsg(entry)}
                                        </p>
                                    </article>
                                    <Show when={isExpanded(index)}>
                                        <AttributeRows
                                            entry={entry}
                                            isActionDisabled={isActionDisabled}
                                            onAttributeAction={props.onAttributeAction}
                                        />
                                    </Show>
                                </div>
                            }
                        >
                            <div class="border-b border-border hover:bg-accent hover:[box-shadow:inset_2px_0_0_var(--primary)] focus-within:[box-shadow:inset_2px_0_0_var(--primary)]">
                                <article
                                    {...rowProps}
                                    class={cx(
                                        'grid cursor-pointer items-start gap-3 px-5 py-2 text-[13px] max-[640px]:gap-2 max-[640px]:px-2.5',
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
                                </article>
                                <Show when={isExpanded(index)}>
                                    <AttributeRows
                                        entry={entry}
                                        isActionDisabled={isActionDisabled}
                                        onAttributeAction={props.onAttributeAction}
                                    />
                                </Show>
                            </div>
                        </Show>
                    );
                }}
            </For>
        </div>
    );
};

export default Lines;
