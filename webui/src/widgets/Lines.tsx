import type { Component } from 'solid-js';
import { For } from 'solid-js';

const cx = (...classes: Array<string | false | undefined>) => classes.filter(Boolean).join(' ');

type Level = 'trace' | 'debug' | 'info' | 'warn' | 'error' | 'critical' | 'unknown';
type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };
type LogEntry = Record<string, JsonValue>;

const timestampKey = '_time';
const msgKey = '_msg';
const levelKeys = ['level', 'lvl', 'l', '@l'];

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

const Lines: Component = () => {
    return (
        <div class="grid min-h-0 content-start overflow-auto bg-background">
            <For each={logs}>
                {(entry) => {
                    const level = getLevel(entry);
                    const styles = levelStyles[level];

                    return (
                        <article
                            class="grid min-h-[32px] grid-cols-[180px_80px_24px_minmax(260px,1fr)] items-center gap-3 border-b border-border px-5 text-[13px] hover:bg-accent hover:[box-shadow:inset_2px_0_0_var(--primary)] max-[640px]:grid-cols-[126px_58px_24px_minmax(180px,1fr)] max-[640px]:gap-2 max-[640px]:px-2.5"
                        >
                            <time class="whitespace-nowrap text-muted-foreground">{getTime(entry)}</time>
                            <span
                                class={cx(
                                    'inline-flex w-fit rounded-[2px] px-1.5 text-[10px] font-extrabold uppercase leading-4',
                                    styles.badge,
                                )}
                            >
                                {level}
                            </span>
                            <button
                                class="grid size-6 cursor-pointer place-items-center border-0 bg-transparent p-0 text-muted-foreground hover:text-foreground"
                                title="Open in side window"
                                aria-label="Open in side window"
                            >
                                <OpenSideWindowIcon />
                            </button>
                            <p class={cx('m-0 overflow-hidden text-ellipsis whitespace-nowrap', styles.message)}>
                                {getMsg(entry)}
                            </p>
                        </article>
                    );
                }}
            </For>
        </div>
    );
};

export default Lines;
