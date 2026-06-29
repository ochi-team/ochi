import type { Component } from 'solid-js';
import { For } from 'solid-js';

const cx = (...classes: Array<string | false | undefined>) => classes.filter(Boolean).join(' ');

type Level = 'debug' | 'info' | 'warn' | 'error';
type LogEntry = {
    time: string;
    level: Level;
    message: string;
};
const logMessages = [
    'Socket connection closed unexpectedly by remote peer',
    'Optimization: Database index \'idx_user_email\' was used for query',
    'Rate limit approaching for IP 185.22.41.12',
    'Health check passed for container \'ochi-api-node-2\'',
    'Failed to load resource: the server responded with a status of 404 (Not Found)',
    'Slow query detected: SELECT * FROM audit_logs WHERE user_id = 9912 (1.2s)',
    'User session established for sid:492...8a',
    'Worker thread 04 released back to pool',
];
const logEntries: LogEntry[] = [
    '14:29:60.43',
    '14:29:59.463',
    '14:29:58.468',
    '14:29:57.467',
    '14:29:56.477',
    '14:29:55.413',
    '14:29:54.458',
    '14:29:53.455',
    '14:29:52.466',
    '14:29:51.477',
    '14:29:50.490',
    '14:29:49.468',
    '14:29:48.414',
    '14:29:47.440',
    '14:29:46.443',
    '14:29:45.471',
    '14:29:44.434',
].map((time, index) => {
    const levels: Level[] = ['error', 'error', 'info', 'warn', 'debug', 'error', 'error', 'warn', 'info', 'debug', 'warn', 'debug'];

    return {
        time: `Oct 24 ${time}`,
        level: levels[index % levels.length],
        message: logMessages[index % logMessages.length],
    };
});

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
};

const OpenSideWindowIcon: Component = () => (
    <svg class="size-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">
        <rect width="18" height="18" x="3" y="3" rx="2" />
        <path d="M9 3v18" />
        <path d="m14 9 3 3-3 3" />
    </svg>
);

const Lines: Component = () => {
    return (
        <div class="grid min-h-0 content-start overflow-auto bg-background">
            <For each={logEntries}>
                {(entry) => (
                    <article
                        class="grid min-h-[46px] grid-cols-[180px_80px_24px_minmax(260px,1fr)] items-center gap-3 border-b border-border px-5 text-[13px] hover:bg-accent hover:[box-shadow:inset_2px_0_0_var(--primary)] max-[640px]:grid-cols-[126px_58px_24px_minmax(180px,1fr)] max-[640px]:gap-2 max-[640px]:px-2.5"
                    >
                        <time class="whitespace-nowrap text-muted-foreground">{entry.time}</time>
                        <span
                            class={cx(
                                'inline-flex w-fit rounded-[2px] px-1.5 text-[10px] font-extrabold uppercase leading-4',
                                levelStyles[entry.level].badge,
                            )}
                        >
                            {entry.level}
                        </span>
                        <button
                            class="grid size-6 cursor-pointer place-items-center border-0 bg-transparent p-0 text-muted-foreground hover:text-foreground"
                            title="Open in side window"
                            aria-label="Open in side window"
                        >
                            <OpenSideWindowIcon />
                        </button>
                        <p class={cx('m-0 overflow-hidden text-ellipsis whitespace-nowrap', levelStyles[entry.level].message)}>
                            {entry.message}
                        </p>
                    </article>
                )}
            </For>
        </div>)

}

export default Lines;
