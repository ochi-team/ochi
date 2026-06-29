import type { Component } from 'solid-js';
import { For } from 'solid-js';

type Level = 'info' | 'debug' | 'warn' | 'error';

type EventBucket = {
    error: number;
    info: number;
    warn: number;
    debug: number;
};

type LogEntry = {
    time: string;
    level: Level;
    message: string;
};

const eventBuckets: EventBucket[] = [
    { error: 4, info: 58, warn: 4, debug: 2 },
    { error: 18, info: 32, warn: 8, debug: 3 },
    { error: 5, info: 62, warn: 3, debug: 2 },
    { error: 0, info: 77, warn: 3, debug: 1 },
    { error: 13, info: 44, warn: 7, debug: 4 },
    { error: 5, info: 60, warn: 5, debug: 3 },
    { error: 0, info: 78, warn: 4, debug: 2 },
    { error: 0, info: 36, warn: 2, debug: 2 },
    { error: 35, info: 0, warn: 5, debug: 4 },
    { error: 10, info: 33, warn: 5, debug: 2 },
    { error: 16, info: 50, warn: 6, debug: 3 },
    { error: 3, info: 58, warn: 3, debug: 2 },
    { error: 0, info: 24, warn: 3, debug: 1 },
    { error: 0, info: 66, warn: 5, debug: 2 },
    { error: 0, info: 31, warn: 3, debug: 2 },
    { error: 0, info: 33, warn: 5, debug: 2 },
    { error: 0, info: 0, warn: 0, debug: 0 },
    { error: 0, info: 78, warn: 5, debug: 2 },
    { error: 0, info: 48, warn: 12, debug: 3 },
    { error: 0, info: 64, warn: 7, debug: 4 },
    { error: 0, info: 71, warn: 10, debug: 4 },
    { error: 29, info: 34, warn: 7, debug: 5 },
    { error: 0, info: 39, warn: 2, debug: 2 },
    { error: 0, info: 32, warn: 2, debug: 2 },
    { error: 0, info: 39, warn: 3, debug: 2 },
    { error: 0, info: 83, warn: 7, debug: 3 },
    { error: 0, info: 41, warn: 4, debug: 3 },
    { error: 0, info: 71, warn: 4, debug: 4 },
    { error: 0, info: 78, warn: 5, debug: 2 },
    { error: 28, info: 40, warn: 5, debug: 2 },
    { error: 0, info: 76, warn: 7, debug: 2 },
    { error: 0, info: 47, warn: 4, debug: 2 },
    { error: 0, info: 0, warn: 0, debug: 0 },
    { error: 0, info: 14, warn: 2, debug: 1 },
    { error: 0, info: 18, warn: 2, debug: 1 },
    { error: 0, info: 0, warn: 0, debug: 0 },
    { error: 0, info: 77, warn: 5, debug: 3 },
    { error: 0, info: 26, warn: 3, debug: 1 },
    { error: 0, info: 74, warn: 5, debug: 2 },
    { error: 0, info: 31, warn: 3, debug: 1 },
    { error: 0, info: 67, warn: 6, debug: 3 },
    { error: 12, info: 35, warn: 3, debug: 2 },
    { error: 0, info: 20, warn: 2, debug: 1 },
    { error: 0, info: 27, warn: 3, debug: 1 },
    { error: 0, info: 81, warn: 5, debug: 2 },
    { error: 0, info: 25, warn: 3, debug: 2 },
    { error: 11, info: 31, warn: 7, debug: 2 },
    { error: 0, info: 55, warn: 5, debug: 2 },
    { error: 0, info: 61, warn: 4, debug: 2 },
    { error: 14, info: 33, warn: 5, debug: 2 },
    { error: 0, info: 71, warn: 5, debug: 2 },
    { error: 0, info: 53, warn: 4, debug: 2 },
    { error: 10, info: 64, warn: 6, debug: 4 },
    { error: 35, info: 33, warn: 4, debug: 1 },
    { error: 0, info: 75, warn: 6, debug: 2 },
    { error: 0, info: 0, warn: 0, debug: 0 },
    { error: 13, info: 26, warn: 4, debug: 2 },
    { error: 0, info: 30, warn: 4, debug: 2 },
    { error: 0, info: 20, warn: 4, debug: 1 },
    { error: 0, info: 18, warn: 3, debug: 1 },
];

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

const sideItems = [
    { icon: 'dns', label: 'Infrastructure', active: true },
    { icon: 'lan', label: 'Network', active: false },
    { icon: 'security', label: 'Security', active: false },
    { icon: 'terminal', label: 'Live Tail', active: false },
    { icon: 'alt_route', label: 'Pipelines', active: false },
];

const levelLabels: Level[] = ['info', 'error', 'warn', 'debug'];

const cx = (...classes: Array<string | false | undefined>) => classes.filter(Boolean).join(' ');

const CloseIcon: Component = () => (
    <svg class="size-3" viewBox="0 0 12 12" aria-hidden="true" focusable="false">
        <path
            d="M2.25 2.25 9.75 9.75M9.75 2.25 2.25 9.75"
            fill="none"
            stroke="currentColor"
            stroke-linecap="round"
            stroke-width="1.8"
        />
    </svg>
);

const borderStrong = 'border-[rgba(255,255,255,0.72)]';
const iconFont = 'font-["Material_Symbols_Outlined",var(--font-mono)] overflow-hidden whitespace-nowrap';
const toolbarIconButton = cx(
    iconFont,
    'grid size-8 cursor-pointer place-items-center border bg-transparent text-xl text-[#a9b7c5]',
    borderStrong,
    'hover:border-[#4be277] hover:bg-[#4be277]/[0.08] hover:text-[#dbe2f8]',
);
const sidebarButton = cx(
    'flex min-h-9 cursor-pointer items-center gap-2.5 border border-transparent bg-transparent px-4 text-left text-[13px] font-bold text-[#a9b7c5]',
    'hover:border-[#4be277] hover:bg-[#4be277]/[0.08] hover:text-[#dbe2f8] max-[640px]:whitespace-nowrap',
);
const titleClass = 'm-0 text-[13px] font-extrabold uppercase leading-[1.2] tracking-[0.08em] text-[#a9b7c5]';
const levelStyles: Record<Level, { bar: string; legend: string; badge: string; message: string }> = {
    info: {
        bar: 'bg-[#4be277]',
        legend: 'before:bg-[#4be277]',
        badge: 'bg-[#4be277]/[0.12] text-[#4be277]',
        message: 'text-[#dbe2f8]',
    },
    error: {
        bar: 'bg-[#ef4444]',
        legend: 'before:bg-[#ef4444]',
        badge: 'bg-[#4a0d0d] text-[#ffb4ab]',
        message: 'text-[#ef4444]',
    },
    warn: {
        bar: 'bg-[#ddb7ff]',
        legend: 'before:bg-[#ddb7ff]',
        badge: 'bg-[#ddb7ff]/[0.14] text-[#ddb7ff]',
        message: 'text-[#dbe2f8]',
    },
    debug: {
        bar: 'bg-[#b0cadf]',
        legend: 'before:bg-[#b0cadf]',
        badge: 'bg-white/5 text-[#8fa3ab]',
        message: 'text-[#dbe2f8]',
    },
};

const LogView: Component = () => {
    return (
        <main class="min-h-screen overflow-hidden bg-[#0b1323] font-sans tracking-normal text-[#dbe2f8]">
            <header
                class={cx(
                    'sticky top-0 z-[5] grid min-h-14 grid-cols-[auto_minmax(0,1fr)_auto] items-center gap-8 border-b bg-[#0b1323] px-4',
                    borderStrong,
                    'max-[900px]:grid-cols-[1fr_auto] max-[900px]:gap-4 max-[900px]:min-h-0 max-[640px]:px-3',
                )}
            >
                <div class='font-["Hanken_Grotesk",var(--font-sans)] text-[22px] font-extrabold text-[#4be277]'>
                    <strong>Ochi</strong>
                </div>

                <nav
                    class={cx(
                        'flex min-w-0 items-center gap-0.5 overflow-x-auto',
                        'max-[900px]:order-3 max-[900px]:col-span-full max-[900px]:-mx-4 max-[900px]:border-t max-[900px]:border-white/20 max-[900px]:px-4',
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
                    <section
                        class={cx('border-b bg-[#0b1628] px-5 pb-3.5 pt-[18px] max-[640px]:px-3', borderStrong)}
                        aria-label="Event Distribution Last 15 minutes"
                    >
                        <div class="mb-[18px] flex items-start justify-between gap-4 max-[640px]:flex-col">
                            <h2 class={titleClass}>Event Distribution (Last 15m)</h2>
                            <div class="flex flex-wrap items-center gap-3.5 text-[10px] uppercase text-[#a9b7c5]">
                                <For each={levelLabels}>
                                    {(level) => (
                                        <span
                                            class={cx(
                                                'before:mr-[5px] before:inline-block before:size-2 before:content-[""]',
                                                levelStyles[level].legend,
                                            )}
                                        >
                                            {level}
                                        </span>
                                    )}
                                </For>
                            </div>
                        </div>
                        <div class="grid h-[106px] grid-cols-[repeat(60,minmax(5px,1fr))] items-end gap-[3px] p-0 max-[900px]:grid-cols-[repeat(30,minmax(6px,1fr))] max-[640px]:grid-cols-[repeat(20,minmax(7px,1fr))] max-[640px]:overflow-x-auto">
                            <For each={eventBuckets}>
                                {(bucket) => (
                                    <div
                                        class="flex h-24 flex-col items-end justify-end overflow-hidden"
                                        aria-label={`INFO: ${bucket.info}% ERROR: ${bucket.error}% WARN: ${bucket.warn}% DEBUG: ${bucket.debug}%`}
                                    >
                                        <span
                                            class={cx('min-h-[3px] w-full', levelStyles.debug.bar)}
                                            style={{ height: `${bucket.debug}%` }}
                                            title={`DEBUG: ${bucket.debug}%`}
                                        />
                                        <span
                                            class={cx('min-h-[3px] w-full', levelStyles.warn.bar)}
                                            style={{ height: `${bucket.warn}%` }}
                                            title={`WARN: ${bucket.warn}%`}
                                        />
                                        <span
                                            class={cx('min-h-[3px] w-full', levelStyles.error.bar)}
                                            style={{ height: `${bucket.error}%` }}
                                            title={`ERROR: ${bucket.error}%`}
                                        />
                                        <span
                                            class={cx('min-h-[3px] w-full', levelStyles.info.bar)}
                                            style={{ height: `${bucket.info}%` }}
                                            title={`INFO: ${bucket.info}%`}
                                        />
                                    </div>
                                )}
                            </For>
                        </div>
                        <div class="mt-1.5 flex items-center justify-between gap-3 text-xs text-[#a9b7c5] max-[640px]:flex-col max-[640px]:items-start">
                            <time>2026-06-28T20:18:36.915Z</time>
                            <time>2026-06-28T20:33:36.915Z</time>
                        </div>
                    </section>

                    <section class={cx('border-b bg-[#131c2b] px-5 py-[18px] max-[640px]:px-3', borderStrong)} aria-label="Log query controls">
                        <div class="flex items-center gap-2 max-[640px]:flex-col max-[640px]:items-stretch">
                            <div class="flex min-h-12 min-w-60 flex-auto items-center gap-2 border border-white/10 bg-[#0b1323]/45 px-2.5 max-[640px]:flex-wrap max-[640px]:py-2">
                                <span class={cx(iconFont, 'w-[22px] text-xl text-[#8fa3ab]')}>search</span>
                                <span class="inline-flex min-h-[26px] items-center gap-1.5 border border-[#4be277]/[0.35] bg-[#0d4a2a]/[0.42] px-1.5 text-[13px] text-[#4be277]">
                                    service: <strong>api-gateway</strong>
                                    <button
                                        class="grid size-[18px] cursor-pointer place-items-center border-0 bg-transparent p-0 text-current"
                                        aria-label="Remove service filter"
                                    >
                                        <CloseIcon />
                                    </button>
                                </span>
                                <span class="inline-flex min-h-[26px] items-center gap-1.5 border border-[#ddb7ff]/[0.35] bg-[#7206c1]/[0.24] px-1.5 text-[13px] text-[#ddb7ff]">
                                    env: <strong>prod</strong>
                                    <button
                                        class="grid size-[18px] cursor-pointer place-items-center border-0 bg-transparent p-0 text-current"
                                        aria-label="Remove environment filter"
                                    >
                                        <CloseIcon />
                                    </button>
                                </span>
                                <button class="min-h-9 cursor-pointer border-0 bg-transparent px-3 text-[#8fa3ab] hover:border-[#4be277] hover:bg-[#4be277]/[0.08] hover:text-[#dbe2f8]">
                                    Add filter...
                                </button>
                            </div>
                            <button class="min-h-12 cursor-pointer border border-[#4be277] bg-[#4be277] px-[18px] font-extrabold text-[#003915]">
                                Run Query
                            </button>
                        </div>
                        <div class="mt-3 flex items-center justify-between gap-3 max-[640px]:flex-col max-[640px]:items-start">
                            <button
                                class={cx(
                                    'inline-flex min-h-9 cursor-pointer items-center gap-2 border bg-transparent px-3 text-[#dbe2f8]',
                                    borderStrong,
                                    'hover:border-[#4be277] hover:bg-[#4be277]/[0.08] hover:text-[#dbe2f8]',
                                )}
                            >
                                <span class={cx(iconFont, 'w-5 text-lg')}>schedule</span>
                                Last 15 minutes
                                <span class={cx(iconFont, 'w-5 text-lg')}>expand_more</span>
                            </button>
                            <p class="m-0 mr-auto text-xs text-[#8fa3ab]">Showing <strong class="text-[#dbe2f8]">1,248</strong> entries</p>
                        </div>
                    </section>

                    <section class="flex min-h-0 flex-auto flex-col overflow-hidden" aria-label="Log stream">
                        <div
                            class={cx(
                                'flex items-center justify-between gap-3 border-b bg-[#131c2b] px-5 py-2.5 max-[640px]:flex-col max-[640px]:items-start max-[640px]:px-3',
                                borderStrong,
                            )}
                        >
                            <div class="flex items-center gap-7 max-[640px]:flex-col max-[640px]:items-start">
                                <h2 class={titleClass}>Log Stream</h2>
                                <div class="flex gap-0 rounded-[2px] bg-[#2d3546] p-0.5" role="tablist" aria-label="Log display format">
                                    <button
                                        class="min-h-7 min-w-[78px] cursor-pointer border-0 bg-[#22c55e] px-3 text-[11px] font-extrabold text-[#004b1e]"
                                        role="tab"
                                        aria-selected="true"
                                    >
                                        MESSAGE
                                    </button>
                                    <button
                                        class="min-h-7 min-w-[78px] cursor-pointer border-0 bg-transparent px-3 text-[11px] font-extrabold text-[#8fa3ab]"
                                        role="tab"
                                        aria-selected="false"
                                    >
                                        JSON
                                    </button>
                                </div>
                            </div>
                        </div>

                        <div class="grid min-h-0 content-start overflow-auto bg-[#0b1323]">
                            <For each={logEntries}>
                                {(entry) => (
                                    <article
                                        class="grid min-h-[46px] grid-cols-[180px_80px_minmax(260px,1fr)] items-center gap-3 border-b border-[rgba(255,255,255,0.72)] px-5 text-[13px] hover:bg-white/[0.02] hover:[box-shadow:inset_2px_0_0_#22c55e] max-[640px]:grid-cols-[126px_58px_minmax(220px,1fr)] max-[640px]:px-2.5"
                                    >
                                        <time class="whitespace-nowrap text-[#8fa3ab]">{entry.time}</time>
                                        <span
                                            class={cx(
                                                'inline-flex w-fit rounded-[2px] px-1.5 text-[10px] font-extrabold uppercase leading-4',
                                                levelStyles[entry.level].badge,
                                            )}
                                        >
                                            {entry.level}
                                        </span>
                                        <p class={cx('m-0 overflow-hidden text-ellipsis whitespace-nowrap', levelStyles[entry.level].message)}>
                                            {entry.message}
                                        </p>
                                    </article>
                                )}
                            </For>
                        </div>
                    </section>
                </section>
            </div>
        </main>
    );
};

export default LogView;
