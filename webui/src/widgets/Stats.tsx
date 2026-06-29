import type { Component } from 'solid-js';
import { For } from 'solid-js';

const cx = (...classes: Array<string | false | undefined>) => classes.filter(Boolean).join(' ');
type Level = 'info' | 'debug' | 'warn' | 'error';

const borderStrong = 'border-border';
const titleClass = 'm-0 text-[13px] font-extrabold uppercase leading-[1.2] tracking-[0.08em] text-muted-foreground';
const levelLabels: Level[] = ['info', 'error', 'warn', 'debug'];
const levelStyles: Record<Level, { bar: string; legend: string; badge: string; message: string }> = {
    info: {
        bar: 'bg-chart-1',
        legend: 'before:bg-chart-1',
        badge: 'bg-chart-1/15 text-chart-1',
        message: 'text-foreground',
    },
    error: {
        bar: 'bg-destructive',
        legend: 'before:bg-destructive',
        badge: 'bg-destructive text-destructive-foreground',
        message: 'text-destructive-foreground',
    },
    warn: {
        bar: 'bg-chart-4',
        legend: 'before:bg-chart-4',
        badge: 'bg-chart-4/15 text-chart-4',
        message: 'text-foreground',
    },
    debug: {
        bar: 'bg-chart-2',
        legend: 'before:bg-chart-2',
        badge: 'bg-muted text-muted-foreground',
        message: 'text-foreground',
    },
};

type EventBucket = {
    error: number;
    info: number;
    warn: number;
    debug: number;
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

const Stats: Component = () => {
    return (

        <section
            class={cx('border-b bg-card px-5 pb-3.5 pt-[18px] max-[640px]:px-3', borderStrong)}
            aria-label="Event Distribution Last 15 minutes"
        >
            <div class="mb-[18px] flex items-start justify-between gap-4 max-[640px]:flex-col">
                <h2 class={titleClass}>Event Distribution (Last 15m)</h2>
                <div class="flex flex-wrap items-center gap-3.5 text-[10px] uppercase text-muted-foreground">
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
            <div class="mt-1.5 flex items-center justify-between gap-3 text-xs text-muted-foreground max-[640px]:flex-col max-[640px]:items-start">
                <time>2026-06-28T20:18:36.915Z</time>
                <time>2026-06-28T20:33:36.915Z</time>
            </div>
        </section>
    );
}

export default Stats;
