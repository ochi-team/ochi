import type { Component } from 'solid-js';

import Lines from "../widgets/Lines";
import Stats from "../widgets/Stats";

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

const borderStrong = 'border-border';
const iconFont = 'font-["Material_Symbols_Outlined",var(--font-mono)] overflow-hidden whitespace-nowrap';
const titleClass = 'm-0 text-[13px] font-extrabold uppercase leading-[1.2] tracking-[0.08em] text-muted-foreground';

const LogView: Component = () => {
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

                    <Stats />
                    <section class={cx('border-b bg-popover px-5 py-[18px] max-[640px]:px-3', borderStrong)} aria-label="Log query controls">
                        <div class="flex items-center gap-2 max-[640px]:flex-col max-[640px]:items-stretch">
                            <div class="flex min-h-12 min-w-60 flex-auto items-center gap-2 border border-input bg-input px-2.5 max-[640px]:flex-wrap max-[640px]:py-2">
                                <span class={cx(iconFont, 'w-[22px] text-xl text-muted-foreground')}>search</span>
                                <span class="inline-flex min-h-[26px] items-center gap-1.5 border border-primary/35 bg-primary/15 px-1.5 text-[13px] text-primary">
                                    service: <strong>api-gateway</strong>
                                    <button
                                        class="grid size-[18px] cursor-pointer place-items-center border-0 bg-transparent p-0 text-current"
                                        aria-label="Remove service filter"
                                    >
                                        <CloseIcon />
                                    </button>
                                </span>
                                <span class="inline-flex min-h-[26px] items-center gap-1.5 border border-chart-3/35 bg-chart-3/15 px-1.5 text-[13px] text-chart-3">
                                    env: <strong>prod</strong>
                                    <button
                                        class="grid size-[18px] cursor-pointer place-items-center border-0 bg-transparent p-0 text-current"
                                        aria-label="Remove environment filter"
                                    >
                                        <CloseIcon />
                                    </button>
                                </span>
                                <button class="min-h-9 cursor-pointer border-0 bg-transparent px-3 text-muted-foreground hover:border-primary hover:bg-primary/10 hover:text-foreground">
                                    Add filter...
                                </button>
                            </div>
                            <button class="min-h-12 cursor-pointer border border-primary bg-primary px-[18px] font-extrabold text-primary-foreground">
                                Run Query
                            </button>
                        </div>
                        <div class="mt-3 flex items-center justify-between gap-3 max-[640px]:flex-col max-[640px]:items-start">
                            <button
                                class={cx(
                                    'inline-flex min-h-9 cursor-pointer items-center gap-2 border bg-transparent px-3 text-foreground',
                                    borderStrong,
                                    'hover:border-primary hover:bg-primary/10 hover:text-foreground',
                                )}
                            >
                                <span class={cx(iconFont, 'w-5 text-lg')}>schedule</span>
                                Last 15 minutes
                                <span class={cx(iconFont, 'w-5 text-lg')}>expand_more</span>
                            </button>
                            <p class="m-0 mr-auto text-xs text-muted-foreground">Showing <strong class="text-foreground">1,248</strong> entries</p>
                        </div>
                    </section>

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
                                        class="min-h-7 min-w-[78px] cursor-pointer border-0 bg-primary px-3 text-[11px] font-extrabold text-primary-foreground"
                                        role="tab"
                                        aria-selected="true"
                                    >
                                        MESSAGE
                                    </button>
                                    <button
                                        class="min-h-7 min-w-[78px] cursor-pointer border-0 bg-transparent px-3 text-[11px] font-extrabold text-muted-foreground"
                                        role="tab"
                                        aria-selected="false"
                                    >
                                        JSON
                                    </button>
                                </div>
                            </div>
                        </div>
                        <Lines />
                    </section>
                </section>
            </div>
        </main>
    );
};

export default LogView;
