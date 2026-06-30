import type { Component } from 'solid-js';

import Lines from "../widgets/Lines";
import QueryBuilder from "../widgets/QueryBuilder";

const cx = (...classes: Array<string | false | undefined>) => classes.filter(Boolean).join(' ');

const borderStrong = 'border-border';
const titleClass = 'm-0 text-[13px] font-extrabold uppercase leading-[1.2] tracking-[0.08em] text-muted-foreground';

const LogView: Component = () => {
    const setQueryToken = (token: string) => {
        console.info('set time range query token', token);
    }
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

                    <QueryBuilder setTimeRangeQueryToken={setQueryToken} />

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
