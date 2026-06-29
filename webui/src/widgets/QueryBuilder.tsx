import type { Component } from 'solid-js';
import QueryInput from './QueryInput';

const borderStrong = 'border-border';

const cx = (...classes: Array<string | false | undefined>) => classes.filter(Boolean).join(' ');

const iconFont = 'font-["Material_Symbols_Outlined",var(--font-mono)] overflow-hidden whitespace-nowrap';

const CloseIcon: Component = () => (
    <svg class="size-3 text-muted-foreground hover:text-foreground" viewBox="0 0 12 12" aria-hidden="true">
        <path
            d="M2.25 2.25 9.75 9.75M9.75 2.25 2.25 9.75"
            fill="none"
            stroke="currentColor"
            stroke-linecap="round"
            stroke-width="1.8"
        />
    </svg>
);

const QueryBuilder: Component = () => {
    return (
        <section class={cx('border-b bg-card px-5 py-[18px] max-[640px]:px-3', borderStrong)} aria-label="Log query controls">
            <div class="flex items-center gap-2 max-[640px]:flex-col max-[640px]:items-stretch">
                <QueryInput />
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

    )

}

export default QueryBuilder;

