import type { Component } from 'solid-js';
import { For, createSignal } from 'solid-js';
import QueryInput from './QueryInput';

const borderStrong = 'border-border';

const cx = (...classes: Array<string | false | undefined>) => classes.filter(Boolean).join(' ');

const iconFont = 'font-["Material_Symbols_Outlined",var(--font-mono)] overflow-hidden whitespace-nowrap';

const timeRangeOptions = [
    'Last 5 minutes',
    'Last 15 minutes',
    'Last 30 minutes',
    'Last 60 minutes',
    'Last 3 hours',
    'Last 6 hours',
    'Last 12 hours',
    'Last 24 hours',
    'Last 2 days',
    'Last 7 days',
    'Last 30 days',
    'Since 1 Jan 1970',
];
const timeRangeQueryTokens = [
    '5m',
    '15m',
    '30m',
    '60m',
    '3h',
    '6h',
    '12h',
    '24h',
    '2d',
    '7d',
    '30d',
    '1970-01-01T00:00:00.000Z',
];

if (timeRangeOptions.length != timeRangeQueryTokens.length) {
    throw Error('timeRangeOptions.length and timeRangeQueryTokens.length must be equal');
}

type QueryBuilderProps = {
    setTimeRangeQueryToken: (token: string) => void,
};

const QueryBuilder: Component<QueryBuilderProps> = (props) => {
    const [timestampRange, setTimestampRange] = createSignal('Last 15 minutes');

    return (
        <section class={cx('border-b bg-card px-5 py-[18px] max-[640px]:px-3', borderStrong)} aria-label="Log query controls">
            <div class="flex items-center gap-2 max-[640px]:flex-col max-[640px]:items-stretch">
                <QueryInput />
                <button class="min-h-12 cursor-pointer border border-primary bg-primary px-[18px] font-extrabold text-primary-foreground">
                    Run Query
                </button>
            </div>
            <div class="mt-3 flex items-center justify-between gap-3 max-[640px]:flex-col max-[640px]:items-start">
                <label
                    class={cx(
                        'relative inline-flex min-h-9 cursor-pointer items-center gap-2 border bg-transparent pl-3 pr-9 text-foreground',
                        borderStrong,
                        'hover:border-primary hover:bg-primary/10 hover:text-foreground',
                    )}
                >
                    <span class={cx(iconFont, 'w-5 text-lg')}>schedule</span>
                    <span class="sr-only">Timestamp range</span>
                    <select class="h-9 cursor-pointer appearance-none border-0 bg-transparent pr-1 font-inherit text-inherit outline-none"
                        aria-label="Timestamp range"
                        onChange={(event) => {
                            const i = timeRangeOptions.indexOf(event.currentTarget.value)
                            setTimestampRange(event.currentTarget.value);
                            const queryToken = timeRangeQueryTokens[i];
                            props.setTimeRangeQueryToken(queryToken);
                        }}
                        value={timestampRange()}>
                        <For each={timeRangeOptions}>
                            {(option) => <option value={option}>{option}</option>}
                        </For>
                    </select>
                    <span class={cx(iconFont, 'pointer-events-none absolute right-3 top-1/2 w-5 -translate-y-1/2 text-lg')}>expand_more</span>
                </label>
                <p class="m-0 mr-auto text-xs text-muted-foreground">Showing <strong class="text-foreground">1,248</strong> entries</p>
            </div>
        </section>

    )

}

export default QueryBuilder;
