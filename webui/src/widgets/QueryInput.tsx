import { For, Show, createEffect, createMemo, createSignal } from 'solid-js';
import type { Component, JSX } from 'solid-js';

const bracketPairs: Record<string, string> = {
    '{': '}',
    '[': ']',
    '(': ')',
};

type CompletionKind = 'regular' | 'historic';

type Completion = {
    value: string;
    kind: CompletionKind;
};

export type QueryInputProps = {
    completions?: readonly string[];
    historicCompletions?: readonly string[];
    historicLabel?: string;
};

const maxMatchesPerKind = 8;

const tokenStartChars = new Set([' ', '\t', '\n', '{', '[', '(', ',', '=']);

const currentToken = (value: string, cursor: number) => {
    let start = cursor;

    while (start > 0 && !tokenStartChars.has(value[start - 1])) {
        start -= 1;
    }

    return {
        value: value.slice(start, cursor),
        start,
    };
};

const uniqueMatches = (values: readonly string[], term: string) => {
    const seen = new Set<string>();
    const normalizedTerm = term.toLocaleLowerCase();
    const matches: string[] = [];

    for (const value of values) {
        if (seen.has(value)) {
            continue;
        }

        if (normalizedTerm.length > 0 && !value.toLocaleLowerCase().includes(normalizedTerm)) {
            continue;
        }

        seen.add(value);
        matches.push(value);

        if (matches.length === maxMatchesPerKind) {
            break;
        }
    }

    return matches;
};

const QueryInput: Component<QueryInputProps> = (props) => {
    const [value, setValue] = createSignal('');
    const [cursor, setCursor] = createSignal(0);
    const [focused, setFocused] = createSignal(false);
    const [activeIndex, setActiveIndex] = createSignal(0);

    let inputRef: HTMLInputElement | undefined;

    const activeToken = createMemo(() => currentToken(value(), cursor()));

    const regularMatches = createMemo(() => uniqueMatches(props.completions ?? [], activeToken().value));
    const historicMatches = createMemo(() => uniqueMatches(props.historicCompletions ?? [], value()));

    const matches = createMemo<Completion[]>(() => [
        ...regularMatches().map((completion) => ({ value: completion, kind: 'regular' as const })),
        ...historicMatches().map((completion) => ({ value: completion, kind: 'historic' as const })),
    ]);

    const showMenu = createMemo(() => focused() && matches().length > 0);

    createEffect(() => {
        if (matches().length === 0 || activeIndex() < matches().length) {
            return;
        }

        setActiveIndex(0);
    });

    const updateCursor = () => {
        if (inputRef == null || inputRef.selectionStart == null) {
            return;
        }

        setCursor(inputRef.selectionStart);
    };

    const commitCompletion = (completion: Completion) => {
        if (inputRef == null) {
            return;
        }

        const nextValue = completion.kind === 'historic'
            ? completion.value
            : `${value().slice(0, activeToken().start)}${completion.value}${value().slice(cursor())}`;
        const nextCursor = completion.kind === 'historic'
            ? completion.value.length
            : activeToken().start + completion.value.length;

        setValue(nextValue);
        setCursor(nextCursor);
        setActiveIndex(0);
        inputRef.focus();

        queueMicrotask(() => {
            inputRef?.setSelectionRange(nextCursor, nextCursor);
        });
    };

    return (
        <div class="relative flex min-w-60 flex-auto">
            <input
                placeholder='{region-eu-west-1} level=error'
                class="flex min-h-12 min-w-60 flex-auto items-center gap-2 border border-border bg-input px-2.5 max-[640px]:flex-wrap max-[640px]:py-2"
            >
            </ input>
        </div>
    );
};

export default QueryInput;
