import type { Component, JSX, Setter } from 'solid-js';
import { For, Show, createMemo, createSignal } from 'solid-js';

type TokenKind =
    | 'space'
    | 'left-square'
    | 'right-square'
    | 'left-curly'
    | 'right-curly'
    | 'left-paren'
    | 'right-paren'
    | 'comma'
    | 'equal'
    | 'not-equal'
    | 'match-regex'
    | 'not-match-regex'
    | 'and'
    | 'or'
    | 'literal'
    | 'pipe'
    | 'comment'
    | 'unknown';

type TokenRole = 'key' | 'value' | 'operation' | 'conjunction' | 'bracket' | 'punctuation' | 'literal' | 'unknown';

type Token = {
    kind: TokenKind;
    text: string;
    start: number;
    end: number;
};

export type QueryFilterOperator = '=' | '!=';

export type QueryFilterClause = {
    key: string;
    operator: QueryFilterOperator;
    value: string;
};

type CompletionKind = 'key' | 'value' | 'history';

type CompletionContext = {
    start: number;
    end: number;
    prefix: string;
} & (
    | {
          kind: 'key';
      }
    | {
          kind: 'value';
          key: string | null;
      }
);

type Completion = {
    kind: CompletionKind;
    label: string;
    insertion: string;
    replaceStart: number;
    replaceEnd: number;
};

type RenderPart = Token & {
    role: TokenRole;
};

type RenderSegment =
    | {
          type: 'token';
          id: string;
          part: RenderPart;
      }
    | {
          type: 'predicate';
          id: string;
          start: number;
          end: number;
          parts: RenderPart[];
      };

const cx = (...classes: Array<string | false | undefined>) => classes.filter(Boolean).join(' ');

const literalChars = new Set(['_', '-', ':', '@', '+', '/', '.']);
const openingToClosing: Record<string, string> = {
    '[': ']',
    '{': '}',
    '(': ')',
    "'": "'",
    '"': '"',
};
const closingToOpening: Record<string, string> = {
    ']': '[',
    '}': '{',
    ')': '(',
    "'": "'",
    '"': '"',
};

const tokenClassByRole: Record<TokenRole, string> = {
    key: 'text-chart-2 font-extrabold',
    value: 'text-chart-1',
    operation: 'text-primary font-extrabold',
    conjunction: 'text-chart-4 font-extrabold uppercase',
    bracket: 'text-muted-foreground',
    punctuation: 'text-muted-foreground',
    literal: 'text-foreground',
    unknown: 'text-destructive',
};

const isLiteralChar = (char: string) => /^[A-Za-z0-9]$/.test(char) || literalChars.has(char);
const isWhitespace = (char: string) => char === ' ' || char === '\t' || char === '\n';
const isOperation = (kind: TokenKind) => kind === 'equal' || kind === 'not-equal' || kind === 'match-regex' || kind === 'not-match-regex';
const isConjunction = (kind: TokenKind) => kind === 'and' || kind === 'or';

const isKeyword = (word: string): TokenKind | null => {
    const lowered = word.toLowerCase();
    if (lowered === 'and') return 'and';
    if (lowered === 'or') return 'or';
    return null;
};

const scanQuery = (query: string): Token[] => {
    const tokens: Token[] = [];
    let index = 0;

    while (index < query.length) {
        const start = index;
        const char = query[index];

        if (isWhitespace(char)) {
            while (index < query.length && isWhitespace(query[index])) index += 1;
            tokens.push({ kind: 'space', text: query.slice(start, index), start, end: index });
            continue;
        }

        if (char === '[' || char === ']' || char === '{' || char === '}' || char === '(' || char === ')' || char === ',') {
            const kindByChar: Record<string, TokenKind> = {
                '[': 'left-square',
                ']': 'right-square',
                '{': 'left-curly',
                '}': 'right-curly',
                '(': 'left-paren',
                ')': 'right-paren',
                ',': 'comma',
            };
            tokens.push({ kind: kindByChar[char], text: char, start, end: start + 1 });
            index += 1;
            continue;
        }

        if (char === '=' || char === '~') {
            tokens.push({ kind: char === '=' ? 'equal' : 'match-regex', text: char, start, end: start + 1 });
            index += 1;
            continue;
        }

        if (char === '!') {
            const next = query[index + 1];
            if (next === '=' || next === '~') {
                tokens.push({
                    kind: next === '=' ? 'not-equal' : 'not-match-regex',
                    text: query.slice(start, start + 2),
                    start,
                    end: start + 2,
                });
                index += 2;
            } else {
                tokens.push({ kind: 'unknown', text: char, start, end: start + 1 });
                index += 1;
            }
            continue;
        }

        if (char === '|') {
            tokens.push({ kind: 'pipe', text: char, start, end: start + 1 });
            index += 1;
            continue;
        }

        if (char === "'" || char === '"') {
            const quote = char;
            let escaped = false;
            index += 1;
            while (index < query.length) {
                const current = query[index];
                index += 1;
                if (current === quote && !escaped) break;
                escaped = current === '\\' && !escaped;
                if (current !== '\\') escaped = false;
            }
            tokens.push({ kind: 'literal', text: query.slice(start, index), start, end: index });
            continue;
        }

        if (isLiteralChar(char)) {
            if (char === '/' && query[index + 1] === '/') {
                index += 2;
                while (index < query.length && query[index] !== '\n') index += 1;
                tokens.push({ kind: 'comment', text: query.slice(start, index), start, end: index });
                continue;
            }

            while (index < query.length && isLiteralChar(query[index])) index += 1;
            const text = query.slice(start, index);
            tokens.push({ kind: isKeyword(text) ?? 'literal', text, start, end: index });
            continue;
        }

        tokens.push({ kind: 'unknown', text: char, start, end: start + 1 });
        index += 1;
    }

    return tokens;
};

const roleForToken = (token: Token): TokenRole => {
    if (isOperation(token.kind)) return 'operation';
    if (isConjunction(token.kind)) return 'conjunction';
    if (
        token.kind === 'left-square' ||
        token.kind === 'right-square' ||
        token.kind === 'left-curly' ||
        token.kind === 'right-curly' ||
        token.kind === 'left-paren' ||
        token.kind === 'right-paren'
    ) {
        return 'bracket';
    }
    if (token.kind === 'comma' || token.kind === 'pipe' || token.kind === 'space' || token.kind === 'comment') return 'punctuation';
    if (token.kind === 'unknown') return 'unknown';
    return 'literal';
};

const asPart = (token: Token, role = roleForToken(token)): RenderPart => ({ ...token, role });

const nextNonSpace = (tokens: Token[], index: number): number => {
    let next = index;
    while (next < tokens.length && tokens[next].kind === 'space') next += 1;
    return next;
};

const renderSegments = (tokens: Token[]): RenderSegment[] => {
    const segments: RenderSegment[] = [];
    let index = 0;

    while (index < tokens.length) {
        const keyIndex = index;
        const opIndex = nextNonSpace(tokens, keyIndex + 1);
        const valueIndex = nextNonSpace(tokens, opIndex + 1);

        if (
            tokens[keyIndex]?.kind === 'literal' &&
            tokens[opIndex] &&
            isOperation(tokens[opIndex].kind) &&
            tokens[valueIndex]?.kind === 'literal'
        ) {
            const parts = tokens.slice(keyIndex, valueIndex + 1).map((token, partIndex) => {
                if (partIndex === 0) return asPart(token, 'key');
                if (token.start === tokens[opIndex].start) return asPart(token, 'operation');
                if (token.start === tokens[valueIndex].start) return asPart(token, 'value');
                return asPart(token);
            });
            segments.push({
                type: 'predicate',
                id: `predicate-${tokens[keyIndex].start}-${tokens[valueIndex].end}`,
                start: tokens[keyIndex].start,
                end: tokens[valueIndex].end,
                parts,
            });
            index = valueIndex + 1;
            continue;
        }

        const token = tokens[index];
        segments.push({
            type: 'token',
            id: `token-${token.start}-${token.end}-${token.kind}`,
            part: asPart(token),
        });
        index += 1;
    }

    return segments;
};

const bracketSequenceIsValid = (query: string): boolean => {
    const expectedClosers: string[] = [];
    let quote: string | null = null;
    let escaped = false;

    for (let index = 0; index < query.length; index += 1) {
        const char = query[index];

        if (quote) {
            if (char === quote && !escaped) quote = null;
            escaped = char === '\\' && !escaped;
            if (char !== '\\') escaped = false;
            continue;
        }

        if (char === "'" || char === '"') {
            quote = char;
            escaped = false;
            continue;
        }

        if (char === '[' || char === '{' || char === '(') {
            expectedClosers.push(openingToClosing[char]);
            continue;
        }

        if (char === ']' || char === '}' || char === ')') {
            if (expectedClosers.pop() !== char) return false;
        }
    }

    return quote === null && expectedClosers.length === 0;
};

const nearestNonSpaceBefore = (tokens: Token[], offset: number): Token | undefined => {
    for (let index = tokens.length - 1; index >= 0; index -= 1) {
        const token = tokens[index];
        if (token.end <= offset && token.kind !== 'space') return token;
    }
    return undefined;
};

const keyBeforeOperation = (tokens: Token[], operation: Token | undefined): string | null => {
    if (!operation) return null;

    const key = nearestNonSpaceBefore(tokens, operation.start);
    return key?.kind === 'literal' ? normalizeQueryLiteral(key.text) : null;
};

const nearestNonSpaceAfter = (tokens: Token[], offset: number): Token | undefined => {
    for (const token of tokens) {
        if (token.start >= offset && token.kind !== 'space') return token;
    }
    return undefined;
};

const includeLeadingWhitespace = (query: string, offset: number): number => {
    let start = offset;
    while (start > 0 && isWhitespace(query[start - 1])) start -= 1;
    return start;
};

const includeTrailingWhitespace = (query: string, offset: number): number => {
    let end = offset;
    while (end < query.length && isWhitespace(query[end])) end += 1;
    return end;
};

const expressionRemovalRange = (query: string, tokens: Token[], start: number, end: number) => {
    let removeStart = start;
    let removeEnd = end;

    let left = removeStart;
    while (left > 0 && isWhitespace(query[left - 1])) left -= 1;

    let right = removeEnd;
    while (right < query.length && isWhitespace(query[right])) right += 1;

    if (query[left - 1] === '(' && query[right] === ')') {
        removeStart = left - 1;
        removeEnd = right + 1;
    }

    const previous = nearestNonSpaceBefore(tokens, removeStart);
    const next = nearestNonSpaceAfter(tokens, removeEnd);

    if (previous && isConjunction(previous.kind)) {
        removeStart = includeLeadingWhitespace(query, previous.start);
    } else if (next && isConjunction(next.kind)) {
        removeEnd = includeTrailingWhitespace(query, next.end);
    }

    return { start: removeStart, end: removeEnd };
};

const textRangeFromSelection = (root: HTMLElement, fallback: number) => {
    const selection = window.getSelection();
    if (!selection || selection.rangeCount === 0) return { start: fallback, end: fallback };

    const range = selection.getRangeAt(0);
    if (!root.contains(range.startContainer) || !root.contains(range.endContainer)) {
        return { start: fallback, end: fallback };
    }

    const beforeStart = document.createRange();
    beforeStart.selectNodeContents(root);
    beforeStart.setEnd(range.startContainer, range.startOffset);

    const beforeEnd = document.createRange();
    beforeEnd.selectNodeContents(root);
    beforeEnd.setEnd(range.endContainer, range.endOffset);

    return {
        start: beforeStart.toString().length,
        end: beforeEnd.toString().length,
    };
};

const restoreCaret = (root: HTMLElement, offset: number) => {
    const range = document.createRange();
    const selection = window.getSelection();
    let consumed = 0;
    let lastText: Text | null = null;
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);

    while (walker.nextNode()) {
        const node = walker.currentNode as Text;
        const length = node.data.length;
        lastText = node;

        if (consumed + length >= offset) {
            range.setStart(node, Math.max(0, offset - consumed));
            range.collapse(true);
            selection?.removeAllRanges();
            selection?.addRange(range);
            return;
        }

        consumed += length;
    }

    if (lastText) {
        range.setStart(lastText, lastText.data.length);
    } else {
        range.setStart(root, 0);
    }
    range.collapse(true);
    selection?.removeAllRanges();
    selection?.addRange(range);
};

const wordRangeAt = (query: string, offset: number) => {
    let start = offset;
    let end = offset;

    while (start > 0 && isLiteralChar(query[start - 1])) start -= 1;
    while (end < query.length && isLiteralChar(query[end])) end += 1;

    return { start, end, prefix: query.slice(start, offset) };
};

const queryLiteralIsSafe = (text: string) => text.length > 0 && isKeyword(text) === null && [...text].every(isLiteralChar);

const quoteQueryLiteral = (text: string) => JSON.stringify(text);

const formatQueryLiteral = (value: unknown): string => {
    const text =
        value === null || typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean'
            ? String(value)
            : JSON.stringify(value);

    return queryLiteralIsSafe(text) ? text : quoteQueryLiteral(text);
};

export const buildFilterClause = (key: string, operator: QueryFilterOperator, value: unknown): string =>
    `${formatQueryLiteral(key)}${operator}${formatQueryLiteral(value)}`;

export const appendFilterClause = (query: string, clause: string): string => {
    const trimmedEnd = query.replace(/\s+$/, '');
    return trimmedEnd ? `${trimmedEnd} ${clause}` : clause;
};

const normalizeQuotedLiteral = (text: string): string => {
    if (text.length < 2) return text;

    const quote = text[0];
    if ((quote !== '"' && quote !== "'") || text[text.length - 1] !== quote) return text;

    if (quote === '"') {
        try {
            return JSON.parse(text);
        } catch {
            return text.slice(1, -1);
        }
    }

    let result = '';
    for (let index = 1; index < text.length - 1; index += 1) {
        const char = text[index];
        if (char === '\\' && index + 1 < text.length - 1) {
            index += 1;
            result += text[index];
        } else {
            result += char;
        }
    }
    return result;
};

const normalizeQueryLiteral = (text: string): string => normalizeQuotedLiteral(text);

const filterKey = (filter: QueryFilterClause): string => `${filter.key}\u0000${filter.operator}\u0000${filter.value}`;

const tokenIsOpeningBracket = (token: Token): boolean =>
    token.kind === 'left-square' || token.kind === 'left-curly' || token.kind === 'left-paren';

const tokenIsClosingBracket = (token: Token): boolean =>
    token.kind === 'right-square' || token.kind === 'right-curly' || token.kind === 'right-paren';

export const basicFilterClauses = (query: string): QueryFilterClause[] => {
    const tokens = scanQuery(query);
    let depth = 0;

    for (const token of tokens) {
        if (tokenIsOpeningBracket(token)) {
            depth += 1;
            continue;
        }

        if (tokenIsClosingBracket(token)) {
            depth = Math.max(0, depth - 1);
            continue;
        }

        if (depth === 0 && token.kind === 'or') {
            return [];
        }
    }

    const clauses: QueryFilterClause[] = [];
    depth = 0;

    for (let index = 0; index < tokens.length; index += 1) {
        const token = tokens[index];

        if (tokenIsOpeningBracket(token)) {
            depth += 1;
            continue;
        }

        if (tokenIsClosingBracket(token)) {
            depth = Math.max(0, depth - 1);
            continue;
        }

        if (depth !== 0 || token.kind !== 'literal') {
            continue;
        }

        const opIndex = nextNonSpace(tokens, index + 1);
        const valueIndex = nextNonSpace(tokens, opIndex + 1);
        const operation = tokens[opIndex];
        const value = tokens[valueIndex];

        if (!operation || !value || value.kind !== 'literal') {
            continue;
        }

        if (operation.kind === 'equal' || operation.kind === 'not-equal') {
            clauses.push({
                key: normalizeQueryLiteral(token.text),
                operator: operation.kind === 'equal' ? '=' : '!=',
                value: normalizeQueryLiteral(value.text),
            });
            index = valueIndex;
        }
    }

    return clauses;
};

export const basicFilterSet = (query: string): Set<string> => new Set(basicFilterClauses(query).map(filterKey));

export const hasBasicFilterClause = (
    filters: Set<string>,
    key: string,
    operator: QueryFilterOperator,
    value: unknown,
): boolean => filters.has(filterKey({ key, operator, value: normalizeQueryLiteral(formatQueryLiteral(value)) }));

const CloseIcon: Component = () => (
    <svg class="size-3.5" viewBox="0 0 12 12" aria-hidden="true">
        <path
            d="M2.25 2.25 9.75 9.75M9.75 2.25 2.25 9.75"
            fill="none"
            stroke="currentColor"
            stroke-linecap="round"
            stroke-width="1.8"
        />
    </svg>
);

type QueryInputProps = {
    query: string;
    setQuery: Setter<string>;
    keys: string[];
    values: ReadonlyMap<string, readonly string[]>;
    historicQueries: string[];
};

const QueryInput: Component<QueryInputProps> = (props) => {
    let editorRef: HTMLDivElement | undefined;
    const [caret, setCaret] = createSignal(0);
    const [completionContext, setCompletionContext] = createSignal<CompletionContext | null>(null);
    const [historyCompletionOpen, setHistoryCompletionOpen] = createSignal(false);
    const [activeCompletion, setActiveCompletion] = createSignal(0);

    const query = () => props.query;
    const setQuery = (nextQuery: string) => props.setQuery(nextQuery);

    const tokens = createMemo(() => scanQuery(query()));
    const segments = createMemo(() => renderSegments(tokens()));
    const completions = createMemo(() => {
        const currentQuery = query();
        const historyNeedle = currentQuery.trim().toLowerCase();
        const seen = new Set<string>();
        const result: Completion[] = [];
        const context = completionContext();

        if (context) {
            const prefix = context.prefix.toLowerCase();
            const source = context.kind === 'key' ? props.keys : context.key ? (props.values.get(context.key) ?? []) : [];

            for (const suggestion of source) {
                const loweredSuggestion = suggestion.toLowerCase();
                if (!loweredSuggestion.startsWith(prefix) || loweredSuggestion === prefix) {
                    continue;
                }

                const insertion = formatQueryLiteral(suggestion);
                const id = `${context.kind}\u0000${insertion}\u0000${context.start}\u0000${context.end}`;
                if (seen.has(id)) continue;

                seen.add(id);
                result.push({
                    kind: context.kind,
                    label: suggestion,
                    insertion,
                    replaceStart: context.start,
                    replaceEnd: context.end,
                });
            }
        }

        if (historyCompletionOpen() && historyNeedle) {
            for (const historicQuery of props.historicQueries) {
                const loweredHistoricQuery = historicQuery.toLowerCase();
                if (!loweredHistoricQuery.includes(historyNeedle) || loweredHistoricQuery === historyNeedle) {
                    continue;
                }

                const id = `history\u0000${historicQuery}`;
                if (seen.has(id)) continue;

                seen.add(id);
                result.push({
                    kind: 'history',
                    label: historicQuery,
                    insertion: historicQuery,
                    replaceStart: 0,
                    replaceEnd: currentQuery.length,
                });
            }
        }

        return result.slice(0, 5);
    });
    const showCompletions = createMemo(() => completions().length > 0);

    const scheduleCaretRestore = (offset: number) => {
        setCaret(offset);
        queueMicrotask(() => {
            if (editorRef) restoreCaret(editorRef, offset);
        });
    };

    const setQueryWithCaret = (nextQuery: string, nextCaret: number) => {
        setQuery(nextQuery);
        scheduleCaretRestore(nextCaret);
    };

    const closeCompletion = () => {
        setCompletionContext(null);
        setHistoryCompletionOpen(false);
        setActiveCompletion(0);
    };

    const updateCompletion = (nextQuery: string, nextCaret: number, forceOpen: boolean) => {
        if (!forceOpen) {
            closeCompletion();
            return;
        }

        const word = wordRangeAt(nextQuery, nextCaret);
        setHistoryCompletionOpen(Boolean(nextQuery.trim()));

        if (word.prefix) {
            const nextTokens = scanQuery(nextQuery);
            const previous = nearestNonSpaceBefore(nextTokens, word.start);
            const valueContext = previous?.kind === 'equal' || previous?.kind === 'not-equal';
            setCompletionContext({
                kind: valueContext ? 'value' : 'key',
                start: word.start,
                end: word.end,
                prefix: word.prefix,
                ...(valueContext ? { key: keyBeforeOperation(nextTokens, previous) } : {}),
            });
            setActiveCompletion(0);
            return;
        }

        const nextTokens = scanQuery(nextQuery);
        const previous = nearestNonSpaceBefore(nextTokens, nextCaret);
        if (previous?.kind === 'equal' || previous?.kind === 'not-equal') {
            setCompletionContext({
                kind: 'value',
                start: nextCaret,
                end: nextCaret,
                prefix: '',
                key: keyBeforeOperation(nextTokens, previous),
            });
            setActiveCompletion(0);
            return;
        }

        closeCompletion();
    };

    const currentRange = () => {
        if (!editorRef) return { start: caret(), end: caret() };
        return textRangeFromSelection(editorRef, caret());
    };

    const replaceRange = (start: number, end: number, text: string, nextCaret = start + text.length, complete = false) => {
        const nextQuery = query().slice(0, start) + text + query().slice(end);
        setQueryWithCaret(nextQuery, nextCaret);
        updateCompletion(nextQuery, nextCaret, complete);
    };

    const insertText = (text: string) => {
        const { start, end } = currentRange();
        const current = query();

        if (text.length === 1 && openingToClosing[text]) {
            const selected = current.slice(start, end);
            const completed = current.slice(0, start) + text + selected + openingToClosing[text] + current.slice(end);
            if (bracketSequenceIsValid(completed)) {
                setQueryWithCaret(completed, selected ? start + selected.length + 2 : start + 1);
                updateCompletion(completed, selected ? start + selected.length + 2 : start + 1, false);
                return;
            }
        }

        if (text.length === 1 && closingToOpening[text] && start === end && current[start] === text && bracketSequenceIsValid(current)) {
            scheduleCaretRestore(start + 1);
            closeCompletion();
            return;
        }

        replaceRange(start, end, text, start + text.length, text.length === 1 && !isWhitespace(text));
    };

    const deleteText = (backward: boolean) => {
        const { start, end } = currentRange();
        const current = query();

        if (start !== end) {
            replaceRange(start, end, '', start, false);
            return;
        }

        if (backward) {
            if (start === 0) return;

            const left = current[start - 1];
            const right = current[start];
            const beforeLeft = current[start - 2];

            if (openingToClosing[left] === right) {
                const next = current.slice(0, start - 1) + current.slice(start + 1);
                if (bracketSequenceIsValid(next)) {
                    setQueryWithCaret(next, start - 1);
                    closeCompletion();
                    return;
                }
            }

            if (closingToOpening[left] === beforeLeft) {
                const next = current.slice(0, start - 2) + current.slice(start);
                if (bracketSequenceIsValid(next)) {
                    setQueryWithCaret(next, start - 2);
                    closeCompletion();
                    return;
                }
            }

            replaceRange(start - 1, start, '', start - 1, false);
            return;
        }

        if (start >= current.length) return;

        const currentChar = current[start];
        const nextChar = current[start + 1];
        const previousChar = current[start - 1];

        if (openingToClosing[currentChar] === nextChar) {
            const next = current.slice(0, start) + current.slice(start + 2);
            if (bracketSequenceIsValid(next)) {
                setQueryWithCaret(next, start);
                closeCompletion();
                return;
            }
        }

        if (closingToOpening[currentChar] === previousChar) {
            const next = current.slice(0, start - 1) + current.slice(start + 1);
            if (bracketSequenceIsValid(next)) {
                setQueryWithCaret(next, start - 1);
                closeCompletion();
                return;
            }
        }

        replaceRange(start, start + 1, '', start, false);
    };

    const removePredicate = (start: number, end: number) => {
        const range = expressionRemovalRange(query(), tokens(), start, end);
        replaceRange(range.start, range.end, '', range.start, false);
        editorRef?.focus();
    };

    const applyCompletion = (completion: Completion) => {
        const nextQuery = query().slice(0, completion.replaceStart) + completion.insertion + query().slice(completion.replaceEnd);
        const nextCaret = completion.replaceStart + completion.insertion.length;
        setQueryWithCaret(nextQuery, nextCaret);
        closeCompletion();
        editorRef?.focus();
    };

    const onBeforeInput: JSX.EventHandlerUnion<HTMLDivElement, InputEvent> = (event) => {
        const inputEvent = event as InputEvent;
        if (inputEvent.isComposing) return;

        inputEvent.preventDefault();

        if (inputEvent.inputType === 'insertText' && inputEvent.data) {
            insertText(inputEvent.data);
            return;
        }

        if (inputEvent.inputType === 'deleteContentBackward') {
            deleteText(true);
            return;
        }

        if (inputEvent.inputType === 'deleteContentForward') {
            deleteText(false);
            return;
        }

        if (inputEvent.inputType === 'insertLineBreak') {
            insertText(' ');
        }
    };

    const onKeyDown: JSX.EventHandlerUnion<HTMLDivElement, KeyboardEvent> = (event) => {
        if (showCompletions()) {
            if (event.key === 'ArrowDown') {
                event.preventDefault();
                setActiveCompletion((activeCompletion() + 1) % completions().length);
                return;
            }

            if (event.key === 'ArrowUp') {
                event.preventDefault();
                setActiveCompletion((activeCompletion() - 1 + completions().length) % completions().length);
                return;
            }

            if (event.key === 'Enter' || event.key === 'Tab') {
                event.preventDefault();
                const completion = completions()[activeCompletion()];
                if (completion) applyCompletion(completion);
                return;
            }

            if (event.key === 'Escape') {
                event.preventDefault();
                closeCompletion();
                return;
            }
        } else if (event.key === 'Enter') {
            event.preventDefault();
        }

        if (event.key === 'Backspace') {
            event.preventDefault();
            deleteText(true);
            return;
        }

        if (event.key === 'Delete') {
            event.preventDefault();
            deleteText(false);
        }
    };

    const onPaste: JSX.EventHandlerUnion<HTMLDivElement, ClipboardEvent> = (event) => {
        event.preventDefault();
        insertText(event.clipboardData?.getData('text/plain') ?? '');
    };

    const onMouseUp = () => {
        const range = currentRange();
        scheduleCaretRestore(range.end);
        closeCompletion();
    };

    const onKeyUp: JSX.EventHandlerUnion<HTMLDivElement, KeyboardEvent> = (event) => {
        if (event.key.startsWith('Arrow') || event.key === 'Home' || event.key === 'End') {
            const range = currentRange();
            setCaret(range.end);
            closeCompletion();
        }
    };

    return (
        <div class="relative flex min-w-60 flex-auto">
            <div
                ref={editorRef}
                contentEditable
                role="textbox"
                aria-label="Query"
                spellcheck={false}
                onBeforeInput={onBeforeInput}
                onKeyDown={onKeyDown}
                onKeyUp={onKeyUp}
                onMouseUp={onMouseUp}
                onPaste={onPaste}
                class={cx(
                    'relative flex min-h-12 min-w-60 flex-auto cursor-text items-center gap-1 overflow-x-auto whitespace-pre-wrap break-words border border-border bg-input px-2.5 py-2 text-sm leading-7',
                    'focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40',
                    'max-[640px]:flex-wrap',
                )}
            >
                <For each={segments()}>
                    {(segment) => (
                        <Show
                            when={segment.type === 'predicate'}
                            fallback={
                                <span class={tokenClassByRole[(segment as Extract<RenderSegment, { type: 'token' }>).part.role]}>
                                    {(segment as Extract<RenderSegment, { type: 'token' }>).part.text}
                                </span>
                            }
                        >
                            <span
                                class="inline-flex min-h-7 items-center gap-1 border border-primary/60 bg-primary/5 px-1.5 align-baseline"
                                data-expression-start={(segment as Extract<RenderSegment, { type: 'predicate' }>).start}
                                data-expression-end={(segment as Extract<RenderSegment, { type: 'predicate' }>).end}
                            >
                                <For each={(segment as Extract<RenderSegment, { type: 'predicate' }>).parts}>
                                    {(part) => <span class={tokenClassByRole[part.role]}>{part.text}</span>}
                                </For>
                                <button
                                    type="button"
                                    contentEditable={false}
                                    class="grid size-5 shrink-0 cursor-pointer place-items-center border-0 bg-transparent p-0 text-muted-foreground hover:text-foreground"
                                    aria-label="Remove expression"
                                    onMouseDown={(event) => event.preventDefault()}
                                    onClick={() =>
                                        removePredicate(
                                            (segment as Extract<RenderSegment, { type: 'predicate' }>).start,
                                            (segment as Extract<RenderSegment, { type: 'predicate' }>).end,
                                        )
                                    }
                                >
                                    <CloseIcon />
                                </button>
                            </span>
                        </Show>
                    )}
                </For>
            </div>

            <Show when={!query()}>
                <span class="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-sm text-muted-foreground">
                    {'{region-eu-west-1} level=error'}
                </span>
            </Show>

            <Show when={showCompletions()}>
                <div class="absolute left-0 top-[calc(100%+4px)] z-20 w-72 border border-border bg-popover shadow-md">
                    <For each={completions()}>
                        {(completion, index) => (
                            <button
                                type="button"
                                class={cx(
                                    'flex min-h-8 w-full cursor-pointer items-center gap-3 border-0 px-3 text-left text-sm text-popover-foreground',
                                    index() === activeCompletion() && 'bg-primary text-primary-foreground',
                                    index() !== activeCompletion() && 'bg-transparent hover:bg-accent hover:text-accent-foreground',
                                )}
                                onMouseDown={(event) => event.preventDefault()}
                                onClick={() => applyCompletion(completion)}
                            >
                                <span class="min-w-0 flex-1 overflow-hidden text-ellipsis whitespace-nowrap">{completion.label}</span>
                                <span class="shrink-0 text-[10px] font-extrabold uppercase opacity-65">{completion.kind}</span>
                            </button>
                        )}
                    </For>
                </div>
            </Show>
        </div>
    );
};

export default QueryInput;
