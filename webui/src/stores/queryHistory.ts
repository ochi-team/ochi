const queryHistoryKey = 'ochi.queryHistory';
const queryHistoryLimit = 10;

function normalizeQuery(query: string): string {
    return query.trim();
}

function isQueryHistory(value: unknown): value is string[] {
    return Array.isArray(value) && value.every((item) => typeof item === 'string');
}

export function loadQueryHistory(): string[] {
    try {
        const raw = window.localStorage.getItem(queryHistoryKey);
        if (!raw) return [];

        const parsed: unknown = JSON.parse(raw);
        return isQueryHistory(parsed) ? parsed.slice(0, queryHistoryLimit) : [];
    } catch {
        return [];
    }
}

export function pushQueryHistory(history: readonly string[], query: string): string[] {
    const normalized = normalizeQuery(query);
    if (!normalized) return [...history];

    const next = [normalized, ...history.filter((item) => item !== normalized)];
    return next.slice(0, queryHistoryLimit);
}

export function saveQueryHistory(history: readonly string[]): void {
    try {
        window.localStorage.setItem(queryHistoryKey, JSON.stringify(history.slice(0, queryHistoryLimit)));
    } catch {
        // Ignore unavailable or full local storage; in-memory history still works.
    }
}
