const queryHistoryKey = "ochi.queryHistory";
const queryHistoryLimit = 10;

export function loadQueryHistory(): string[] {
  try {
    const raw = window.localStorage.getItem(queryHistoryKey);
    if (!raw) return [];

    return JSON.parse(raw) as string[];
  } catch (err) {
    console.error("failed to load query from history", err);
    return [];
  }
}

export function pushQueryHistory(history: readonly string[], query: string): readonly string[] {
  try {
    if (!query) return history;

    const res = [query, ...history].slice(0, queryHistoryLimit);
    const json = JSON.stringify(res);
    window.localStorage.setItem(queryHistoryKey, json);
    return res;
  } catch (err) {
    console.error("failed to push query to history", err);
    return [];
  }
}
