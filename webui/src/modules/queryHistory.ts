const QUERY_HISTORY_KEY = "ochi.queryHistory";
const QUERY_HISTORY_LIMIT = 10;

export function loadQueryHistory(): string[] {
  try {
    const raw = window.localStorage.getItem(QUERY_HISTORY_KEY);
    if (!raw) return [];

    return JSON.parse(raw) as string[];
  } catch (err) {
    console.error("failed to load query from history", err);
    return [];
  }
}

export function pushQueryHistory(query: string): readonly string[] {
  try {
    const history = loadQueryHistory();
    if (!query) return history;

    const res = [query, ...history].slice(0, QUERY_HISTORY_LIMIT);
    const json = JSON.stringify(res);
    window.localStorage.setItem(QUERY_HISTORY_KEY, json);
    return res;
  } catch (err) {
    console.error("failed to push query to history", err);
    return [];
  }
}
