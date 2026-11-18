const queryHistoryKey = "ochi.queryHistory";
const queryHistoryLimit = 10;

export function loadQueryHistory(): string[] {
  const raw = window.localStorage.getItem(queryHistoryKey);
  if (!raw) return [];

  return JSON.parse(raw) as string[];
}

export function pushQueryHistory(history: readonly string[], query: string): readonly string[] {
  const normalized = normalizeQuery(query);
  if (!normalized) return history;

  const res = [normalized, ...history].slice(0, queryHistoryLimit);
  const json = JSON.stringify(res);
  window.localStorage.setItem(queryHistoryKey, json);
  return res;
}

function normalizeQuery(query: string): string {
  return query.trim();
}
