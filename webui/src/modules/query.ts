export const queryTimeRangeRegex =
  /^\s*\[\s*([^\s,\[\]][^,\[\]]*?)\s*,\s*[^\s,\[\]][^,\[\]]*?\s*\]\s*/;

export function setQueryExtractingRange(query: string) {
  const range = /^\s*\[\s*(-\d+[smhd])\s*,\s*now\s*\]/.exec(query)?.[1] ?? "";
  return { query, range };
}

export function setRangeSubstitutingQuery(query: string, range: string) {
  return {
    query: query.trim() ? query.replace(queryTimeRangeRegex, `[${range}, now] `) : `[${range}, now]`,
    range,
  };
}
