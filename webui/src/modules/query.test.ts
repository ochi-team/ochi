import { expect, test } from "vitest";

import { setQueryExtractingRange, setRangeSubstitutingQuery } from "./query";

test.each([
  ["[-5m, now] {}", "-5m"],
  ["[-30s, now] {}", "-30s"],
  ["[-2d, now] {}", "-2d"],
  ["[yesterday, now] {}", ""],
  ["[-5x, now] {}", ""],
  ["  [ -1h , now ] {}", "-1h"],
  ["  [ -1h , -5m ] {service=api}", ""],
  ["[-1h, -5m] {}", ""],
  ["  [ -1h , -5m ] {}", ""],
  ["  [ -1h , -5m ]", ""],
  ["[2026-01-01, 2026-01-02] {}", ""],
  ["[2026-01-01, now] {}", ""],
  ["{}", ""],
  ["", ""],
  ["[-5m,", ""],
  ["[-1h, ] {}", ""],
  ["[-1h, -5m, now] {}", ""],
  ["[, now] {}", ""],
  ['{message="[-5m, now]"}', ""],
])("setQueryExtractingRange: %s", (query, range) => {
  expect(setQueryExtractingRange(query)).toEqual({ query, range });
});

test.each([
  ["[-5m, now] {}", "-15m", "[-15m, now] {}"],
  ["  [ -1h , -5m ]", "-15m", "[-15m, now] "],
  ["  [ -1h , -5m ]   {service=api}", "-30m", "[-30m, now] {service=api}"],
  ["[2026-01-01, 2026-01-02] {}", "-1h", "[-1h, now] {}"],
  ['[-1h, now] {message="[-5m, now]"}', "-15m", '[-15m, now] {message="[-5m, now]"}'],
  ["{}", "-15m", "{}"],
  ["", "-15m", ""],
  ["[-5m,", "-15m", "[-5m,"],
  ["[-1h, ] {}", "-15m", "[-1h, ] {}"],
  ['{message="[-5m, now]"}', "-15m", '{message="[-5m, now]"}'],
])("setRangeSubstitutingQuery: %s → %s", (query, range, expectedQuery) => {
  expect(setRangeSubstitutingQuery(query, range)).toEqual({ query: expectedQuery, range });
});

test("query and range updates compose without retaining a stale range", () => {
  // range -1h
  const initial = setQueryExtractingRange("[-1h, -5m] {service=api}");
  // range -15m
  const substituted = setRangeSubstitutingQuery(initial.query, "-15m");
  // must not mutate, keep the same
  expect(setQueryExtractingRange(substituted.query)).toEqual(substituted);

  // no range
  expect(setQueryExtractingRange("{service=worker}")).toEqual({
    query: "{service=worker}",
    range: "",
  });
});
