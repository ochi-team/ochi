import { expect, test } from "vitest";

import { recognizeQueryComparisons, scanQuery, type TokenKind } from "./querySyntax";

function expectTokens(source: string, parts: [TokenKind, string][]) {
  let offset = 0;
  const expected = parts.map(([kind, text]) => {
    const start = offset;
    offset += text.length;
    return { kind, start, end: offset };
  });
  const tokens = scanQuery(source);
  expect(tokens).toEqual(expected);
  expect(tokens.map(({ start, end }) => source.slice(start, end)).join("")).toBe(source);
  expect(offset).toBe(source.length);
}

test("scanQuery preserves unquoted literals and whitespace", () => {
  expectTokens("", []);
  expectTokens(" \t\n", [["whitespace", " \t\n"]]);
  expectTokens("tenant=42 AND level=ERROR", [
    ["literal", "tenant"],
    ["equal", "="],
    ["literal", "42"],
    ["whitespace", " "],
    ["and", "AND"],
    ["whitespace", " "],
    ["literal", "level"],
    ["equal", "="],
    ["literal", "ERROR"],
  ]);
  expectTokens("_a-1:/health@host.example+2", [["literal", "_a-1:/health@host.example+2"]]);
});

test("scanQuery recognizes whole-word Boolean keywords case-insensitively", () => {
  expectTokens("aNd Or andy OR_1 /AND AND.foo", [
    ["and", "aNd"],
    ["whitespace", " "],
    ["or", "Or"],
    ["whitespace", " "],
    ["literal", "andy"],
    ["whitespace", " "],
    ["literal", "OR_1"],
    ["whitespace", " "],
    ["literal", "/AND"],
    ["whitespace", " "],
    ["literal", "AND.foo"],
  ]);
  expectTokens("(AND)OR", [
    ["leftParenthesis", "("],
    ["and", "AND"],
    ["rightParenthesis", ")"],
    ["or", "OR"],
  ]);
});

test("scanQuery keeps comparison operators atomic and covers delimiters", () => {
  expectTokens("=!=~!~!<", [
    ["equal", "="],
    ["notEqual", "!="],
    ["matchRegex", "~"],
    ["notMatchRegex", "!~"],
    ["unknown", "!"],
    ["unknown", "<"],
  ]);
  expectTokens("[]{}(),|", [
    ["leftSquareBracket", "["],
    ["rightSquareBracket", "]"],
    ["leftCurlyBracket", "{"],
    ["rightCurlyBracket", "}"],
    ["leftParenthesis", "("],
    ["rightParenthesis", ")"],
    ["comma", ","],
    ["pipe", "|"],
  ]);
});

test("scanQuery preserves quoted literals without recognizing their contents as syntax", () => {
  expectTokens(`"AND OR = //" 'OR AND !='`, [
    ["literal", `"AND OR = //"`],
    ["whitespace", " "],
    ["literal", "'OR AND !='"],
  ]);
  expectTokens(`""''"a'b"'a"b'`, [
    ["literal", '""'],
    ["literal", "''"],
    ["literal", `"a'b"`],
    ["literal", `'a"b'`],
  ]);
  expectTokens('"日本語😀\nOR=値"=42', [
    ["literal", '"日本語😀\nOR=値"'],
    ["equal", "="],
    ["literal", "42"],
  ]);
});

test("scanQuery respects escaped quotes and backslash parity", () => {
  expectTokens(String.raw`"a\" AND=b" 'c\' OR=d'`, [
    ["literal", String.raw`"a\" AND=b"`],
    ["whitespace", " "],
    ["literal", String.raw`'c\' OR=d'`],
  ]);
  expectTokens(String.raw`"a\\" AND 'b\\'`, [
    ["literal", String.raw`"a\\"`],
    ["whitespace", " "],
    ["and", "AND"],
    ["whitespace", " "],
    ["literal", String.raw`'b\\'`],
  ]);
});

test("scanQuery retains unterminated quotes as incomplete literals", () => {
  expectTokens('"', [["unterminatedLiteral", '"']]);
  expectTokens("'AND =😀\nOR", [["unterminatedLiteral", "'AND =😀\nOR"]]);
  expectTokens('"escaped\\"', [["unterminatedLiteral", '"escaped\\"']]);
  expectTokens("'trailing\\", [["unterminatedLiteral", "'trailing\\"]]);
});

test("scanQuery preserves schema in http", () => {
  expectTokens("/health https://host/path", [
    ["literal", "/health"],
    ["whitespace", " "],
    ["literal", "https://host/path"],
  ]);
});

test("scanQuery preserves unknown characters", () => {
  expectTokens("!<>?", [
    ["unknown", "!"],
    ["unknown", "<"],
    ["unknown", ">"],
    ["unknown", "?"],
  ]);
});

function expectComparisons(source: string, expressions: [string, string, string, string][]) {
  let offset = 0;
  const expected = expressions.map(([text, key, operator, value]) => {
    const start = source.indexOf(text, offset);
    expect(start).toBeGreaterThanOrEqual(offset);
    const keyEnd = start + key.length;
    const operatorStart = source.indexOf(operator, keyEnd);
    const valueStart = source.indexOf(value, operatorStart + operator.length);
    offset = start + text.length;
    return {
      kind: "comparison",
      start,
      end: offset,
      key: { start, end: keyEnd },
      operator: { start: operatorStart, end: operatorStart + operator.length },
      value: { start: valueStart, end: offset },
    };
  });
  expect(recognizeQueryComparisons(source)).toEqual(expected);
}

test("recognizeQueryComparisons exposes complete comparison and role ranges", () => {
  expectComparisons("tenant=42", [["tenant=42", "tenant", "=", "42"]]);
  expectComparisons(" \tlevel \t!=\n ERROR  ", [["level \t!=\n ERROR", "level", "!=", "ERROR"]]);
  expectComparisons("path~/health AND service!~api", [
    ["path~/health", "path", "~", "/health"],
    ["service!~api", "service", "!~", "api"],
  ]);
});

test("recognizeQueryComparisons preserves quoted roles and UTF-16 offsets", () => {
  const key = '"日本語😀"';
  const value = String.raw`'AND OR = \'値'`;
  expectComparisons(`  ${key} = ${value} `, [[`${key} = ${value}`, key, "=", value]]);
  expectComparisons(`"AND"="" 'OR'!=''`, [
    ['"AND"=""', '"AND"', "=", '""'],
    ["'OR'!=''", "'OR'", "!=", "''"],
  ]);
});

test("recognizeQueryComparisons distinguishes repeated and adjacent comparisons", () => {
  expectComparisons("(a=1 OR a=1) a=1", [
    ["a=1", "a", "=", "1"],
    ["a=1", "a", "=", "1"],
    ["a=1", "a", "=", "1"],
  ]);
  expectComparisons("raw {tenant=42} | ? level=ERROR", [
    ["tenant=42", "tenant", "=", "42"],
    ["level=ERROR", "level", "=", "ERROR"],
  ]);
});

test("recognizeQueryComparisons leaves incomplete and nonliteral comparisons raw", () => {
  for (const source of [
    "",
    "tenant",
    "tenant=",
    "tenant != \t",
    "=42",
    "tenant==42",
    "tenant=AND",
    "OR=42",
    'tenant="unfinished',
    "'unfinished=42",
    "tenant=?",
    "tenant=(42)",
  ]) {
    expectComparisons(source, []);
  }
  expectComparisons("tenant= AND level=ERROR", [["level=ERROR", "level", "=", "ERROR"]]);
});
