import { expect, test } from "vitest";

import { scanQuery, type TokenKind } from "./querySyntax";

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

test("scanQuery preserves unknown characters", () => {
  expectTokens("!<>?", [
    ["unknown", "!"],
    ["unknown", "<"],
    ["unknown", ">"],
    ["unknown", "?"],
  ]);
});
