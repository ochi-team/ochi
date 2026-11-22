import { expect, test } from "vitest";

import {
  parseQuerySyntax,
  recognizeQueryComparisons,
  scanQuery,
  type QueryExpressionNode,
  type TokenKind,
} from "./querySyntax";

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

function expectBooleanTree(
  source: string,
  build: (
    comparisons: ReturnType<typeof recognizeQueryComparisons>,
    boolean: (
      kind: "and" | "or",
      left: QueryExpressionNode,
      right: QueryExpressionNode,
    ) => QueryExpressionNode,
    group: (start: number, end: number, expression: QueryExpressionNode) => QueryExpressionNode,
  ) => QueryExpressionNode[],
) {
  const expected = build(
    recognizeQueryComparisons(source),
    (kind, left, right) => {
      const operatorStart = source.toLowerCase().indexOf(kind, left.end);
      return {
        kind,
        start: left.start,
        end: right.end,
        operator: { start: operatorStart, end: operatorStart + kind.length },
        left,
        right,
      };
    },
    (start, end, expression) => ({
      kind: "group",
      start,
      end,
      open: { start, end: start + 1 },
      close: { start: end - 1, end },
      expression,
    }),
  );
  const parsed = parseQuerySyntax(source);
  expect(parsed.roots).toEqual(expected);

  const parents = new Map<QueryExpressionNode, QueryExpressionNode>();
  const pending = [...parsed.roots];
  while (pending.length) {
    const node = pending.pop()!;
    if (node.kind === "comparison") continue;
    if (node.kind === "group") {
      parents.set(node.expression, node);
      pending.push(node.expression);
      continue;
    }
    parents.set(node.left, node);
    parents.set(node.right, node);
    pending.push(node.left, node.right);
  }
  expect(parsed.parents).toEqual(parents);
  for (const [child, parent] of parents) {
    expect(parsed.parents.get(child)).toBe(parent);
  }
  for (const root of parsed.roots) expect(parsed.parents.has(root)).toBe(false);
}

test("parseQuerySyntax gives AND precedence over OR", () => {
  expectBooleanTree("a=1 OR b=2 AND c=3", ([a, b, c], op) => [op("or", a, op("and", b, c))]);
  expectBooleanTree(" a=1 aNd b=2 oR c=3 AnD d=4 ", ([a, b, c, d], op) => [
    op("or", op("and", a, b), op("and", c, d)),
  ]);
});

test("parseQuerySyntax associates explicit Boolean chains to the left", () => {
  expectBooleanTree("a=1 AND b=2 AND c=3", ([a, b, c], op) => [op("and", op("and", a, b), c)]);
  expectBooleanTree("a=1 or b=2 OR c=3", ([a, b, c], op) => [op("or", op("or", a, b), c)]);
});

test("parseQuerySyntax preserves distinct operands, roles, and UTF-16 source ranges", () => {
  expectBooleanTree("a=1 AND a=1 OR a=1", ([a, b, c], op) => [op("or", op("and", a, b), c)]);
  expectBooleanTree(` \t"😀" != 'AND'\nOr\tpath !~ "OR =" `, ([a, b], op) => [op("or", a, b)]);
});

test("parseQuerySyntax only links comparisons separated by one explicit Boolean operator", () => {
  expectBooleanTree("", () => []);
  expectBooleanTree(" a=1 ", ([a]) => [a]);
  expectBooleanTree("a=1 b=2", ([a, b]) => [a, b]);
  expectBooleanTree("a=1 AND ? b=2 OR c=3", ([a, b, c], op) => [a, op("or", b, c)]);
  expectBooleanTree("a=1 OR AND b=2", ([a, b]) => [a, b]);
  expectBooleanTree("a=1 AND", ([a]) => [a]);
  expectBooleanTree("a=1 | b=2", ([a, b]) => [a, b]);
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

test("parseQuerySyntax lets groups override Boolean precedence", () => {
  expectBooleanTree("(a=1 OR b=2) AND c=3", ([a, b, c], op, group) => [
    op("and", group(0, 12, op("or", a, b)), c),
  ]);
  expectBooleanTree("a=1 AND (b=2 OR c=3)", ([a, b, c], op, group) => [
    op("and", a, group(8, 20, op("or", b, c))),
  ]);
  expectBooleanTree("(a=1) OR (b=2)", ([a, b], op, group) => [
    op("or", group(0, 5, a), group(9, 14, b)),
  ]);
});

test("parseQuerySyntax retains nested and redundant group ownership", () => {
  expectBooleanTree("((a=1))", ([a], _op, group) => [group(0, 7, group(1, 6, a))]);
  expectBooleanTree("(a=1 AND (b=2 OR c=3))", ([a, b, c], op, group) => [
    group(0, 22, op("and", a, group(9, 21, op("or", b, c)))),
  ]);
});

test("parseQuerySyntax preserves group delimiters, trivia, and UTF-16 ranges", () => {
  expectBooleanTree(' \t( "😀"=1\n)  ', ([a], _op, group) => [group(2, 12, a)]);
  expectBooleanTree("( a=1 ) ( a=1 )", ([a, b], _op, group) => [group(0, 7, a), group(8, 15, b)]);
});

test("parseQuerySyntax does not invent expressions for empty or unsupported groups", () => {
  expectBooleanTree("() ( )", () => []);
  expectBooleanTree("(a=1 ? b=2)", ([a, b]) => [a, b]);
  expectBooleanTree("(a=1 b=2)", ([a, b]) => [a, b]);
});

