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
      kind: "and" | "or" | "implicitAnd",
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
        kind: kind === "implicitAnd" ? "and" : kind,
        start: left.start,
        end: right.end,
        operator:
          kind === "implicitAnd"
            ? null
            : { start: operatorStart, end: operatorStart + kind.length },
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

test("parseQuerySyntax leaves unsupported gaps and incomplete operators unlinked", () => {
  expectBooleanTree("", () => []);
  expectBooleanTree(" a=1 ", ([a]) => [a]);
  expectBooleanTree("a=1 AND ? b=2 OR c=3", ([a, b, c], op) => [a, op("or", b, c)]);
  expectBooleanTree("a=1 OR AND b=2", ([a, b]) => [a, b]);
  expectBooleanTree("a=1 AND", ([a]) => [a]);
  expectBooleanTree("a=1 | b=2", ([a]) => [a]);
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

test("recognizeQueryComparisons rejects ambiguous chained comparison fragments", () => {
  for (const source of ["a=1=2", "a=1!=2", "a~=1", "a==b=2", "a=1=", "a=1 !~"]) {
    expectComparisons(source, []);
    expect(parseQuerySyntax(source).roots).toEqual([]);
  }
  expectComparisons("!=a=1=2", []);
  expectComparisons("a=1=2=3 AND safe=4", [["safe=4", "safe", "=", "4"]]);
  expectComparisons("a=1=2 ? safe=3", [["safe=3", "safe", "=", "3"]]);
  expectComparisons("a==b=2 AND safe=3", [["safe=3", "safe", "=", "3"]]);
});

test("recognizeQueryComparisons skips invalid leading tokens until the next key", () => {
  expectComparisons("=a=1", [["a=1", "a", "=", "1"]]);
  expectComparisons("!= ? safe=3", [["safe=3", "safe", "=", "3"]]);
  expectComparisons("== a=1 AND b=2", [
    ["a=1", "a", "=", "1"],
    ["b=2", "b", "=", "2"],
  ]);
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
  expectBooleanTree("( a=1 ) ( a=1 )", ([a, b], op, group) => [
    op("implicitAnd", group(0, 7, a), group(8, 15, b)),
  ]);
});

test("parseQuerySyntax does not invent expressions for empty or unsupported groups", () => {
  expectBooleanTree("() ( )", () => []);
  expectBooleanTree("(a=1 ? b=2)", ([b]) => [b]);
  expectBooleanTree("(a=1 raw b=2)", ([a, b]) => [a, b]);
});

test("parseQuerySyntax gives adjacent comparisons implicit conjunction ownership without operator text", () => {
  expectBooleanTree("a=1 b=2", ([a, b], op) => [op("implicitAnd", a, b)]);
  expectBooleanTree(" \ta=1\n a=1\t a=1 ", ([a, b, c], op) => [
    op("implicitAnd", op("implicitAnd", a, b), c),
  ]);
  expectBooleanTree(
    `"😀"='AND OR =' level!=ERROR path~/health service!~api`,
    ([a, b, c, d], op) => [op("implicitAnd", op("implicitAnd", op("implicitAnd", a, b), c), d)],
  );
});

test("parseQuerySyntax gives implicit AND the same precedence and associativity as explicit AND", () => {
  expectBooleanTree("a=1 OR b=2 c=3", ([a, b, c], op) => [op("or", a, op("implicitAnd", b, c))]);
  expectBooleanTree("a=1 b=2 OR c=3", ([a, b, c], op) => [op("or", op("implicitAnd", a, b), c)]);
  expectBooleanTree("a=1 AND b=2 c=3", ([a, b, c], op) => [op("implicitAnd", op("and", a, b), c)]);
  expectBooleanTree("a=1 b=2 AND c=3", ([a, b, c], op) => [op("and", op("implicitAnd", a, b), c)]);
});

test("parseQuerySyntax links adjacent grouped operands with implicit AND", () => {
  expectBooleanTree("a=1(b=2 OR c=3)", ([a, b, c], op, group) => [
    op("implicitAnd", a, group(3, 15, op("or", b, c))),
  ]);
  expectBooleanTree("(a=1)b=2", ([a, b], op, group) => [op("implicitAnd", group(0, 5, a), b)]);
});

test("parseQuerySyntax does not infer conjunctions through quoted words or unsupported adjacent text", () => {
  for (const source of ['a=1 "AND" b=2', "a=1 'OR' b=2", "a=1 raw b=2", "a=1 incomplete= ? b=2"]) {
    expectBooleanTree(source, ([a, b]) => [a, b]);
  }

  expectBooleanTree("a=1 ? b=2", ([b]) => [b]);
  expectBooleanTree("a=1, b=2", ([b]) => [b]);
  expectBooleanTree(`a="b=2 AND c=3"`, ([a]) => [a]);
});

function expectRegions(source: string, expected: [string, string, string[]?][]) {
  const parsed = parseQuerySyntax(source);
  let offset = 0;
  const regions = expected.map(([kind, text, expressions]) => {
    const start = source.indexOf(text, offset);
    expect(start).toBeGreaterThanOrEqual(offset);
    const end = start + text.length;
    offset = end;
    const range = { kind, start, end };
    if (kind === "timeRange" || kind === "tagBlock") {
      return {
        ...range,
        open: { start, end: start + 1 },
        close: { start: end - 1, end },
        ...(kind === "tagBlock" ? { roots: expressions } : {}),
      };
    }
    return kind === "queryExpression" ? { ...range, roots: expressions } : range;
  });
  expect(
    parsed.regions.map((region) =>
      "roots" in region
        ? { ...region, roots: region.roots.map((root) => source.slice(root.start, root.end)) }
        : region,
    ),
  ).toEqual(regions);

  const roots = parsed.regions.flatMap((region) => ("roots" in region ? region.roots : []));
  expect(parsed.roots).toEqual(roots);
  roots.forEach((root, index) => {
    expect(parsed.roots[index]).toBe(root);
    expect(parsed.parents.has(root)).toBe(false);
  });
}

test("parseQuerySyntax exposes ordered time, tag, and query regions with exact ranges", () => {
  expectRegions(" \t[-15m, now] {tenant=42} level=ERROR  ", [
    ["timeRange", "[-15m, now]"],
    ["tagBlock", "{tenant=42}", ["tenant=42"]],
    ["queryExpression", "level=ERROR", ["level=ERROR"]],
  ]);
  expectRegions("{tenant=42} level=ERROR", [
    ["tagBlock", "{tenant=42}", ["tenant=42"]],
    ["queryExpression", "level=ERROR", ["level=ERROR"]],
  ]);
  expectRegions("[-15m, now] {} level=ERROR", [
    ["timeRange", "[-15m, now]"],
    ["tagBlock", "{}", []],
    ["queryExpression", "level=ERROR", ["level=ERROR"]],
  ]);
  expectRegions("{}", [["tagBlock", "{}", []]]);
  expectRegions("[-15m, now]", [["timeRange", "[-15m, now]"]]);
  expectRegions(" \t\n", []);
});

test("parseQuerySyntax keeps time-range contents opaque", () => {
  expectRegions("[a=1, b=2] {tenant=42} level=ERROR", [
    ["timeRange", "[a=1, b=2]"],
    ["tagBlock", "{tenant=42}", ["tenant=42"]],
    ["queryExpression", "level=ERROR", ["level=ERROR"]],
  ]);
  expectRegions("[a=1 AND b=2] ", [["timeRange", "[a=1 AND b=2]"]]);
});

test("parseQuerySyntax retains Boolean and group ownership only within structural regions", () => {
  expectRegions("{a=1 OR b=2} c=3 d=4", [
    ["tagBlock", "{a=1 OR b=2}", ["a=1 OR b=2"]],
    ["queryExpression", "c=3 d=4", ["c=3 d=4"]],
  ]);
  expectBooleanTree("{a=1 OR b=2} c=3 d=4", ([a, b, c, d], op) => [
    op("or", a, b),
    op("implicitAnd", c, d),
  ]);
  expectBooleanTree("{(a=1)}(b=2)", ([a, b], _op, group) => [group(1, 6, a), group(7, 12, b)]);
});

test("parseQuerySyntax does not promote late or repeated structural blocks to regions", () => {
  expectRegions("a=1 {b=2} c=3", [["queryExpression", "a=1", ["a=1"]]]);
  expectRegions("[now, now] [now, now] {a=1}", [["timeRange", "[now, now]"]]);
  expectRegions("{a=1} {b=2} c=3", [["tagBlock", "{a=1}", ["a=1"]]]);
  expectRegions("{a=1} [now, now] b=2", [["tagBlock", "{a=1}", ["a=1"]]]);
});

test("parseQuerySyntax retains the entire first-pipe suffix opaquely", () => {
  for (const suffix of [
    "| b=2 AND c=3",
    "| {b=2} (c=3)",
    "| stats count() | d=4  ",
    "| stats | [now, now] {b=2} c=3 | more",
  ]) {
    const source = "[-15m, now] {tenant=42} level=ERROR " + suffix;
    expectRegions(source, [
      ["timeRange", "[-15m, now]"],
      ["tagBlock", "{tenant=42}", ["tenant=42"]],
      ["queryExpression", "level=ERROR", ["level=ERROR"]],
      ["pipeSuffix", suffix],
    ]);
  }
  expectRegions(" | a=1 | {b=2} ", [["pipeSuffix", "| a=1 | {b=2} "]]);
  expectRegions("[now | a=1] {b=2}", []);
});

test("parseQuerySyntax ignores malformed structural tails without resuming recognition", () => {
  expectRegions("a=1 [now, now] b=2", [["queryExpression", "a=1", ["a=1"]]]);
  expectRegions("[now, now] [later, now] b=2", [["timeRange", "[now, now]"]]);
  expectRegions("a=1 {b=2} c=3", [["queryExpression", "a=1", ["a=1"]]]);
  expectRegions("{a=1} {b=2} c=3", [["tagBlock", "{a=1}", ["a=1"]]]);
  expectRegions("{a=1} [now, now] b=2", [["tagBlock", "{a=1}", ["a=1"]]]);
  for (const source of ["[a=1", "{a=1", "[{a=1}] b=2", "{a=1 [now]} b=2", "] a=1", "} a=1"]) {
    expectRegions(source, []);
  }
  expectRegions("a=1 } b=2", [["queryExpression", "a=1", ["a=1"]]]);
  expectRegions("{a=1 | b=2} [now] c=3 | d=4", []);
  expectRegions("[now] {a=1 | b=2} | more", [["timeRange", "[now]"]]);
  expectRegions("a=1 {b=2 | c=3} | more", [["queryExpression", "a=1", ["a=1"]]]);
});

test("parseQuerySyntax ignores unmatched opening tails and never links across unmatched closers", () => {
  expectBooleanTree("(a=1 AND b=2", () => []);
  expectBooleanTree("a=1 AND (b=2 OR c=3", ([a]) => [a]);
  expectBooleanTree("a=1 (b=2) (c=3", ([a, b], op, group) => [
    op("implicitAnd", a, group(4, 9, b)),
  ]);
  expectBooleanTree("a=1) AND b=2", ([a, b]) => [a, b]);
  expectBooleanTree("a=1 AND ) b=2", ([a, b]) => [a, b]);
});

test("parseQuerySyntax keeps incomplete operators and unknown text as ownership barriers", () => {
  expectBooleanTree("AND a=1 OR", ([a]) => [a]);
  expectBooleanTree("a=1 AND OR b=2", ([a, b]) => [a, b]);
  expectBooleanTree("a=1 ? AND b=2", ([b]) => [b]);
  expectBooleanTree('a=1 AND b="unfinished OR c=3', ([a]) => [a]);
  expectBooleanTree("a=1 AND b= ? c=3", ([a, c]) => [a, c]);
  expectBooleanTree("a=1 AND b= xx c=3", ([a, b, c], op) => [
    op("implicitAnd", op("and", a, b), c),
  ]);
});

test("parseQuerySyntax bounds nesting while retaining ordinary deep and wide trees", () => {
  function expectDepth(depth: number, expectedDepth: number) {
    const source = "(".repeat(depth) + "a=1" + ")".repeat(depth);
    const parsed = parseQuerySyntax(source);
    expect(parsed.roots.length).toBe(expectedDepth ? 1 : 0);
    expect(parsed.parents.size).toBe(expectedDepth);
  }
  expectDepth(128, 128);
  expectDepth(129, 0);
  expectDepth(10000, 0);
  const source = Array.from({ length: 10000 }, () => "a=1").join(" AND ");
  const parsed = parseQuerySyntax(source);
  expect(parsed.roots.length).toBe(1);
  expect(parsed.parents.size).toBe(19998);
  expectRegions("{" + "(".repeat(129) + "a=1" + ")".repeat(129) + "} b=2", [
    ["tagBlock", "{" + "(".repeat(129) + "a=1" + ")".repeat(129) + "}", []],
    ["queryExpression", "b=2", ["b=2"]],
  ]);
});

test("parseQuerySyntax ignores quoted delimiters when recognizing regions", () => {
  expectRegions(`["a=1 | {}", now] {tag=1} key=2`, [
    ["timeRange", `["a=1 | {}", now]`],
    ["tagBlock", "{tag=1}", ["tag=1"]],
    ["queryExpression", "key=2", ["key=2"]],
  ]);
  expectRegions(`{tag="[now] | {x}"} key='{} | []' AND other=1`, [
    ["tagBlock", `{tag="[now] | {x}"}`, [`tag="[now] | {x}"`]],
    ["queryExpression", `key='{} | []' AND other=1`, [`key='{} | []' AND other=1`]],
  ]);
  expectBooleanTree(`{tag="[now] | {x}"} key='{} | []' AND other=1`, ([a, b, c], op) => [
    a,
    op("and", b, c),
  ]);
});
