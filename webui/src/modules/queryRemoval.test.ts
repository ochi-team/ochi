import { expect, test } from "vitest";

import { removeQueryNode } from "./queryRemoval";
import { parseQuerySyntax, type QueryComparisonNode } from "./querySyntax";

function expectRemovalFirstRoot(source: string, expected: { query: string; caret: number }) {
  const node = parseQuerySyntax(source).roots[0];
  expect(node?.kind).toBe("comparison");
  expect(removeQueryNode(source, node)).toEqual(expected);
}

test("removeQueryNode removes a standalone comparison with a valid caret", () => {
  expectRemovalFirstRoot("tenant=42", { query: "", caret: 0 });
  expectRemovalFirstRoot("level!=ERROR", { query: "", caret: 0 });
  expectRemovalFirstRoot('message~"😀 OR ="', { query: "", caret: 0 });
  expectRemovalFirstRoot("message!~'warning'", { query: "", caret: 0 });
});

test("removeQueryNode preserves text outside the standalone comparison", () => {
  expectRemovalFirstRoot(" \ttenant = 42\n ", { query: " \t\n ", caret: 2 });
  expectRemovalFirstRoot('"😀" ? tenant=42 | count', { query: '"😀" ?  | count', caret: 7 });
  expectRemovalFirstRoot("[-15m, now] tenant=42 | count", {
    query: "[-15m, now]  | count",
    caret: 12,
  });
});

test("removeQueryNode removes either implicit operand and all chain positions", () => {
  expectOperandRemoval("tenant=42 level=ERROR", 0, { query: "level=ERROR", caret: 0 });
  expectOperandRemoval("tenant=42 level=ERROR", 10, { query: "tenant=42", caret: 9 });
  expectOperandRemoval("a=1 b=2 c=3", 0, { query: "b=2 c=3", caret: 0 });
  expectOperandRemoval("a=1 b=2 c=3", 4, { query: "a=1 c=3", caret: 3 });
  expectOperandRemoval("a=1 b=2 c=3", 8, { query: "a=1 b=2", caret: 7 });
  expectOperandRemoval("a=1 a=1 a=1", 4, { query: "a=1 a=1", caret: 3 });
});

test("removeQueryNode follows implicit ownership through groups and mixed operators", () => {
  expectOperandRemoval("a=1 OR b=2 c=3", 7, { query: "a=1 OR c=3", caret: 7 });
  expectOperandRemoval("a=1 OR b=2 c=3", 0, { query: "b=2 c=3", caret: 0 });
  expectOperandRemoval("a=1 b=2 OR c=3", 4, { query: "a=1 OR c=3", caret: 3 });
  expectOperandRemoval("a=1 (b=2 c=3)", 5, { query: "a=1 (c=3)", caret: 5 });
  expectOperandRemoval("((a=1)) b=2", 2, { query: "b=2", caret: 0 });
  expectGroupRemoval("(a=1 OR b=2) c=3", "(a=1 OR b=2)", { query: "c=3", caret: 0 });
  expectGroupRemoval("a=1 (b=2 OR c=3)", "(b=2 OR c=3)", { query: "a=1", caret: 3 });
});

test("removeQueryNode keeps implicit removal within structural regions", () => {
  const source = "[-15m, now] {tenant=42 env=prod} level=ERROR service=api | count";
  expectOperandRemoval(source, source.indexOf("tenant"), {
    query: "[-15m, now] {env=prod} level=ERROR service=api | count",
    caret: 13,
  });
  expectOperandRemoval(source, source.indexOf("env"), {
    query: "[-15m, now] {tenant=42} level=ERROR service=api | count",
    caret: 22,
  });
  expectOperandRemoval(source, source.indexOf("level"), {
    query: "[-15m, now] {tenant=42 env=prod} service=api | count",
    caret: 33,
  });
  expectOperandRemoval(source, source.indexOf("service"), {
    query: "[-15m, now] {tenant=42 env=prod} level=ERROR | count",
    caret: 44,
  });
  expectRemovalFirstRoot("[-15m, now] {tenant=42} level=ERROR", {
    query: "[-15m, now] {} level=ERROR",
    caret: 13,
  });
  expectRemovalFirstRoot("{tenant=42} level=ERROR | count", {
    query: "{} level=ERROR | count",
    caret: 1,
  });
});

function expectOperandRemoval(
  source: string,
  start: number,
  expected: { query: string; caret: number },
) {
  const syntax = parseQuerySyntax(source);
  const node = [...syntax.parents.keys()].find(
    (node) => node.kind === "comparison" && node.start === start,
  );
  expect(node).toBeDefined();
  expect(removeQueryNode(source, node!)).toEqual(expected);
}

test("removeQueryNode removes either operand with its explicit parent operator", () => {
  for (const operator of ["AND", "OR", "aNd", "oR"]) {
    const source = `tenant=42 ${operator} level=ERROR`;
    expectOperandRemoval(source, 0, { query: "level=ERROR", caret: 0 });
    expectOperandRemoval(source, source.indexOf("level"), { query: "tenant=42", caret: 9 });
  }
});

test("removeQueryNode handles all explicit chain positions and repeated comparisons", () => {
  for (const operator of ["AND", "OR"]) {
    const source = `a=1 ${operator} b=2 ${operator} c=3`;
    expectOperandRemoval(source, 0, { query: `b=2 ${operator} c=3`, caret: 0 });
    expectOperandRemoval(source, source.indexOf("b=2"), {
      query: `a=1 ${operator} c=3`,
      caret: 3,
    });
    expectOperandRemoval(source, source.indexOf("c=3"), {
      query: `a=1 ${operator} b=2`,
      caret: source.indexOf("b=2") + 3,
    });

    const repeated = `a=1 ${operator} a=1 ${operator} a=1`;
    expectOperandRemoval(repeated, 0, { query: `a=1 ${operator} a=1`, caret: 0 });
    expectOperandRemoval(repeated, repeated.indexOf("a=1", 1), {
      query: `a=1 ${operator} a=1`,
      caret: 3,
    });
    expectOperandRemoval(repeated, repeated.lastIndexOf("a=1"), {
      query: `a=1 ${operator} a=1`,
      caret: repeated.lastIndexOf("a=1") - operator.length - 2,
    });
  }
});

test("removeQueryNode follows mixed precedence rather than adjacent operators", () => {
  expectOperandRemoval("a=1 OR b=2 AND c=3", 7, { query: "a=1 OR c=3", caret: 7 });
  expectOperandRemoval("a=1 OR b=2 AND c=3", 15, { query: "a=1 OR b=2", caret: 10 });
  expectOperandRemoval("a=1 AND b=2 OR c=3", 8, { query: "a=1 OR c=3", caret: 3 });
  expectOperandRemoval("a=1 AND b=2 OR c=3", 0, { query: "b=2 OR c=3", caret: 0 });
});

test("removeQueryNode preserves source outside the operand and operator interval", () => {
  expectOperandRemoval(" \ta=1  AND\t b=2\n | count", 2, {
    query: " \tb=2\n | count",
    caret: 2,
  });
  expectOperandRemoval(" \ta=1  AND\t b=2\n | count", 12, {
    query: " \ta=1\n | count",
    caret: 5,
  });
  expectOperandRemoval('"😀"=1 OR b=2 AND c=3', 10, {
    query: '"😀"=1 OR c=3',
    caret: 10,
  });
});

test("removeQueryNode removes singleton groups and redundant nesting with their comparison", () => {
  expectOperandRemoval("(a=1)", 1, { query: "", caret: 0 });
  expectOperandRemoval("(((a=1)))", 3, { query: "", caret: 0 });
  expectOperandRemoval(" \t( ( a=1 ) )\n | count", 6, {
    query: " \t\n | count",
    caret: 2,
  });
});

test("removeQueryNode promotes siblings when singleton groups become empty", () => {
  expectOperandRemoval("a=1 AND ((b=2))", 10, { query: "a=1", caret: 3 });
  expectOperandRemoval("((a=1)) OR b=2", 2, { query: "b=2", caret: 0 });
  expectOperandRemoval("((a=1) AND (b=2))", 2, { query: "((b=2))", caret: 1 });
  expectOperandRemoval("((a=1) AND (b=2))", 12, { query: "((a=1))", caret: 6 });
});

test("removeQueryNode preserves balanced surviving groups and surrounding regions", () => {
  expectOperandRemoval("a=1 AND (b=2 OR c=3)", 9, {
    query: "a=1 AND (c=3)",
    caret: 9,
  });
  expectOperandRemoval("a=1 AND (b=2 OR c=3)", 16, {
    query: "a=1 AND (b=2)",
    caret: 12,
  });
  expectOperandRemoval("((a=1 OR (b=2 AND c=3)))", 10, {
    query: "((a=1 OR (c=3)))",
    caret: 10,
  });
  expectOperandRemoval("[-15m, now] {(tenant=42)} level=ERROR | count", 14, {
    query: "[-15m, now] {} level=ERROR | count",
    caret: 13,
  });
});

function expectGroupRemoval(
  source: string,
  expression: string,
  expected: { query: string; caret: number },
) {
  const syntax = parseQuerySyntax(source);
  const start = source.indexOf(expression);
  const node = [...syntax.roots, ...syntax.parents.keys()].find(
    (node) =>
      node.kind === "group" && node.start === start && node.end === start + expression.length,
  );
  expect(node).toBeDefined();
  expect(removeQueryNode(source, node!)).toEqual(expected);
}

test("removeQueryNode rejects Boolean nodes, including inside groups", () => {
  for (const source of ["a=1 AND b=2", "a=1 OR b=2 AND c=3", "(a=1 OR b=2)", "a=1 b=2"]) {
    const syntax = parseQuerySyntax(source);
    for (const node of [...syntax.roots, ...syntax.parents.keys()]) {
      if (node.kind === "and" || node.kind === "or") {
        expect(removeQueryNode(source, node)).toBeNull();
      }
    }
  }
});

test("removeQueryNode removes complete parenthesized groups", () => {
  for (const expression of ["a=1", "a=1 AND b=2", "a=1 OR b=2 AND c=3"]) {
    const group = `(${expression})`;
    expectGroupRemoval(group, group, { query: "", caret: 0 });
    expectGroupRemoval(` \t${group}\n | count`, group, {
      query: " \t\n | count",
      caret: 2,
    });
    expectGroupRemoval(`(${group})`, group, { query: "", caret: 0 });
  }
});

test("removeQueryNode promotes siblings when deleting a group operand", () => {
  expectGroupRemoval("a=1 AND ((b=2 OR c=3))", "(b=2 OR c=3)", {
    query: "a=1",
    caret: 3,
  });
  expectGroupRemoval("((a=1 OR b=2)) AND c=3", "(a=1 OR b=2)", {
    query: "c=3",
    caret: 0,
  });
  expectGroupRemoval("(a=1 OR (b=2 AND c=3))", "(b=2 AND c=3)", {
    query: "(a=1)",
    caret: 4,
  });
  expectOperandRemoval("a=1 OR b=2 AND c=3", 0, {
    query: "b=2 AND c=3",
    caret: 0,
  });
});

test("removeQueryNode preserves regions and trivia outside group removal", () => {
  expectGroupRemoval("[-15m, now] {(a=1 AND b=2)} level=ERROR | count", "(a=1 AND b=2)", {
    query: "[-15m, now] {} level=ERROR | count",
    caret: 13,
  });
  expectGroupRemoval(' \t"😀"=1 OR (b=2 AND c=3)\n | count', "(b=2 AND c=3)", {
    query: ' \t"😀"=1\n | count',
    caret: 8,
  });
});

test("removeQueryNode preserves adjacent raw comments and standalone trivia", () => {
  expectRemovalFirstRoot("// before\n\ta=1\t// after\n", {
    query: "// before\n\t\t// after\n",
    caret: 11,
  });
  expectOperandRemoval("a=1\tAND\nb=2 // trailing\n | count", 0, {
    query: "b=2 // trailing\n | count",
    caret: 0,
  });
  expectOperandRemoval("a=1\tAND\nb=2 // trailing\n | count", 8, {
    query: "a=1 // trailing\n | count",
    caret: 3,
  });
  expectRemovalFirstRoot("a=1 // note\n AND b=2", {
    query: " // note\n AND b=2",
    caret: 0,
  });
});

test("removeQueryNode uses only proven local ownership in malformed input", () => {
  expectRemovalFirstRoot("a=1 AND b=", { query: " AND b=", caret: 0 });
  expectRemovalFirstRoot("a=1 AND (b=2", { query: " AND (b=2", caret: 0 });
  expectRemovalFirstRoot("a=1) AND b=2", { query: ") AND b=2", caret: 0 });
  expectRemovalFirstRoot("a=1 ? b=2 | opaque {c=3}", {
    query: "a=1 ?  | opaque {c=3}",
    caret: 6,
  });
  expectRemovalFirstRoot("a=1 [invalid] b=2", {
    query: " [invalid] b=2",
    caret: 0,
  });
});

test("removeQueryNode rejects ambiguous comparison fragments", () => {
  const node = parseQuerySyntax("a=1").roots[0];
  for (const source of ["a=1=2", "a=1!=2", "a=1=", "a=1 !~", "(a=1"]) {
    expect(removeQueryNode(source, node)).toBeNull();
  }
});

test("removeQueryNode rejects targets absent from the parsed source", () => {
  const node = parseQuerySyntax("tenant=42").roots[0] as QueryComparisonNode;
  expect(removeQueryNode("", node)).toBeNull();
  expect(removeQueryNode("(tenant=42", node)).toBeNull();
  expect(removeQueryNode("| tenant=42", { ...node, start: 2, end: 11 })).toBeNull();
});
