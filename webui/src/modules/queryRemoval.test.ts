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

test("removeQueryNode does not remove implicit operands or unsupported nodes", () => {
  for (const source of ["a=1 b=2", "(a=1)"]) {
    const syntax = parseQuerySyntax(source);
    expect(removeQueryNode(source, syntax.roots[0])).toBeNull();
    for (const node of syntax.parents.keys()) {
      expect(removeQueryNode(source, node)).toBeNull();
    }
  }
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

test("removeQueryNode keeps conjunction subtree removal deferred", () => {
  for (const source of ["a=1 AND b=2", "a=1 OR b=2 AND c=3"]) {
    const syntax = parseQuerySyntax(source);
    expect(removeQueryNode(source, syntax.roots[0])).toBeNull();
    for (const node of syntax.parents.keys()) {
      if (node.kind !== "comparison") expect(removeQueryNode(source, node)).toBeNull();
    }
  }
});

test("removeQueryNode rejects targets absent from the parsed source", () => {
  const node = parseQuerySyntax("tenant=42").roots[0] as QueryComparisonNode;
  expect(removeQueryNode("", node)).toBeNull();
  expect(removeQueryNode("(tenant=42", node)).toBeNull();
  expect(removeQueryNode("| tenant=42", { ...node, start: 2, end: 11 })).toBeNull();
});
