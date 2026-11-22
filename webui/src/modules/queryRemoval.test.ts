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

test("removeQueryNode does not remove owned operands or unsupported nodes", () => {
  for (const source of ["a=1 AND b=2", "a=1 b=2", "(a=1)"]) {
    const syntax = parseQuerySyntax(source);
    expect(removeQueryNode(source, syntax.roots[0])).toBeNull();
    for (const node of syntax.parents.keys()) {
      expect(removeQueryNode(source, node)).toBeNull();
    }
  }
});

test("removeQueryNode rejects targets absent from the parsed source", () => {
  const node = parseQuerySyntax("tenant=42").roots[0] as QueryComparisonNode;
  expect(removeQueryNode("", node)).toBeNull();
  expect(removeQueryNode("(tenant=42", node)).toBeNull();
  expect(removeQueryNode("| tenant=42", { ...node, start: 2, end: 11 })).toBeNull();
});
