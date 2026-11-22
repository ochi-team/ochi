import { parseQuerySyntax, type QueryExpressionNode } from "./querySyntax";

export interface QueryRemovalResult {
  query: string;
  caret: number;
}

export function removeQueryNode(
  source: string,
  target: QueryExpressionNode,
): QueryRemovalResult | null {
  if (target.kind !== "comparison") return null;

  const syntax = parseQuerySyntax(source);
  const node = [...syntax.roots, ...syntax.parents.keys()].find(
    (node) => node.start === target.start && node.end === target.end && node.kind === target.kind,
  );
  if (!node) return null;

  let start = node.start;
  let end = node.end;
  const parent = syntax.parents.get(node);
  if (parent) {
    if (parent.kind === "group" || !parent.operator) return null;

    if (parent.left === node) {
      end = parent.right.start;
    } else {
      start = parent.left.end;
    }
  }

  return {
    query: source.slice(0, start) + source.slice(end),
    caret: start,
  };
}
