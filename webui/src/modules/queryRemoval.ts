import { parseQuerySyntax, type QueryExpressionNode } from "./querySyntax";

export interface QueryRemovalResult {
  query: string;
  caret: number;
}

export function removeQueryNode(
  source: string,
  target: QueryExpressionNode,
): QueryRemovalResult | null {
  if (target.kind === "and" || target.kind === "or") return null;

  const syntax = parseQuerySyntax(source);
  let node = [...syntax.roots, ...syntax.parents.keys()].find(
    (node) => node.start === target.start && node.end === target.end && node.kind === target.kind,
  );
  if (!node) return null;

  let parent = syntax.parents.get(node);
  while (parent?.kind === "group") {
    node = parent;
    parent = syntax.parents.get(node);
  }

  let start = node.start;
  let end = node.end;
  if (parent) {
    if (!parent.operator) return null;

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
