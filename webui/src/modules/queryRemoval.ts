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
  const node = syntax.roots.find(
    (root) => root.start === target.start && root.end === target.end && root.kind === target.kind,
  );
  if (!node) return null;

  return {
    query: source.slice(0, node.start) + source.slice(node.end),
    caret: node.start,
  };
}
