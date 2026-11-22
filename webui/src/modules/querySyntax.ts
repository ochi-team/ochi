// TODO: revisit the module if the parsing could be repeated using a grammar file,
// so we don't duplicate the language implementation in both js and zig,
// but rather apply a simpelr algorithm walking through the query and its grammar
export type TokenKind =
  | "literal"
  | "unterminatedLiteral"
  | "whitespace"
  | "equal"
  | "notEqual"
  | "matchRegex"
  | "notMatchRegex"
  | "and"
  | "or"
  | "leftSquareBracket"
  | "rightSquareBracket"
  | "leftCurlyBracket"
  | "rightCurlyBracket"
  | "leftParenthesis"
  | "rightParenthesis"
  | "comma"
  | "pipe"
  | "unknown";

const maxGroupDepth = 128;

const comparisonTokens: TokenKind[] = ["equal", "notEqual", "matchRegex", "notMatchRegex"];

const punctuation = new Map<string, TokenKind>([
  ["[", "leftSquareBracket"],
  ["]", "rightSquareBracket"],
  ["{", "leftCurlyBracket"],
  ["}", "rightCurlyBracket"],
  ["(", "leftParenthesis"],
  [")", "rightParenthesis"],
  [",", "comma"],
  ["|", "pipe"],
  ["=", "equal"],
  ["~", "matchRegex"],
]);

const literalCharacter = /[a-zA-Z0-9_\-:/@.+]/;
const whitespaceCharacter = /[ \t\n]/;

export interface QuerySourceRange {
  start: number;
  end: number;
}

export interface QueryToken extends QuerySourceRange {
  kind: TokenKind;
}

export interface QueryComparisonNode extends QuerySourceRange {
  kind: "comparison";
  key: QuerySourceRange;
  operator: QuerySourceRange;
  value: QuerySourceRange;
}

export interface QueryBooleanNode extends QuerySourceRange {
  kind: "and" | "or";
  // null is implicit conjunctions
  operator: QuerySourceRange | null;
  left: QueryExpressionNode;
  right: QueryExpressionNode;
}

export interface QueryGroupNode extends QuerySourceRange {
  kind: "group";
  open: QuerySourceRange;
  close: QuerySourceRange;
  expression: QueryExpressionNode;
}

export type QueryExpressionNode = QueryComparisonNode | QueryBooleanNode | QueryGroupNode;

export interface QueryTimeRangeRegion extends QuerySourceRange {
  kind: "timeRange";
  open: QuerySourceRange;
  close: QuerySourceRange;
}

export interface QueryTagBlockRegion extends QuerySourceRange {
  kind: "tagBlock";
  open: QuerySourceRange;
  close: QuerySourceRange;
  roots: QueryExpressionNode[];
}

export interface QueryExpressionRegion extends QuerySourceRange {
  kind: "queryExpression";
  roots: QueryExpressionNode[];
}

export interface QueryPipeSuffixRegion extends QuerySourceRange {
  kind: "pipeSuffix";
}

export type QueryRegion =
  | QueryTimeRangeRegion
  | QueryTagBlockRegion
  | QueryExpressionRegion
  | QueryPipeSuffixRegion;

export interface QuerySyntax {
  regions: QueryRegion[];
  roots: QueryExpressionNode[];
  parents: Map<QueryExpressionNode, QueryBooleanNode | QueryGroupNode>;
}

function operandAt(
  index: number,
  tokens: QueryToken[],
  groups: Map<number, QueryGroupNode>,
  comparisons: Map<number, QueryComparisonNode>,
): QueryExpressionNode | undefined {
  return groups.get(index) ?? comparisons.get(tokens[index]?.start);
}

interface BooleanOperator {
  kind: "and" | "or";
  range: QuerySourceRange | null;
}

function reduce(operators: BooleanOperator[], operands: QueryExpressionNode[]): void {
  const operator = operators.pop()!;
  const right = operands.pop()!;
  const left = operands.pop()!;
  const node: QueryBooleanNode = {
    kind: operator.kind,
    start: left.start,
    end: right.end,
    operator: operator.range,
    left,
    right,
  };
  operands.push(node);
}

function parseRange(
  start: number,
  end: number,
  tokens: QueryToken[],
  groups: Map<number, QueryGroupNode>,
  comparisons: Map<number, QueryComparisonNode>,
): QueryExpressionNode[] {
  const roots: QueryExpressionNode[] = [];
  let index = start;
  while (index < end) {
    const first = operandAt(index, tokens, groups, comparisons);
    if (!first) {
      index++;
      continue;
    }
    const operands: QueryExpressionNode[] = [first];
    const operators: BooleanOperator[] = [];
    while (index < end && tokens[index].end <= first.end) index++;

    while (index < end) {
      const token = tokens[index];
      const explicit = token.kind === "and" || token.kind === "or";
      const rightIndex = explicit ? index + 1 : index;
      const right =
        rightIndex < end ? operandAt(rightIndex, tokens, groups, comparisons) : undefined;
      if (!right) break;

      // rhs of implicit is "and"
      const operator: BooleanOperator = {
        kind: token.kind === "or" ? "or" : "and",
        range: explicit ? { start: token.start, end: token.end } : null,
      };

      // Reduce equal precedence for left associativity; AND binds tighter than OR.
      while (
        operators.length &&
        (operators[operators.length - 1].kind === "and" || operator.kind === "or")
      ) {
        reduce(operators, operands);
      }

      operators.push(operator);
      operands.push(right);
      index = rightIndex;
      while (index < end && tokens[index].end <= right.end) index++;
    }

    while (operators.length) reduce(operators, operands);
    roots.push(operands[0]);
  }
  return roots;
}

export function parseQuerySyntax(source: string): QuerySyntax {
  const tokens = scanQuery(source).filter((it) => it.kind !== "whitespace");
  const comparisons = new Map(recognizeQueryComparisons(source).map((node) => [node.start, node]));
  const regions = recognizeRegions(tokens, comparisons, source.length);
  const roots = regions.flatMap((region) => ("roots" in region ? region.roots : []));
  const parents = new Map<QueryExpressionNode, QueryBooleanNode | QueryGroupNode>();
  const pending = [...roots];
  while (pending.length) {
    const node = pending.pop()!;
    if (node.kind === "comparison") continue;
    const children = node.kind === "group" ? [node.expression] : [node.left, node.right];
    for (const child of children) {
      parents.set(child, node);
      pending.push(child);
    }
  }
  return { regions, roots, parents };
}

export function scanQuery(source: string): QueryToken[] {
  const tokens: QueryToken[] = [];
  let offset = 0;

  while (offset < source.length) {
    const start = offset;
    const char = source[offset];
    let kind: TokenKind;

    if (whitespaceCharacter.test(char)) {
      kind = "whitespace";
      while (offset < source.length && whitespaceCharacter.test(source[offset])) offset++;
    } else if (char === '"' || char === "'") {
      kind = "unterminatedLiteral";
      offset++;
      while (offset < source.length) {
        const current = source[offset++];
        if (current === "\\") {
          // A backslash escapes exactly one UTF-16 unit, including another backslash.
          if (offset < source.length) offset++;
        } else if (current === char) {
          kind = "literal";
          break;
        }
      }
    } else if (literalCharacter.test(char)) {
      while (offset < source.length && literalCharacter.test(source[offset])) offset++;
      const word = source.slice(start, offset).toLowerCase();
      kind = word === "and" ? "and" : word === "or" ? "or" : "literal";
    } else if (char === "!" && (source[offset + 1] === "=" || source[offset + 1] === "~")) {
      kind = source[offset + 1] === "=" ? "notEqual" : "notMatchRegex";
      offset += 2;
    } else {
      kind = punctuation.get(char) ?? "unknown";
      offset += 1;
    }

    tokens.push({ kind, start, end: offset });
  }
  return tokens;
}

function isStructuralDelimiter(kind: TokenKind): boolean {
  return (
    kind === "leftSquareBracket" ||
    kind === "rightSquareBracket" ||
    kind === "leftCurlyBracket" ||
    kind === "rightCurlyBracket"
  );
}

function blockEnd(tokens: QueryToken[], start: number, close: TokenKind): number {
  for (let index = start + 1; index < tokens.length; index++) {
    const kind = tokens[index].kind;
    if (kind === close) return index;
    // fast path: if it's not close, but another block - query is invalid
    if (isStructuralDelimiter(kind)) return -1;
  }
  return -1;
}

function recognizeRegions(
  tokens: QueryToken[],
  comparisons: Map<number, QueryComparisonNode>,
  sourceEnd: number,
): QueryRegion[] {
  const regions: QueryRegion[] = [];
  const pipeIndex = tokens.findIndex((token) => token.kind === "pipe");
  const prefix = pipeIndex < 0 ? tokens : tokens.slice(0, pipeIndex);
  let index = 0;

  if (prefix[index]?.kind === "leftSquareBracket") {
    const end = blockEnd(prefix, index, "rightSquareBracket");
    if (end < 0) index = prefix.length;
    else {
      const open = { start: prefix[index].start, end: prefix[index].end };
      const close = { start: prefix[end].start, end: prefix[end].end };
      regions.push({ kind: "timeRange", start: open.start, end: close.end, open, close });
      index = end + 1;
    }
  }

  if (prefix[index]?.kind === "leftCurlyBracket") {
    const end = blockEnd(prefix, index, "rightCurlyBracket");
    if (end < 0) index = prefix.length;
    else {
      const open = { start: prefix[index].start, end: prefix[index].end };
      const close = { start: prefix[end].start, end: prefix[end].end };
      regions.push({
        kind: "tagBlock",
        start: open.start,
        end: close.end,
        open,
        close,
        roots: parseRegion(prefix.slice(index + 1, end), comparisons),
      });
      index = end + 1;
    }
  }

  const start = index;
  // A late/repeated block is not another region; do not resume expression ownership beyond it.
  while (index < prefix.length && !isStructuralDelimiter(prefix[index].kind)) index++;
  if (index > start) {
    regions.push({
      kind: "queryExpression",
      start: prefix[start].start,
      end: prefix[index - 1].end,
      roots: parseRegion(prefix.slice(start, index), comparisons),
    });
  }

  if (pipeIndex >= 0) {
    regions.push({ kind: "pipeSuffix", start: tokens[pipeIndex].start, end: sourceEnd });
  }
  return regions;
}

function parseRegion(
  tokens: QueryToken[],
  comparisons: Map<number, QueryComparisonNode>,
): QueryExpressionNode[] {
  const groups = new Map<number, QueryGroupNode>();

  // Unclosed groups cannot establish safe ownership, including within their tail.
  // A depth overflow conservatively disables expression targets for this region.
  const unmatched: number[] = [];
  for (let index = 0; index < tokens.length; index++) {
    if (tokens[index].kind === "leftParenthesis") {
      unmatched.push(index);
      if (unmatched.length > maxGroupDepth) return [];
    } else if (tokens[index].kind === "rightParenthesis" && unmatched.length) {
      unmatched.pop();
    }
  }
  if (unmatched.length) tokens = tokens.slice(0, unmatched[0]);

  const openings: number[] = [];
  for (let index = 0; index < tokens.length; index++) {
    if (tokens[index].kind === "leftParenthesis") {
      openings.push(index);
      continue;
    }

    if (tokens[index].kind === "rightParenthesis" && openings.length) {
      const start = openings.pop()!;
      const expressions = parseRange(start + 1, index, tokens, groups, comparisons);
      const expression = expressions[0];
      // skip this group unless its inner is exactly one expression
      if (
        expressions.length !== 1 ||
        expression.start !== tokens[start + 1].start ||
        expression.end !== tokens[index - 1].end
      )
        continue;
      const open = tokens[start];
      const close = tokens[index];
      groups.set(start, {
        kind: "group",
        start: open.start,
        end: close.end,
        open: { start: open.start, end: open.end },
        close: { start: close.start, end: close.end },
        expression,
      });
    }
  }

  return parseRange(0, tokens.length, tokens, groups, comparisons);
}

export function recognizeQueryComparisons(source: string): QueryComparisonNode[] {
  const tokens = scanQuery(source).filter((token) => token.kind !== "whitespace");
  const nodes: QueryComparisonNode[] = [];

  for (let index = 0; index + 2 < tokens.length; index++) {
    const key = tokens[index];
    const operator = tokens[index + 1];
    const value = tokens[index + 2];
    if (key.kind !== "literal" || value.kind !== "literal") continue;
    if (!comparisonTokens.includes(operator.kind)) continue;

    nodes.push({
      kind: "comparison",
      start: key.start,
      end: value.end,
      key: { start: key.start, end: key.end },
      operator: { start: operator.start, end: operator.end },
      value: { start: value.start, end: value.end },
    });
    index += 2;
  }
  return nodes;
}
