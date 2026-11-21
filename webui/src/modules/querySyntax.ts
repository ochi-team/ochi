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
  operator: QuerySourceRange;
  left: QueryExpressionNode;
  right: QueryExpressionNode;
}

export type QueryExpressionNode = QueryComparisonNode | QueryBooleanNode;

export interface QuerySyntax {
  roots: QueryExpressionNode[];
  parents: Map<QueryExpressionNode, QueryBooleanNode>;
}

export function parseQuerySyntax(source: string): QuerySyntax {
  const tokens = scanQuery(source).filter((token) => token.kind !== "whitespace");
  const comparisons = new Map(recognizeQueryComparisons(source).map((node) => [node.start, node]));

  const roots: QueryExpressionNode[] = [];
  const parents = new Map<QueryExpressionNode, QueryBooleanNode>();

  let index = 0;
  while (index < tokens.length) {
    const first = comparisons.get(tokens[index].start);
    if (!first) {
      index++;
      continue;
    }
    const operands: QueryExpressionNode[] = [first];
    const operators: QueryToken[] = [];
    while (index < tokens.length && tokens[index].end <= first.end) index++;

    const reduce = () => {
      const operator = operators.pop()!;
      const right = operands.pop()!;
      const left = operands.pop()!;
      const node: QueryBooleanNode = {
        kind: operator.kind as "and" | "or",
        start: left.start,
        end: right.end,
        operator: { start: operator.start, end: operator.end },
        left,
        right,
      };
      parents.set(left, node);
      parents.set(right, node);
      operands.push(node);
    };

    while (index + 1 < tokens.length) {
      const operator = tokens[index];
      if (operator.kind !== "and" && operator.kind !== "or") break;

      const right = comparisons.get(tokens[index + 1].start);
      if (!right) break;

      // Reduce equal precedence for left associativity; AND binds tighter than OR.
      while (
        operators.length &&
        (operators[operators.length - 1].kind === "and" || operator.kind === "or")
      ) {
        reduce();
      }
      operators.push(operator);
      operands.push(right);
      index++;
      while (index < tokens.length && tokens[index].end <= right.end) index++;
    }

    while (operators.length) reduce();
    roots.push(operands[0]);
  }
  return { roots, parents };
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
