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

export interface QueryToken {
  kind: TokenKind;
  start: number;
  end: number;
}

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
