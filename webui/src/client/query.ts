export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };
export type LogEntry = Record<string, JsonValue>;

type QueryResponse = {
  lines: LogEntry[];
  status: number;
};

export type QuerySyntaxErrorLoc = {
  line: number;
  col: number;
  msg: string;
};

export class QuerySyntaxError extends Error {
  readonly locs: QuerySyntaxErrorLoc[];

  constructor(locs: QuerySyntaxErrorLoc[]) {
    super(locs[0]?.msg ?? "Query syntax error");
    this.name = "QuerySyntaxError";
    this.locs = locs;
  }
}

type QueryLogsParams = {
  query: string;
  signal?: AbortSignal;
  tenantID?: string;
};

const queryEndpoint = `${import.meta.env.OCHI_HOST ?? ""}/query`;

export function buildLoqlQuery(timeRangeQueryToken: string, query: string): string {
  return `[${timeRangeQueryToken}, now] ${query}`;
}

export async function queryLogs(params: QueryLogsParams): Promise<QueryResponse> {
  const response = await fetch(queryEndpoint, {
    method: "POST",
    headers: {
      "content-type": "application/loql",
    },
    body: params.query,
    signal: params.signal,
  });

  if (!response.ok) {
    try {
      const body = await response.json();
      const parsedError = parseQueryErrorResponse(body);
      if (parsedError) throw parsedError;
      throw new Error(`Query failed with HTTP ${response.status}: ${body}`);
    } catch (err) {
      if (err instanceof QuerySyntaxError) throw err;

      console.error("Query request failed", err);
      throw new Error("Unexpected error", { cause: err });
    }
  }

  return parseQueryLogsResponse(await response.json());
}

function parseQueryLogsResponse(obj: object): QueryResponse {
  return {
    lines: [],
    status: 1,
  };
}

function parseQueryErrorResponse(obj: object): Error {
  return new Error("unexpected error");
}
