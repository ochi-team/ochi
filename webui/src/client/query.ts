export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };
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
        super(locs[0]?.msg ?? 'Query syntax error');
        this.name = 'QuerySyntaxError';
        this.locs = locs;
    }
}

type QueryLogsParams = {
    query: string;
    signal?: AbortSignal;
    tenantID?: string;
};

const queryEndpoint = '/query';

function isJsonRecord(value: unknown): value is LogEntry {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseQueryResponse(value: unknown, status: number): QueryResponse {
    if (!isJsonRecord(value) || !Array.isArray(value.lines) || !value.lines.every(isJsonRecord)) {
        throw new Error('Unexpected query response shape');
    }

    return { lines: value.lines, status };
}

function isQuerySyntaxErrorLoc(value: unknown): value is QuerySyntaxErrorLoc {
    return (
        isJsonRecord(value) &&
        typeof value.line === 'number' &&
        typeof value.col === 'number' &&
        typeof value.msg === 'string'
    );
}

function parseQueryErrorResponse(value: unknown): Error | null {
    if (
        isJsonRecord(value) &&
        value.code === 'QUERY_SYNTAX' &&
        isJsonRecord(value.meta) &&
        Array.isArray(value.meta.locs) &&
        value.meta.locs.every(isQuerySyntaxErrorLoc)
    ) {
        return new QuerySyntaxError(value.meta.locs);
    }

    return null;
}

export function buildLoqlQuery(timeRangeQueryToken: string, query: string): string {
    return `[-${timeRangeQueryToken}, now] ${query}`;
}

export function loqlQueryPrefix(timeRangeQueryToken: string): string {
    return `[-${timeRangeQueryToken}, now] `;
}

export async function queryLogs(params: QueryLogsParams): Promise<QueryResponse> {
    const response = await fetch(queryEndpoint, {
        method: 'POST',
        headers: {
            'content-type': 'application/loql',
        },
        body: params.query,
        signal: params.signal,
    });

    if (!response.ok) {
        const body = await response.text();
        try {
            const parsedError = parseQueryErrorResponse(JSON.parse(body));
            if (parsedError) throw parsedError;
        } catch (err) {
            if (err instanceof QuerySyntaxError) throw err;
        }
        throw new Error(body || `Query failed with HTTP ${response.status}`);
    }

    return parseQueryResponse(await response.json(), response.status);
}
