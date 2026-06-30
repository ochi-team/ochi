export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };
export type LogEntry = Record<string, JsonValue>;

type QueryResponse = {
    lines: LogEntry[];
};

type QueryLogsParams = {
    query: string;
    signal?: AbortSignal;
    tenantID?: string;
};

const queryEndpoint = '/query';

function isJsonRecord(value: unknown): value is LogEntry {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseQueryResponse(value: unknown): QueryResponse {
    if (!isJsonRecord(value) || !Array.isArray(value.lines) || !value.lines.every(isJsonRecord)) {
        throw new Error('Unexpected query response shape');
    }

    return { lines: value.lines };
}

export function buildLoqlQuery(timeRangeQueryToken: string, query: string): string {
    return `[-${timeRangeQueryToken}, now] ${query}`;
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
        throw new Error(body || `Query failed with HTTP ${response.status}`);
    }

    return parseQueryResponse(await response.json());
}
