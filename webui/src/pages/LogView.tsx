import type { Component } from 'solid-js';
import { For } from 'solid-js';

type Level = 'info' | 'debug' | 'warn' | 'error';

type EventBucket = {
    error: number;
    info: number;
    warn: number;
    debug: number;
};

type LogEntry = {
    time: string;
    level: Level;
    message: string;
};

const eventBuckets: EventBucket[] = [
    { error: 4, info: 58, warn: 4, debug: 2 },
    { error: 18, info: 32, warn: 8, debug: 3 },
    { error: 5, info: 62, warn: 3, debug: 2 },
    { error: 0, info: 77, warn: 3, debug: 1 },
    { error: 13, info: 44, warn: 7, debug: 4 },
    { error: 5, info: 60, warn: 5, debug: 3 },
    { error: 0, info: 78, warn: 4, debug: 2 },
    { error: 0, info: 36, warn: 2, debug: 2 },
    { error: 35, info: 0, warn: 5, debug: 4 },
    { error: 10, info: 33, warn: 5, debug: 2 },
    { error: 16, info: 50, warn: 6, debug: 3 },
    { error: 3, info: 58, warn: 3, debug: 2 },
    { error: 0, info: 24, warn: 3, debug: 1 },
    { error: 0, info: 66, warn: 5, debug: 2 },
    { error: 0, info: 31, warn: 3, debug: 2 },
    { error: 0, info: 33, warn: 5, debug: 2 },
    { error: 0, info: 0, warn: 0, debug: 0 },
    { error: 0, info: 78, warn: 5, debug: 2 },
    { error: 0, info: 48, warn: 12, debug: 3 },
    { error: 0, info: 64, warn: 7, debug: 4 },
    { error: 0, info: 71, warn: 10, debug: 4 },
    { error: 29, info: 34, warn: 7, debug: 5 },
    { error: 0, info: 39, warn: 2, debug: 2 },
    { error: 0, info: 32, warn: 2, debug: 2 },
    { error: 0, info: 39, warn: 3, debug: 2 },
    { error: 0, info: 83, warn: 7, debug: 3 },
    { error: 0, info: 41, warn: 4, debug: 3 },
    { error: 0, info: 71, warn: 4, debug: 4 },
    { error: 0, info: 78, warn: 5, debug: 2 },
    { error: 28, info: 40, warn: 5, debug: 2 },
    { error: 0, info: 76, warn: 7, debug: 2 },
    { error: 0, info: 47, warn: 4, debug: 2 },
    { error: 0, info: 0, warn: 0, debug: 0 },
    { error: 0, info: 14, warn: 2, debug: 1 },
    { error: 0, info: 18, warn: 2, debug: 1 },
    { error: 0, info: 0, warn: 0, debug: 0 },
    { error: 0, info: 77, warn: 5, debug: 3 },
    { error: 0, info: 26, warn: 3, debug: 1 },
    { error: 0, info: 74, warn: 5, debug: 2 },
    { error: 0, info: 31, warn: 3, debug: 1 },
    { error: 0, info: 67, warn: 6, debug: 3 },
    { error: 12, info: 35, warn: 3, debug: 2 },
    { error: 0, info: 20, warn: 2, debug: 1 },
    { error: 0, info: 27, warn: 3, debug: 1 },
    { error: 0, info: 81, warn: 5, debug: 2 },
    { error: 0, info: 25, warn: 3, debug: 2 },
    { error: 11, info: 31, warn: 7, debug: 2 },
    { error: 0, info: 55, warn: 5, debug: 2 },
    { error: 0, info: 61, warn: 4, debug: 2 },
    { error: 14, info: 33, warn: 5, debug: 2 },
    { error: 0, info: 71, warn: 5, debug: 2 },
    { error: 0, info: 53, warn: 4, debug: 2 },
    { error: 10, info: 64, warn: 6, debug: 4 },
    { error: 35, info: 33, warn: 4, debug: 1 },
    { error: 0, info: 75, warn: 6, debug: 2 },
    { error: 0, info: 0, warn: 0, debug: 0 },
    { error: 13, info: 26, warn: 4, debug: 2 },
    { error: 0, info: 30, warn: 4, debug: 2 },
    { error: 0, info: 20, warn: 4, debug: 1 },
    { error: 0, info: 18, warn: 3, debug: 1 },
];

const logMessages = [
    'Socket connection closed unexpectedly by remote peer',
    'Optimization: Database index \'idx_user_email\' was used for query',
    'Rate limit approaching for IP 185.22.41.12',
    'Health check passed for container \'ochi-api-node-2\'',
    'Failed to load resource: the server responded with a status of 404 (Not Found)',
    'Slow query detected: SELECT * FROM audit_logs WHERE user_id = 9912 (1.2s)',
    'User session established for sid:492...8a',
    'Worker thread 04 released back to pool',
];

const logEntries: LogEntry[] = [
    '14:29:60.43',
    '14:29:59.463',
    '14:29:58.468',
    '14:29:57.467',
    '14:29:56.477',
    '14:29:55.413',
    '14:29:54.458',
    '14:29:53.455',
    '14:29:52.466',
    '14:29:51.477',
    '14:29:50.490',
    '14:29:49.468',
    '14:29:48.414',
    '14:29:47.440',
    '14:29:46.443',
    '14:29:45.471',
    '14:29:44.434',
].map((time, index) => {
    const levels: Level[] = ['error', 'error', 'info', 'warn', 'debug', 'error', 'error', 'warn', 'info', 'debug', 'warn', 'debug'];

    return {
        time: `Oct 24 ${time}`,
        level: levels[index % levels.length],
        message: logMessages[index % logMessages.length],
    };
});

const navItems = ['Dashboard', 'Logs', 'Metrics', 'Traces', 'Notebooks'];
const sideItems = [
    { icon: 'dns', label: 'Infrastructure', active: true },
    { icon: 'lan', label: 'Network', active: false },
    { icon: 'security', label: 'Security', active: false },
    { icon: 'terminal', label: 'Live Tail', active: false },
    { icon: 'alt_route', label: 'Pipelines', active: false },
];

const levelLabels: Level[] = ['info', 'error', 'warn', 'debug'];

const LogView: Component = () => {
    return (
        <main class="log-shell">
            <header class="topbar">
                <div class="brand-block">
                    <strong>Ochi</strong>
                </div>

                <nav class="main-nav" aria-label="Primary">
                    <For each={navItems}>
                        {(item) => <button classList={{ active: item === 'Logs' }}>{item}</button>}
                    </For>
                </nav>

                <div class="top-actions" aria-label="Account actions">
                    <button aria-label="Settings">settings</button>
                    <button aria-label="Help">help</button>
                    <button aria-label="Notifications">notifications</button>
                    <div class="avatar" aria-label="User profile" />
                </div>
            </header>

            <div class="log-workspace">
                <aside class="log-sidebar" aria-label="Log navigation">
                    <div class="sidebar-product">
                        <div class="sidebar-mark">analytics</div>
                        <div>
                            <h2>Ochi Logs</h2>
                            <p>Production Cluster</p>
                        </div>
                    </div>
                    <div class="sidebar-section">
                        <For each={sideItems}>
                            {(item) => (
                                <button classList={{ active: item.active }}>
                                    <span>{item.icon}</span>
                                    {item.label}
                                </button>
                            )}
                        </For>
                    </div>
                    <div class="sidebar-footer">
                        <button>
                            <span>menu_book</span>
                            Docs
                        </button>
                        <button>
                            <span>contact_support</span>
                            Support
                        </button>
                    </div>
                </aside>

                <section class="log-content" aria-label="Ochi Logs">
                    <section class="event-panel" aria-label="Event Distribution Last 15 minutes">
                        <div class="panel-title">
                            <h2>Event Distribution (Last 15m)</h2>
                            <div class="legend">
                                <For each={levelLabels}>
                                    {(level) => <span class={`legend-${level}`}>{level}</span>}
                                </For>
                            </div>
                        </div>
                        <div class="event-chart">
                            <For each={eventBuckets}>
                                {(bucket) => (
                                    <div
                                        class="event-bar"
                                        aria-label={`INFO: ${bucket.info}% ERROR: ${bucket.error}% WARN: ${bucket.warn}% DEBUG: ${bucket.debug}%`}
                                    >
                                        <span
                                            class="bar-debug"
                                            style={{ height: `${bucket.debug}%` }}
                                            title={`DEBUG: ${bucket.debug}%`}
                                        />
                                        <span
                                            class="bar-warn"
                                            style={{ height: `${bucket.warn}%` }}
                                            title={`WARN: ${bucket.warn}%`}
                                        />
                                        <span
                                            class="bar-error"
                                            style={{ height: `${bucket.error}%` }}
                                            title={`ERROR: ${bucket.error}%`}
                                        />
                                        <span class="bar-info" style={{ height: `${bucket.info}%` }} title={`INFO: ${bucket.info}%`} />
                                    </div>
                                )}
                            </For>
                        </div>
                        <div class="chart-range">
                            <time>2026-06-28T20:18:36.915Z</time>
                            <time>2026-06-28T20:33:36.915Z</time>
                        </div>
                    </section>

                    <section class="query-panel" aria-label="Log query controls">
                        <div class="filter-row">
                            <div class="query-input">
                                <span class="query-icon">search</span>
                                <span class="filter-chip filter-chip-service">
                                    service: <strong>api-gateway</strong>
                                    <button aria-label="Remove service filter">close</button>
                                </span>
                                <span class="filter-chip filter-chip-env">
                                    env: <strong>prod</strong>
                                    <button aria-label="Remove environment filter">close</button>
                                </span>
                                <button class="add-filter">Add filter...</button>
                            </div>
                            <button class="run-query">Run Query</button>
                        </div>
                        <div class="query-meta">
                            <button class="time-select">
                                <span>schedule</span>
                                Last 15 minutes
                                <span>expand_more</span>
                            </button>
                            <p>Showing <strong>1,248</strong> entries</p>
                            <div class="stream-actions">
                                <button aria-label="Bookmark">bookmark</button>
                                <button aria-label="Share">share</button>
                                <button aria-label="Download">download</button>
                            </div>
                        </div>
                    </section>

                    <section class="stream-panel" aria-label="Log stream">
                        <div class="stream-toolbar">
                            <div class="stream-heading">
                                <h2>Log Stream</h2>
                                <div class="stream-tabs" role="tablist" aria-label="Log display format">
                                    <button class="active" role="tab" aria-selected="true">MESSAGE</button>
                                    <button role="tab" aria-selected="false">JSON</button>
                                </div>
                            </div>
                            <div class="status-pill">
                                <span />
                                Live Tail Active
                                <button aria-label="More stream options">more_vert</button>
                            </div>
                        </div>

                        <div class="log-list">
                            <For each={logEntries}>
                                {(entry) => (
                                    <article class={`log-row level-${entry.level}`}>
                                        <time>{entry.time}</time>
                                        <span class="level">{entry.level}</span>
                                        <p>{entry.message}</p>
                                    </article>
                                )}
                            </For>
                        </div>
                    </section>
                </section>
            </div>
        </main>
    );
};

export default LogView;
