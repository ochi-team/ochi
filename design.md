# Ochi Design

Design reference for ochi.dev (Astro + Starlight + Tailwind) and the Ochi UI.

## Product voice

- **Ochi** (oh-chee): "Eyes" in Slavic languages. Cost-effective, Loki-compatible log database, written in Zig.
- **Tone:** direct, technical, terse. Explicit over implicit; no marketing filler.
- **Audience:** engineers operating logs in production (Linux), Grafana users migrating from Loki.
- **Docs structure:** Guides (minimum friendly intro) → Reference (the contract) → Changelog (history).

## Design-system source of truth

[Paper · Ochi · Design system](https://app.paper.design/file/01M50MEWHC0TWFBGD5V0Q63FGJ/p-2-0) is authoritative for visual tokens and shared component styling. This document records its values and compatibility mappings; if they diverge, sync from Paper rather than introducing a competing palette. Reference sheets: Foundations, Controls & query, Data & inspection, and Feedback & overlays. This authority covers design-system styling only, not application scope, screen plans, or task completion.

## Theme

- **Dark-first.** Dark remains the application's primary/default theme; selectors are `.dark, .dark-theme`.
- Paper defines light `--color-*` tokens and explicit `--color-dark-*` counterparts. Use the dark set for the working UI; light tokens do not change the app's default theme.
- Green accents and subtly violet-tinted neutrals replace the old blue-gray Radix-style scales. Semantic error and warning colors are the only additional hues.
- Preserve the exact OKLCH values below. The former separate sRGB/P3 scales and alpha ramps are superseded; do not restore them as gamut-specific overrides.

## Color tokens

### Semantic palette

Each row names the light token and its dark counterpart (insert `dark-` after `--color-`). Values are copied from Paper, not rounded hex approximations.

| Light token | Light value | Dark value |
|---|---|---|
| `--color-background` | `oklch(95.1% 0.004 301.4)` | `oklch(18.2% 0 0)` |
| `--color-foreground` | `oklch(18.2% 0 0)` | `oklch(95.1% 0.004 301.4)` |
| `--color-card` | `oklch(100% 0 0)` | `oklch(21.7% 0.007 314.7)` |
| `--color-card-foreground` | `oklch(18.2% 0 0)` | `oklch(95.1% 0.004 301.4)` |
| `--color-popover` | `oklch(100% 0 0)` | `oklch(21.7% 0.007 314.7)` |
| `--color-popover-foreground` | `oklch(18.2% 0 0)` | `oklch(95.1% 0.004 301.4)` |
| `--color-primary` | `oklch(55.7% 0.111 153.3)` | `oklch(55.7% 0.111 153.3)` |
| `--color-primary-foreground` | `oklch(22.1% 0.016 149.1)` | `oklch(22.1% 0.016 149.1)` |
| `--color-secondary` | `oklch(91.8% 0.006 297.7)` | `oklch(29% 0.011 300.9)` |
| `--color-secondary-foreground` | `oklch(29% 0.011 300.9)` | `oklch(95.1% 0.004 301.4)` |
| `--color-muted` | `oklch(95.1% 0.004 301.4)` | `oklch(25.6% 0.010 303.8)` |
| `--color-muted-foreground` | `oklch(35.8% 0.013 305.1)` | `oklch(77.8% 0.019 303.3)` |
| `--color-accent` | `oklch(89.2% 0.054 150.1)` | `oklch(26% 0.022 147.6)` |
| `--color-accent-foreground` | `oklch(26% 0.022 147.6)` | `oklch(89.2% 0.054 150.1)` |
| `--color-destructive` | `oklch(63.2% 0.131 21.5)` | `oklch(39.1% 0.083 21.6)` |
| `--color-destructive-foreground` | `oklch(98.5% 0.002 247.8)` | `oklch(95.1% 0.004 301.4)` |
| `--color-border` | `oklch(35.8% 0.013 305.1)` | `oklch(35.8% 0.013 305.1)` |
| `--color-input` | `oklch(35.8% 0.013 305.1)` | `oklch(35.8% 0.013 305.1)` |
| `--color-ring` | `oklch(73.5% 0.104 151.4)` | `oklch(73.5% 0.104 151.4)` |
| `--color-chart-1` | `oklch(39.4% 0.051 151.1)` | `oklch(39.4% 0.051 151.1)` |
| `--color-chart-2` | `oklch(49.4% 0.069 152.1)` | `oklch(49.4% 0.069 152.1)` |
| `--color-chart-3` | `oklch(55.7% 0.111 153.3)` | `oklch(55.7% 0.111 153.3)` |
| `--color-chart-4` | `oklch(73.5% 0.104 151.4)` | `oklch(73.5% 0.104 151.4)` |
| `--color-chart-5` | `oklch(89.2% 0.054 150.1)` | `oklch(89.2% 0.054 150.1)` |
| `--color-sidebar` | `oklch(98.5% 0.002 247.8)` | `oklch(21.7% 0.007 314.7)` |
| `--color-sidebar-foreground` | `oklch(18.2% 0 0)` | `oklch(95.1% 0.004 301.4)` |
| `--color-sidebar-primary` | `oklch(55.7% 0.111 153.3)` | `oklch(55.7% 0.111 153.3)` |
| `--color-sidebar-primary-foreground` | `oklch(22.1% 0.016 149.1)` | `oklch(22.1% 0.016 149.1)` |
| `--color-sidebar-accent` | `oklch(89.2% 0.054 150.1)` | `oklch(34.9% 0.045 151.1)` |
| `--color-sidebar-accent-foreground` | `oklch(26% 0.022 147.6)` | `oklch(89.2% 0.054 150.1)` |
| `--color-sidebar-border` | `oklch(93% 0.008 260.7)` | `oklch(35.8% 0.013 305.1)` |
| `--color-sidebar-ring` | `oklch(73.5% 0.104 151.4)` | `oklch(73.5% 0.104 151.4)` |

### Status extensions

| Token | Value | Role |
|---|---|---|
| `--color-status-error-text` | `oklch(78% 0.105 21.6)` | Readable error labels, icons, annotations |
| `--color-status-warning-text` | `oklch(82% 0.110 85)` | Warning labels and icons |
| `--color-status-warning-surface` | `oklch(28% 0.035 85)` | Subtle warning/search-match fill |

Destructive is a fill, not readable error text on the dark canvas. Pair destructive fills with `--color-dark-destructive-foreground`; use `--color-status-error-text` for error foregrounds.

### Compatibility aliases

These aliases exist in Paper so current component references can keep their names. They are semantic mappings, **not** a complete or ordered 12-step scale. Do not infer missing gray/green steps or alpha variants.

```css
.dark, .dark-theme {
  --bg: var(--color-dark-background);
  --gray-2: var(--color-dark-card);
  --gray-3: var(--color-dark-secondary);
  --gray-4: var(--color-dark-muted);
  --gray-6: var(--color-dark-border);
  --gray-7: var(--color-dark-input);
  --gray-8: var(--color-dark-muted-foreground);
  --gray-10: var(--color-dark-muted-foreground);
  --gray-11: var(--color-dark-muted-foreground);
  --gray-12: var(--color-dark-foreground);
  --green-3: var(--color-dark-accent);
  --green-7: var(--color-dark-sidebar-accent);
  --green-8: var(--color-dark-ring);
  --green-9: var(--color-dark-primary);
  --green-10: var(--color-dark-primary);
  --green-11: var(--color-dark-ring);
  --green-12: var(--color-dark-accent-foreground);
  --status-error: var(--color-dark-destructive);
}
```

`--green-9` and `--green-10` intentionally resolve to the same primary color. Do not invent a darker hover token. `--gray-8`, `--gray-10`, and `--gray-11` share the muted foreground; focus rings use `--color-dark-ring`, not a gray step. `--status-error` is a destructive-fill alias, not the error-text token.

## Semantic mapping

| Semantic | Dark token / compatibility alias |
|---|---|
| Background | `--color-dark-background` / `--bg` |
| Card / supporting surface | `--color-dark-card` / `--gray-2` |
| Popover | `--color-dark-popover` |
| Hover / secondary fill | `--color-dark-secondary` / `--gray-3` |
| Muted fill | `--color-dark-muted` / `--gray-4` |
| Border / input border | `--color-dark-border` / `--color-dark-input` (`--gray-6` / `--gray-7`) |
| Primary text | `--color-dark-foreground` / `--gray-12` |
| Secondary / placeholder text | `--color-dark-muted-foreground` / `--gray-11` |
| Primary action fill / foreground | `--color-dark-primary` / `--color-dark-primary-foreground` |
| Accent text / links / focus ring | `--color-dark-ring` / `--green-11` / `--green-8` |
| Selected fill / foreground | `--color-dark-accent` / `--color-dark-accent-foreground` (`--green-3` / `--green-12`) |
| Selected border | `--color-dark-sidebar-accent` / `--green-7` |
| Histogram bars | `--color-dark-chart-3` (same value as primary) |

Starlight mapping (in `src/styles/theme.css`):

```css
:root[data-theme="dark"] {
  --sl-color-bg: var(--bg);
  --sl-color-bg-nav: var(--gray-2);
  --sl-color-bg-sidebar: var(--gray-2);
  --sl-color-hairline: var(--gray-6);
  --sl-color-text: var(--gray-11);
  --sl-color-white: var(--gray-12);
  --sl-color-accent: var(--green-9);
  --sl-color-accent-high: var(--green-11);
  --sl-color-accent-low: var(--green-3);
}
```

## Typography

- **Body/UI:** `--font-sans: Inter`; `--font-ui: var(--font-sans)`. CSS fallback: sans-serif.
- **Code, LOQL, log lines, timestamps and values:** `--font-mono: Fira Code`. CSS fallback: monospace. Logs remain a first-class typographic role.
- **Reserved serif:** `--font-serif: Georgia` (fallback serif); its presence in the token set does not introduce serif text into the workspace.

| Tokens | Values |
|---|---|
| `--text-xs`, `--text-code`, `--text-sm`, `--text-base`, `--text-xl`, `--text-2xl` | 12px, 13px, 14px, 16px, 24px, 32px |
| `--leading-4`, `--leading-5`, `--leading-6`, `--leading-8`, `--leading-10` | 16px, 20px, 24px, 32px, 40px |
| `--font-weight-normal`, `--font-weight-medium`, `--font-weight-semibold` | 400, 500, 600 |
| `--tracking-normal` | -0.01em |

Use 14px UI labels, 13px query/message text, and 12px metadata. Dense working rows use 20px line height; line-height tokens remain independent of size tokens. Paper's primary Run specimen uses 16px UI text. Use tabular numerals for timestamps.

- Docs heading hierarchy follows the content: `###` sections, `######` for sub-concepts (e.g. "Timestamps", "Relative time").
- Inline code: `--gray-3` background, `--gray-12` text; compact 2px local radius where needed. Do not treat that local value as a replacement for the named radius scale.

## Components (from docs content)

- **Steps** (installation guide): numbered, accent-green markers (`--green-9`).
- **Code blocks** (`sh`, LOQL, JSON): `--gray-2` bg, `--gray-6` border, copy button on hover.
- **Tables** (API, LOQL operators): `--gray-6` row dividers, header in `--gray-11`.
- **Callouts:** note = neutral surface/border; tip = accent fill (`--green-3`) + selected border (`--green-7`).
- **Changelog entries:** `.changelog-date` in `--gray-10`, sections as `###` (Features, Fixes, Others).
- **Sidebar:** three groups: Guides, Reference, Changelog (newest first via negative `order`).

## Layout & spacing

| Tokens | Values |
|---|---|
| `--spacing` | 4px base unit |
| `--spacing-1`, `--spacing-2`, `--spacing-3`, `--spacing-4`, `--spacing-6`, `--spacing-8`, `--spacing-12` | 4px, 8px, 12px, 16px, 24px, 32px, 48px |
| `--radius-panel`, `--radius-sm` | 4px |
| `--radius-md` | 6px |
| `--radius`, `--radius-lg` | 8px |
| `--radius-xl` | 12px |
| `--opacity-disabled` | 45% |
| `--breakpoint-md`, `--breakpoint-lg` | 768px, 1280px |
| `--container-rail`, `--container-inspector` | 240px, 360px |

- Controls use the 6px medium radius; panels use 4px; overlays may use 8px. Do not impose a universal pill radius on chips.
- Content max-width ~ 72ch for prose; code blocks may scroll horizontally, never wrap logs.
- Borders over shadows; elevation uses semantic surface, secondary, and muted fills rather than numeric gray-step ordering.
- Breakpoint/container tokens describe design-system primitives only. They do not add an inspector screen or change the existing 1024px compact-screen deliverables.

## Accessibility

- Validate primary and muted text against the actual semantic surfaces at WCAG AA (4.5:1 for normal text); do not carry contrast claims over from the superseded palette.
- Solid primary buttons use `--color-dark-primary-foreground` text/icons on `--color-dark-primary`, including hover. Do not substitute white or the page-background token for the explicit foreground pairing.
- Use `--color-status-error-text` for error text on dark surfaces; `--status-error` / destructive is a fill. Disabled opacity is not appropriate for readable placeholders or essential labels.
- Never convey state by color alone (pair with icon/label), e.g. error vs. ok in query results.
- Visible focus ring (`--color-dark-ring` / `--green-8`, 2px, 2px offset) on all interactive elements. Verify meaningful control/focus boundaries at 3:1.

## Brand

- Logo: `src/assets/logo.svg` (site), `logo.png` (README). Render on `--bg`; don't recolor outside the semantic green/neutral palette.
- OG image: `/og.png`, 1200×630.
- Links: Discord (`https://discord.gg/AsCKpCNp5c`), GitHub (`https://github.com/ochi-team/ochi`).

## Ochi UI application

The implementation sequence and feature acceptance criteria live in [design_plan.md](design_plan.md). This section is the visual contract for the application; the Paper design system linked above is the source of truth for tokens and shared styling.

### Composition and density

- One primary log exploration view. Full viewport width; do not apply the prose max-width to the app.
- Top bar: 48px. Query region: approximately 104–136px, growing with multiline input. Main body: collapsible 240px left rail and a flexible results column. Histogram: 80–96px. Results toolbar: 36px. Collapsed rows: 28px, with a 32px comfortable option.
- Use 13px Fira Code / 20px line height for log messages and LOQL, 12px for timestamps/metadata, and 14px Inter for UI labels/controls (16px in the primary Run specimen). Timestamp columns use tabular numerals.
- At 1024px, collapse the rail by default into an accessible overlay; retain an explicit Streams / Fields toggle. Keep Run and the editor visible; wrap secondary query controls. Popovers must fit the viewport.
- Sticky log headers, independent vertical scrolling for the rail and logs, horizontal scrolling inside the log table. Single-line messages by default; expanded details may wrap with preserved whitespace.
- Borders are 1px. Avoid shadows, ornamental cards, gradients, and oversized headings in the working view. An optional radial glow derived from the primary color at 8% opacity is allowed only in the initial empty state.

### Component token sheet

| Component / state | Surface / border | Text / accent |
|---|---|---|
| Page / log canvas | `--bg` / `--gray-6` separators | `--gray-12` messages, `--gray-11` timestamps |
| Top bar, rail, query editor, popovers | `--gray-2` / `--gray-6` | `--gray-12` primary, `--gray-11` secondary |
| Neutral control hover / active | `--gray-3` / `--gray-4`; border `--color-dark-input` | `--gray-12`; focused outline `--color-dark-ring` |
| Run | `--color-dark-primary` (same primary on hover) | `--color-dark-primary-foreground` text and icon |
| Selected row / selected time chip | `--green-3` / `--green-7` | `--gray-12`, `--green-11` marker |
| Field chips | `--gray-3` / `--gray-6` | `--gray-11` keys, `--gray-12` values; actions `--green-11` |
| LOQL time / tags / fields | Editor surface | Time `--green-11`; tag keys `--green-12`; field keys and values `--gray-12`; operators and punctuation `--gray-11` |
| Histogram | `--bg`; grid `--gray-6`; bars `--green-9` | Axes `--gray-11`; brush `--green-3` with `--green-11` handles |
| Links, copy/filter actions, eye logo | Inherit surface | `--green-11`, hover `--green-12` |
| Focus | Existing component surface and border | 2px `--green-8` outline, 2px offset |
| Empty state | `--bg`, optional empty-state glow | `--gray-12` title, `--gray-11` help, `--green-11` example action |

Syntax categories also have structural cues: square brackets for time, braces for tags, and field expressions after tags. Do not introduce a multicolor code theme.

### Levels and status

- Semantic foregrounds: `--color-status-error-text` for ERROR/FATAL, syntax and request errors, and Down; `--color-status-warning-text` for WARN and Degraded; `--green-11` for Ready and INFO. DEBUG/TRACE/unknown levels use `--gray-11`. Use `--color-status-warning-surface` for warning/search-match fills and destructive for error fills.
- Use semantic colors for foreground labels, icons, and thin markers on `--bg` or `--gray-2`. Selected-row fills remain green. Verify contrast on every actual background.
- Pair every level with its text label; pair readiness dots with Ready, Checking, or Down. Error locations use an underline, icon, and readable message.
- No purple badge token exists in Paper. Use existing semantic roles for optional demo/beta badges; do not add a separate accent hue.

### Interaction and states

- `/` focuses the query; Cmd+Enter on macOS and Ctrl+Enter elsewhere run it. `j`/`k` select rows while results have focus, Enter expands, Esc closes details or the foremost overlay and restores focus.
- Editing text, IME composition, and autocomplete take priority over global shortcuts. Menus support arrows, Enter, and Esc. Use visible action buttons as keyboard alternatives to Alt-click exclusion.
- Use semantic table markup with row expansion buttons, `aria-expanded`, and a focus strategy that survives virtualization. Announce loading, result counts, and errors without announcing every line.
- Required designs: initial empty, loading, results, expanded row, no results, syntax error, server down. Preserve the query in every state. Retained results must show the query/time/tenant that produced them and a Stale label when they no longer match.
- The empty state offers an explicit example query; opening a fresh app does not automatically scan logs. A URL containing a complete query may run it intentionally.
- Copy and filter actions appear on keyboard focus as well as hover. Respect reduced motion. Validate text contrast at 4.5:1 and meaningful control/focus boundaries at 3:1, including browser gamut handling of the OKLCH values.
