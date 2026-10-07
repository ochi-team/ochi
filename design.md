# Ochi Design

Design reference for ochi.dev (Astro + Starlight + Tailwind) and the Ochi UI.

## Product voice

- **Ochi** (oh-chee): "Eyes" in Slavic languages. Cost-effective, Loki-compatible log database, written in Zig.
- **Tone:** direct, technical, terse. Explicit over implicit; no marketing filler.
- **Audience:** engineers operating logs in production (Linux), Grafana users migrating from Loki.
- **Docs structure:** Guides (minimum friendly intro) → Reference (the contract) → Changelog (history).

## Theme

- **Dark-first.** Dark theme is the primary and default; selectors are `.dark, .dark-theme`.
- Two scales only: **green** (accent) and **gray** (neutral, blue-tinted). No other hues except semantic states.
- Wide-gamut (P3) values override sRGB hex under `@supports (color: color(display-p3 1 1 1))` + `@media (color-gamut: p3)`.

### Background

| Role | Token | Hex |
|---|---|---|
| Page background | `--bg` | `#121212` |
| Raised surface (cards, sidebar, code blocks) | `--gray-2` | `#131926` |
| Subtle fill / hover | `--gray-3` | `#192235` |
| Active / selected | `--gray-4` | `#1d2941` |

> `#121212` is **not** part of the gray scale (the scale is blue-tinted, `--gray-1` is `#0c121e`). It is a standalone token: `--bg: #121212`. Surfaces and fills stay on the gray scale, so they will read slightly bluer than the page.

### Usage by step (Radix-style 12-step)

| Steps | Purpose |
|---|---|
| 1–2 | App / surface backgrounds |
| 3–5 | Component backgrounds (normal, hover, active) |
| 6–8 | Borders and separators (subtle → focus ring) |
| 9–10 | Solid fills: buttons, badges, indicators (10 = hover) |
| 11 | Low-contrast text (secondary, muted) |
| 12 | High-contrast text (headings, body) |

- Text on solid accent (`--green-9`) uses `--green-contrast` (`#fff`).
- `a1–a12` are the alpha variants: use for overlays and anything over non-flat backgrounds.

## Color tokens

### Accent: green

| Token | sRGB | P3 (oklch) |
|---|---|---|
| `--green-1` | `#0d140e` | `oklch(18.2% 0.0156 149.9)` |
| `--green-2` | `#131b14` | `oklch(21% 0.0163 149.9)` |
| `--green-3` | `#192b1c` | `oklch(26.7% 0.0357 149.9)` |
| `--green-4` | `#193b22` | `oklch(31.9% 0.0599 149.9)` |
| `--green-5` | `#20492a` | `oklch(36.6% 0.0706 149.9)` |
| `--green-6` | `#275833` | `oklch(41.6% 0.0814 149.9)` |
| `--green-7` | `#2d693d` | `oklch(46.8% 0.0939 149.9)` |
| `--green-8` | `#347b47` | `oklch(52.5% 0.1093 149.9)` |
| `--green-9` | `#1ca04d` | `oklch(62% 0.1623 149.9)` |
| `--green-10` | `#009341` | `oklch(57.8% 0.1623 149.9)` |
| `--green-11` | `#5ed47d` | `oklch(78% 0.1623 149.9)` |
| `--green-12` | `#b7f2c2` | `oklch(91% 0.0887 149.9)` |

Alpha (`--green-a1…a12`), `--green-contrast: #fff`, `--green-surface: #14241680`, `--green-indicator` / `--green-track`: `#1ca04d`.

```css
.dark, .dark-theme {
  --green-1: #0d140e;  --green-2: #131b14;  --green-3: #192b1c;  --green-4: #193b22;
  --green-5: #20492a;  --green-6: #275833;  --green-7: #2d693d;  --green-8: #347b47;
  --green-9: #1ca04d;  --green-10: #009341; --green-11: #5ed47d; --green-12: #b7f2c2;

  --green-a1: #00bc0003;  --green-a2: #2cf8460a;  --green-a3: #55ff711b;  --green-a4: #3afb6d2d;
  --green-a5: #4efc783c;  --green-a6: #58fd814c;  --green-a7: #5cff875e;  --green-a8: #5fff8a71;
  --green-a9: #23ff7599;  --green-a10: #00ff698b; --green-a11: #6fff95d1; --green-a12: #c0fecbf2;

  --green-contrast: #fff;
  --green-surface: #14241680;
  --green-indicator: #1ca04d;
  --green-track: #1ca04d;
}

@supports (color: color(display-p3 1 1 1)) {
  @media (color-gamut: p3) {
    .dark, .dark-theme {
      --green-1: oklch(18.2% 0.0156 149.9);  --green-2: oklch(21% 0.0163 149.9);
      --green-3: oklch(26.7% 0.0357 149.9);  --green-4: oklch(31.9% 0.0599 149.9);
      --green-5: oklch(36.6% 0.0706 149.9);  --green-6: oklch(41.6% 0.0814 149.9);
      --green-7: oklch(46.8% 0.0939 149.9);  --green-8: oklch(52.5% 0.1093 149.9);
      --green-9: oklch(62% 0.1623 149.9);    --green-10: oklch(57.8% 0.1623 149.9);
      --green-11: oklch(78% 0.1623 149.9);   --green-12: oklch(91% 0.0887 149.9);

      --green-a1: color(display-p3 0 0.9451 0 / 0.009);
      --green-a2: color(display-p3 0.3804 1 0.3804 / 0.038);
      --green-a3: color(display-p3 0.4784 0.9961 0.4784 / 0.106);
      --green-a4: color(display-p3 0.4431 1 0.4902 / 0.169);
      --green-a5: color(display-p3 0.502 1 0.5373 / 0.228);
      --green-a6: color(display-p3 0.5294 1 0.5686 / 0.292);
      --green-a7: color(display-p3 0.5373 1 0.5804 / 0.363);
      --green-a8: color(display-p3 0.549 1 0.5961 / 0.435);
      --green-a9: color(display-p3 0.4588 1 0.5255 / 0.591);
      --green-a10: color(display-p3 0.4078 1 0.4824 / 0.536);
      --green-a11: color(display-p3 0.5961 1 0.6314 / 0.806);
      --green-a12: color(display-p3 0.8118 1 0.8235 / 0.937);

      --green-contrast: #fff;
      --green-surface: color(display-p3 0.0941 0.1333 0.0941 / 0.5);
      --green-indicator: oklch(62% 0.1623 149.9);
      --green-track: oklch(62% 0.1623 149.9);
    }
  }
}
```

### Neutral: gray (blue-tinted, hue 263.5)

| Token | sRGB | P3 (oklch) |
|---|---|---|
| `--gray-1` | `#0c121e` | `oklch(18.2% 0.0258 263.5)` |
| `--gray-2` | `#131926` | `oklch(21.4% 0.0265 263.5)` |
| `--gray-3` | `#192235` | `oklch(25.4% 0.0372 263.5)` |
| `--gray-4` | `#1d2941` | `oklch(28.3% 0.0477 263.5)` |
| `--gray-5` | `#22304c` | `oklch(31.1% 0.0536 263.5)` |
| `--gray-6` | `#2a3957` | `oklch(34.6% 0.0557 263.5)` |
| `--gray-7` | `#374765` | `oklch(39.8% 0.0557 263.5)` |
| `--gray-8` | `#506081` | `oklch(49% 0.0557 263.5)` |
| `--gray-9` | `#5d6e8f` | `oklch(53.7% 0.0557 263.5)` |
| `--gray-10` | `#6a7b9d` | `oklch(58.3% 0.0557 263.5)` |
| `--gray-11` | `#a1b4d8` | `oklch(76.8% 0.0557 263.5)` |
| `--gray-12` | `#e8eefb` | `oklch(94.9% 0.0182 263.5)` |

Alpha (`--gray-a1…a12`), `--gray-contrast: #FFFFFF`, `--gray-surface: rgba(0,0,0,0.05)`, `--gray-indicator` / `--gray-track`: `#5d6e8f`.

```css
.dark, .dark-theme {
  --gray-1: #0c121e;  --gray-2: #131926;  --gray-3: #192235;  --gray-4: #1d2941;
  --gray-5: #22304c;  --gray-6: #2a3957;  --gray-7: #374765;  --gray-8: #506081;
  --gray-9: #5d6e8f;  --gray-10: #6a7b9d; --gray-11: #a1b4d8; --gray-12: #e8eefb;

  --gray-a1: #0013fe0d;  --gray-a2: #1e64fa16;  --gray-a3: #417efd26;  --gray-a4: #4985fd33;
  --gray-a5: #528bfc3f;  --gray-a6: #6497fd4b;  --gray-a7: #7ba8fd5a;  --gray-a8: #95b7fd78;
  --gray-a9: #a0c0ff87;  --gray-a10: #a8c5ff96; --gray-a11: #bcd3fed6; --gray-a12: #ecf2fffb;

  --gray-contrast: #FFFFFF;
  --gray-surface: rgba(0, 0, 0, 0.05);
  --gray-indicator: #5d6e8f;
  --gray-track: #5d6e8f;
}

@supports (color: color(display-p3 1 1 1)) {
  @media (color-gamut: p3) {
    .dark, .dark-theme {
      --gray-1: oklch(18.2% 0.0258 263.5);  --gray-2: oklch(21.4% 0.0265 263.5);
      --gray-3: oklch(25.4% 0.0372 263.5);  --gray-4: oklch(28.3% 0.0477 263.5);
      --gray-5: oklch(31.1% 0.0536 263.5);  --gray-6: oklch(34.6% 0.0557 263.5);
      --gray-7: oklch(39.8% 0.0557 263.5);  --gray-8: oklch(49% 0.0557 263.5);
      --gray-9: oklch(53.7% 0.0557 263.5);  --gray-10: oklch(58.3% 0.0557 263.5);
      --gray-11: oklch(76.8% 0.0557 263.5); --gray-12: oklch(94.9% 0.0182 263.5);

      --gray-a1: color(display-p3 0 0.0706 0.9922 / 0.047);
      --gray-a2: color(display-p3 0.1686 0.4078 0.9922 / 0.081);
      --gray-a3: color(display-p3 0.3176 0.5098 1 / 0.144);
      --gray-a4: color(display-p3 0.3373 0.5451 1 / 0.19);
      --gray-a5: color(display-p3 0.3882 0.5686 1 / 0.237);
      --gray-a6: color(display-p3 0.4471 0.6118 1 / 0.283);
      --gray-a7: color(display-p3 0.5294 0.6784 1 / 0.342);
      --gray-a8: color(display-p3 0.6314 0.7412 1 / 0.456);
      --gray-a9: color(display-p3 0.6627 0.7647 1 / 0.515);
      --gray-a10: color(display-p3 0.6902 0.7804 1 / 0.574);
      --gray-a11: color(display-p3 0.7725 0.8392 1 / 0.823);
      --gray-a12: color(display-p3 0.9333 0.9529 1 / 0.979);

      --gray-contrast: #FFFFFF;
      --gray-surface: color(display-p3 0 0 0 / 5%);
      --gray-indicator: oklch(53.7% 0.0557 263.5);
      --gray-track: oklch(53.7% 0.0557 263.5);
    }
  }
}
```

## Semantic mapping

| Semantic | Token |
|---|---|
| Background | `--bg` (`#121212`) |
| Surface | `--gray-2` |
| Border (default / hover / focus) | `--gray-6` / `--gray-7` / `--gray-8` |
| Text primary | `--gray-12` |
| Text secondary | `--gray-11` |
| Text disabled / placeholder | `--gray-10` |
| Accent solid (button, active nav marker) | `--green-9` (hover `--green-10`) |
| Accent text / links | `--green-11` (hover `--green-12`) |
| Accent subtle bg (callout, selected row) | `--green-3` / `--green-a3` |
| Accent border | `--green-7` |
| Focus ring | `--green-8` |

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

- **Body/UI:** system sans stack (`ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif`).
- **Code, LOQL, log lines:** monospace (`ui-monospace, "JetBrains Mono", "SF Mono", Menlo, monospace`). Logs are the product; mono is a first-class face, not an afterthought.
- Docs heading hierarchy follows the content: `###` sections, `######` for sub-concepts (e.g. "Timestamps", "Relative time").
- Inline code (`` `{tag1=alpha AND tag2=beta}` ``): `--gray-3` bg, `--gray-12` text, 4px radius.

## Components (from docs content)

- **Steps** (installation guide): numbered, accent-green markers (`--green-9`).
- **Code blocks** (`sh`, LOQL, JSON): `--gray-2` bg, `--gray-6` border, copy button on hover.
- **Tables** (API, LOQL operators): `--gray-6` row dividers, header in `--gray-11`.
- **Callouts:** note = gray, tip = green; use the `3` step bg + `7` step border.
- **Changelog entries:** `.changelog-date` in `--gray-10`, sections as `###` (Features, Fixes, Others).
- **Sidebar:** three groups: Guides, Reference, Changelog (newest first via negative `order`).

## Layout & spacing

- 4px base grid (4, 8, 12, 16, 24, 32, 48, 64).
- Radii: 4px (inline), 8px (cards, code blocks), 9999px (pills).
- Content max-width ~ 72ch for prose; code blocks may scroll horizontally, never wrap logs.
- Borders over shadows; elevation is expressed by moving up a gray step (1 → 2 → 3).

## Accessibility

- Body text `--gray-11` on `--bg` and headings `--gray-12` on `--bg` pass WCAG AA.
- `--green-11` on `--bg` for links passes AA; `--green-9` is for fills only, with `#fff` text at ≥ 14px bold or 18px regular.
- Never convey state by color alone (pair with icon/label), e.g. error vs. ok in query results.
- Visible focus ring (`--green-8`, 2px, 2px offset) on all interactive elements.

## Brand

- Logo: `src/assets/logo.svg` (site), `logo.png` (README). Render on `--bg`; don't recolor outside the green/gray scales.
- OG image: `/og.png`, 1200×630.
- Links: Discord (`https://discord.gg/AsCKpCNp5c`), GitHub (`https://github.com/ochi-team/ochi`).
