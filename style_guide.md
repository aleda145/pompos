# Pompos UI Style Guide

## Direction

**1998 Unix admin tool × Japanese appliance design.**

Utilitarian, compact, calm, physical, dependable.

Appliance influence is visual and interactional only. Avoid retro cosplay.

## Rules

- Make state obvious.
- Keep controls few and explicit.
- Show real system details: tables, paths, YAML, timestamps, row counts, errors.
- Prefer tables and sections over cards.
- Use color on status indicators, borders, and control backgrounds. Keep UI text black; preserve syntax highlighting in code.
- Keep interactions immediate.
- Use terse, literal copy.
- Treat config and logs as first-class UI.

## Style

- Warm gray background, not pure white.
- UI text is pure black, including secondary text, placeholders, and status labels. Code keeps its syntax highlighting colors.
- Build text hierarchy with font size. Never fade text with gray, beige, or reduced opacity.
- 1px visible borders.
- 2–4px radius.
- Almost no shadows.
- Rectangular controls, no pills.
- Inset surfaces for status/control panels.
- Dense layouts with clear hierarchy.

## Type

- Sans for UI: Inter / Helvetica / system-ui.
- Mono for machine values: IBM Plex Mono / Geist Mono / ui-monospace.
- UI: 12–14px.
- Supporting text: 12px, with the same black color as body text.
- Headings: 16–20px.
- Use mono for identifiers, config, logs, timestamps, counts, durations.

## Color

```css
--bg: #e9e8e2;
--surface: #f3f2ed;
--surface-inset: #deded8;
--text: #000000;
--border: #b9bab4;
--border-strong: #777872;
--accent: #d97932;
--success: #008a2e;
--warning: #a66d1f;
--danger: #e02020;
```

Keep surface colors muted. Success and failure indicators use vivid green and red so they stand out clearly. UI text uses `--text`; do not add a muted UI text color. Syntax highlighting may use separate token colors, including gray comments. Status must still be understandable without color.

## Controls

Buttons should look like controls, not CTAs. Disabled controls keep black text; use an inset background and dashed border to show their state.

`[ RUN ]  [ STOP ]  [ EDIT ]`

Use a small fixed status vocabulary:

`IDLE` `RUNNING` `SUCCESS` `FAILED` `CANCELLED`

Avoid CRT effects, pixel fonts, and terminal-green-on-black styling.
