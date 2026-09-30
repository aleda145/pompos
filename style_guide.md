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
- Use color mainly for state and primary actions.
- Keep interactions immediate.
- Use terse, literal copy.
- Treat config and logs as first-class UI.

## Style

- Warm gray background, not pure white.
- Dark gray text, not pure black.
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
- Headings: 16–20px.
- Use mono for identifiers, config, logs, timestamps, counts, durations.

## Color

```css
--bg: #e9e8e2;
--surface: #f3f2ed;
--surface-inset: #deded8;
--text: #242522;
--text-muted: #686964;
--border: #b9bab4;
--border-strong: #777872;
--accent: #d97932;
--success: #42764a;
--warning: #a66d1f;
--danger: #a4463d;
```

Keep colors muted. Status must still be understandable without color.

## Controls

Buttons should look like controls, not CTAs.

`[ RUN ]  [ STOP ]  [ EDIT ]`

Use a small fixed status vocabulary:

`IDLE` `RUNNING` `SUCCESS` `FAILED` `CANCELLED`

Avoid CRT effects, pixel fonts, and terminal-green-on-black styling.
