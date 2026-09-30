# Pompos UI Style Guide

## Direction

**1998 Unix admin tool × Japanese appliance design.**

Utilitarian, compact, calm, physical, dependable.

Appliance influence is visual and interactional only. Avoid retro cosplay.

## Rules

- Make state obvious.
- Keep controls few and explicit.
- Show relevant system values: tables, paths, YAML, timestamps, row counts, errors. Show each once, where it is useful.
- Prefer tables and sections over cards.
- Use color on status indicators, borders, and control backgrounds. Keep UI text black; preserve syntax highlighting in code.
- Keep interactions immediate.
- Use terse, literal copy.
- Treat config and logs as first-class UI.

## Copy

Respect the user's attention. Every visible word must help identify a control, understand current state, make a decision, or resolve an error. Otherwise, remove it.

- Prefer deletion over shortening filler. A heading and controls usually need no introduction.
- Do not narrate the workflow, explain obvious controls, announce autosaving, or reassure users about internal implementation steps.
- Put units and constraints in labels: `Cron · UTC`. Use concise placeholders for empty states: `Manual`.
- Show values directly: `10 / 42 rows`. Avoid sentences describing the same values.
- Keep confirmations brief: `Schedule saved.` Keep errors specific and actionable.
- State consequences only where they affect a decision. Do not repeat them as permanent helper text.
- Keep technical explanations in documentation and implementation details in config or logs.
- Apply these rules to templates and dynamically generated UI alike. Preserve accessible labels.

Remove copy such as “Saved conversations and their ingestions,” “This exact script was source-tested before saving,” and “Five fields, evaluated in UTC. Leave blank to disable.”

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

- Sans for UI: IBM Plex Sans / system-ui.
- Mono for machine values: IBM Plex Mono / ui-monospace.
- Bundle WOFF2 fonts locally; no external font requests. Use `font-display: swap`.
- Ordinary UI text and controls: 14px.
- Regular (400) for content, medium (500) for labels and controls, semibold (600) for headings.
- Use sentence case for labels and headings; keep the fixed status vocabulary uppercase.
- Supporting text: 12px, with the same black color as body text.
- Page headings: 16–20px. Section headings: 14px.
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
