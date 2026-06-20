# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Goal

Rewrite RxJS operator names into human-readable, intuitive alternatives. For example, `map` → `transformWith`. The `readme.md` is the canonical list of all ~80 RxJS operators awaiting friendly names — it is the primary design artifact.

## Commands

```bash
npm run dev       # Start Vite dev server with hot reload
npm run build     # TypeScript compile + Vite bundle (tsc && vite build)
npm run preview   # Preview the production build
```

No test runner or linter is configured yet.

## Stack

- **Vite + TypeScript** (ESNext target, bundler module resolution, strict mode)
- **RxJS ^7.8.2** — the library whose operators are being renamed
- **VitePress ^1.6.4** — likely intended for documentation/demo site
- Entry: `src/main.ts` → `index.html`

## Architecture Intent

The project is in early planning. The current `src/` is Vite boilerplate. The actual work is:

1. Defining the operator name mappings (`<rxjsName>` → `<friendlyName>`)
2. Implementing re-exports or wrappers using those friendly names
3. Possibly a VitePress docs/demo site to showcase the naming rationale

`readme.md` tracks all operators by category (Creation, Transformation, Filtering, Join, Multicasting, Error Handling, Utility, Conditional, Mathematical). `map → transformWith` is the only completed example so far.
