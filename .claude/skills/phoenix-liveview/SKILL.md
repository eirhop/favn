---
name: phoenix-liveview
description: Use when working in apps/favn_view with LiveViews, HEEx, function components, page components, layouts, LiveView tests, the /design-system browser, Tidewave-assisted UI work, or small colocated hooks/JS.
---

# Phoenix LiveView Skill

Favn's UI is components-first. The architecture, component contracts, naming,
design-system strategy, and testing rules are documented — read them and follow
them rather than inventing a structure:

- [`docs/design/component-patterns.md`](../../../docs/design/component-patterns.md)
  — the four layers, LiveView thinness, page components, contracts, naming, the
  design system, testing, boundaries, adding a screen.
- [`docs/design/style-guide.md`](../../../docs/design/style-guide.md) — the
  visual contract and the definition of done.
- [`docs/structure/favn_view.md`](../../../docs/structure/favn_view.md) — what
  `favn_view` owns and what it must never call.

Load `favn-design-system` as well before touching anything in
`apps/favn_view/lib/favn_view/ui/`.

## The shape of the work

```text
LiveView -> page component -> section components -> FavnView.UI elements
```

A LiveView loads data through the public `FavnOrchestrator` facade, handles
params and events, prepares view-model assigns, and renders exactly one page
component per branch. It contains no page markup.

Start from `/design-system`, not from a blank file: check what already exists,
reuse it, and only then write something new.

## Non-negotiables

- `favn_view` calls backend behaviour only through the public orchestrator
  facade. Never storage, scheduler, runner, repo, compiler, adapter, or plugin
  internals. Runtime inspection does not relax this.
- Mutating events authorise server-side. Hiding or disabling a control is
  presentation only.
- A route added to, removed from, or renamed in `router.ex` is catalogued in
  `deployment/docker-compose/security/catalog.json` in the same change, or a
  required CI job fails. Sub-routes of one screen each need their own entry.
  Follow
  [`docs/production/security_qualification.md`](../../../docs/production/security_qualification.md#add-or-change-a-route)
  — it also states the behaviour a catalogued route must have, including
  answering 200 for an id that does not exist.
- Reusable components declare explicit `attr` and `slot`, and keep stable DOM ids
  and `data-testid` selectors.
- Every page component covers content, loading, empty, and error.
- Prefer LiveView-native behaviour over custom JavaScript.
- Do not add product UI features that were not asked for.

## Tools

`/design-system` and `/tidewave/mcp` live on the umbrella development server
only. Start it, and register the MCP endpoint, as described in
[`docs/contributing/dev-server.md`](../../../docs/contributing/dev-server.md).

- Tidewave for runtime inspection: logs, assigns, source lookup, docs. One
  endpoint covers the View and the orchestrator. Say so explicitly if the server
  is not running and you are working from static inspection only.
- `/design-system` for the component contract, and `/design-system/render?id=…`
  to render only what you are inspecting.
- A browser for rendered behaviour, at `390x844`, `768x1024`, and `1440x1000`.
- A Favn project such as `examples/basic-workflow-tutorial` for checking the UI
  against a real workspace and real data. It has neither the design system nor
  Tidewave.

On a design-system render page, measure before you look:

```javascript
window.favn.audit()      // verdicts, measurements, and the box of every example
window.favn.summary()    // just the failures and the boxes
```

One call returns both the verdicts and the geometry, so cropping a screenshot to
one component afterwards needs no second call. Convert a box to screenshot pixels
with `screenshot_width / viewport.inner_width` from that same call.

Browser inspection is not a substitute for tests. Important flows get a
`Phoenix.LiveViewTest` test.

## Verification

```bash
mix do --app favn_view cmd mix compile --warnings-as-errors
MIX_ENV=test mix do --app favn_view cmd mix test --no-compile
```

Use the umbrella suite only when the change crosses app boundaries.

## Reference

- Function components, attrs, slots: https://hexdocs.pm/phoenix_live_view/Phoenix.Component.html
- DaisyUI: https://daisyui.com/llms.txt
