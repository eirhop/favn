# Operator UX Review and Refactor Plan

The plan for [#524](https://github.com/eirhop/favn/issues/524). It records what
the operator UI gets wrong today, why, and the order in which to fix it. Visual
language and component rules live in [`style-guide.md`](style-guide.md) and
[`component-patterns.md`](component-patterns.md); this page is about whether an
operator can actually do their job.

## Diagnosis

The UI answers "what exists" and rarely "what is wrong and what do I do about
it". Every route is a catalogue or a detail page reached by knowing what to look
for. There is no page that says *here is what needs you*, so finding a failure
means visiting `/runs`, guessing which group is red, expanding it, and then
opening logs that do not explain the failure.

Three structural causes, all measured rather than asserted:

**The element library exists but is ~20% adopted.** `FavnView.UI` owns colour,
surface, radius, and state, and four page components honour it. Against that:
43 raw `<button class="btn …">` versus 6 `<.button>`, 7 raw `<table>` versus 1
`<.data_table>`, and `<.control_surface>` and `<.rail>` are used zero times in
production while their raw classes are pasted into markup 47 times across 12
files. `log_viewer.ex` bypasses the theme entirely with hardcoded `slate-*`
colours. The consequence is not only inconsistency: it is that fixing a surface
means editing twelve files.

**The four-state contract is documented and unenforced.** `ui/state.ex` says
every list and page has content, loading, empty, and error states, and that a
page rendering only content is incomplete. Five page components declare none of
the other three: `asset_detail_page`, `run_detail_page`, `rebuild_page`,
`pipeline_detail_page`, and `log_viewer`. Worse, `loading` is hardcoded to
`false` at mount in every list LiveView and there is no `assign_async` anywhere,
so even the loading states that *are* written are unreachable — every page
blocks in `mount/3` and shows nothing until it can show everything.

**Controls exist that do nothing.** Not stubs marked as such, but buttons that
look live: the asset timeline's previous/next window and calendar controls have
no `phx-click`; the run timeline's "Group by" select is hardcoded `disabled`;
lineage ships a disabled search advertising "coming soon", four of five view
tabs disabled, and inspector buttons that render labels and wire no event. Six
of the nine filters on `/runs` are assembled by the LiveView and silently
dropped by the facade, which reads only `:limit` and a derived status. Twelve
event handlers end in a catch-all that discards the click with no feedback —
including `start_rebuild`, a destructive action.

## The frame: what an operator needs first

Everything below follows from one question. When an operator opens Favn, they
are in one of three situations, and the UI should answer the right one without
being asked:

| Situation | The question | Where it is answered today |
| --- | --- | --- |
| Something broke | What failed, why, and can I retry it? | nowhere directly — `/runs`, then guesswork, then logs |
| Something is late or missing | Which assets are stale or have coverage gaps? | `/assets/:id`, one asset at a time |
| I need to do a thing | Run, backfill, rebuild, recover this target | scattered across four routes with inline forms |

A page per noun (assets, pipelines, runs, schedules) is a data model, not a
workflow. The refactor reorganises around those three situations.

## Cross-cutting work

These land before or alongside page work because every page depends on them.

### 1. Muted text fails contrast in both themes

The audit measures 22 text-contrast failures in the dark theme and 25 in the
light one across the element library alone. Nearly all are opacity-based muted
text: `text-base-content/45` and friends. On dark, near-white at 45% over
near-black survives; the same ramp on the light theme washes out at 2.94:1
against a 4.5:1 limit. This cannot be fixed by tuning a token — a 45% alpha of
dark-on-light caps near 3:1 — so the opacity ramp must be replaced with semantic
tokens (`--favn-text-muted`, `--favn-text-subtle`) resolved per theme, then
swept through the components.

### 2. Faint borders on identifying boundaries

Five boundary-contrast failures at 1.5–2.5:1 against a 3:1 limit, all on
controls whose border *is* their affordance: `list_card` in its navigable and
selected states, checkboxes, and the surface icon button. A selected list card
must be distinguishable from an unselected one by more than a 1.6:1 hairline.

### 3. No idempotency, and UI commands are unaudited

Every facade command accepts `:idempotency`/`:command_id`; no LiveView passes
one. Two consequences. A double-click submits two unrelated commands, which
violates the repository rule against retrying possibly completed side effects.
And `Operator.Schedules.activation_command_id/1` returns
`{:error, :idempotency_key_required}` when the key is absent, so the enable and
disable buttons on `/schedules/:id` appear to fail unconditionally — worth
confirming against a running stack, but the code path is unambiguous and has no
test. Separately, three facade functions carry a stale TODO and do not audit:
runs and backfills submitted from the browser are unaudited while the same
commands through the CLI are.

### 4. Dead code to delete before it gets refactored

`run_overview_hud.ex` (270 lines) had no caller anywhere — no LiveView, no
example, no test — and was superseded by `run_detail_page/overview.ex`. Deleted.
`run_detail_page/samples.ex` (781 lines of fake data) is compiled into `lib/`
and re-exported through 14 `defdelegate sample_*` lines on a public page module;
it belongs in the design-system fixtures. `lineage_page/1` is an orphan page
component with no route. Deleting these first avoids migrating markup nobody
renders.

`Telemetry.metrics/0` is *not* dead, despite the commented-out `ConsoleReporter`
next to it: `live_dashboard "/dashboard", metrics: FavnView.Telemetry` calls it.
Left alone.

### 5. Facade functions the UI needs and does not have

Four gaps require orchestrator work before the UI can close them, because
`favn_view` may only call the public facade and the API reaches these through
internals: `get_operator_backfill/2`, `page_operator_backfill_windows/3`,
`plan_operator_pipeline_backfill/4`, and a manifest-release trio (list, get,
activate). Also delete `submit_operator_asset_run` and
`submit_operator_pipeline_run`, which have zero callers and whose own docs tell
callers not to use them.

## Page-by-page

### New: `/` status home

Today `/` redirects to `/assets`; there is no home. It becomes the answer to
"what needs me": failing and stuck runs, assets breaching freshness, coverage
gaps, schedules disabled or erroring, rebuilds and recoveries awaiting
reconciliation. Each row states the problem in operator language and links
straight to the one action that addresses it. Nothing decorative, no counters
without a next step. When nothing is wrong the page says so — that is a real
state, not an empty one.

### `/assets` — a browsable catalogue

Currently a flat table plus a lineage mode. It should be browsable by the
namespace/module hierarchy the assets already declare, so an operator navigates
a tree they recognise instead of scanning or searching. Status becomes a colour
coded icon with a hover/focus explanation rather than a text badge: the badges
are the widest column and the least information per pixel, and the audit shows
they are what pushes rows past the viewport. Keep search, keep `<.data_table>`,
lose the hand-rolled filters.

### `/assets/:id` — the default view is the timeline

The strongest page conceptually and the worst structurally: 49 attrs, none of
them a state, three top-level components in one `render/1`, one identical panel
class stack copy-pasted ten times, and three dead controls. Target: asset title,
health, and the window timeline; everything else behind progressive disclosure.
Coverage, freshness, and compatibility stay but get real styling and honest
empty states. Window paging gets wired or removed — a dead arrow is worse than
no arrow.

### Running an asset becomes a dialog

Run configuration is currently inline, always visible, and shows every option at
once. It becomes a dialog opened by the primary action, defaulting to the common
case with advanced options collapsed. This is also where the CLI gap closes: the
dialog should carry `refresh_mode`, `dependency_mode`, `window`, and, behind
advanced, `retry_policy` and `timeout_ms` — and pass an idempotency key. The
pipeline run button currently sends no options at all, so `/pipelines/:id` cannot
re-run last month's window; the same dialog serves both.

### `/runs` and `/runs/:id`

Make the six inert filters real (facade work) or remove them, and thread the
`next_cursor` that `page_execution_groups` already returns instead of a fixed
limit of 100. On run detail, `timeline.ex` is 670 lines with 95 raw utility
stacks and zero library components — the largest single migration in the app.
Add the missing loading and empty states; replace the bespoke not-found panel
with the library one.

### Logs become a terminal

The weakest page against its purpose. Hand-rolled states, hardcoded `slate-*`
colours outside the theme, and — the real failure — it does not explain a
failure. Target: a dense monospace view that reads like a terminal, with
severity and stream visible at a glance, the failing step and its error surfaced
at the top rather than buried in the tail, stable anchors for linking to a line,
and search within the run. An operator arriving from a failed run should see the
error without scrolling.

### Pipelines, schedules, rebuilds, recovery

The same three fixes in each: adopt the element library, add the missing states,
and stop swallowing clicks. Specifics: `rebuild_page` declares only an error
attr across 506 lines; `rebuild_detail_live` renders a bare `<p>` inside the app
shell for its error case while `ErrorPage` already exists; `pipeline_detail`'s
backfill submit can vanish silently; schedule enable/disable needs the
idempotency fix above. Add the missing backfill status and window views — an
operator who submits a pipeline backfill today is redirected to a run and can
never see expected versus succeeded versus failed windows, or the failed-window
list with its per-window error. That is the largest operational blind spot in
the product.

### Explicitly deferred

**Lineage.** The single largest concentration of unfinished work: 1053 lines,
disabled search, four of five modes disabled, unwired inspector actions, a
hardcoded subtitle that ignores the graph, decorative minimap controls, a
120-line fake DAG inside the page component, and two dangling function
references. It needs a design, not a refactor. Revisit after the rest lands,
first as immediate upstream/downstream on the asset detail page — which is where
the question is actually asked — and only then as a whole-graph view.

**Design-system examples for the 75 uncovered components.** Deliberately last:
components will be created, changed, and deleted by this work, so writing
examples first means writing them twice.

## Order of work

1. **Done in part.** Dead code: `run_overview_hud.ex` and `page_live.ex` deleted;
   `samples.ex` and the `lineage_page/1` orphan remain. The facade functions and
   the idempotency fix are **not started** — they change `favn_orchestrator`, so
   they need a decision about scope beyond `favn_view`.
2. **Done.** Semantic text tokens replaced 348 opacity classes, boundary contrast
   fixed, and the element library measures 295 pass / 0 fail in both themes. A
   second gate, `FavnView.Dev.DesignSystem.Palette`, now checks every token pair
   in `mix test` without a browser.
3. **Done.** `/` is the status home.
4. **Done.** Asset state is three glyphs instead of three word badges, the
   catalogue browses by connection · catalogue namespace, and run
   configuration is a dialog (`FavnView.UI.Dialog`) leading with the two
   sentences that matter.
5. Runs list and run detail, including the timeline migration. Not started.
6. **Done.** The log viewer is a dense terminal: one line per entry on a
   fixed-dark canvas (`--favn-terminal-*`, palette-gated), identity behind a
   per-line disclosure, error lines marked and jumpable, live tail that
   pauses in scrollback, and a step strip on run-scoped pages naming the
   failed step.
7. Pipelines, schedules, rebuilds, recovery. Not started.
8. Design-system examples and coverage; then lineage. Not started.

Each step ends with `window.favn.audit()` clean for the components it touched,
and the flow exercised against the example project rather than fixtures alone.

## Open questions

Decisions that need a human, recorded rather than guessed:

1. **Do the four missing facade functions get added?** Backfill status, backfill
   windows, pipeline-backfill planning, and the manifest-release trio all require
   new `favn_orchestrator` functions, because the API reaches them through
   internals that `favn_view` may not call. Without them those gaps cannot close.
2. **Is a solid primary button variant wanted?** The primary action is outlined
   now. A filled variant reserved for confirm-in-a-modal was deferred rather than
   built speculatively; the run dialog is the first real caller.
3. **How far does "namespace hierarchy" go for the catalogue?** Assets carry
   connection and catalogue, which gives a natural two-level tree
   (connection → catalogue → asset). If the intent was the *module* hierarchy of
   the defining pipeline instead, that is a different tree and a different query.
4. **Dark labels on bright fills.** Solid `primary`, `secondary`, and `error`
   surfaces now carry dark labels, matching what `info`, `success`, and `warning`
   already did, because near-white on those fills measured 2.6–3.2:1. It is the
   accessible choice but it does read differently from a conventional filled
   button; worth a look before it spreads further.
5. **Where do connection icons come from?** The catalogue draws one neutral
   glyph per connection, because connection names are project-author-chosen
   (`warehouse`, `sf_prod`) and a name-to-vendor mapping guesses wrong outside
   the examples. The honest mechanism is the adapter behind a connection
   declaring its own icon (plugin manifest → facade → view model); that is a
   cross-app change and needs a decision. Asset *type* icons (table, view,
   file, metric) come from the declared type and need no configuration.
6. **Light theme needs its own colour pass.** The dark theme carried the
   design; light was derived and it shows. Revisit base surfaces, tone
   weights, and the action colour in light mode as one deliberate pass.
