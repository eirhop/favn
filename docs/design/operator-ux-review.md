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

## What the example project showed

Every screen above had been judged against design-system fixtures. Run against
`examples/basic-workflow-tutorial` with two real pipeline runs and two real
schedules, the fixtures turn out to have been flattering. These are findings
from that walkthrough, worst first.

### Wrong, not just ugly

1. **The runs list drops data the run detail has.** Every row shows
   `Target: No target`, `Trigger: Unknown`, `Window range: No explicit window`,
   and `Duration: -`. The same runs on `/runs/:id` show the pipeline, the
   trigger, `Jul 23`, and `9.1 s`, and `mix favn.runs list` shows the target
   too. The list's view model is not reading fields that are present.
2. **A finished run reports queued work.** On run detail, the asset-window grid
   marks every cell where an asset did not run in that window as `Queued` —
   twelve of them, on a run that completed. "Did not apply" and "waiting to
   start" must not share a label.
3. **Asset detail says `Healthy` above `Incomplete`.** The header badge reads
   Healthy while the page below reports coverage incomplete, six missing
   windows, and "no successful freshness evidence exists". One of the two is
   lying; the badge is.
4. **A cron is unreadable in the header.** `0 2 * * *` reads as `0 2***`. The
   markup is innocent — the DOM holds the spaces and sets `nowrap` — but the
   value is set in a proportional sans face at 12px, where `* * *` merges into
   one glyph cluster. A cron is code and has to be set as code.
5. **A stale sign-in flash sits on the status home.** A red "Please sign in to
   continue" is shown *while signed in*, left over from the login redirect, so a
   healthy page opens looking like a failure.

### Identity is unreadable

6. **Raw target ids as titles.** The status home lists
   `asset:Elixir.CrmDemo.Warehouse.Core.Sales.Events.Engagement:asset` — scheme
   prefix, `Elixir.` prefix, and all. It is the widest thing on the page.
7. **Six catalogue rows named `asset`.** An asset whose module declares no name
   defaults to `asset`, so the Asset column shows six identical rows. The leaf
   name alone cannot identify an asset; `/pipelines` already solves this by
   showing the name bold with its module underneath, and it is the most readable
   list in the product because of it.
8. **A 90-character base64 id printed under every schedule name**, which widens
   the table until the activation and runtime columns are pushed off-screen
   behind a horizontal scrollbar. The id belongs behind the copy button.
9. **Namespace reads `unknown · uncatalogued`** for every Elixir asset, in
   colour, so the dependent filters added in step 4 have nothing to narrow.
   Either derive the namespace from the module path or leave the cell empty.
10. **Window keys are raw.** Asset detail's gap list shows
    `day:Etc/UTC:2026-07-22T00:00:00.000000Z` six times where it means "Jul 22".

### The logs terminal's premise does not hold

11. Every orchestrator log message is `asset execution started`,
    `asset execution finished`, or `step queued`, with `orchestrator` as the
    source. One line per entry was designed on the assumption that the message
    distinguishes the line; against real data forty identical lines do not. The
    asset ref has to be *on* the line, not behind its disclosure.

### Buttons outside the family

12. Twenty-three raw DaisyUI colour buttons remain across ten files
    (`btn-primary`, `btn-warning`, `btn-info`, `btn-error`), six in
    `rebuild_page.ex` alone. `/rebuilds` therefore shows a solid bright blue
    "Create immutable plan" — the loudest control in the product — next to
    `/recoveries`, which uses the action colour correctly.

### Smaller, still worth fixing

13. Seven status-home rows give the identical reason "Declared windows are
    missing", so the page cannot be triaged, only clicked through one row at a
    time. Two disabled schedules — exactly "what needs me" — do not appear at
    all.
14. The state glyph on each status row carries the full target id as its
    accessible name, so a screen reader hears the id twice per row.
15. Run detail's header spends six cards on four zeros, and its back link says
    "Back to asset" for a pipeline run reached from `/runs`.
16. `1 windows`.
17. `/pipelines` shows `Selected assets: Engagement +1` for a pipeline that ran
    fourteen assets.
18. Schedule and rebuild pages ask for a target id in a bare text field with no
    picker, placeholder, or format hint.
19. Filter select labels clip on `/schedules` (`Runtime▾`, `Window▾`), and
    `Clear` renders as though active when no filter is set.

## Run detail: what the screen should say

Run detail is the screen an operator opens when something matters, so it is the
one worth designing from the question rather than from the data model. Today it
is designed from the data model, and that is the whole problem.

### What the operator is actually asking

Four questions, in this order, and nothing else:

1. Is it done, and did it work?
2. If it failed: what failed, why, and what does that block?
3. If it is running: what is it doing now, and is it stuck?
4. Did the data come out right?

Everything the page currently shows that answers none of these is forensic:
execution group ids, manifest version ids, window keys, event sequences,
`effective_window_count`. Forensics are not deleted — they move behind an
explicit request to see the receipts. A page that shows them by default is
telling the operator that it does not know which facts matter either.

### Why the current structure cannot answer them

**Five modes are one question wearing five hats.** Overview, Timeline,
Failures, Window runs, and Events are all projections of a single list of asset
attempts — filtered, grouped by parent, laid on a time axis, or dumped raw.
Splitting one question across five peer tabs is not progressive disclosure; it
is a filing cabinet, and it makes the operator reconstruct the run in their head
from four partial views.

**Seven stat cards are one number and its own breakdown.** "Requested windows
1/2", "Effective windows 2", "Asset attempts 5/12", "Succeeded 5", "Failed 0",
"Running 0", "Queued 7" — four of those are a decomposition of the third, and
the first two are compiler vocabulary. Seven cards of equal weight answer
question 1 by making the reader do arithmetic. Against the example project this
was six cards showing four zeros (finding 15).

**Both primary visuals throw away the run's structure.** A run is a DAG
executed over time. The matrix keeps neither: it is asset × window, so
dependency order and duration are both absent, and it grows as the product of
two dimensions — twelve assets over a thirty-day backfill is 360 cells. The
timeline keeps time but sorts rows into Running / Ran / Queued / Skipped, which
actively destroys dependency order, the single most useful ordering a run has.
Neither can show the thing an operator most needs after a failure: what the
failure *blocked*.

### The design

One primary visual, and everything else is a selection on it.

**Header: one sentence and one bar.** The status, the counts inline, the
elapsed time, and what triggered it, as prose — then a single slim meter
segmented by outcome. That meter is the seven cards, readable without
arithmetic, and it doubles as the colour legend for the rest of the page.

**Primary visual: asset lanes, grouped by stage, positioned in time.** Rows are
assets, grouped by execution stage, which *is* the dependency order. Each lane
carries one bar per window, placed on a shared time axis. This is the only
layout that answers all four questions at once:

- order and dependencies, from the stage grouping
- what is running now, from a bar with a live edge
- what a failure blocked, from empty lanes in the stages after it
- duration and the critical path, from bar lengths

**Failures render where they happened.** The failing asset's error appears in
its own lane, with retry next to it. There is no Failures mode: a run with
failures shows them in place, and a healthy run shows no failure chrome at all,
which is the correct amount for a healthy run.

**Windows become a filter, not a mode.** A chip row over the same visual.

**Selecting an asset answers question 4 in one line.** Not twenty flattened
metadata rows — the verdict: rows written, the relation written to, and the
check outcome. `rows_written`, the target relation, and the quality verdict are
the three facts worth promoting; the remaining metadata and the raw JSON stay
behind disclosure, and check *names* appear only when a check warned or failed.

**Events leave the rail.** An event stream is a debug view of a table. It stays
reachable; it is not a peer of "what happened in this run".

Net effect: five modes and seven cards become one screen, one meter, one visual,
and one detail panel.

### Performance: this screen is live

Run detail subscribes to run events and refreshes on a 100 ms coalesce, so
everything below happens up to ten times a second while a run is active.

1. **The whole run re-projects on every refresh.** `load_run/4` refetches and
   rebuilds one large `@run` map, so every component taking `run={@run}` is
   invalidated and LiveView re-diffs the entire page when one attempt changed.
   Volatile state has to be separated from stable state, and attempt rows have
   to be a keyed `stream` so a status change patches one row.
2. **Three quadratic passes.** `child_runs_from_public/3` filters every attempt
   per child run, `timeline_from_public/2` searches every attempt per timeline
   entry, and `matrix/3` walks every window per asset. All three are single
   grouped passes.
3. **A large part of the DOM exists only for tests.** `overview.ex` renders four
   separate `sr-only` regions that repeat every attempt's name, key, status,
   stage, window, and error as text, plus `legacy_asset_text` — a concatenation
   of every field of every asset result, rebuilt on every refresh and rendered
   into a hidden div so `data-testid` assertions can grep it. The run's data is
   rendered twice per refresh, and the second copy is invisible. Tests should
   assert against the real markup.

### Not a UI finding, but it blocks UI review

`mix favn.dev` in the example project intermittently tears the whole stack down
about forty seconds after it starts serving, with
`** (Mix) failed to start Favn development: :runner_start_timeout`. The runner
does leave evidence, and the evidence is not a timeout: `erl_crash.dump` in the
project root decodes to

```
Runtime terminating during boot ({badarg,[{io,put_chars,[standard_error, …
  ** (ArgumentError) errors were found at the given arguments:
    * 1st argument: the device does not exist
```

So the runner raised something — the masked text begins
`** (ErlangError) Erlang error: :term…` — and then died *reporting* it, because
`FavnLocal.RunnerProcessLauncher` starts it without a `standard_error` device.
The launcher's 30 s `@runner_start_timeout_ms` then reports the silence as a
timeout, which is why the symptom names the wrong cause.

Two separate defects: a runner that cannot report its own errors, and a
supervisor that translates "child said nothing" into "child was slow". The
first hides every runner boot failure in this launch mode.

### Confirmed working against a real backend

Schedule enable and disable, which the review had recorded as "appears to fail
unconditionally", now works: `crm_daily` went Disabled → Enabled from the
browser, the activation, runtime, and next-due cards all updated, and
`mix favn.schedules list` reports `state=enabled`. The control did not exist
before; the handler behind it had never been reachable.

## Runs list: four questions, not nine filters

The list carried nine filter controls across two rows, a five-metric summary
band, and a black "Showing 1-8 of 8 runs" bar pinned to the bottom. Six of the
nine filters did nothing: only `status`, `only_failed`, and `only_running`
reached the store, and `search`, `trigger`, `target`, `window`,
`only_incomplete`, and `sort` were decoration. Setting `Trigger = Backfill` in
the browser left all eleven rows on screen, still showing Pipeline and Manual.

### What the operator is actually asking

1. What is currently running?
2. What has failed today?
3. What has run today — did all my expected assets run?
4. Has pipeline X run each day this month?

Each is a pair of a time range and a status, which is the whole filter model.
Everything else the old page offered was a knob without a question behind it.

### The design

One control per axis. The status is four buttons — Running (including queued),
Failed, Succeeded, All — each carrying its own count from the store rather than
from the loaded page, so the counts stay true when the page is truncated. The time
range is a select, with a search beside it. A button that set both axes at once
was tried and rejected: it took the range away from whatever the operator had set,
so four buttons meant four combinations rather than one axis.

The counts narrow by everything the list narrows by except the status, so a count
is the number of rows clicking it produces.

Question 4 is a question about *absence*, and a list can only answer it if the
days with nothing in them are on screen. So whenever the range covers more than
one day the table grows day headers, and every day in the range gets a row.
Consecutive empty days collapse into one line — twenty-eight rows each saying
"no runs" is the right answer told badly, and `28 Jul to 1 Jul · no runs · 28
days` is the same fact readable at a glance.

A truncated page never claims a day was empty: day enumeration shrinks to the
span actually loaded, because a day past the end of a page is unknown, not empty.

The bottom bar is gone. The count lives on the active button, and a page with
more behind it ends in a "Load 50 more" button that scrolls with the content.

### What the store had to learn

`page_execution_groups/2` now narrows by `search`, `trigger_type`,
`started_after`, `started_before`, and a status that may be a list, and orders by
when the group's root run started rather than by last activity — so ordering,
filtering, and the keyset cursor all agree about what "recent" means. `search`
matches the root run id and the module or name of any target any run in the group
declared; it does not reach connection, catalogue, or schema, which are not part
of this read model.

`count_execution_groups/2` is new: one query with filtered aggregates returning
`active`, `failed`, `succeeded`, and `total`, narrowed by the same filters as the
page read.

### Known cost: the sort key is not on the projection

Ordering by when the root run started means ordering by `runs.inserted_at` across
a left join, and neither `execution_group_overviews` nor `runs` has an index that
serves it — the old `latest_event_id` ordering did. At thousands of groups this is
a join plus a sort of the whole filtered set, repeated on every live refresh. The
fix is to denormalise `started_at` onto the group projection with an index on
`(workspace_id, started_at desc, root_run_id)`, which would also make the window
filters and the counts indexable. Not done yet.

Paging is also only half wired: the store has a keyset cursor, but the list grows
by re-reading with a larger `limit`, capped at 500. Beyond that an operator has to
narrow rather than scroll.

### Three things the page was saying wrongly

- **A pipeline run was named by the first of its assets.** Fourteen asset targets
  rendered as `activities +13 more`, which describes the plan rather than the
  submission. The group projection now carries `pipeline_refs` separately, and the
  row reads `crm_daily · 14 assets`.
- **Run counts were labelled as assets.** `total_asset_attempts` on the group
  projection is the group's *run* count; the list reported `1 / 1 assets` for
  every ordinary run. It now says runs, and says nothing at all when there is one
  — a meter reading `1 / 1` is chrome pretending to be information, and a whole
  column of it was the page's widest dead space.
- **Backfills showed `Duration 0 ms`.** A backfill's root run finishes as soon as
  it has submitted its windows. For a multi-run group the duration is now the
  group's own span, which is what the operator means by how long it took.

### The inline window-run expansion is gone

Each expanded row cost a `get_execution_group_detail/3` call, repeated on every
refresh — at 1.5 s under live events that is one facade call per expanded row per
tick. Run detail owns window runs now, and the whole row navigates there.

### Deliberately not built yet

A per-day calendar for question 4 needs a new aggregate read and is a separate
screen's worth of work; the day-grouped table answers the question without it.
Sorting by duration would need SQL ordering on a computed column and was not
asked for.

## Open questions

Decisions that need a human, recorded rather than guessed:

1. **Resolved: the missing facade functions are added.**
   `get_operator_backfill/2`, `page_operator_backfill_windows/3`,
   `plan_operator_pipeline_backfill/4`, `list_operator_manifests/1`, and
   `get_operator_manifest/2` now exist on `FavnOrchestrator`, and the dead
   `submit_operator_asset_run`/`submit_operator_pipeline_run` are deleted.
   Manifest *activation* from the browser is still open: the API's activation
   path requires a platform service context and idempotent-command plumbing
   that a same-BEAM operator session cannot currently express. The backfill
   status/windows UI that consumes the new reads is the next piece of work.
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
