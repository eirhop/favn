# Favn View Component Patterns

This document is the canonical code contract for UI work in `apps/favn_view`.
The visual rules live in [`style-guide.md`](style-guide.md); the app's ownership
boundaries live in [`../structure/favn_view.md`](../structure/favn_view.md). What
the current UI gets wrong against this contract, and the order in which it is
being fixed, is in [`operator-ux-review.md`](operator-ux-review.md).

## The four layers

```text
LiveView            data, params, events, view-model assigns
  └─ page component one per screen; chooses the state; composes sections
     └─ section     a meaningful region: shell, filters, table, panel group
        └─ element  FavnView.UI.*: knows no domain, owns all styling
```

| Layer | Lives in | Knows about the domain? | Owns styling? |
| --- | --- | --- | --- |
| LiveView | `lib/favn_view/*_live.ex` | yes | no |
| Page component | `lib/favn_view/components/*_page.ex` | yes | no |
| Section component | `lib/favn_view/components/*.ex` | yes | layout only |
| Element | `lib/favn_view/ui/*.ex` | no | yes |

Each layer may only call downwards. An element never reaches for a page's
helper; a page never re-implements an element's border.

### LiveViews are thin

A LiveView loads data through the public orchestrator facade, handles params and
events, prepares simple view-model assigns, and renders **one** page component
per branch:

```elixir
def render(assigns) do
  ~H"""
  <AssetCataloguePage.asset_catalogue_page
    assets={@assets}
    filters={@filters}
    active_mode={@active_mode}
    nav_items={@nav_items}
    flash={@flash}
  />
  """
end
```

A LiveView must not contain page markup. If a branch needs a whole different
screen — not found, backend failure — that branch renders
`FavnView.Components.ErrorPage`, not an inline shell plus a panel.

### Page components own the state machine

The page component decides which of the four states renders, and nothing else
decides it:

```elixir
<.loading_state :if={@loading} label="Loading assets" />
<.error_state :if={!@loading && @error} title="Could not load assets" description={@error} />
<.empty_state :if={ready? && @assets == []} title="No assets found" ... />
<.asset_table :if={ready? && @assets != []} assets={@assets} />
```

Page components own domain vocabulary: which atom means which tone, and what
each status is called in English. They own no colour classes.

### Elements are the design system

`FavnView.UI` is imported by `use FavnView, :html`, so every component can call
`<.button>`, `<.panel>`, `<.status_badge>`, `<.data_table>` directly. See
`FavnView.UI` for the module map.

Because they are imported everywhere, element function names are effectively
reserved. Do not define a local `status_badge/1`, `empty_state/1`, or
`icon_button/1` in a page module — Elixir will refuse to compile it, and that
error is the design system working as intended.

## Where domain mapping belongs

Mapping a backend status to a tone and a label is presentation, and it belongs in
the page or section component that renders it:

```elixir
defp status_tone(:succeeded), do: :success
defp status_tone(status) when status in [:queued, :pending], do: :warning
defp status_label(:succeeded), do: "Succeeded"
```

Use `FavnView.UI.Tokens.tone/1` when the atom names already carry the meaning.
Write the mapping out explicitly when they do not. Never duplicate the same
mapping in two modules — move it to the section component both pages use.

## Component contracts

Every reusable component declares explicit `attr` and `slot`:

- `attr` with `values:` for enumerations, so a typo is a compile-time warning.
- `attr :rest, :global` when callers legitimately need to pass `data-testid`,
  `phx-*`, or ARIA attributes.
- Required attrs are genuinely required. Everything with a sensible default gets
  one.
- Document *when to use which variant* in the `@moduledoc`, not just what the
  variants are.
- Booleans that describe state read as questions: `loading`, `selected`,
  `content_scroll?`.

Stable selectors are part of the contract. Keep `data-testid` on the elements
tests and agents navigate by; renaming one is a breaking change to the tests.

## Naming

| Thing | Pattern | Example |
| --- | --- | --- |
| Page component module | `FavnView.Components.<Screen>Page` | `AssetCataloguePage` |
| Page component function | `<screen>_page/1` | `asset_catalogue_page/1` |
| Section component module | `FavnView.Components.<Thing>` | `NavRail`, `ModeRail`, `AppShell` |
| Element module | `FavnView.UI.<Concept>` | `UI.Badge`, `UI.Surface` |
| LiveView | `FavnView.<Screen>Live` | `AssetCatalogueLive` |
| Design-system example | keyed by entry id | `"asset_catalogue_page/asset_catalogue_page"` |

Names are boring and literal. A component called `panel` renders a panel.

## The design system

`/design-system` on the umbrella development server is the UI contract for humans
and agents. Check it before building anything new; reuse before adding. See
[`../contributing/dev-server.md`](../contributing/dev-server.md) for how to start
the server and how to query it.

It is a development-only surface, compiled from `apps/favn_view/dev/`, which is
outside `elixirc_paths` for `:prod`.

### Nothing is registered

The catalogue is built from `Phoenix.Component.__components__/0`. A new component
appears the moment it compiles, its attrs and slots are read from the component
itself, and a component with no example is listed as a gap rather than being
absent. There is no file to remember to add and nothing that can silently drift
out of date.

Two rendering modes need no code at all:

- `mode=defaults` renders the component with every optional attr at its declared
  default. If that looks wrong, the defaults are wrong.
- `mode=matrix&axis=size` walks an attr's declared `values`, so a new size shows
  up on the commit that adds it. `axis=tone` walks
  `FavnView.UI.Tokens.tones/0` instead, because components delegate the tone
  vocabulary there rather than restating it.

### What examples are for

An example supplies what metadata cannot know: realistic copy, slot content, and
the combinations that only matter together. They live in
`apps/favn_view/dev/design_system/examples/`, keyed by entry id.

| Provider | Covers | Should show |
| --- | --- | --- |
| `Examples.Elements` | `FavnView.UI.*` | every variant side by side, and the long label that must not wrap |
| `Examples.Components` | section components | the section in the states the backend can actually be in |
| `Examples.Pages` | page components | the whole screen, including loading, empty, and error |

Rules:

- An example is either a map of attrs or a HEEx block, never both. Use attrs when
  the component takes no slots — it is data, so it cannot drift into a second
  implementation of the component. Use HEEx when the example needs slots, `:let`,
  or several components composed together.
- **Every page has loading, empty, and error examples.** A page with only its
  content state is incomplete.
- Give an example a doc whenever the interesting thing is *why* the state exists.
  `:rebuild_required` and `:unexpected_drift` must not look alike, and the doc is
  where that intent lives.
- View models come from `apps/favn_view/dev/design_system/fixtures/` and from the
  `sample_*` functions next to the components, so an example and a component test
  render the same page. Examples must not call orchestrator, storage, runner,
  compiler, or plugin internals.

`test/favn_view/dev/design_system_test.exs` renders every curated example and
fails if any of them raise, so a broken example is a failing test rather than a
page you have to open to discover.

### Verifying a change

Measure first, look second. On a render page, `window.favn.audit()` returns
contrast, target size, accessible names, and clipping as pass, fail, or skipped —
together with the bounding box of every example, so a screenshot can be cropped
to one component without a second call. Thresholds live in
`FavnView.Dev.DesignSystem.Audit` and are unit-tested there.

Editing a reusable component is not finished until it has been rendered and
looked at, at mobile and desktop width, in dark and — if the change touches a
surface, border, or tone — light.

## Variant, or new component?

Grow the system by adding variants, not siblings. Ask in this order:

1. **Can an existing element already do it?** Reuse it, even if the result is
   slightly less pretty than a bespoke one. Consistency beats local optimisation.
2. **Is it the same thing with a different weight, tone, or size?** Add a variant
   to the existing element. `variant`, `size`, and `tone` are the three axes that
   grow; anything expressible on those axes is not a new component.
3. **Is it the same thing with different content?** Add a slot.
4. **Is it genuinely a different thing?** Add an element, in its own module,
   with a moduledoc that says when to choose it over the neighbour it resembles,
   and an example that covers what its defaults cannot show.

Two elements that differ only by a class are one element with a variant. Two
elements whose moduledocs cannot explain when to pick each are one element.

A one-off that only one page will ever use is not an element. Keep it as a
section component next to that page, and promote it the second time it is
needed — not the first.

## Deprecation

Favn is private pre-v1 software, so the design system takes clean breaking
changes over compatibility layers:

- Renaming or removing an element updates every call site in the same change.
- Delete compatibility aliases rather than leaving them. `favn-glass-panel`,
  `favn-control-glass`, and `favn-icon-rail` are legacy aliases of the semantic
  surface classes; new work must not use them, and they should shrink over time.
- If a component must diverge from the design system, say why in its moduledoc or
  in its example's doc. An undocumented divergence is a defect.

## Testing

- Prefer `Phoenix.LiveViewTest` `element/2`, `has_element?/2`, and form helpers
  over raw HTML assertions.
- Test behaviour and outcomes. A test that asserts a class name is testing the
  design system, not the page.
- Component tests render the page component directly with sample data; they do
  not need a LiveView.
- Scope the suite to the app:

  ```bash
  cd apps/favn_view && mix test
  ```

  Use the umbrella suite only when the change crosses app boundaries.
- The design system and browser inspection are not a substitute for tests.
  Important flows get a LiveView test.

## Boundaries

`favn_view` is a UI boundary. It calls backend behaviour only through the public
`FavnOrchestrator` facade, and never storage, scheduler, runner, repo, compiler,
adapter, or plugin internals. Runtime inspection tooling does not relax this.

Mutating events authorise on the server. Hiding or disabling a control is
presentation only.

## Adding a new screen

1. Check `/design-system` for an existing page or section that already does this.
2. Add the route in `FavnView.Router` and the destination in
   `FavnView.Components.Navigation` in the same change.
3. Write the page component: shell, four states, sections. Compose elements;
   add a new element only if the design system genuinely lacks one, and then
   document it and give it an example.
4. Add page examples for all four states in `Examples.Pages`.
5. Write the LiveView: load, handle, assign, render the one page component.
6. Add LiveView tests for the flows that matter.
7. Verify at 390, 768, and 1440 wide, in both themes.
