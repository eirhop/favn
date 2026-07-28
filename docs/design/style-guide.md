# Favn UI Style Guide

This is the canonical visual contract for `apps/favn_view`. It defines what
things look like and when to use which variant. The code that encodes these
rules lives in `FavnView.UI`; the composition rules live in
[`component-patterns.md`](component-patterns.md).

If a rule here and the code disagree, the code is authoritative and this document
must be corrected in the same change.

## Design intent

Favn is a calm operator HUD, not an admin dashboard. An operator opens it to
answer one question — *is this healthy, and if not, why* — so every screen shows
the fewest things that answer that question.

| Do | Don't |
| --- | --- |
| One primary visual per screen | A grid of cards competing for attention |
| One primary action per state | Rows of equally-weighted buttons |
| Icon rails and progressive disclosure | Dense tab bars and permanent filter panels |
| Compact rows that scan quickly | Tall rows that show three items per viewport |
| Soft borders, blue-tinted glass, subtle glow | Opaque grey panels, hard shadows |

Dark is the default theme. Light is fully supported and must be checked whenever
a surface, border, or tone changes.

## Tone

Colour carries exactly one meaning in Favn, and that meaning is a **tone**.
`FavnView.UI.Tokens` owns the vocabulary. Components accept a tone; they never
accept a palette class.

| Tone | Means | Typical use |
| --- | --- | --- |
| `:neutral` | no judgement | counts, ids, inert metadata, unknown status |
| `:primary` | the operator's current focus | selection, active mode, the one primary action |
| `:info` | in progress or advisory | running, queued, fresh |
| `:success` | healthy, complete, satisfied | healthy, succeeded, compatible |
| `:warning` | degraded, needs attention, not broken | stale, missed, incomplete coverage |
| `:error` | failed, blocked, unsafe | failed, blocked, target drift |

Rules:

- Never hardcode a Tailwind palette colour for product UI. `base-*`, `primary`,
  `info`, `success`, `warning`, and `error` are the whole palette.
- Never encode meaning in colour alone. Every tone is paired with a label, an
  icon, or adjacent text so it survives colour-blindness and greyscale.
- `Tokens.tone/1` normalises common domain atoms (`:healthy`, `:failed`,
  `:queued`, …). Map your own statuses explicitly when the name does not make
  the meaning obvious — for example runs treat `:cancelled` as neutral, not as a
  warning.
- Escalation order is neutral → info → success → warning → error. When a row has
  several statuses, the most severe one decides what the operator sees first.

## Type

One scale, defined in `FavnView.UI.Typography`. Headings are light and
low-contrast on purpose: emphasis comes from tone and surface, not weight.

| Step | Component | Use |
| --- | --- | --- |
| `:page_title` | `page_title/1` | the one heading naming the screen |
| `:compact_title` | `page_title/1` with `compact` | the shell's dense header |
| `:section_title` | `section_title/1` | a panel or group heading |
| `:eyebrow` | `eyebrow/1` | small uppercase label above a value or group |
| `:body` | — | prose inside panels |
| `:meta` | `meta/1` | timestamps, counts, and ids on a row's second line |

Rules:

- Exactly one `page_title` per screen, and the shell renders it. A page
  component must not add a second one.
- Long values truncate; they never wrap a row. Anything truncated must expose
  the full value through `title`.
- Ids, fingerprints, and hashes use `FavnView.UI.Data.mono/1`, which breaks on
  any character rather than overflowing.

## Surfaces

Favn has exactly four glass levels, defined in `assets/css/app.css` and exposed
as components by `FavnView.UI.Surface`. Choosing a surface is choosing what the
thing *is*, not how strong it should look.

| Component | Class | Use |
| --- | --- | --- |
| `panel/1` | `favn-surface-panel` | page sections, large content containers |
| `list_card/1` | `favn-surface-list` | repeated rows where scan density matters |
| `control_surface/1` | `favn-surface-control` | search fields, selects, compact controls |
| `rail/1` | `favn-surface-rail` | nav rails, mode rails, mobile docks |

Rules:

- Never hand-roll a `border` + `background` + `shadow` utility stack for a
  container. If a surface needs a new variant, add the variant to the component.
- The radius family is `--radius-box` for containers and `--radius-field` for
  controls and icon tiles. Similar surfaces must share a radius.
- A rail is one continuous rounded card. Its items may light up inside the
  group, but they are never styled as separate buttons or as sharp segmented
  tabs.
- Controls are visually related to the list they filter, but slightly quieter.
  The list is the content; the controls are not.

## Buttons

`FavnView.UI.Button` owns every button in the product.

| Variant | Use | Rule |
| --- | --- | --- |
| `:primary` | the action that advances the current state | at most one per view state |
| `:secondary` | supporting actions beside a primary one | any number |
| `:ghost` | tertiary actions, toolbar controls, dismissals | any number |
| `:danger` | destructive or irreversible actions | always confirm, always authorise server-side |
| `:link` | inline navigation in prose or a table cell | no surface, no padding |

Rules:

- An action with no label is an `icon_button/1`, and `label` is required: it is
  both the accessible name and the tooltip.
- `loading` keeps the label so the button does not change width mid-submit.
- Disabling a button is presentation. Authorisation happens on the server, every
  time.
- Sizes are `:xs`, `:sm` (default), `:md`, `:lg`. Toolbars and list rows use
  `:sm` or smaller.

## Badges

`FavnView.UI.Badge`.

- `status_badge/1` is the lifecycle state of the thing the row or panel is
  about: a dot plus one word.
- `badge/1` is any other qualifier — kind, mode, compatibility.
- `count_badge/1` is a number attached to a label or a tab.
- Two `status_badge` next to each other is a bug. When a row carries several
  independent states, the lifecycle state is the `status_badge` and the rest are
  `badge/1` with `variant={:outline}`.
- `glow` is reserved for the shell's live status.

## The four states

Every list, panel, and page has four states, and all four are part of the
component contract:

| State | Meaning | Recovery |
| --- | --- | --- |
| content | the normal case | — |
| `loading_state/1` | the read is in flight | none; never offer an action |
| `empty_state/1` | the read succeeded and returned nothing | change the filters, or create the thing |
| `error_state/1` | the read failed | retry, or a link to the failure |

Rules:

- An empty result is not an error. Never render `error_state/1` for zero rows.
- Distinguish *nothing exists yet* from *nothing matches these filters*. They
  have different titles, different icons, and different recoveries.
- `inline_loading/1` is for a region refreshing while its content stays visible.
  Never show it and `loading_state/1` for the same region.
- `notice/1` is an inline condition inside a panel that otherwise rendered.
  Flash is for the outcome of something the operator just did. A page that
  failed to load is an `error_state/1`, not a toast.
- A route that resolves to nothing is a whole page:
  `FavnView.Components.ErrorPage`, which keeps navigation and a way back.

## Spacing

One scale, owned by `FavnView.UI.Layout`. A page that writes `space-y-7` or
`gap-11` has bypassed the scale and will drift out of rhythm with every other
screen.

| Step | Gap | Use |
| --- | --- | --- |
| `:none` | 0 | children that must touch, such as table rows |
| `:xs` | 0.375rem | inside a row: badges, icon and label |
| `:sm` | 0.625rem | between list rows on mobile |
| `:md` | 0.875rem | between page sections on mobile |
| `:lg` | 1.25rem | between page sections on desktop |
| `:xl` | 2rem | between major regions on desktop |

Use the primitives rather than utilities: `stack/1` for vertical rhythm,
`inline/1` for a wrapping row, `columns/1` for equal-weight children. Both
`stack` and `inline` take `{mobile, desktop}` to widen from `lg` up — page
sections are `{:md, :lg}`, list rows are `:sm`.

`columns/1` is only for children of equal weight. Anything with a dominant child
is a flex layout.

## Density

- Mobile list rows must show several items per viewport.
  `favn-density-list-card` and `favn-density-list-card-icon` set the compact
  rhythm; use them rather than ad-hoc padding.
- Panels use the default `:md` padding. `:none` is for panels that own a scroll
  region or wrap a table that sets its own padding.
- One rhythm per screen: control gap, list gap, and dock spacing must look
  deliberate together.

## Radius, elevation, and motion

These come from the DaisyUI theme in `assets/css/app.css`. They are not
per-component decisions.

| Token | Value | Use |
| --- | --- | --- |
| `--radius-box` | 1.5rem | panels, cards, rails, list rows |
| `--radius-field` | 1rem | inputs, selects, icon tiles |
| `--radius-selector` | 999rem | dots, pills, avatars |

Elevation is expressed by the surface classes, not by ad-hoc shadows. There are
three levels: panel (strongest), list and rail (medium), control (weakest). Do
not add a fourth.

Motion is 160ms `ease` for colour, border, shadow, and transform on hover and
focus; 200–300ms for enter and exit transitions. Anything that animates on load
must respect `motion-safe:`. Nothing loops except a loading indicator.

## Layering

Stacking order is fixed so overlays cannot fight:

| Layer | `z-index` | Contains |
| --- | --- | --- |
| content | 10 | the page frame and main content |
| nav rail | 20 | the desktop navigation rail, the desktop mode rail |
| overlays | 30 | mobile nav dropdown, mobile mode dock |
| flash | 50 | toasts |

## Tables versus cards

A table is a desktop affordance. The same rows render as `list_card/1` on
mobile. Do not shrink a table to fit a phone, and do not ship only one of the
two.

`FavnView.UI.Data.data_table/1` renders the desktop table: `:col` slots with
labels, optional `row_navigate` for a trailing chevron, and `:action` slots for
per-row controls.

## Icons

- Heroicons only, via `FavnView.UI.Icon`. Outline is the default; `-solid` and
  `-mini` are available.
- Pass `size` (`:xs`, `:sm`, `:md`, `:lg`), not a `size-*` utility. Passing both
  leaves the winner to stylesheet order.
- Icons are decorative and are hidden from assistive technology. The meaning
  must come from adjacent text or an `aria-label`.

## Responsive breakpoints

Verify every UI change at:

- `390x844` — mobile. Navigation is a dropdown menu; lists are cards; the mode
  dock sits at the bottom.
- `768x1024` — tablet. The nav rail appears at `md`.
- `1440x1000` — desktop. Tables appear at `lg`; the mode rail moves to the right
  edge.

At every size, primary navigation, mode controls, the theme toggle, focus
states, and the main content must stay reachable and usable.

## Accessibility

- Every interactive element has an accessible name. Icon-only controls require
  `label`.
- Focus is always visible: `focus-visible:outline focus-visible:outline-2
  focus-visible:outline-offset-2 focus-visible:outline-primary`. Never remove an
  outline without replacing it.
- Filters carry a screen-reader-only name. A placeholder is not a label.
- Status changes that matter are announced: `role="status"` for loading,
  `role="alert"` for errors.
- Colour is never the only signal.

## Words

The interface is read faster than it is looked at, so wording is part of the
design system.

- **Sentence case everywhere.** "Run pipeline", not "Run Pipeline". Only the
  `eyebrow` step is uppercase, and that is done by CSS.
- **Buttons are verbs**: "Run pipeline", "Plan backfill", "Cancel run". Never
  "OK", "Submit", or "Yes".
- **Statuses are one word**: Healthy, Running, Stale, Failed, Queued.
- **Empty states say what is missing and what to do**: "No assets found" plus
  "Try changing the search or filters." Never just "No data".
- **Errors say what failed and what happens next**, in the operator's terms. Map
  backend atoms to sentences; never render a raw atom or an inspected tuple at an
  operator.
- **Never blame the operator.** "No runs match these filters", not "You entered
  an invalid filter".
- **Do not promise.** No "coming soon", no "not yet implemented" — if it is not
  built, it is not on screen.
- **Use Favn's vocabulary consistently**: asset, pipeline, schedule, run,
  backfill, window, target, generation, manifest. Do not introduce a synonym for
  a concept that already has a name.

## Definition of done

A UI change is finished when all of these are true:

1. It uses elements from `FavnView.UI`; it adds no border, background, shadow, or
   spacing utility that an element already owns.
2. All four states exist for anything that loads data.
3. Tones come from the tone vocabulary, and no tone is the only signal.
4. Every interactive element has an accessible name and a visible focus state.
5. A design-system example exists for anything the component's own defaults do
   not already show, including loading, empty, error, and overflow where they
   apply.
6. `window.favn.audit()` reports no failures for it, and it has been rendered and
   looked at at 390 and 1440 wide, in dark, and in light if a surface, border, or
   tone changed.
7. No control on screen does nothing.
8. `mix compile --warnings-as-errors` and the `favn_view` suite pass.

## Dead controls

A control that does nothing is worse than a missing control: it teaches the
operator that the UI lies.

- A navigation entry must point at a live route. `FavnView.Components.Navigation`
  is the single source of truth, and adding an entry there means adding the route
  in the same change.
- A mode rail with one mode is not a rail. Remove it.
- No `href="#"`, no permanently disabled mode, no "coming soon" panel. Ship the
  feature or ship nothing.
