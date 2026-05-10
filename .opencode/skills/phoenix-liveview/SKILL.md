---
name: phoenix-liveview
description: Use when working in apps/favn_view with LiveViews, HEEx, function components, LiveComponents, layouts, LiveView tests, PhoenixStorybook, Tidewave-assisted UI work, or small colocated hooks/JS.
---

# Phoenix LiveView Skill

Use this skill for work in `apps/favn_view`: LiveViews, HEEx templates,
function components, LiveComponents, layouts, LiveView tests,
PhoenixStorybook, Tidewave-assisted UI work, and small colocated hooks or
JavaScript when necessary.

## Core Rules

- Always use the `tidewave_view` MCP when the `apps/favn_view` runtime is running and the task involves LiveView/UI routes, rendered behavior, logs, source lookup, runtime inspection, or Phoenix/LiveView docs.
- If `tidewave_view` is unavailable because the runtime is not running, say so explicitly and continue with static inspection only when that is sufficient.
- `favn_view` is a thin UI/API boundary.
- LiveViews may call backend behavior only through the public orchestrator facade.
- Do not call storage, scheduler, runner, persistence, repos, compiler internals, or plugin internals from `favn_view`.
- Avoid product UI feature work unless the user explicitly requested it.
- Prefer LiveView-native behavior over custom JavaScript.
- Prefer small function components over large templates.
- Everything reusable, page-shaped, or visually meaningful should be a component with an explicit Storybook story.
- LiveViews should render only one top-level page component after preparing assigns.
- Reusable components must declare explicit `attr` and `slot` contracts.
- Mutating LiveView events must authorize server-side; hiding buttons is not authorization.
- Use stable DOM ids and `data-testid` selectors for tests and agent navigation.
- Add or update PhoenixStorybook stories for reusable UI components.
- Build responsive designs by default and verify at multiple viewport sizes before finishing UI work.
- Use Tidewave only in dev, and do not use runtime inspection to bypass Favn app boundaries.

## Components-First LiveView Approach

Use a components-first architecture in `apps/favn_view`.

A LiveView should be thin. It should:

- load data
- handle params/events
- call public orchestrator-facing APIs/functions
- prepare simple view-model assigns
- render one top-level page component

A LiveView should not contain page markup directly. It should render exactly one
top-level page component unless there is a narrow framework reason not to.

Preferred pattern:

```elixir
def render(assigns) do
  ~H"""
  <.asset_detail_page
    asset={@asset}
    runs={@runs}
    selected_window={@selected_window}
  />
  """
end
```

The page component then composes smaller components:

```text
LiveView
  -> Page component
    -> Feature components
      -> UI primitives
```

Example:

```text
AssetDetailLive
  -> asset_detail_page
    -> asset_header
    -> run_timeline
    -> dependency_panel
    -> runs_table
    -> empty_state / error_panel / loading_panel
```

## Storybook Rule

Every reusable component, page component, and small item component should have a
Phoenix Storybook story.

Use Storybook as the explicit UI contract for agents and humans:

- component API
- attrs
- slots
- normal state
- loading state
- empty state
- error state
- long text / overflow state
- realistic Favn examples

If a component is introduced without a story, treat that as incomplete work.

Agents should check Storybook before creating new UI. Reuse existing components
first.

When creating or changing a reusable/page component, update the matching
Storybook story in the same change.

## Boundary Rule

Storybook stories must use local sample data or public view-model shaped data.

Do not call orchestrator internals, storage adapters, Ecto queries, runner
internals, or compiler internals from Storybook stories.

`favn_view` remains the UI/API boundary only.

## Tidewave + Storybook Workflow

Phoenix Storybook is not an MCP server.

Use:

- `tidewave_view` for runtime inspection, logs, source lookup, and LiveView debugging
- `/storybook` for visual component review and component API discovery

## Browser/UI Verification With Playwright MCP

Use `playwright` MCP when a task requires interacting with the rendered UI.

Use it for:

- opening `apps/favn_view` in a browser
- navigating LiveView pages
- checking `/storybook`
- clicking buttons and links
- filling forms
- validating loading, empty, error, and success states
- checking responsive/narrow layouts
- taking screenshots for visual verification

For responsive UI work, verify at least:

- mobile viewport around `390x844`
- desktop viewport around `1440x1000`
- any intermediate/tablet viewport when layout changes at that breakpoint

Check that primary navigation, mode controls, theme controls, focus states, and the main content remain reachable and usable at each verified viewport.

Use `tidewave_view` for Phoenix/LiveView runtime inspection:

- logs
- source lookup
- assigns/runtime state
- framework docs
- debugging LiveView behavior

Use `/storybook` as the visual component contract.

Recommended workflow:

1. Check the existing Storybook story first.
2. Use Playwright MCP to inspect and interact with the rendered component/page.
3. Use Tidewave MCP to trace problems back to Phoenix/LiveView source, logs, assigns, and runtime behavior.
4. Fix reusable UI in the component/page component and update the Storybook story.
5. Only then wire or adjust the LiveView.

Do not use Playwright MCP as a replacement for real tests. For important flows,
add Phoenix/LiveView tests, and only introduce a Playwright E2E suite if the
project explicitly decides to support browser-level regression tests.

## Playwright MCP Setup

Playwright MCP is used by OpenCode agents for rendered browser interaction.

Requirements:

- Node.js 18+
- A working browser environment

The MCP itself is launched through OpenCode using:

```bash
npx -y @playwright/mcp@latest
```

No project-local Playwright dependency is required for MCP-only usage. If browser
launch fails, install Playwright browsers/system dependencies locally:

```bash
npx playwright install --with-deps chromium
```

On WSL/Linux, this may be required before Playwright MCP can open a browser.

To use Playwright MCP with OpenCode:

1. Start the relevant Phoenix app.
2. Start OpenCode from the repo root.
3. Ask the agent to use `playwright` to inspect the rendered UI.

Example:

```text
Use playwright to open http://127.0.0.1:4173/storybook and inspect the asset card component story. Use tidewave_view if you need runtime/source/log context.
```

## Docs

Use these docs for details when implementing:

- Phoenix Storybook: https://hexdocs.pm/phoenix_storybook/
- Phoenix Storybook stories: https://hexdocs.pm/phoenix_storybook/PhoenixStorybook.Story.html
- Phoenix Storybook component stories: https://hexdocs.pm/phoenix_storybook/components.html
- Phoenix Storybook variations: https://hexdocs.pm/phoenix_storybook/PhoenixStorybook.Stories.Variation.html
- Phoenix function components, attrs, and slots: https://hexdocs.pm/phoenix_live_view/Phoenix.Component.html

## Testing

- Prefer `Phoenix.LiveViewTest` selectors such as `element/2`, `has_element?/2`, and forms over raw HTML assertions.
- Test behavior and outcomes, not implementation details.
- Keep placeholder stories/tooling stories separate from product UI work.

## Favn LiveView UI Philosophy

When working on `favn_view`, first read:

- https://daisyui.com/llms.txt

Use that as the source of truth for DaisyUI syntax, component classes, theme setup, and current best practices. Do not guess DaisyUI APIs from memory.

### Design direction

Favn should feel like a calm operator HUD, not a crowded admin dashboard.

The visual language is:

- Apple Liquid Glass-inspired
- dark-first, but with a supported light theme
- minimal, spacious, and atmospheric
- floating panels, soft borders, subtle glow
- icon-first navigation
- progressive disclosure instead of visible clutter

Default screens should show only the essential thing the operator needs.

For asset detail, the default view is:

- asset title
- health/status badge
- window timeline

Everything else should be secondary and reachable through icons, hover/focus previews, pinned modes, or drill-downs.

### Interaction philosophy

Prefer:

- one primary visual per screen
- one primary action per state
- icon rails instead of dense tab bars
- hover/focus preview plus click-to-pin
- keyboard-accessible controls
- clear empty/error/loading states

Avoid:

- dashboards full of cards
- large tables as the first view
- visible debug/raw JSON by default
- too many filters, buttons, and knobs
- backend DTO shapes leaking into the UI

### DaisyUI usage

Read these DaisyUI docs before changing Favn UI foundations or shared components:

- https://daisyui.com/llms.txt
- https://daisyui.com/docs/themes/
- https://daisyui.com/docs/layout-and-typography/
- https://daisyui.com/docs/utilities/
- https://daisyui.com/docs/base/

Use DaisyUI primitives first.

Use Tailwind only for layout, spacing, positioning, and light custom polish.

Prefer semantic DaisyUI/theme tokens over hardcoded colors.

Create or use Favn-specific light and dark themes, but keep custom CSS small. The goal is not to build a new component library; it is to make DaisyUI feel like Favn.

Preserve Favn boundaries: UI components and stories may use local sample data or public view models, but must not call orchestrator, runner, storage, compiler, or plugin internals directly.

### Favn boundaries

`favn_view` owns the browser/operator experience only.

It must not reach into orchestrator, runner, storage, or internal runtime apps directly. UI data should come through explicit view/API boundaries and stable view models.
