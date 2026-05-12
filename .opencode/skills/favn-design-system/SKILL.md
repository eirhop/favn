---
name: favn-design-system
description: Use when creating or changing reusable low-level UI components, design primitives, visual surface classes, core component APIs, Favn theme tokens, glass/HUD styling, responsive density, or Storybook visual contracts in apps/favn_view.
---

# Favn Design System Skill

Use this skill before designing or changing reusable low-level UI components in
`apps/favn_view`.

## Core Rules

- Load `phoenix-liveview` as well for LiveView, HEEx, components, and Storybook work.
- Read `https://daisyui.com/llms.txt` before changing DaisyUI-backed primitives or theme styling.
- DaisyUI primitives come first, but Favn-specific polish belongs in named design-system classes, not one-off component utility piles.
- Keep the visual system dark-first and light-mode compatible through DaisyUI theme tokens.
- Do not hardcode Tailwind palette colors for product UI surfaces. Use `base-*`, `primary`, `info`, `success`, `warning`, `error`, and token mixes.
- Every reusable visual primitive needs a Storybook story or an existing Storybook story that exercises the changed state.

## Visual Language

Favn should feel like a calm operator HUD:

- dark atmospheric shell
- blue-tinted glass surfaces
- sparse, high-signal content
- icon-first controls
- soft borders and glows
- progressive disclosure through rails/docks
- compact lists, not crowded dashboards

Avoid:

- generic admin cards
- opaque gray glass blocks
- inconsistent border radii between similar surfaces
- local component classes that override the design system without a reason
- oversized list rows that make operational scanning slow

## Surface Levels

Use the shared surface classes from `apps/favn_view/assets/css/app.css`:

- `favn-surface-panel`: large panels and page sections.
- `favn-surface-list`: repeated list cards/rows where scan density matters.
- `favn-surface-control`: search fields, selects, dropdown triggers, and compact controls.
- `favn-surface-rail`: global nav rails, page mode rails, and mobile docks.

Page-local mode rails and mobile mode docks should read as one grouped Favn card
surface with fully rounded corners. Do not style rail items as disconnected
buttons or sharp segmented tabs. The active mode may be lit inside the group,
but the group itself should remain a continuous rounded glass card.

Compatibility aliases may exist, such as `favn-glass-panel`, `favn-control-glass`,
and `favn-icon-rail`, but new component work should prefer the semantic surface
classes above.

## Density Rules

- Mobile catalogue/list rows should show multiple items per viewport.
- Keep repeated list cards compact: small padding, tight metadata, and one clear action affordance.
- Controls should be visually related to cards, but slightly less dominant than the list itself.
- Rails/docks may be more chrome-like, but should not compete with primary content.
- Use one spacing rhythm per screen: control gap, list gap, and dock spacing should feel intentional.

## Component Rules

- Reusable Favn-specific components, page components, and customized primitives need Storybook coverage. Plain DaisyUI primitives do not need wrappers or stories unless they are reused as a Favn-specific pattern or customized beyond local composition.
- Editing a reusable Favn-specific component is incomplete until its Storybook story has been opened with Playwright and visually inspected.
- Always evaluate pixel-perfectness against the approved design reference or current Storybook contract. If spacing, surface strength, radius, typography, icon weight, focus state, or density is visibly off, improve it before finishing.
- Low-level components should expose variants before callers hand-roll surface utilities.
- Prefer classes like `favn-surface-control` or `favn-surface-list` over copying border/background/shadow utility stacks.
- Component-specific layout belongs in the component. Cross-component color, glass, border, glow, and interaction styling belongs in CSS design-system classes.
- If a component must diverge from the surface system, document why in the component or Storybook story.

## Storybook Review

For reusable visual work, inspect Storybook with Playwright at minimum:

- mobile viewport around `390x844`
- desktop viewport around `1440x1000`
- dark theme first
- light theme when the changed primitive is theme-sensitive

Look for:

- consistent surface strength across panels, controls, list cards, and rails
- consistent radius family
- readable text contrast
- usable focus states
- no DaisyUI default styling leaking through unintentionally

Do not rely on automated tests alone for UI component changes. Storybook plus
Playwright visual inspection is required for reusable component edits.

