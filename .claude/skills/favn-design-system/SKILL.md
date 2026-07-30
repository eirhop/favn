---
name: favn-design-system
description: Use when creating or changing UI elements, design tokens, tones, the type or spacing scale, surface classes, theme tokens, glass/HUD styling, responsive density, or the /design-system visual contract in apps/favn_view.
---

# Favn Design System Skill

The design system is documented, not described here. Read the canonical
documents first and follow them:

- [`docs/design/style-guide.md`](../../../docs/design/style-guide.md) — the
  visual contract: tones, type, surfaces, buttons, badges, the four states,
  spacing, radius, elevation, motion, layering, words, and the definition of
  done.
- [`docs/design/component-patterns.md`](../../../docs/design/component-patterns.md)
  — the code contract: the four layers, component APIs, naming, the design-system
  browser, variant-or-new-component, and deprecation.

Do not restate those rules in a plan or a summary. Apply them.

## Before you change anything

- Load `phoenix-liveview` as well. The two skills are used together.
- Read `FavnView.UI` for the module map, then read the element module you are
  about to touch. Its moduledoc is the contract.
- Check `/design-system` for an element that already does the job. Reuse before
  adding; add a variant before adding a component.
- Read `https://daisyui.com/llms.txt` before changing a DaisyUI-backed primitive
  or theme styling.

## Non-negotiables

- Elements own styling. Sections and pages compose them and add no border,
  background, shadow, radius, or spacing utility an element already owns.
- Tones and spacing come from `FavnView.UI.Tokens` and `FavnView.UI.Layout`.
  Hardcoded palette colours and off-scale gaps are defects.
- A class name built at runtime is invisible to the Tailwind scanner. Adding one
  means adding it to the `@source inline(...)` list in `assets/css/app.css` in the
  same change, or it will be missing from the stylesheet.
- Changing an element means updating its design-system example in the same change
  when the change is one the component's own defaults cannot show.
- A control that does nothing does not ship. No `href="#"`, no permanently
  disabled mode, no "coming soon".

## Overriding DaisyUI: assume your rule did nothing

A CSS rule that looks correct and has no effect has cost this project several
rounds. `@layer` ordering beats specificity outright, and DaisyUI emits from
layers that outrank ours, so three failures repeat:

- **A layered override is discarded.** DaisyUI emits `color` and `border-color`
  from `@layer utilities`, which outranks `@layer components` no matter how
  specific the selector. This is why `.favn-text-muted` and `.favn-text-subtle`
  are unlayered: inside `@layer components` they lost to `.label` and painted
  nothing.
- **Setting DaisyUI's own custom property is not a way in.** `.btn` declares
  `--btn-border`, `--btn-fg`, and friends itself, and its declaration wins — so
  every `--btn-border` the Favn button variants set was ignored for months, and
  the edge that appeared on screen was DaisyUI's depth shadow tinted by the fill.
  Set the real property (`border-color`), not the variable DaisyUI reads.
- **Unlayered is not the strongest thing.** An unlayered rule outranks every
  layer but still loses to a layered `!important`. `.btn.favn-control-boundary`
  therefore lives *inside* `@layer components`, after `.favn-surface-control`,
  with `!important` — because what it overrides is a layered `!important`.

So: never conclude from the source that a rule applied. Measure the computed
value in the browser and compare it to the value you wrote.

```javascript
getComputedStyle(document.querySelector(".favn-btn-action")).borderTopColor
```

Two traps in the measurement itself. A property listed in `transition` returns
its *interpolated* value, so a read taken immediately after load or a class
change is mid-animation — wait, or read a settled element. And `getComputedStyle`
returns `oklab(...)` or `oklch(...)`, which regex-extracting three numbers turns
into nonsense; to get sRGB, paint the value on a canvas and read the pixel.

## Finishing

Measure, then look. On a `/design-system/render` page, `window.favn.audit()`
returns contrast, target size, accessible names, and clipping as pass, fail, or
skipped, plus the bounding box of every example — one call, so a cropped
screenshot afterwards needs no second lookup. A `skipped` check means the browser
could not measure it; it is not a pass.

An element change is not done until it has been rendered and inspected at
`390x844` and `1440x1000`, dark first, and light too when the change touches a
surface, border, or tone. Automated tests do not replace this.

Then run, from the umbrella root:

```bash
mix do --app favn_view cmd mix compile --warnings-as-errors
MIX_ENV=test mix do --app favn_view cmd mix test --no-compile
```
