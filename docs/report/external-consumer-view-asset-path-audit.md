# External-consumer View asset path audit

Date: 2026-07-27

This point-in-time audit checked View asset paths used by the Favn umbrella,
Git `subdir:` consumers, and `FAVN_CHECKOUT` consumers.

## Fixed findings

- `apps/favn_view/assets/vendor/heroicons.js` traversed from `__dirname` to the
  umbrella `deps/` directory. Tailwind now receives the active Mix dependency
  directory through `FAVN_HEROICONS_PATH`.
- The umbrella Tailwind configuration and `FavnLocal.Assets` now provide the
  same explicit Heroicons and `NODE_PATH` contract.
- `mix favn.dev` and the `favn_view` `assets.build` alias now generate CSS and
  JavaScript before the hash-bearing Storybook compilation.
- Storybook tracks the generated stylesheet as an external resource but only
  configures it when the file exists. A dependency compile before asset
  generation is therefore quiet, and the post-build compile installs the valid
  stylesheet path and hash.

## Safe umbrella-only or Mix-owned paths

- `config/config.exs` is the umbrella asset entry point. External consumers do
  not load it; `FavnLocal.Assets` replaces its Tailwind and esbuild profiles
  with paths from the active consumer Mix project.
- The `../../deps`, `../../_build`, and `../../mix.lock` values in umbrella app
  `mix.exs` files are Mix project metadata, not JavaScript dependency
  resolution. External asset builds use the active consumer paths supplied by
  `FavnLocal.Assets`.
- `apps/favn_view/asset_installer/mix.exs` is an isolated umbrella build helper,
  not a runtime or external-consumer asset entry point.
- `rel/control_plane/Dockerfile` builds from the repository root and invokes
  the umbrella-owned `favn_view` asset tasks.

No other View JavaScript plugin resolved dependencies by traversing from
`__dirname`, and no runtime asset code contained a matching `_build` traversal.
