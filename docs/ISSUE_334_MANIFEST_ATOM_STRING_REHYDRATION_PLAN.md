# Issue 334 Manifest Atom/String Rehydration Semantics Plan

## Reflection

This issue is not just a serializer bug. It is a manifest contract problem at the
boundary between authoring-time Elixir values, JSON persistence, and runtime
selection behavior.

Atoms and strings can both be valid authored values in selector-facing metadata,
but JSON encodes both as strings today. Rehydrating every atom-shaped JSON string
back into an atom makes roundtrips deterministic only by changing some valid
string values into atoms. It also creates atoms from persisted data, which is a
production safety concern because atoms are not garbage collected by the BEAM.

The production-ready design needs to separate two classes of values:

- manifest contract identifiers that are intentionally atoms, modules, refs, or
  bounded enums and may be recreated under explicit validation
- user-facing metadata values such as tags and categories, where matching
  behavior should not depend on JSON losing the original Elixir type

## Goal

Define and implement deterministic manifest atom/string semantics so persisted
manifests roundtrip safely, selector resolution is stable before and after JSON
persistence, and atom creation from manifest data is bounded and intentional.

## Current State

- `Favn.Manifest.Serializer` normalizes all atoms with `Atom.to_string/1`, so
  `:daily` and `"daily"` both persist as `"daily"`.
- `Favn.Manifest.Rehydrate` recreates known contract atoms with
  `decode_known_atom/2` and local names with `decode_atom_optional/1`.
- `decode_atom_or_binary/1` currently converts atom-shaped strings into new
  atoms. This is used for asset/pipeline metadata `:category`, metadata `:tags`,
  and pipeline `{:tag, value}` / `{:category, value}` selectors.
- `validate_manifest_atom_budget/1` counts atom-shaped strings globally before
  rehydration. That limits damage, but it still treats free-form selector
  metadata as atom candidates.
- `Favn.Manifest.PipelineResolver` compares tag/category selectors with strict
  equality, so `:sales` and `"sales"` do not match unless rehydration happens to
  coerce both sides the same way.
- `Favn.Asset.normalize_meta!/1` currently accepts atom categories only, while
  pipeline selector docs allow `category atom_or_string` and tag values can be
  atoms or strings.
- `apps/favn_core/test/manifest/version_test.exs` has a regression test that
  currently expects atom-like string metadata and selectors to be atomized. That
  test encodes the behavior this issue should replace.

## Decision

Prefer string-normalized selector-facing metadata for issue 334.

Tags and categories are user-facing classification labels, not runtime identity
atoms. Normalize asset metadata `category`, asset metadata `tags`, pipeline
metadata `category`, pipeline metadata `tags`, and tag/category selector values to
strings at the manifest boundary. Selector resolution should compare these
normalized strings.

Keep atom recreation only for manifest-owned contract fields where atoms are the
actual runtime representation: modules, refs, function names, known enums,
execution pools, outputs, runtime config scopes/fields, schedule kinds, window
fields, SQL definition names, and similar bounded or manifest-owned identifiers.

Do not introduce broad typed JSON encoding for all atoms in this issue. It is the
right long-term tool if future manifest fields need to preserve atom-vs-string
identity, but applying it globally now would make free-form metadata more complex
and would still require an explicit atom policy for persisted data.

## Design

- Add a small manifest-owned normalization seam in `favn_core`, for example
  `Favn.Manifest.Labels` or `Favn.Manifest.SelectorValue`, rather than keeping
  metadata coercion as private ad hoc helpers in `Rehydrate` and
  `PipelineResolver`.
- The seam should expose boring pure functions such as `normalize_label/1`,
  `normalize_labels/1`, and `match_label?/2`, with docs that state tags and
  categories are persisted and matched as strings.
- Normalize atom and string labels with `to_string/1`; reject or drop non-atom,
  non-string values consistently at the existing validation boundary.
- Change `Favn.Manifest.Rehydrate.build_metadata/1` so `:category` and `:tags`
  use string normalization instead of `decode_atom_or_binary/1`.
- Change `Favn.Manifest.Rehydrate.decode_selector/1` so tag/category selectors
  use string normalization instead of `decode_atom_or_binary/1`.
- Keep selector tuple kinds as known atoms (`:tag`, `:category`, `:asset`,
  `:module`) because those are manifest enum fields, not user labels.
- Change `Favn.Manifest.PipelineResolver` to resolve tag/category selectors
  through the same label normalization seam. Resolver matching should be stable
  even if it receives already-built structs from authoring code rather than a
  JSON-rehydrated manifest.
- Change authoring metadata validation to align with the contract. Either allow
  string categories explicitly or normalize atom categories to strings during
  manifest generation. Prefer allowing string categories because pipeline docs
  already advertise `category atom_or_string`.
- Keep strict atom rehydration helpers for contract identifiers, but narrow atom
  budget collection so free-form label strings are not counted as atom refs just
  because they look like atoms.
- Update module docs and feature docs to say manifest labels are JSON strings and
  atom/string labels match the same after persistence.
- Avoid a manifest schema version bump unless persisted data incompatibility
  requires one. This is pre-v1 and existing persisted local manifests can be
  regenerated; if production persistence compatibility becomes required later,
  introduce a `json-v2` typed encoding slice separately.

## Landing Slices

1. Add focused failing tests for the desired behavior in `favn_core`: atom and
   string tags, atom and string categories, JSON roundtrip, and selector
   resolution after rehydration.
2. Introduce the manifest label normalization seam with unit tests for atoms,
   strings, duplicate labels, invalid values, and comparison behavior.
3. Replace `decode_atom_or_binary/1` usage for metadata labels and tag/category
   selectors in `Favn.Manifest.Rehydrate`.
4. Update `Favn.Manifest.PipelineResolver` to normalize both selector values and
   asset metadata labels before matching.
5. Align authoring validation/docs so asset categories and pipeline tag/category
   selectors accept the same label contract.
6. Narrow atom-budget collection or atom-ref traversal so label fields are not
   treated as future atoms.
7. Update `README.md`, `docs/FEATURES.md`, and relevant structure docs with the
   final atom/string manifest semantics.
8. Remove or rewrite the current test that expects atom-like strings to become
   atoms.

## Tests

- Manifest JSON roundtrip preserves deterministic content hashes when metadata
  labels are authored as atoms.
- Manifest JSON roundtrip preserves user-authored string labels as strings.
- Atom tag metadata and atom tag selectors resolve to the same assets before and
  after JSON persistence.
- String tag metadata and string tag selectors resolve to the same assets before
  and after JSON persistence.
- Atom category metadata and atom category selectors resolve to the same assets
  before and after JSON persistence.
- String category metadata and string category selectors resolve to the same
  assets before and after JSON persistence.
- Mixed atom/string authored labels normalize to one deterministic string value
  for selector resolution.
- Persisted atom-shaped strings such as `"sales"` do not create new atoms during
  metadata/selector rehydration.
- Known manifest enum/module/ref fields still rehydrate to atoms and modules and
  still reject invalid shapes.
- Atom budget tests cover contract atom fields without counting free-form label
  values as atom refs.

## Refactoring Wins

- Remove `decode_atom_or_binary/1` from selector-facing metadata paths; its name
  hides an important atom-creation policy decision.
- Centralize tag/category normalization so `Rehydrate`, `PipelineResolver`, and
  authoring validation do not drift.
- Make selector resolution easier to read by replacing strict ad hoc equality
  with one manifest-owned label comparison contract.
- Clarify `Favn.Manifest.Asset.metadata` and `Favn.Manifest.Pipeline.selectors`
  typespecs so label values are `String.t()` at runtime after manifest
  normalization.
- Keep typed atom recreation helpers scoped to contract identifiers and enums,
  making future security reviews easier.

## Performance And Stability Gains

- Avoid creating BEAM atoms for user-facing labels loaded from persisted
  manifests.
- Reduce atom-budget traversal pressure by ignoring label fields that will never
  be atomized.
- Normalize labels once at manifest/version boundaries, then compare plain
  strings during selector resolution.
- Future optimization can add `tags_by_label` and `category_by_label` maps to
  `Favn.Manifest.Index` if selector resolution becomes hot for large manifests;
  do not add that index until tests or profiling show resolver scans are a real
  bottleneck.
- Stable string labels make API/UI payloads more predictable because JSON clients
  never see atom-vs-string ambiguity.

## Risks And Tradeoffs

- This is a breaking pre-v1 semantic change for code that expected rehydrated
  labels to be atoms. The safer production contract is worth the break.
- Existing in-memory authoring structs may still contain atom labels until they
  pass through manifest normalization. Resolver normalization must handle both to
  keep pre-persistence and post-persistence behavior aligned.
- String normalization means `:sales` and `"sales"` intentionally become the same
  label. If Favn later needs atom/string identity for non-label manifest fields,
  add typed encoding only for those fields.
- Content hashes for manifests with atom label metadata may change. That is
  acceptable before v1, but should be documented as a manifest contract change.

## Non-Goals

- Do not globally typed-encode every atom in JSON.
- Do not preserve atom-vs-string identity for tags and categories.
- Do not create atoms from persisted free-form metadata.
- Do not move manifest metadata semantics into `favn_orchestrator` or
  `favn_view`; this belongs in `favn_core`.
- Do not add manifest label indexes until selector resolution performance needs
  them.
