# Advanced Manifest Notes

Most users do not need to call manifest functions directly.

Use `mix favn.dev`, `mix favn.run`, and the UI for normal local development. Use
the functions in this guide when building tools, debugging discovery, comparing
versions, or preparing deployment artifacts.

Favn still uses manifests internally. A manifest is the stable description Favn
builds from your DSL modules: assets, pipelines, schedules, dependencies, and the
runtime metadata needed to run work.

Runtime systems then use a pinned manifest version instead of rediscovering your
modules while runs are in progress.

## When To Use Manifest Functions

Use manifest functions when you need to:

- inspect what Favn discovered from your modules
- debug why an asset or pipeline is missing
- compare two versions of authored work
- serialize a manifest for deployment tooling
- validate compatibility before sending a manifest to a runtime

Do not start with these functions in the basic tutorial. They are advanced
tooling APIs.

## Why Manifests Exist

If a runtime reads authoring modules directly, a run can change because code was
reloaded, a release was deployed, or module discovery found something different.

That makes runs harder to explain and repeat.

## Flow

Favn uses this flow:

```text
authoring modules
  -> Favn DSL declarations
  -> Favn.generate_manifest/1
  -> manifest
  -> Favn.pin_manifest_version/2
  -> versioned manifest
  -> local/runtime registration
  -> runner-executed work
```

The manifest is the handoff point. It says what assets exist, how they depend on
each other, what pipelines select, their non-secret static settings, and what
runtime config is required.

## Common Manifest Calls

Generate from configured discovery:

```elixir
{:ok, manifest} = Favn.generate_manifest()
```

Generate from explicit modules:

```elixir
{:ok, manifest} =
  Favn.generate_manifest(
    asset_modules: [MyApp.Lakehouse.Raw.Sales.Orders],
    pipeline_modules: [MyApp.Pipelines.DailySales]
  )
```

Pin, serialize, hash, and validate:

```elixir
{:ok, version} = Favn.pin_manifest_version(manifest)
{:ok, json} = Favn.serialize_manifest(manifest)
{:ok, hash} = Favn.hash_manifest(manifest)
:ok = Favn.validate_manifest_compatibility(manifest)
```

Inspect authored work:

```elixir
{:ok, assets} = Favn.list_assets()
{:ok, asset} = Favn.get_asset(MyApp.Lakehouse.Raw.Sales.Orders)
{:ok, pipeline} = Favn.get_pipeline(MyApp.Pipelines.DailySales)
{:ok, resolution} = Favn.resolve_pipeline(MyApp.Pipelines.DailySales)
{:ok, plan} = Favn.plan_asset_run([{MyApp.Lakehouse.Raw.Sales.Orders, :asset}])
```

## What Goes In A Manifest

A manifest can include:

- assets and their stable refs
- dependencies between assets
- pipelines
- schedules
- relation metadata for SQL-backed assets
- typed SQL output contracts, generated/custom check definitions, and explicit
  column lineage
- freshness and window metadata
- JSON-safe asset and pipeline settings
- runtime config requirements
- schema and runner contract version 7 data used by the runtime

The exact struct is managed by Favn. Application code should normally build
manifests with `Favn.generate_manifest/1`, not by constructing manifest structs
by hand.

## Pinning

`Favn.pin_manifest_version/2` wraps a manifest with stable identity:

```elixir
{:ok, manifest} = Favn.generate_manifest(asset_modules: [MyApp.Assets.Orders])
{:ok, version} = Favn.pin_manifest_version(manifest)
```

The pinned version can be compared, transferred, registered, and selected by
runtime tooling.

## What A Manifest Does Not Do

A manifest does not execute assets.

It also does not store secret values. If an asset declares that it needs runtime
configuration, the manifest records the requirement. The value must still be
provided by the runtime environment.

## Failure Modes

| Step | What can fail |
| --- | --- |
| Generate | A module cannot load, an asset is invalid, dependencies are invalid, or the graph has a cycle. |
| Validate | The manifest is missing required data or uses an unsupported version. |
| Serialize or hash | The manifest cannot be encoded into the canonical payload. |
| Pin | Version metadata is invalid or the manifest cannot be validated. |
| Runtime registration | The runtime cannot accept or store the pinned version. |

## Why This Helps

- Runs are tied to a specific manifest version.
- Operators can see which version a run used.
- Deploying new code does not silently change already accepted work.
- Planning can be deterministic because the graph is explicit.

## Related Docs

- [Getting Started](getting-started.md)
- [Runtime Model](runtime-model.md)
- `docs/architecture/manifest-first.md` in the Favn monorepo for contributor-facing architecture notes
