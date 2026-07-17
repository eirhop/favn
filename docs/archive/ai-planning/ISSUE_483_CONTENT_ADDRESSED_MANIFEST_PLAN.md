# Issue 483: content-addressed manifest packages

Status: implementation plan for the schema 8 replacement.

## Assumptions and non-goals

- Favn has no production users or persisted compatibility obligation.
- Schema 7 is replaced, not migrated at runtime. There is no schema 7 decoder,
  fallback publication route, feature flag, or dual manifest representation.
- The existing gzip and publication limits remain safety boundaries. Scaling is
  achieved by changing the data model, not by continually raising those limits.
- SQL execution trees are immutable build artifacts. Asset metadata, graph,
  pipeline, and schedule data remain in the manifest index because planning and
  catalogue operations need them.
- This change solves publication, persistence, registration, and execution for a
  catalogue of thousands of assets. Cross-node package distribution and remote
  object storage are not required for the first production deployment.

## Decision

Manifest schema and runner contract version 8 consist of:

1. One compact, canonical `%Favn.Manifest{}` index. SQL assets contain an
   `execution_package_hash` but no embedded `%Favn.Manifest.SQLExecution{}`.
2. Immutable `%Favn.Manifest.ExecutionPackage{}` values keyed by the SHA-256 of
   their canonical payload. A package owns one asset's SQL execution tree.
3. One `%Favn.Manifest.Publication{}` build result containing the pinned index
   version and the exact package set required by that index.

This is one manifest format with separately addressed artifacts, not two
manifests. A package cannot be substituted because its canonical content hash,
asset ref, and the index reference are all validated.

## Public contracts

### Core

- Bump `Favn.Manifest.Compatibility` to schema 8 and runner contract 8 only.
- Add `execution_package_hash` to `Favn.Manifest.Asset`.
- Remove `sql_execution` from the canonical asset struct. Runtime code receives
  a verified package explicitly rather than hydrating a second asset shape.
- Add `Favn.Manifest.ExecutionPackage` with canonical serialization,
  validation, content hashing, and JSON rehydration.
- Add `Favn.Manifest.Publication` with validation that every package hash in the
  index exists exactly once, every package points at the correct asset, and no
  unreferenced package is included.
- `Favn.Manifest.Build` carries packages alongside the compact manifest.
- `Favn.build_manifest/1` is the authoring/build entry point. Local publication
  pins the build into a publication. `Favn.generate_manifest/1` remains a
  low-level index generator and does not produce executable packages.

### Storage

Replace the foundation schema in place:

- `favn_manifest_versions.manifest_index_json` stores only the compact index.
- `favn_execution_packages(content_hash, asset_module, asset_name,
  package_json, inserted_at)` stores immutable packages.
- `favn_manifest_execution_packages(manifest_version_id, package_hash)` records
  reachability and enforces referential integrity.

Storage behaviour additions:

- put an idempotent batch of execution packages;
- return missing hashes for a bounded hash query;
- get one package by hash;
- atomically store a manifest index and all package references after verifying
  every referenced package exists and its stored asset ref matches the index.

Memory, SQLite, and Postgres adapters must have identical conflict, missing,
ownership, idempotency, and list-order semantics. SQL package and reference
writes use bounded multi-row chunks rather than one query per asset. SQL
index/reference writes run in the adapter transaction already used by run
transitions. An index is never visible without all of its packages.

### HTTP publication

Authenticated, exact-path endpoints under `/api/orchestrator/v1`:

- `POST /execution-packages/missing` accepts at most 10,000 unique SHA-256
  hashes and returns the missing subset in deterministic order.
- `POST /execution-packages` accepts at most 100 packages per request.
- `POST /manifests` accepts the compact version/index only and succeeds only
  when all referenced packages are stored.

The dedicated publication parser handles all three endpoints before the general
1 MB parser. Plain and gzip JSON retain independent compressed and expanded
limits, authentication happens before body reads, and package shape/count errors
are stable 422 responses. A single package may not exceed 4 MiB expanded; the
request remains bounded by the existing configured publication limits.

The local orchestrator client queries missing hashes and the orchestrator's
effective publication limits, uploads only missing packages in gzip batches
bounded by count, expanded bytes, and compressed bytes, then publishes the
index. A repeated publication uploads zero packages. There is no monolithic
request option.

### Runner

- Runner manifest registration stores only the compact index.
- `Favn.Contracts.RunnerWork` carries the one verified execution package
  required for its selected SQL asset. Relation inspection stays index-only.
- The orchestrator loads that package from storage only after execution
  admission, then performs runtime-input resolution and submission. A wide
  queued stage therefore retains compact work descriptors, not every SQL tree.
- The runner validates package hash and asset ref against the registered index
  in the caller process before entering the singleton lifecycle server. Elixir
  and source assets do not receive a package.
- SQL runtime functions accept an asset descriptor and execution package as
  separate explicit inputs. There is no runtime reconstruction of schema 7.

The selected package travels with work, so execution cannot race a cache miss or
depend on an orchestrator callback. A runner may later add a bounded verified
package cache, but correctness and catalogue-scale memory do not depend on it:
memory is bounded by registered compact indexes and concurrent work packages.

## Implementation sequence

1. Add package/publication core contracts and schema 8 generation.
2. Change serialization/rehydration and delete schema 7 asset payload handling.
3. Replace storage foundation columns/tables and implement all three adapters.
4. Add package/index facade operations and atomic publication validation.
5. Add bounded package API routes and reuse the authenticated gzip parser.
6. Replace local/dev publisher calls with missing-package batch publication.
7. Attach selected packages to SQL work and update runner execution to consume
   the explicit package. Relation inspection remains index-only because it does
   not execute authored SQL.
8. Update repository, HexDocs, and AI breadcrumbs to describe the only current
   manifest contract.
9. Add focused core, adapter parity, API, publisher, runner, and scalability
   regression tests.
10. Obtain an independent development review, address findings, then run format,
    warnings-as-errors compile, fast umbrella, acceptance umbrella, and slow umbrella.

## Acceptance and budgets

- A 6,600-asset index remains below the 32 MiB expanded publication ceiling and
  does not contain SQL templates or statement trees.
- Changing one SQL asset produces one new package; unchanged package hashes are
  reused and skipped by publication.
- Re-publishing the same build uploads zero package bodies.
- Missing package, wrong asset ref, wrong hash, conflicting immutable content,
  and incomplete index publication all fail deterministically.
- An index publication is atomic across its version row and reference rows.
- Registering a large manifest with a runner transfers only the compact index.
- Executing one SQL asset loads/transfers one package, never the catalogue
  package set. Relation inspection loads no execution package.
- Package queries are capped at 10,000 hashes; upload batches are capped at 100;
  each package is capped at 4 MiB expanded; client batches also fit the 8 MiB
  compressed and 32 MiB expanded defaults; request limits remain configurable
  and bounded by the existing publication parser maxima.

## Failure and lifecycle semantics

- Package upload is immutable and idempotent by content hash.
- Upload can leave an unreferenced package if final index publication fails.
  Reference rows make those orphans identifiable. Garbage collection is a
  separate bounded maintenance operation and must never delete a referenced
  package; it is not coupled to request latency.
- Manifest versions remain immutable and content-addressed. Existing version-id
  conflict behavior remains, now over the compact index.
- Storage corruption or a missing referenced package fails before runner work is
  submitted and returns a bounded internal error; it never silently falls back
  to authored modules or an embedded payload.

## Verification matrix

- `favn_core`: deterministic package hashes, compact index, publication
  validation, rehydration, schema compatibility, direct 300-asset regression,
  and a conservative 6,600-asset index projection under the 32 MiB ceiling.
- `favn_orchestrator`: memory adapter semantics, manifest/package facade,
  authenticated/bounded/gzip endpoints, missing-package publication workflow,
  work package attachment, and index-only inspection.
- `favn_storage_sqlite`: migration shape, atomic index/reference write,
  idempotent packages, missing lookup, package retrieval, corruption cases.
- `favn_storage_postgres`: the same adapter contract in integration tests.
- `favn_runner`: compact registration, package validation, SQL execution,
  missing/wrong package rejection, non-SQL execution without packages.
- Full umbrella: format, compile with warnings as errors, fast, acceptance, slow.
