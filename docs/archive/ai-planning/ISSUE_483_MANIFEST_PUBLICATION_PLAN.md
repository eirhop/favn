# Issue 483: Scalable Manifest Publication Plan

Status: temporary implementation plan for issue #483.

## Problem

The orchestrator currently parses every JSON request through the same 1 MB
limit. A representative 66-asset SQL-heavy manifest produces an approximately
4.5 MB publication request, so normal local development fails before the
manifest reaches validation.

The current manifest also carries repeated SQL source, template IR, spans,
checks, definitions, asset identity, dependency, and graph data. Raising one
HTTP limit will unblock current projects, but a monolithic manifest will not be
an appropriate production transport for catalogues containing thousands of
assets.

## Assumptions

- Existing uncompressed JSON publication remains compatible during this phase.
- Gzip is an HTTP transfer encoding only; canonical manifest serialization and
  content hashing do not change.
- This phase targets reliable development and early production use for a few
  hundred assets, not the final several-thousand-asset representation.
- Ordinary API endpoints retain the existing 1 MB JSON request limit.

## Phase 1: Bounded Publication Transport

Implement now:

1. Route manifest publication through an orchestrator-owned request parser
   before the general `Plug.Parsers` pipeline.
2. Authenticate service credentials before reading or decompressing the body.
3. Accept `application/json` with either identity or gzip content encoding.
4. Default the local development publisher to gzip.
5. Give publication a bounded 60-second client response timeout so larger
   decode/hash/persistence work does not inherit the generic 5-second budget.
6. Enforce independent configurable limits:
   - 8 MiB compressed input;
   - 32 MiB decompressed or uncompressed JSON.
7. Decompress incrementally and stop after the decompressed limit rather than
   calling an unbounded gzip helper.
8. Close the HTTP connection on authentication or validation rejections made
   before reading the body, so the server does not drain an untrusted unread
   body to preserve keepalive.
9. Return stable JSON errors for unsupported encodings, malformed gzip, invalid
   JSON, and oversized compressed or decompressed bodies.
10. Keep canonical hashing, persistence, activation, and runner registration
   identical for compressed and uncompressed requests.

Configuration paths:

```elixir
config :favn_orchestrator, :manifest_publication,
  compressed_limit_bytes: 8 * 1024 * 1024,
  decompressed_limit_bytes: 32 * 1024 * 1024
```

Production environment overrides:

```text
FAVN_ORCHESTRATOR_MANIFEST_COMPRESSED_LIMIT_BYTES
FAVN_ORCHESTRATOR_MANIFEST_DECOMPRESSED_LIMIT_BYTES
```

The server must not consume an arbitrarily large rejected body merely to report
its exact size. Errors report the declared `Content-Length` when it caused an
early rejection; otherwise they report the number of bytes observed before the
limit was crossed.

## Phase 2: Measure and Normalize

Before selecting a final representation, add scalable fixtures covering at
least 66, 300, 1,000, and 6,600 SQL-heavy assets. Record:

- authored SQL bytes;
- canonical JSON and gzip bytes;
- bytes attributed to templates, spans, checks, definitions, and graphs;
- decoded BEAM memory;
- generation, serialization, hashing, decoding, persistence, and runner
  registration time; and
- peak memory during publication and registration.

Use those measurements to define a new manifest schema and runner contract that
stores each runtime fact once. Likely candidates include removing derivable
asset identity fields, graph projections, duplicate raw/template SQL, duplicate
template requirements, and compile-time-only position data.

Required runtime semantics, checks, contracts, lineage, and executable SQL must
remain available without loading consumer authoring modules.

## Phase 3: Manifest Index and Execution Packages

If the normalized several-thousand-asset fixture remains outside explicit
memory, storage, or latency budgets, replace monolithic publication with a
content-addressed bundle:

1. Keep a small canonical manifest index containing catalogue metadata,
   dependencies, schedules, and execution-package hashes.
2. Store each asset's SQL execution payload as an immutable package addressed by
   its canonical content hash.
3. Upload only packages missing from orchestrator storage.
4. Publish the index only after all referenced packages are present and
   verified.
5. Activate the complete manifest atomically.
6. Let runners fetch and cache execution packages by hash instead of receiving
   the complete manifest term during registration.
7. Define retention and garbage collection around manifest references so old
   pinned runs remain reproducible.

Compression and upload chunking are transport details and must never affect
logical identity. Partial uploads must be safe to retry and must never produce
an activatable incomplete manifest.

## Phase 1 Verification

- Existing uncompressed JSON publication succeeds.
- Gzip and identity requests publish the same canonical manifest.
- Requests immediately below each limit succeed; requests above each limit
  return structured `413` responses.
- HTTP/1 chunked requests at the exact limit succeed; one extra byte fails.
- Malformed gzip and unsupported content encodings fail safely.
- Unauthorized requests fail before decompression.
- Non-manifest API requests retain the 1 MB limit.
- `mix favn.dev` publishes the representative 66-asset manifest.
- Focused client, API, configuration, compatibility, and security tests pass.

## Deferred Decisions

- Exact normalized schema shape and schema-version migration.
- Package granularity: one package per asset or a bounded grouped package.
- Package storage callbacks and adapter migrations.
- Runner cache ownership, eviction, and recovery behavior.
- Bundle identity construction and compatibility with existing manifest hashes.
- Retention and garbage collection policy.
