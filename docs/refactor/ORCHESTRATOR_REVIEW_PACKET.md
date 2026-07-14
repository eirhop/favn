# Orchestrator refactor review packet

This packet defines the independent review target. It is not an approval record.

## Review baseline

- Worktree: `/home/eirik/code/favn-orchestrator-cleanup`.
- Branch: `codex/orchestrator-cleanup`.
- Review base: `origin/main` at
  `58eea6702c7d368a923cc433cdbe1d03307da986`.
- Review the complete pull-request diff. It includes the extracted production
  modules, their focused tests and support files, and the tracked refactors.
- Primary scope: `apps/favn_orchestrator` production code and tests.
- Contract-preserving cross-app scope:
  - `apps/favn/lib/favn.ex`
  - `apps/favn_storage_postgres/lib/favn/storage/adapter/postgres.ex`
  - `apps/favn_storage_sqlite/lib/favn/storage/adapter/sqlite.ex`
  - focused SQLite and PostgreSQL adapter tests
  - the production contract and runbook changes under `docs/production`.
- Read the cleanup ledger in
  `docs/refactor/ORCHESTRATOR_CLEANUP_LEDGER.md` for the implementation summary
  and deliberate non-changes.

## Intended architecture

- `FavnOrchestrator` remains the documented cross-app facade. Internal operator,
  storage, scheduler, execution, and persistence modules are not new public
  cross-app entry points.
- The API is a thin authenticated Plug boundary. Resource routers parse and
  shape HTTP data, then call facade or operator command functions.
- Run submission builds a normalized request before admission and dispatch.
  Execution processes own live lifecycle state; durable storage remains the
  source for recovery and operator read models.
- Active operator projections load one indexed current-state event per step;
  they do not depend on a fixed recent-event window for lifecycle correctness.
- Storage adapters implement one behavior with matching memory, SQLite, and
  PostgreSQL semantics. Derived query metadata and summaries are repairable.
- Persistence codecs accept explicit versioned JSON DTOs. Production code must
  not decode Erlang terms from durable or user-controlled input.

## Invariants to challenge

### Public boundary

- Removed or changed facade functions must be intentional for private pre-v1
  software, documented, typed, and covered by boundary tests.
- `favn_view` and other applications must not gain dependencies on orchestrator
  internals.
- Return shapes and error vocabulary must remain predictable across facade,
  API, and storage boundaries.

### Execution lifecycle

- Validate success, failure, retry, timeout, cancellation, process crash,
  restart/recovery, admission conflict, partial dispatch, and cleanup.
- Persist dispatch intent before external dispatch where recovery requires it.
- Durable terminal state must not be undone by a late message or cleanup error.
- Ownership, leases, claims, admission waiters, tasks, and subscriptions must be
  released exactly once without deleting a newer generation of state.
- GenServers must stay responsive: no unbounded work, broad storage scan, remote
  call, or infinite wait inside a callback.

### Persistence and read models

- Memory, SQLite, and PostgreSQL must agree on filtering, ordering, limits,
  transactions, conflict behavior, and repair behavior.
- Codec validation must reject malformed structures, unknown enums, invalid
  timestamps, oversized values, atom-growth attacks, and unsafe nested terms.
- Stored identities must round-trip without accepting arbitrary atom creation.
- Query paths must be bounded and avoid N+1 reads; mutations must update or
  repair every affected derived summary atomically.

### API and security

- Authentication, actor/session rehydration, authorization, bootstrap, token
  parsing, login throttling, audit, and idempotency must fail closed.
- Request limits, filters, SSE framing, headers, JSON encoding, diagnostics, and
  logs must resist injection, invalid UTF-8, secret disclosure, and oversized
  data.
- Unknown-outcome command failures must not be replayed as successful or safely
  retryable outcomes without evidence.

### Complexity and performance

- Extracted modules must own a coherent concept rather than merely relocate
  private functions.
- Reject duplicated policies, generic helper layers, unnecessary wrappers, dead
  compatibility paths, repeated scans, quadratic accumulation, and accidental
  serialization.
- Do not request mechanical splitting solely because a cohesive module remains
  large; identify a concrete ownership, correctness, or performance benefit.

## Review order

1. Public facade, runtime configuration, application supervision, and cross-app
   adapter contract changes.
2. Run submission, admission, execution, cancellation, persistence, recovery,
   and cleanup.
3. Storage behavior, codecs, memory adapter families, SQLite/PostgreSQL parity,
   repair passes, and read models.
4. API authentication/authorization, idempotency, audit, SSE, filtering, and
   response shaping.
5. Scheduler, backfill, freshness, target status, catalogue, and lineage.
6. Tests, production docs, leftover duplication, dead code, and performance
   hazards.

## Reproducing the review target

Run these from the checkout root:

```bash
git status --short
git diff --name-only origin/main...HEAD
git diff origin/main...HEAD -- apps/favn_orchestrator apps/favn apps/favn_storage_postgres \
  apps/favn_storage_sqlite docs/production
```

The validated focused baseline is:

```text
mix format --check-formatted                                      pass
MIX_ENV=test mix compile --warnings-as-errors                     pass
strict Credo over orchestrator production code (196 files)        pass
Sobelow --private --strict over the orchestrator API router       pass
mix deps.audit                                                    pass
elixir scripts/check_test_tag_tiers.exs                           pass
favn_orchestrator fast tier (acceptance/slow/browser excluded)    752 pass
focused SQLite adapter/storage tests                              53 pass
Tidewave Phoenix runtime initialization and evaluation            pass
full SQLite suite after umbrella fixture correction               82 pass
full favn_view suite after hardened-contract fixture correction   165 pass
```

After the independent review, the user requested the full umbrella gate. Every
app ran under a 45-minute watchdog with the ExUnit timeout forwarded to child
apps. The orchestrator stayed green. The remaining umbrella failures are two
`favn_local` production-acceptance tests and one environment-sensitive DuckDB
permission test. Under native Linux `/tmp`, the DuckDB test and one `favn_local`
test pass; the packaged-runtime restart test still fails. Their separate fixes
are not part of this orchestrator review.

## Required review output

Report only actionable findings, ordered by severity:

- P0: exploitable security issue, data loss/corruption, or system-wide outage.
- P1: likely correctness/security failure in a normal or recovery path.
- P2: bounded bug, meaningful performance regression, broken contract, or test
  gap that can conceal one.
- P3: concrete maintainability problem with a simpler demonstrated design.

Each finding must name the file and tight line range, show the failing scenario
or invariant, and explain why existing tests do not protect it. Avoid style-only
comments. If there are no findings, state that explicitly and list residual
risks or unverified areas.
