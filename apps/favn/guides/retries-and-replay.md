# Retries, Replay, And Runtime-Input Pins

Reader: authors and operators deciding whether Favn may repeat work.

Favn has several mechanisms that repeat an operation. They solve different
problems. There is intentionally no global “retry everything” switch.

## The Mental Model

| Mechanism | What may repeat | Consumes an asset attempt? | Input behavior |
| --- | --- | --- | --- |
| SQL safety retry | Proven-safe session creation/bootstrap or read-only inspection/query | No | Same call input; never blindly retries a write |
| Persistence retry | A failed control-plane state write | No | Same state transition; does not rerun the asset |
| Node attempt retry | One explicitly safe failed asset node in the same run | Yes | Reuses that run/node runtime-input pin |
| Resource recovery | Safe remaining work after a resource probe succeeds | Starts new attempt counts | Opt-in linked run with inherited existing pins |
| Rerun or replay | Creates a new run | Starts new attempt counts | `:fresh`, `:inherit`, or `:pinned` as described below |
| Backfill | Creates child runs for windows | Each child has its own attempts | Normal children resolve fresh inputs |
| Schedule overlap/missed handling | Decides whether a separate run is submitted | No relation to an older run's attempts | Every admitted run has independent pins |
| HTTP command idempotency | Replays the response to the same command key | No | Prevents duplicate commands; it does not retry execution |

An internal retry never reruns an asset. A node retry never creates a new run.
A rerun or resource recovery always creates a new run. Schedule overlap decides
whether another run exists at all.

## Configure Node Attempts

`max_attempts` includes the first attempt. The default is `1`, which means no
automatic node retry.

A pipeline can supply a default:

```elixir
pipeline :daily_orders do
  assets [MyApp.Orders.Raw, MyApp.Orders.Mart]

  retry max_attempts: 3,
        backoff: {:exponential, initial: 5_000, max: 300_000, jitter: 0.2}
end
```

An Elixir or SQL asset uses the same override:

```elixir
retry max_attempts: 6,
       backoff: {:exponential, initial: 10_000, max: 600_000}
def asset(ctx), do: fetch_orders(ctx)
```

```elixir
retry max_attempts: 4, backoff: 2_000
materialized :table
query do
  ~SQL"select * from staged_orders"
end
```

The effective policy is chosen once and stored on every planned node:

```text
explicit operator submission override
→ asset retry
→ pipeline retry
→ max_attempts: 1
```

An operator override applies only to the new run. It does not edit the manifest.
Run details show the effective policy and `operator`, `asset`, `pipeline`, or
`default` source.

For an Elixir submission, pass the same typed policy under `retry_policy`:

```elixir
FavnOrchestrator.submit_pipeline_run(MyApp.Pipelines.Daily,
  retry_policy: [
    max_attempts: 4,
    backoff: {:exponential, initial: 5_000, max: 300_000, jitter: 0.2}
  ]
)
```

HTTP/operator JSON uses the serialized struct field names:

```json
{
  "retry_policy": {
    "max_attempts": 4,
    "backoff": {
      "strategy": "exponential",
      "initial_ms": 5000,
      "max_ms": 300000,
      "jitter": 0.2
    }
  }
}
```

`mix favn.run` and `mix favn.backfill` expose the fixed-backoff shorthand
`--retry-max-attempts` plus `--retry-backoff-ms`. Supplying only the backoff
keeps the default `max_attempts: 1`, so no automatic retry occurs. Use the DSL,
Elixir submission option, or HTTP policy object for exponential backoff.

### Backoff

- An integer backoff is a fixed delay in milliseconds.
- `{:fixed, delay: milliseconds}` is the explicit fixed form.
- `{:exponential, initial: ..., max: ..., jitter: ...}` doubles after each
  failed attempt up to `max`.
- `jitter` is bounded from `0.0` to `1.0` and spreads concurrent wakeups.
- A typed error may supply bounded `retry_after_ms`. Favn waits for the larger
  of policy backoff and retry-after, subject to the global one-day bound.

Changing authored policy after a run starts does not change that run.

## Policy Does Not Make A Failure Safe

The policy answers “how often and when?” The normalized failure answers “may
this node be repeated?” Both must allow another attempt.

Potentially retryable failures must be explicitly classified as a known safe
failure. Examples are connection failure before work begins, rate limiting, a
safe source read interruption, or a runtime-input resolver that reports that an
immutable manifest is not ready yet.

These are non-retryable by default:

- invalid SQL, configuration, or resolver output;
- data-quality, check, contract, constraint, or permanent source failure;
- cancellation;
- custom exceptions without an explicit safe classification; and
- any write, materialization, transaction, or external side effect whose
  outcome is unknown.

Favn does not infer safety from exception messages. A larger `max_attempts`
never authorizes an unknown write to repeat.

## One Node Retries; Successful Siblings Stay Complete

Given independent nodes A and B in one stage:

```text
attempt 1: A succeeds, B fails safely
attempt 2: A remains complete, B runs with B's original pin
next stage: starts only after required upstream work succeeds
```

Independent siblings continue after terminal failure. Required downstream nodes
become durably blocked, so the run records a terminal result for every planned
node. There is no automatic whole-pipeline rerun. An operator may create a new
run with retry-remaining, resume, exact replay, or fresh rerun semantics.

## Recover After A Shared Resource Returns

Resource circuit breakers are configured on an execution pool or named SQL
connection, not in retry policy. A circuit opens after its configured number of
consecutive explicit resource failures. While open, only nodes that need that
resource are blocked. Once the delay expires, one normal eligible node gets the
exclusive probe permit. Probe success closes the circuit; probe failure reopens
it.

Pipeline recovery is off by default. Opt in explicitly:

```elixir
pipeline :daily_orders do
  assets [MyApp.Orders.Raw, MyApp.Orders.Mart]

  resource_recovery :retry_remaining,
    max_age_ms: :timer.hours(6)
end
```

When a probe succeeds, Favn may submit one linked recovery run containing nodes
that were circuit-blocked and failed nodes whose runner outcome explicitly says
they are safe to repeat. Candidates older than `max_age_ms` are ignored. Existing
runtime-input pins are inherited where present; nodes never reached resolve their
inputs normally. The source run stays terminal and immutable. Unknown-outcome
writes, materializations, transactions, and external side effects are never
included merely because the circuit closed.

Recovery candidates are durable. A supervised bounded sweep resumes pending
work after an orchestrator restart, and each claimed candidate set derives a
deterministic recovery run id. Replaying an uncertain submission therefore
returns the same linked run instead of creating a duplicate.

## Runtime-Input Resolve, Pin, Execute

For a SQL asset with `runtime_inputs`, Favn completes this handshake before SQL
rendering, session acquisition, or materialization:

1. The orchestrator loads `{run_id, planned_node_key}` from the pin store.
2. If absent, the runner invokes and validates the selection-only resolver.
3. The orchestrator atomically persists the exact normalized parameters.
4. Only the persisted winner is sent to normal execution.
5. Later attempts and safe restart recovery load the same pin and do not invoke
   the resolver again.

Racing equivalent results reuse the winner. A different identity or payload
fingerprint is a conflict and execution stops. Resolver code may read external
state to select immutable input, but must not claim, consume, delete, or write
external state.

Raw parameters never belong in generic run metadata, events, logs, telemetry,
or errors. Safe detail exposes identity, fingerprint, resolver, and source-pin
lineage only. Pins with sensitive parameters require protected storage. Configure
a 32-byte key (`FAVN_RUNTIME_INPUT_PIN_KEY` locally, or the versioned
`FAVN_RUNTIME_INPUT_PIN_KEYS` JSON keyring in production). Missing or invalid
protection fails before materialization; sensitive values are never silently
stored in plaintext.

## New Runs And Replay Input Modes

| Operation | Default input mode | Meaning |
| --- | --- | --- |
| automatic node attempt | current pin | Same run/node pin is mandatory |
| normal manual run | `:fresh` | Resolve selected nodes for the new run |
| normal scheduled run | `:fresh` | Each admitted occurrence resolves independently |
| normal backfill child | `:fresh` | Each child run/window has independent pins |
| fresh rerun | `:fresh` | Deliberately resolve selected nodes again in the new run |
| exact replay | `:pinned` | Require selected source-run pins; missing required pins fail |
| resume from failure | `:inherit` | Copy existing source pins; resolve nodes never reached |
| retry remaining | `:inherit` | Copy existing source pins; resolve nodes never reached |

`:pinned` never substitutes a fresh value and still calls the result exact.
`:inherit` records source run, source node, and source fingerprint lineage.
`:fresh` deliberately selects again. An explicit rerun override may choose fresh
input, but that operation is not an exact replay.

## Schedules Are Separate Runs

```elixir
schedule cron: "*/5 * * * *",
  overlap: :forbid,
  missed: :one
```

- `overlap: :allow` admits another run while the tracked run is active.
- `overlap: :forbid` skips submission while it is pending/running.
- `overlap: :queue_one` remembers one pending occurrence.
- `missed: :skip | :one | :all` controls catch-up after delayed evaluation.

Timeline:

```text
10:00 Run A pins manifest A
10:02 A's node fails safely and waits
10:05 the next schedule occurrence is due
10:06 A retries with manifest A
```

| Overlap | 10:05 result |
| --- | --- |
| `:allow` | Run B is admitted and resolves its own pin; A and B can overlap |
| `:forbid` | No Run B is submitted; A keeps its pin |
| `:queue_one` | One occurrence waits; its new run resolves only when admitted |

`missed` decides how many due occurrences are considered when evaluation
resumes; it does not retry A. Execution pools, pipeline `max_concurrency`, SQL
`write_concurrency`, and materialization claims are admission controls. They do
not make separate runs share attempts or pins.

## Cancellation And Restart

Cancellation during backoff wins: the timer is cancelled and no next attempt
is dispatched. A persisted safe retry wait contains the current attempt,
effective policy, normalized failure state, pin identity, and absolute
`next_retry_at`. An orchestrator restart resumes a future wait or dispatches a
due retry without incrementing the attempt early.

For a pipeline stage, Favn persists one compact retry checkpoint before the
individual retry-scheduled events. Recovery validates that exact checkpoint against
the pinned plan; a missing or corrupt checkpoint fails closed instead of guessing
which successful work may be repeated.

Active ownership with an unconfirmed outcome is different. Recovery reconciles
ownership/cancellation evidence and does not dispatch replacement work when the
previous write may have succeeded. The run becomes terminal or requires
operator action. Persistence bookkeeping retries do not consume attempts.

## Transactions And External Side Effects

Retry is not an exactly-once guarantee. Favn can roll back work only where the
owning adapter provides a confirmed transaction boundary. Custom Elixir code,
HTTP calls, message publication, and external files are not automatically
rolled back. Make such operations idempotent with a business key, or record an
immutable upstream result and let a later Favn node consume it.

Safe recipes:

- API ingestion: page/read into immutable raw storage using a source idempotency
  key; retry only a failure known to have happened before the write.
- Immutable files: pin an immutable manifest or snapshot ID, not a mutable
  “latest” listing.
- Windowed ETL: use deterministic window keys and transactional replacement or
  merge semantics owned by the SQL adapter.
- Latest-state refresh: prefer an atomic view/table swap; never blindly repeat
  an unknown partial overwrite.

## What Operators Should Inspect

Before taking action, inspect effective policy/source, current/max attempt,
failure retryability and outcome, retry exhaustion, `next_retry_at`, input mode,
safe pin identity/lineage, active runner ownership/cancellation outcome, and the
schedule occurrence's overlap/missed settings. HTTP idempotency status answers
whether a command was duplicated; it does not prove asset success.

Read [Runtime Inputs For SQL Assets](sql-runtime-inputs.html) for resolver
authoring and `Favn.Pipeline`, `Favn.Asset`, and `Favn.SQLAsset` for placement of
the retry declarations.
