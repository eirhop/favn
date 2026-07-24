# Runtime Model

This guide explains what happens after you have a pinned manifest version.

It is user documentation for the public `:favn` package. It does not make Favn's
private runtime code public API.

## Runtime Flow

```text
authoring modules
  -> manifest
  -> pinned manifest version
  -> registered runtime version
  -> run or schedule command
  -> runner-executed work
  -> recorded run state, events, logs, and diagnostics
```

The important rule is that runtime work is tied to a manifest version. A run is
not created by loading an asset module directly.

## Manifest Versions

A manifest version is the runtime input.

Operators or local tooling register a version and select which version is active.
New default runs and schedules use the active version. Existing accepted work
keeps the version it was created with.

If the same version id points to different content, Favn treats that as a
conflict. A version id should identify one manifest payload.

## Runs

A run is a recorded attempt to execute asset or pipeline work.

Typical run states are:

| State | Meaning |
| --- | --- |
| `:pending` | Favn accepted the run but execution has not finished. |
| `:running` | A runner is executing work. |
| `:ok` | The run completed successfully. |
| `:partial` | Some planned work did not complete successfully. |
| `:error` | The run failed. |
| `:cancelled` | Cancellation was recorded. |
| `:timed_out` | The run exceeded its timeout. |

Inspect runs through public commands such as `mix favn.runs`,
and `mix favn.diagnostics`.

## Asset Runtime Context

Asset code and SQL runtime-input resolvers receive a typed `Favn.Run.Context`.
Each kind of value has one path:

| Value | Path |
| --- | --- |
| Current manifest asset ref, relation, and static settings | `ctx.asset` |
| Non-secret asset settings | `ctx.asset.settings` |
| Non-secret pipeline settings | `ctx.pipeline.settings` |
| Submitted per-run values | `ctx.params` |
| Resolved environment-dependent values and secrets | `ctx.runtime_config` |
| Runtime window and absolute deadline | `ctx.window`, `ctx.deadline_at` |

There is no generic `ctx.config` or `ctx.current_ref`. Use `ctx.asset.ref` for
the current ref. Metadata is descriptive manifest/operator data and does not
act as an arbitrary runtime settings channel.

## Runner-Local Services

The consumer application's normal supervision tree may not be running in an
isolated Favn runner. `Favn.Runner.Plugin` is the public lifecycle for services
that must be there before assets execute. `Favn.Runner.SupervisedChildren` is the
simple path for ordinary OTP child specs.

Plugin state can outlive one asset run, but only inside that runner. A runner
restart, replacement, or reschedule deletes it. Use it for rebuildable caches,
credential/session reuse, pools, and rate limiting. Do not use it for durable
business state or correctness-sensitive communication between runs.

Read [Runner Plugins And Runner-Local Services](runner-plugins.md).

## Schedules

Schedules are declared in the manifest and acted on by the runtime.

Enabling a schedule affects future submissions. It does not automatically submit
missed work from the past. Disabling a schedule stops future submissions but does
not cancel runs already in progress.

## Backfills

A backfill runs work over a range of windows.

Favn records the parent backfill and the child runs it creates. Each child run is
still tied to a manifest version.

## Cancellation And Retry

Cancellation records intent first, then asks in-flight work to stop when possible.
Already completed work is not changed into a successful cancellation.

Retry and rerun operations use recorded run state and the pinned manifest version.
They do not use UI state as the source of truth.

One terminal branch does not stop independent siblings. Nodes that require a
failed or resource-blocked upstream become terminally blocked. A configured
resource circuit blocks only work that uses that execution pool or named SQL
connection. Pipeline `resource_recovery` may create a linked new run after a
successful half-open probe; it never changes the terminal source run.

## Common Runtime Failures

| Failure | What it means |
| --- | --- |
| Manifest not found | Register or activate the intended manifest version first. |
| Invalid target | The asset, pipeline, or schedule is not in the selected manifest. |
| Not authorized | The operator session or role is not valid for the command. |
| Persistence failure | Favn could not store runtime state; inspect readiness and diagnostics before retrying. |
| Runner unavailable | Work cannot execute until runner connectivity or startup is fixed. |
| Resource circuit open | The node's execution pool or SQL connection is temporarily blocked; one eligible node probes it after the configured delay. |
| Timeout or crash | Execution failed; inspect run events and logs. |
| Operator view is stale | Operator views may need repair from recorded runtime state. |

## Related Docs

- [Manifest-First](manifest-first.md)
- [Local Development](local-development.md)
- [Runner Plugins And Runner-Local Services](runner-plugins.md)
- `docs/architecture/runtime-model.md` for contributor-facing architecture notes
- `docs/operators/runs-and-schedules.md` for operator procedures
