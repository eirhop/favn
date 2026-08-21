# Change Record: Runner Control-Plane Connectivity

| Field | Value |
| --- | --- |
| Status | Implementing |
| Type | Bug fix |
| Primary issue | [#633](https://github.com/eirhop/favn/issues/633) |
| Pull request | [#656](https://github.com/eirhop/favn/pull/656) |
| Related work | The repository change-record standard is introduced in the same PR at the user's request. |
| Affected areas | Packaged runner startup, distributed-BEAM connection, registration, lifecycle, readiness, deployment templates, logs, and diagnostics |
| Approved plan commit | Not available. The temporary plan was reviewed and removed before permanent change records were adopted. |
| Original code baseline | `87e91127` |
| Implementation commit before this record | `ed4abc65` |
| Last updated | 2026-08-21 |

## One-minute summary

A packaged runner could accept a short control-plane hostname even though it
uses long distributed-Erlang node names. OTP then rejected the connection, but
the runner kept retrying with little useful evidence. At the same time, the
runner reported itself as accepting and ready before it had connected or
registered.

The fix rejects an invalid hostname during startup, keeps a remote runner
non-accepting until connection and registration both succeed, and makes
readiness depend on that evidence. It also adds capped retry delays, safe
failure classes, rate-limited logs, telemetry, and redacted diagnostics.

This record was made permanent after implementation because the repository
adopted the change-record process while PR #656 was already open. The known
decisions from the approved temporary plan are reconstructed here from the
issue, original source, implementation review, and reviewer evidence. This is
not an exact copy of the deleted plan, and no baseline commit is claimed.

## Impact

Before this change, an operator could see a running, ready-looking runner while
queued work never moved. A simple hostname error looked like a scaling or
scheduling failure.

After this change:

- an invalid control-plane hostname stops packaged runner startup with a clear,
  redacted configuration error;
- a disconnected or unregistered runner reports not ready and does not accept
  new work;
- diagnostics show which layer is failing and, when available, the connection
  and registration retry schedules; the subscription retry remains internal;
- repeated failures produce useful evidence without flooding logs.

## Problem analysis

The failure had three root causes:

1. **Validation gap:** the runner accepted a dotless host even in long-name
   distributed-Erlang mode.
2. **Lifecycle gap:** `RuntimeBootstrap` marked every runner accepting without
   connection or registration evidence.
3. **Readiness and diagnostic gap:** readiness mostly proved that the manifest
   process existed. It did not require connection, registration, or lifecycle
   acceptance, and retries exposed little bounded evidence.

These gaps combined into a false healthy state. The connection loop itself was
not enough to establish whether the runner could claim work.

### Assumptions

- The issue concerns a packaged runner with `FAVN_CONTROL_PLANE_NODE` configured.
- Packaged production runners use long distributed-Erlang names, mutual TLS, a
  high-entropy cookie, and one explicit control-plane node.
- Embedded and test operation without a control plane must keep immediate local
  acceptance.
- Code can validate FQDN syntax. Private DNS routing remains an infrastructure
  responsibility.
- Existing assignment, lease, fencing, drain, and unknown-outcome behavior is
  correct and must not change.
- `Node.connect/1` returning `false` cannot safely distinguish every TCP, TLS,
  certificate, or cookie failure.
- Invalid startup configuration is terminal. Transient connection or
  registration failures continue with bounded retry frequency.
- No runner wire-protocol change is required.

### Evidence

| Evidence | What it proves | What it does not prove |
| --- | --- | --- |
| Issue #633 and the observed Azure Container Apps failure | A dotless hostname can leave a runner running but unable to register or claim work. | The behavior of every infrastructure provider. |
| Original source at `87e91127` | Startup marked lifecycle accepting independently of connection and readiness omitted connection and registration. | Live network behavior. |
| Focused runner tests in PR #656 | Invalid-host, disconnect, retry, registration, readiness, and redaction behavior are deterministic. | A real two-node TLS deployment. |
| Independent plan-to-code review | The implementation preserves the approved invariants and identifies material deviations. | Post-push CI or live deployment proof. |

## Current behavior before the change

```mermaid
flowchart LR
    A[Start packaged runner] --> B[Accept dotless host]
    B --> C[Mark lifecycle accepting]
    B --> D[Try distributed connection]
    D -->|OTP rejects host| E[Retry every second]
    E --> D
    C --> F[Readiness checks local manifest process]
    F --> G[Runner appears ready]
    E --> H[Registration and claims cannot succeed]
```

### Old call and event sequence

```mermaid
sequenceDiagram
    participant App as Runner application
    participant Config as Runtime config
    participant Life as Lifecycle
    participant Conn as Connection process
    participant Agent as Runner agent
    participant Ready as Readiness

    App->>Config: Validate environment
    Config-->>App: Accept dotless target
    App->>Life: Start in starting state
    App->>Conn: Start connection process
    App->>Agent: Start registration process
    par Independent connection loop
        Conn->>Conn: Node.connect returns false
        Conn->>Conn: Retry after one second
    and Independent registration loop
        Agent->>Conn: Request gateway
        Conn-->>Agent: Control plane unavailable
        Agent->>Agent: Retry registration
    and Independent bootstrap gate
        App->>Life: Mark accepting without remote evidence
    end
    Ready->>Ready: Check manifest process
    Ready-->>App: Report ready
```

The important ordering bug is visible above: acceptance and readiness could
happen before the connection and registration loops succeeded.

## Reconstructed approved plan

For a configured remote runner, startup must establish evidence in this order:

```mermaid
flowchart LR
    A[Validate FQDN target] --> B[Start non-accepting]
    B --> C[Connect to control plane]
    C -->|Transient failure| D[Record safe evidence]
    D --> E[Wait with capped backoff]
    E --> C
    C -->|Connected| F[Register runner]
    F -->|Unavailable| D
    F -->|Ordinary rejection| J[Stop runner with error]
    F -->|Stale resume on resident| K[Discard fenced assignment]
    K --> F
    F -->|Accepted| G[Mark accepting]
    G --> H[Readiness becomes true]
    G --> I[Claim work]
```

### New call and event sequence

```mermaid
sequenceDiagram
    participant App as Runner application
    participant Config as Runtime config
    participant Life as Lifecycle
    participant Conn as Connection process
    participant Agent as Runner agent
    participant CP as Control plane
    participant Ready as Readiness

    App->>Config: Validate and freeze FQDN target
    alt Invalid target
        Config-->>App: Redacted configuration error
        App-->>App: Stop startup
    else Valid target
        App->>Life: Start non-accepting
        App->>Conn: Start connection process
        App->>Agent: Start registration process
        par Connection loop
            Conn->>CP: Connect
        and Connection-state subscription
            Agent->>Conn: Subscribe to current and future state
        end
        alt Connection fails
            Conn->>Conn: Classify and schedule capped retry
            Conn-->>Agent: Connecting or unavailable
            Agent->>Life: Mark connecting
        else Connection succeeds
            Conn-->>Agent: Connected snapshot or notification
            Agent->>CP: Register runner
            alt Registration unavailable
                CP-->>Agent: Transient failure
                Agent->>Agent: Schedule registration retry
            else Ordinary rejection
                CP-->>Agent: Rejected
                Agent-->>App: Stop with error
            else Stale resume on resident
                CP-->>Agent: Stale assignment rejected
                Agent->>Agent: Discard fenced assignment and register fresh
            else Registration accepted
                CP-->>Agent: Accepted acknowledgement
                Agent->>Life: Mark accepting
            end
        end
        Ready->>Life: Read lifecycle evidence
        Ready->>Conn: Read connection evidence
        Ready->>Agent: Read registration evidence
        Ready-->>App: Ready only when all required evidence agrees
    end
```

The connection and subscription loops may start in either order. A late
subscriber receives the current connection snapshot. The enforced order is
connected evidence, registration, accepted acknowledgement, then lifecycle
acceptance.

### Contracts and invariants

- Only the frozen, validated target node may be contacted.
- A configured remote runner accepts work only after connection and accepted
  registration.
- Every disconnect requires a fresh accepted registration.
- `draining` and `stopping` remain monotonic. They cannot regress to connecting
  or accepting.
- Connection loss rejects new admission owners but preserves an existing
  monitored permit so admitted work can finish.
- Existing assignments, leases, buffered results, fencing, and unknown outcomes
  keep their previous semantics.
- Possibly completed writes or side effects are never blindly retried.
- The connection process owns transport state and timing.
- The runner agent owns registration and assignment-resume state.
- Lifecycle owns local admission. The orchestrator remains durable work
  authority.
- Failure classes are bounded atoms derived from stable public OTP results.
- Diagnostics and logs never include cookies, tokens, certificate contents, TLS
  paths, private paths, or arbitrary exception terms.
- Telemetry still fires when a matching log is suppressed.
- No orchestrator dependency is added to `favn_runner`.

### Scope

- Validate the packaged runner node topology before the supervision tree starts.
- Make connection, registration, lifecycle, and readiness state agree.
- Add bounded connection and registration retry ownership.
- Expose safe operational evidence through diagnostics, logs, and telemetry.
- Update generated Compose node aliases and canonical runner documentation.
- Add focused regression, recovery, readiness, logging, and redaction tests.

### Non-goals

- Database, storage, manifest, or public DSL changes.
- A runner wire-protocol change.
- A generic distributed-Erlang discovery framework.
- Reworking durable task recovery, fencing, or unknown outcomes.
- Proving or deploying a live Azure or distributed-TLS environment.

### Implementation slices

| Slice | Outcome | Owner or area | Depends on |
| --- | --- | --- | --- |
| 1 | Reject invalid long-name targets before startup | Production runtime configuration | Existing release configuration |
| 2 | Own connection state, classification, diagnostics, and one capped retry timer | Connection process | Validated target |
| 3 | Register only after connection and own registration recovery | Runner agent | Connection notifications |
| 4 | Keep remote runners connecting until registration is accepted | Lifecycle and bootstrap | Registration state |
| 5 | Fail readiness closed from composed evidence | Public runner facade | Lifecycle, connection, and registration diagnostics |
| 6 | Emit debug failed attempts, rate-limited warning summaries, success events, and per-event telemetry | Runner operational events | Stable failure classes |
| 7 | Use one consistent long-name topology | Deployment template and canonical docs | FQDN contract |
| 8 | Prove failure, recovery, concurrency, and redaction behavior | Runner and acceptance tests | All earlier slices |

### Implementation map

| Concept | Code area | Responsibility |
| --- | --- | --- |
| Startup validation | `production_runtime_config.ex` and `application.ex` | Reject unsafe targets and pass frozen configuration to children. |
| Transport state | `control_plane_connection.ex` | Connect, monitor, classify, retry, notify, and report diagnostics. |
| Registration state | `runner_agent.ex` | Register, recover, fence stale results, and control lifecycle acceptance. |
| Admission state | `lifecycle.ex` and `runtime_bootstrap.ex` | Reject new work until remote registration is accepted. |
| Readiness | `favn_runner.ex` | Compose release, local, connection, and registration evidence. |
| Operational evidence | `operational_events.ex` | Separate telemetry from rate-limited structured logs. |
| Deployment contract | Compose template and production docs | Use fully-qualified long-name aliases consistently. |

## Operational design

### Failures and recovery

- An invalid hostname is a startup error and is not retried.
- A transient connection failure records a safe class and schedules one capped,
  jittered retry.
- Connection loss clears the usable gateway, marks registration and lifecycle
  connecting, and requires fresh registration.
- Registration failure has a separate coalesced retry because transport may
  remain connected while the control-plane registration endpoint is unavailable.
- Ordinary registration rejection stops the runner with an error. The one
  recoverable rejection is a stale assignment on a resident runner: it discards
  the fenced assignment and registers fresh.
- A late registration acknowledgement from before a disconnect is fenced and
  cannot make the runner accepting.
- Existing admitted work may finish. New work waits until recovery.
- The runner may stay unavailable for an unbounded outage, but retry rate and
  diagnostic payloads remain bounded.

### Logs and diagnostics

| Event or state | Level or surface | Safe fields | Rate limit |
| --- | --- | --- | --- |
| Failed connection attempt | Debug log and telemetry | Retry number, bounded target identity, safe class, next delay | Every failed attempt |
| Connection failure summary | Warning log and telemetry | Safe class, retry number, next delay, suppressed count, bounded target identity | Log at retries 1, 2, 4, 8, every 20, and when class changes; telemetry every failure |
| Registration failure | Warning log and telemetry | Safe class, retry number, next delay, sanitized pool | Log at retries 1, 2, 4, 8, every 20, and when class changes; telemetry every failure |
| Connection success | Info log and telemetry | Connection duration, prior retry count, bounded target identity | Once for initial connection and once per recovery |
| Registration accepted | Info log and telemetry | Sanitized pool | Once for each accepted registration |
| Runner diagnostics | `FavnRunner.diagnostics/0` | Lifecycle, target, connection, registration, retry count, timestamps, next delay, manifest state | On request; remote evidence uses short bounded calls |
| Runner readiness | Health-check surface | Ready or not ready from required evidence | On request; remote evidence fails closed when unavailable |

Connection diagnostics include status, connected state, validated target,
retry count, last safe failure class and time, connected time, and next retry.
Registration diagnostics include status, registered state, agent phase, retry
count, last safe failure class and time, and next retry.

Raw credentials, certificate material, TLS paths, arbitrary exceptions, and
unbounded rejection values are excluded.

### Deployment, migration, and compatibility

There is no database migration or wire-protocol compatibility window. Existing
valid FQDN configurations continue to work. Dotless, IP, malformed, and
underscore-bearing control-plane hosts now fail startup and must be corrected
before deployment.

Generated Compose artifacts give Control Plane, View, and Runner consistent
FQDN aliases because long-name distributed-Erlang nodes cannot safely mix with
short-name nodes. Rollback uses the previous immutable runner release and its
matching manifest. Rolling back also restores the old false-ready risk, so
correcting configuration is preferred.

## Verification plan

| Acceptance criterion | Planned evidence | Owning layer |
| --- | --- | --- |
| Invalid short or malformed targets fail before startup | Runtime configuration and application tests | Runner startup |
| Valid FQDN targets continue working | Runtime configuration tests | Runner startup |
| Disconnected or rejected runners are never accepting or ready | Lifecycle, runner-agent, and facade tests | Runner lifecycle |
| Accepted registration restores accepting and ready | Runner-agent and facade tests | Runner registration |
| Reconnection requires fresh registration | Connection and runner-agent recovery tests | Transport and registration |
| Active permits survive disconnect while new owners are rejected | Lifecycle concurrency tests | Local admission |
| Retries are coalesced, capped, and truthful | Connection and runner-agent timer tests | Retry owners |
| Logs are rate-limited while telemetry remains complete | Operational-event tests | Diagnostics |
| Sensitive canaries never appear | Redaction tests | Configuration, diagnostics, and logs |
| Generated services use qualified names | Deployment artifact acceptance test | Deployment template |

Evidence must separate source inspection, focused tests, broader CI, and live
deployment proof. Live Azure or two-node TLS verification was not part of this
change.

## Risks and open questions

| Risk or question | Impact | Mitigation or decision |
| --- | --- | --- |
| OTP collapses several handshake failures into one public result | Diagnostics may be less specific than the underlying failure. | Use an honest aggregate class rather than false precision. |
| Registration completes after disconnect | A stale acknowledgement could restore acceptance. | Tokenize and fence the operation; test the late-result path. |
| Repeated timers or node monitors duplicate events | Retry storms and noisy logs. | Coalesce timers, enable monitoring once, and ignore stale tokens. |
| Remote diagnostic calls exceed the container health-check budget | Health checks can time out or mistake restart windows for embedded operation. | Use short connection and registration calls and return explicit unavailable evidence. |
| Infrastructure accepts an FQDN that does not route privately | Startup validation passes but connection fails. | Report bounded runtime failure; infrastructure owns DNS and routing. |

## Plan review

| Field | Result |
| --- | --- |
| Reviewer | Independent sub-agent `issue_633_plan_review` |
| Reviewed against | Issue #633, temporary plan, original source, implementation, focused tests, logs, diagnostics, and canonical docs |
| Findings | Concurrency, stale-registration, duplicate-monitoring, retry-counter, redaction, and health-check-budget hardening findings |
| Findings addressed and rechecked | Yes. The reviewer reran focused checks against the corrected implementation. |
| Verdict | PR-ready with no remaining P0-P2 findings; one unused-state P3 cleanup was non-blocking. |

The temporary plan was deliberately removed before PR creation. Because the
permanent-record standard did not yet exist, there is no approved-plan commit.
This record keeps that limitation visible instead of rewriting history.

---

## Implementation outcome

The implementation follows the approved ordering: validate, connect, register,
then accept work. Disconnect reverses availability and a fresh accepted
registration is required for recovery.

```mermaid
stateDiagram-v2
    [*] --> Starting
    Starting --> Connecting: Remote runner starts
    Starting --> Accepting: Embedded runner starts
    Connecting --> Accepting: Connected and registered
    Accepting --> Connecting: Connection lost
    Connecting --> Draining: Shutdown requested
    Accepting --> Draining: Shutdown requested
    Draining --> Stopping: Active work is safe
    Stopping --> [*]
```

### Actual scope and complexity

- Issue implementation: 17 files across runner runtime, tests, deployment
  artifacts, and canonical documentation.
- Process documentation added at the user's request: `AGENTS.md`, documentation
  routing, the change-record README and template, and this record.
- Ownership boundaries affected: runner startup, transport connection,
  registration, lifecycle admission, readiness, operational evidence, and
  deployment generation. Durable orchestrator and storage ownership did not
  change.
- Implementation complexity: **High** — several concurrent state owners, timers,
  disconnect races, readiness composition, redaction, and recovery invariants
  had to agree.
- Operational complexity: **Moderate** — deployment configuration becomes
  stricter and diagnostics improve, but there is no migration, protocol change,
  or new operator service.
- Canonical documentation updated: `docs/production/elastic_runners.md` and
  `docs/structure/favn_runner.md`.

## Deviations from the approved plan

| Planned | Implemented | Reason | Impact | Reviewer verdict |
| --- | --- | --- | --- | --- |
| Distinguish EPMD unavailable from no registered node. | Use aggregate `epmd_probe_failed`. | OTP's public EPMD API returns the same `:noport` result for both cases. | Less specific but truthful diagnostics. | Justified; avoids false precision. |
| Qualify the control-plane destination in generated defaults. | Give Control Plane, View, and Runner consistent FQDN aliases. | Long-name distributed nodes cannot safely mix with short-name nodes. | Broader generated Compose change, no protocol change. | Justified and required for a coherent topology. |
| Keep one coalesced transport timer; registration remains runner-agent state. | Add a separate tokenized registration timer and connection-subscription retry. | Transport may be connected while registration is unavailable, and subscription can initially time out. | More state, but each failure layer has one clear retry owner. | Justified after concurrency review. |
| Make remote diagnostics bounded and require configured connection and registration evidence. | Add short timeouts for connection and registration evidence and derive requiredness from frozen configuration. | Process presence can briefly disappear during one-for-all restart, and default remote calls can exceed the three-second health check. | Remote evidence fails closed quickly; this does not claim a total readiness deadline. | Justified hardening. |
| Log recovery at info. | Log one initial success and one event per recovery. | Initial positive evidence is useful and does not repeat. | One extra bounded startup event. | Justified; low noise. |
| Keep the PR limited to issue #633. | Add the repository change-record standard and this permanent record. | The user explicitly adopted the process after implementation and asked to update this PR. | Six documentation files broaden the review but do not alter runtime behavior. | Accepted as an explicit documentation-only scope addition. |

Review also found implementation defects that were corrected without changing
the approved architecture: stale registration acknowledgements are fenced,
node monitoring is enabled once, duplicate disconnect events are ignored, retry
counters remain truthful, periodic summaries continue after retry 8, underscore
hosts are rejected, runner-pool metadata is sanitized, and remote connection and
registration evidence fails closed within the health-check budget.

## Decision log

| Date | Decision | Reason | Review needed |
| --- | --- | --- | --- |
| 2026-08-21 | Aggregate EPMD probe failures. | OTP cannot expose the planned distinction reliably. | Reviewed and accepted. |
| 2026-08-21 | Use FQDN aliases for every generated BEAM service. | One long-name topology must be internally consistent. | Reviewed and accepted. |
| 2026-08-21 | Separate transport, subscription, and registration retry ownership. | They can fail independently and need coalesced recovery. | Reviewed and accepted. |
| 2026-08-21 | Add the permanent change-record standard and record to PR #656. | User requested adoption before the next push. | Reviewed and accepted. |

## Verification evidence

| Check | Result | Evidence boundary |
| --- | --- | --- |
| Runner compile with warnings as errors | Passed before this documentation update. | Compile qualification, not runtime proof. |
| Five affected runner test files | 66 passed after recompiling final source. | Focused deterministic behavior. |
| Runner fast suite | 238 passed. | Runner regression qualification. |
| Public `favn` fast suite | 183 passed, 3 excluded. | Public facade regression qualification. |
| Deployment artifact acceptance test | 1 passed. | Generated Compose contract. |
| Umbrella compile with warnings as errors | Passed. | Compile qualification across applications. |
| Test-tier guard | Passed. | CI-routing consistency. |
| Scoped diff check | Passed. | Patch formatting only. |
| Independent implementation review | PR-ready; no P0-P2 findings remained. | Plan-to-code and focused-test comparison. |
| Initial PR CI | Mixed: fast tests and image qualification passed; quick, Dialyzer, acceptance, and slow jobs failed. | Stale pre-rebase CI; not final evidence for the rebased head. |

### Not verified

- Post-rebase local checks and CI for the final pushed commit are pending.
- No live two-node TLS distribution test was run.
- Private-DNS routing, certificate SANs, cookie compatibility, and Azure
  deployment were not proven.
- The umbrella fast suite was incomplete in the original shell because
  `FAVN_DATABASE_URL` was absent; one unrelated 100 ms connection-guard
  assertion also missed its message.

## Final review

| Field | Result |
| --- | --- |
| Reviewer | Independent sub-agent `change_record_standard_review` |
| Compared | Approved temporary plan, implementation, tests, diagnostics, canonical docs, and this reconstructed record |
| Deviations complete | Yes. Six material plan or PR-scope deviations are recorded. |
| Findings | One P1, five P2, and one P3 accuracy or precision findings in the reconstructed record |
| Findings addressed and rechecked | Yes. The reviewer rechecked every correction. |
| Verdict | Record approved with no remaining P0-P3 findings. Recheck after the rebase and final code or evidence updates before setting status to `Implemented`. |
