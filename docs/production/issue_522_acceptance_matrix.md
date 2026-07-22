# Issue 522 Production Acceptance Matrix

The executable container suites are:

- `apps/favn_local/test/acceptance/local_compose_acceptance_test.exs`;
- `apps/favn_local/test/acceptance/local_compose_execution_acceptance_test.exs`.

Run both through `mix test.container` with
`FAVN_CONTROL_PLANE_CANDIDATE` set to the exact repository-built candidate
image. The suites require Docker Engine, Compose v2, and PostgreSQL 18. They do
not substitute in-memory or SQLite persistence.

| Contract | Executable evidence |
| --- | --- |
| Artifact truthfulness | Builds and starts the minimal control-plane release and customer runner release without source or Mix in either final image. |
| Compose isolation | Runs PostgreSQL, control plane, and runner on one private project network; only View and private API ports bind to loopback. |
| Release-safe storage | Runs migrate, grant, schema verification, and workspace provisioning as one-shot control-plane release operations before startup. |
| Runner alignment | Verifies the baked runner descriptor, publishes an aligned manifest, rejects forged mismatches, and activates only the exact release. |
| Product execution | Authenticates through View and executes one SQL asset plus one Elixir asset across the control-plane/runner boundary. |
| Manifest-only update | Publishes and activates a SQL-only change without changing the runner image or restarting either runtime service. |
| Durable restart | Restarts control plane and runner independently and proves persisted deployment and run state remain usable. |
| Shutdown honesty | Exercises idle and active SIGTERM, bounded drain, cancellation, abrupt loss, and recovery without invented success. |
| Manual rotation | Rotates service tokens, runtime-input keys, and the View session key through controlled container recreation. |
| Upgrade/rollback | Qualifies a compatible control-plane upgrade and rollback against the preserved PostgreSQL authority. |
| Security | Checks non-root/read-only containers, secret redaction, private BEAM/PostgreSQL ports, immutable image identity, and the absence of unexpected applications. |

The single control-plane and single runner topology is the first supported
scale point, not a single-BEAM runtime. Multi-node coordination remains a
separate scope.
