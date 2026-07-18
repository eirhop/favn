# PostgreSQL Single-Node Acceptance Matrix

Executable suite:
`apps/favn_local/test/acceptance/single_node_production_acceptance_test.exs`.

The suite uses a live PostgreSQL database and a unique provisioned workspace per
scenario. It never substitutes memory or SQLite persistence.

| Contract | Evidence |
| --- | --- |
| Build truthfulness | Generates the documented project-local PostgreSQL launcher and immutable metadata/scripts. |
| Configuration fail-closed | Missing storage, database URL, encryption key, or required auth values fail before readiness; SQLite is rejected. |
| Workspace isolation | Every runtime is bound to an explicitly provisioned unique workspace and bootstrap authenticates in that workspace. |
| Schema ownership | Migrations run as a separate precondition; runtime startup only validates readiness. |
| Bootstrap idempotency | Repeating manifest publication, deployment activation, and runner registration produces the same logical result. |
| Command idempotency | Repeating run submission with the same key returns one run. |
| Product path | An authenticated, manifest-pinned pipeline executes through the private API and reaches a terminal successful state. |
| Durable restart | Active deployment and completed run remain readable after stopping and restarting the BEAM node. |
| Secret failure | A missing runtime value fails before user asset execution and does not expose other configured secrets. |
| Artifact immutability | Start, duplicate start, stop, and restart do not mutate `dist_dir`; runtime files stay under `FAVN_SINGLE_NODE_HOME`. |

The acceptance suite does not replace the PostgreSQL store tests for competing
connections, fencing, query plans, privileges, restore drills, multi-node
coordination, or negative cross-workspace access. Those live under
`apps/favn_storage_postgres/test/storage_v2/` and the production gates in the
Storage V2 architecture document.
