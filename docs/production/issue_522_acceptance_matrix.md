# Issue 522 Production Acceptance Matrix

The production container gate runs
`apps/favn_local/test/acceptance/user_owned_runner_container_test.exs` through
`mix test.container`. Set `FAVN_CONTROL_PLANE_CANDIDATE` to the exact
repository-built control-plane candidate. The suite requires Docker Engine,
Compose v2, and PostgreSQL 18.

The test acts as a customer rather than calling a Favn runner build API:

1. it scaffolds the editable runner Dockerfile in a fixture project;
2. it builds that Dockerfile directly with an explicit runner release ID;
3. it gives the resulting external image to Favn local tooling;
4. it starts PostgreSQL, the supplied control-plane candidate, and the customer
   runner as separate containers;
5. it builds and activates a manifest bound to the same release ID;
6. it stops and restarts the stack using the same exact runner image ID.

| Contract | Executable evidence |
| --- | --- |
| Customer ownership | The test invokes Docker directly. Product code only inspects and selects the finished customer image. |
| Artifact truthfulness | The generated customer Dockerfile produces a non-root runner release with immutable Favn compatibility labels. |
| Control-plane separation | The candidate starts without runner or Mix code in its final image. |
| Runner isolation | Customer modules are packaged and loaded, while the customer OTP application is not started automatically. Runner plugins remain the explicit service-start boundary. |
| Compose topology | PostgreSQL, control plane, and runner become healthy as separate services on the project-scoped Compose deployment. |
| Runner alignment | The inspected image release ID matches the activated manifest and the runtime-reported runner identity. |
| Immutable reuse | Stop/start reuses the recorded exact runner image ID and manifest version. Compose lifecycle commands use `--no-build`. |

Focused non-container tests cover malformed image metadata, unsupported targets,
manifest/release mismatch, reload classification, recovery state, Compose role
validation, and scaffold pre-VM input validation. The acceptance slice also
builds a manifest from the same operator-owned release ID used by the scaffold.

This gate deliberately does not claim to qualify every possible customer
Dockerfile, native dependency, plugin, or managed PostgreSQL deployment. Those
artifacts remain operator-owned. The repository-owned gate proves Favn's public
boundary against one representative customer build.

Fast CI also runs the warning-grade Credo baseline, whole-umbrella Dialyzer,
strict Sobelow scans for both Phoenix boundaries, dependency audits, and the
repository test-tag guard. All slices feed the stable required `CI / CI`
aggregate.

The single control-plane and single runner topology is the first supported
scale point, not a single-BEAM runtime. Multi-node coordination remains a
separate scope.
