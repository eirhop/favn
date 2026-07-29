# Issue 522 Production Acceptance Matrix

Production qualification is separate from source development. No development
task builds or starts deployment images.

| Contract | Repository evidence | Target-environment evidence |
| --- | --- | --- |
| Direct control-plane build | `docker build -f rel/control_plane/Dockerfile .` | Pull and start the exact published digest. |
| Control-plane contents | `scripts/control_plane_image_contract.sh IMAGE` verifies non-root release contents and excludes runner/Mix/local apps. | Platform health and shutdown drill. |
| Customer runner ownership | `mix favn.init --target deployment` copies a non-overwriting example. | Customer CI builds, scans, signs, and publishes its runner digest. |
| Runner/manifest alignment | Manifest-builder tests require one exact release ID per effective logical pool. | Registered runners advertise the exact pool/release pairs required by the manifest. |
| PostgreSQL ownership | Templates contain no PostgreSQL service and startup only verifies schema/workspace. | Managed PostgreSQL migration, grants, restore, TLS, load, and failover evidence. |
| Environment-only secrets | Runtime config tests reject missing/invalid values; images contain no secret defaults. | Platform secret references populate process environment and rotation is drilled. |
| One control plane plus elastic runners | Registration, durable claims, capacity demand, self-exit, and release-overlap contracts are tested. | Private-network connectivity, scale 0-N-0, representative runs, upgrade, and rollback. |

The repository gate cannot qualify every customer plugin, native library,
registry, network, or managed database. Those are customer deployment inputs
and require target-environment evidence.
