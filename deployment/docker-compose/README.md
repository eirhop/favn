# Docker Compose elastic-runner qualification

This example proves Favn's production deployment contracts on one Docker host:

- PostgreSQL 18 uses a separate migrator and least-privilege runtime role over
  verified TLS;
- one immutable control-plane image starts separate View and Orchestrator roles
  and remains ready with zero runners;
- the CRM tutorial is packaged as an immutable customer runner release;
- manifest publication and activation use the same exact runner release ID;
- a bounded local scaler reads authenticated demand and starts one-slot,
  one-shot runner containers; and
- three staggered assets make the live runner count observable as
  `0 -> 3 -> 2 -> 1 -> 0`.

This is a qualification harness, not a production platform template. The
generated certificates expire after seven days, Caddy uses a disposable local
CA, credentials live in an ignored local file, and the scaler has
root-equivalent access to the local Docker daemon. Kubernetes/KEDA, Azure
Container Apps, ECS, Nomad, or another production scheduler must implement the
same [provider-neutral demand contract](../README.md).

For sustained PostgreSQL load and crash recovery, use the longer
[issue #523 qualification](../../docs/production/issue_523_acceptance_matrix.md).
That test builds on this smoke drill and records structured evidence rather
than replacing it.

## Prerequisites

- Docker Engine or Docker Desktop with Linux containers and Compose;
- a POSIX-compatible shell; and
- free loopback ports `4173`, `4101`, and `4443`.

The first build downloads base images and Hex dependencies and can take several
minutes. Run every command from this directory.

## Run the complete drill

```sh
sh ./prepare.sh
sh ./run-simulation.sh
```

`prepare.sh` uses a short-lived Alpine container to create `.env.local` with
disposable random credentials. The image tag, runner release ID, image-build
project, and build timestamp are deterministic for the checked-out revision, so
repeating the drill does not create another set of images or invalidate compiled
layers. It refuses dirty checkouts and refuses to overwrite the file unless
`--force` is explicit.

The build runs through the dedicated `favn-qualification-v1` Buildx builder.
Its cache is reusable but capped at 12 GB and configured to preserve at least
20 GB of free disk space. This keeps qualification cache policy separate from
Docker's shared default builder. The wrappers disable default provenance
attestations through the Compose flag when available and the equivalent Buildx
environment setting on older Compose releases.

`run-simulation.sh` then:

1. builds the certificate, PostgreSQL, control-plane, operator, and runner
   images;
2. generates a local CA and separate PostgreSQL, Orchestrator, View, and runner
   certificates in isolated volumes so no service receives another service's
   private key;
3. starts PostgreSQL and runs the packaged one-command database bootstrap, which
   creates and hardens the migrator/runtime roles, migrates, provisions the
   `elastic-simulation` workspace with its explicitly configured initial
   administrator, and verifies through the runtime role;
4. starts the Orchestrator and View roles with zero runner capacity;
5. publishes the exact manifest/release pair at zero runner capacity;
6. activates it with a one-runner bootstrap scaler for target compatibility
   inspection, then returns to zero runners;
7. submits the 20, 40, and 60-second probe assets as three independent runs
   while capacity is zero, then starts the three-runner local scaler; and
8. requires successful workload completion, runner exit code zero, and an
   observed `0 -> 3 -> 2 -> 1 -> 0` runner sequence.

The scaler writes `simulation-results/simulation-timeline.jsonl` without tokens
or other secrets. PostgreSQL, the control plane, and exited runner containers
remain available after success for inspection:

```sh
docker compose --env-file .env.local --project-name favn-elastic-simulation -f compose.yml ps --all
docker compose --env-file .env.local --project-name favn-elastic-simulation -f compose.yml logs control-plane
docker compose --env-file .env.local --project-name favn-elastic-simulation -f compose.yml logs runner
```

The direct View listener is reachable only on loopback at
`http://127.0.0.1:4173`. The optional proxy-security profile exposes the real
HTTPS edge on `https://127.0.0.1:4443` with the expected host
`favn.localhost`. Its certificate chains to a disposable local CA and is not
for browser use.

## Run the trusted-proxy security check

```sh
sh ./run-trusted-proxy-security.sh
```

This focused, production-image check starts real Caddy and gives it the exact
trusted address `172.31.58.2/32`. A second container on the same network has a
different address and acts as an untrusted peer. The check records stable
assertions in `proxy-security-results/trusted-proxy-security.json`:

- `TP-001`: HTTPS through the proxy reaches View without a redirect loop.
- `TP-002`: a one-shot upstream receiver captures the actual proxy request and
  proves that Caddy removed forged forwarding identity and wrote its own.
- `TP-003`: a non-proxy peer cannot use forwarding headers to make direct
  plaintext HTTP appear HTTPS.
- `TP-004`: the redirect authority remains the configured public origin, not
  a forged host.

The application tests add bounded malformed-chain cases and verify that
standard `Forwarded` plus every consumed `X-Forwarded-*` header is removed.
Run `cleanup.sh` before or after this check when the named Compose project
already contains containers.

## How scaling is simulated

The demand endpoint includes queued and active runner tasks. A KEDA ScaledJob's
default strategy subtracts already-running Jobs before creating more. The local
Linux scaler container mirrors that calculation:

```text
new runners = min(max runners - running runners, outstanding - running runners)
```

It never stops a runner. Each runner claims one task at a time and exits only
after the orchestrator returns no work for the configured idle grace. This is
why `docker compose up --scale runner=...` is not used: reducing a Compose
service replica count may stop arbitrary busy containers and would test a
different lifecycle.

The scaler is deliberately limited to three runners, a four-minute drill, a
five-second API timeout, and one exact `{default, runner_release_id}` partition.
It mounts `/var/run/docker.sock`, which grants root-equivalent authority over
the local Docker host. Use it only as this disposable local qualification
harness; never deploy this scaler as a production service.

## Manual operation

For debugging, the same phases can be invoked separately:

```sh
sh ./ensure-image-builder.sh favn-qualification-v1
docker compose --env-file .env.local --project-name favn-qualification-images -f compose.yml \
  build --builder favn-qualification-v1 --provenance=false \
  certificates postgres control-plane operator runner scaler
docker compose --env-file .env.local --project-name favn-elastic-simulation -f compose.yml up certificates
docker compose --env-file .env.local --project-name favn-elastic-simulation -f compose.yml up -d postgres
docker compose --env-file .env.local --project-name favn-elastic-simulation -f compose.yml run --rm database-bootstrap
docker compose --env-file .env.local --project-name favn-elastic-simulation -f compose.yml up -d control-plane
docker compose --env-file .env.local --project-name favn-elastic-simulation -f compose.yml run --rm operator publish
```

Activation of persisted SQL targets requires temporary runner capacity for
compatibility inspection. `run-simulation.sh` supplies that with a bounded
one-runner scaler and waits for both activation and runner exit.

After activation returns to zero runners, submit all three workload runs
without capacity:

```sh
for workload in fast medium slow; do
  docker compose --env-file .env.local --project-name favn-elastic-simulation -f compose.yml \
    run -d --no-deps operator "run-$workload"
done
```

Then start the scaler:

```sh
docker compose --env-file .env.local --project-name favn-elastic-simulation -f compose.yml up scaler
```

## Cleanup

```sh
sh ./cleanup.sh
rm -- .env.local
```

`cleanup.sh` removes the explicitly named Compose project's containers, network,
PostgreSQL data volume, generated-certificate volume, diagnostic images, and
obsolete revision-addressed qualification tags. Images for the current clean
revision and the three newest clean revisions per repository remain available
for reuse and concurrent runs. The credential file is removed separately so
deletion remains visible.

The harness never prunes Docker's shared default builder or unrelated images.
The dedicated builder applies its own bounded garbage collection automatically.

## Sustained PostgreSQL qualification

Start the default four-hour API load test after reviewing its
[acceptance matrix](../../docs/production/issue_523_acceptance_matrix.md):

```sh
sh ./run-qualification.sh
```

The POSIX wrapper runs this complete smoke drill first. It then starts a
detached qualification controller inside the scaler image and prints the run
ID, controller ID, and evidence directory. The controller owns API load,
sampling, recovery after a killed runner, control-plane and PostgreSQL crash
recovery, drain, log collection, redaction checks, and final assertions.
The default verdict also enforces the documented API p95, queue age/depth,
submission-rate, sample-completeness, and busy-runner recovery budgets.
Every disposable credential generated by `prepare.sh` is included in the
evidence leak scan.

Check it without interrupting it:

```sh
sh ./qualification-status.sh
```

The long test remains a single-host qualification. Managed-service backup,
PITR, failover, alerts, and multi-week operation are deliberately deferred to
the target work test platform.

## What this does not prove

Use this example to find packaging, configuration, publication, durable-queue,
registration, release-matching, demand, and self-exit defects. It does not
qualify:

- managed PostgreSQL backup, failover, or point-in-time recovery;
- KEDA or any managed provider's polling and job-accounting behavior;
- private cloud networking, registry policy, secret stores, or certificate
  rotation;
- browser sessions or managed-platform ingress behavior; or
- DuckDB data-plane durability across disposable runner containers.

Those remain target-environment tests. Do not use local success as a production
readiness claim.
