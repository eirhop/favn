# Local registration recovery qualification

Run the issue #763 workload manually on OrbStack: a 0.5-CPU/1-GiB orchestrator,
five one-slot runners, PostgreSQL, a production View and a database fault proxy.
The CRM tutorial is extended with 35 independent SQL table assets. Each writes
1,000 rows without artificial sleeps. This adds no CI simulation job.

Read the [change record](../../../docs/change-records/2026/issue-763-pr-766-initial-registration.md)
for invariants and acceptance criteria. Successful data writes, unresolved writes,
and registration failures are separate outcomes. A terminal error is not by itself
proof of the missing-marker defect.

## Prepare

Use the existing [Compose prerequisites and credential preparation](../README.md).
Only one case may use the fixed loopback ports and proxy subnet at a time. Keep
`.env.local` private; use distinct API/View ports if the umbrella development
server is running. The investigation used API `4102`, direct View `4174`, HTTPS
`4443` and the Toxiproxy control port `8476`.

From the repository root:

```sh
python3 deployment/docker-compose/registration-stress/prepare.py --revision HEAD
docker --context orbstack compose \
  --env-file deployment/docker-compose/.env.local \
  --env-file .favn/registration-stress/build.env \
  --project-name favn-763-images \
  -f deployment/docker-compose/compose.yml \
  -f deployment/docker-compose/registration-stress/compose.yml \
  build --builder favn-qualification-v1 --provenance=false certificates postgres operator runner
python3 deployment/docker-compose/registration-stress/control.py --project favn-763-example up
```

Preparation archives an exact source commit and records its fixture hash and
runner-release identity in `.favn/registration-stress/build.json`. It refuses to
overwrite a source snapshot. The default control image is the digest-pinned rc19
release. The profile uses shared DuckLake data and initializes catalogs serially.
Both images are amd64; on an ARM host the local `+JMsingle true` setting permits
emulation. Report emulation and enabled image health probes with all CPU results.
These are comparative local measurements, not native production capacity claims.

The View is reachable through `https://favn.localhost:4443/runners`. Handle its
local certificate warning personally and use an authorized simulation account.
Keep the authenticated page open and verify its live connection before labeling
a case as including browser subscription load. Merely starting View is insufficient.

## Capture a case

Use unique evidence files and key prefixes. The driver records intent before each
request and tracks the API's returned run IDs; stored idempotency keys are hashed.
An uncertain response stops submission. Resolve the original key and exact body
before any retry; do not hide an uncertain request behind a new key.

```sh
python3 deployment/docker-compose/registration-stress/control.py --project favn-763-example \
  observe --seconds 300 --interval 5 --output .favn/registration-stress/evidence/example-timeline.jsonl
```

Run the observer in a separate terminal, then submit a bounded backlog:

```sh
python3 deployment/docker-compose/registration-stress/load.py --project favn-763-example \
  --runs 10 --max-in-flight 2 --timeout 900 --key-prefix example-short-assets \
  --output .favn/registration-stress/evidence/example-load.jsonl
```

This is a bounded-backlog workload, not fixed-rate arrival traffic. A fresh case
exercises initial registration; repeated runs exercise already-active targets.
Snapshots distinguish durable open-session rows, live registry presence and
container state. Compare CPU counters only within the same container ID/start
segment. Observer API reads also contribute load; use identical instrumentation
for before/after comparisons. Do not compile images during a measurement window.

Add delay in each direction, then explicitly clear it:

```sh
python3 deployment/docker-compose/registration-stress/control.py --project favn-763-example latency --ms 10 --jitter 3
python3 deployment/docker-compose/registration-stress/control.py --project favn-763-example clear-faults
```

For an outage, arm this command **before** submitting the first run in a fresh
case. It waits for a durable successful materialization with a building generation
and no marker task, records the evidence, then disconnects only the orchestrator's
control-database traffic. Runner data/catalog connections and evidence reads remain
available. It restores the proxy on ordinary completion or termination; inspect
and clear faults explicitly after host shutdown or forced process termination.

```sh
python3 deployment/docker-compose/registration-stress/control.py --project favn-763-example \
  outage-after-receipt --next-run --phase materialized --seconds 90 --timeout 300 \
  --output .favn/registration-stress/evidence/example-outage.jsonl
```

For a known run use `--run-id` instead. `--phase receipt` targets the earlier gap
between the durable asset receipt and materialization settlement. No qualifying
receipt means no fault. Audit physical data with the same Compose arguments and
`run --rm --no-deps catalog-tool audit`; each table should have 1,000 rows, 1,000
distinct IDs and sum 499,500. Data existence alone cannot resolve an unknown write.

## Compare a candidate and retain failures

```sh
python3 deployment/docker-compose/registration-stress/build-control.py --revision HEAD
```

Use its printed immutable local image tag through `FAVN_STRESS_CONTROL_IMAGE` when
starting a fresh case. Keep the runner/operator images, fixture, quotas, network
profile, browser state and measurement settings fixed. This is appropriate for
control-plane-only changes; changes to runner contracts need matched runner builds.
The builder records source revision, Dockerfile hash, local adjustment and image ID.

Stop all profiles with the same Compose arguments plus `--profile '*'` and
`down --remove-orphans`. Do not add `--volumes`: retain failed case data and raw
evidence for upgrade/repair qualification. Full `up` initializes a fresh case;
resume retained cases with explicit Compose service starts rather than re-running
publication, activation or workloads. Never use global Docker cleanup for this drill.
