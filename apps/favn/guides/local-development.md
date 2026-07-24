# Local Development Commands

Favn exposes local development through `mix favn.*` tasks.

Use these commands from your application project. Do not depend on Favn runtime,
storage, or UI implementation apps directly.

Docker Engine and Docker Compose v2 are mandatory. Favn supports Linux amd64 and
amd64 WSL2 using Linux containers. Native Windows, arm64, macOS emulation, and
Podman are not supported in v1. Authenticate Docker to the private GHCR package
with a pull-only credential before `mix favn.install`. Run the host Mix commands
with Elixir `1.20.2` on Erlang/OTP `29`.

## First Local Flow

```bash
mix favn.init
mix favn.install
mix favn.dev
```

Open the printed UI URL, usually `http://127.0.0.1:4173`.

Projects that declare the optional `favn_duckdb_adbc` plugin can scaffold its
tested native driver with:

```bash
mix favn.init --include duckdb-adbc
```

In another terminal:

```bash
mix favn.run MyApp.Pipelines.LocalSmoke
mix favn.runs list
mix favn.logs RUN_ID
mix favn.stop
```

## Setup Commands

| Command | Use it for | Common options |
| --- | --- | --- |
| `mix favn.init` | Create the complete customer-owned local Compose and runner scaffold. | `--include duckdb-adbc[@VERSION]` |
| `mix favn.init --duckdb --sample` | Generate local Favn files and a sample DuckDB pipeline. | `--duckdb`, `--sample` |
| `mix favn.init --target compose` | Create a consumer-owned local or single-host Compose starting template. | `--profile local\|single-host`, `--output PATH` |
| `mix favn.init --target runner` | Create only the editable customer-owned runner template under `deploy/runner/`. | `--include duckdb-adbc[@VERSION]`, `--output DIRECTORY` |
| `mix favn.doctor` | Check local configuration before running. | `--skip-compile` |
| `mix favn.install` | Pull and verify the version-matched control-plane image. | `--force`, `--root-dir PATH` |
| `mix favn.dev` | Build the local customer runner by default, align its manifest, and start PostgreSQL, runner, and control plane. | `--runner-image IMAGE`, `--compose-file PATH`, `--scheduler`, `--root-dir PATH` |
| `mix favn.maintainer.dev` | Build or reuse an unpublished control plane from `FAVN_CHECKOUT`, then use the normal automatic or explicit customer runner selection. | `--runner-image IMAGE`, `--compose-file PATH`, `--scheduler`, `--root-dir PATH` |

Runner templates are never overwritten. After a Favn upgrade, use
`mix favn.init --target runner --output deploy/runner-next` to create a
fully rendered comparison, merge or adopt it, and build the result with a new
runner release ID.

`mix favn.dev` stays in the foreground. It prints service logs and URLs. Stop it
with `mix favn.stop` from another terminal or by ending the foreground process.
Install never compiles the control plane or customer runner. On startup,
`mix favn.dev` generates an opaque local release ID and runs `docker build
--pull` against `deploy/runner/Dockerfile` unless an existing runner image was
selected. It then validates and pins the exact local image ID, builds the
aligned manifest, and starts containers with Compose `--no-build`. Use
`mix favn.install --force` to repull and revalidate the version-matched
control-plane tag.

The runner selection order is `--runner-image`, then
`config :favn, :local, runner_image:`, then `FAVN_RUNNER_IMAGE`. When none is
set, Favn builds the generated customer Dockerfile under an automatic
project-scoped local image name.
Control-plane and generated runner releases default to `info` logging. Set
`FAVN_LOG_LEVEL=debug` only while diagnosing a problem; the Compose templates
pass this setting to both releases. Supported values are `debug`, `info`,
`notice`, `warning`, `error`, `critical`, `alert`, and `emergency`; any other
value stops release startup safely.
The Compose selection order is `mix favn.dev --compose-file PATH`, then
`config :favn, :local, compose_file: PATH`, then
`deploy/local/compose.yml`. A successful start records the canonical path for
reload, stop, status, logs, and diagnostics. Extra unlabeled services and normal
Compose settings are allowed; the versioned Favn role labels must remain unique
and complete.

Before startup, `mix favn.dev` loads the project `.env` and starts a fresh Mix
process that evaluates `config/runtime.exs`. This means runtime config can branch
on env-file values for host-side tooling and manifest compilation. Existing
shell values take precedence. `mix favn.reload`, `mix favn.inspect`, and
`mix favn.query` repeat that host bootstrap. It does not modify the immutable
runner image: rebuild and select a new image and release ID for
`config/runtime.exs` changes that affect runner behavior. Environment-value-only
changes may recreate the existing selected image. The inspection commands start
only `:favn_sql_runtime`, not the consumer application or its plugins.

## What Favn Reads And Writes

The customer Dockerfile decides which project files, dependencies, native
libraries, and configuration enter the runner. Favn supplies local build
identity and invokes Docker; it does not reinterpret the Dockerfile, construct a
production build context, or publish the image.

Local lifecycle commands inspect the selected image's platform and Favn labels,
read the explicit `.env`, runtime config, and selected Compose file, compile the
manifest, and operate only the labeled Favn roles. Generated selection,
credential, manifest, and lifecycle state stays below `.favn/`. Customer
Dockerfiles, images, Compose services, and registry credentials remain
customer-owned.

## Testing A Local Favn Checkout

This is an explicit, non-production workflow for a Favn maintainer testing a
framework change in a real consumer project. Keep an approved release checkout
as the normal default and let `FAVN_CHECKOUT` replace it only when needed:

```elixir
defp favn_root do
  System.get_env("FAVN_CHECKOUT", "../favn-release")
  |> Path.expand(__DIR__)
end

defp deps do
  [
    {:favn, path: Path.join(favn_root(), "apps/favn")},
    {:favn_duckdb, path: Path.join(favn_root(), "apps/favn_duckdb")}
  ]
end
```

The default checkout should be clean and detached at the approved release tag
or commit. `FAVN_CHECKOUT` changes the dependency specification returned by
`mix.exs`; it does not modify that file. Because Mix resolves dependencies
before Favn can load the project's `.env`, put the override in the shell or use
direnv. For example, add `.env.local` to the consumer project's `.gitignore` and
store the uncommitted override there:

```bash
FAVN_CHECKOUT=/home/me/code/favn
```

Then load it from a committed `.envrc`:

```bash
dotenv_if_exists .env.local
```

After `direnv allow`, run:

```bash
mix deps.get
mix favn.maintainer.dev
```

The command verifies that all loaded Favn path dependencies come from the same
checkout. It builds or reuses that checkout's local control-plane image and
starts the normal local stack. The runner follows the ordinary rule: use
`--runner-image` or local configuration when supplied; otherwise build the
customer Dockerfile automatically. For that automatic maintainer build, Favn
materializes the validated Favn source selection as a separate Docker build
context without widening the customer project's normal build context.

This explicit maintainer workflow accepts a branch or deliberate uncommitted
framework changes and reports the exact checkout state. A View, Orchestrator,
Storage, or Core change may require a new control-plane candidate; a
runner-facing framework or customer change requires a matching customer image
and release ID. Use `--runner-image` only when you want to bypass the automatic
customer Dockerfile build.

Run `mix favn.install` to switch the project back to its version-matched official
control-plane image. Remove `FAVN_CHECKOUT` or leave the directory to switch Mix
dependencies back to the approved release checkout.

## Runtime Commands

### Run Work

```bash
mix favn.run MyApp.Pipelines.LocalSmoke
mix favn.run MyApp.Pipelines.LocalSmoke --no-wait
mix favn.run MyApp.Assets.MonthlyOrders --window month:2026-01
mix favn.run MyApp.Source.Events:movement --window month:2026-07 \
  --dependencies none --refresh force_selected
```

Options:

| Option | Meaning |
| --- | --- |
| `--window VALUE` | Window request such as `month:2026-01`. |
| `--timezone TZ` | Timezone for `--window`. |
| `--dependencies all\|none` | Asset planning scope. Defaults to `all`. Asset-only. |
| `--refresh MODE` | Refresh behavior. Defaults to `auto`. |
| `--idempotency-key KEY` | Reuse key for safe command retry. |
| `--wait` / `--no-wait` | Wait for completion or return after submission. Wait is default. |
| `--timeout-ms N` | Alias for wait/run timeout when specific values are absent. |
| `--wait-timeout-ms N` | Local polling timeout. Default is 60 seconds. |
| `--run-timeout-ms N` | Runtime execution timeout. |
| `--poll-interval-ms N` | Poll interval. Default is 1 second. |

Asset refresh modes are `auto`, `missing`, `force_selected`,
`force_selected_upstream`, and `force_all`. Pipeline targets do not accept
`--dependencies` and support only `auto`, `missing`, and `force_all` refresh.
`force_selected_upstream` requires dependency scope `all`.

Use `--dependencies none --refresh force_selected` for a targeted repair only
after confirming the selected asset's upstream inputs are suitable. Dependency
scope chooses which nodes are planned; refresh chooses how freshness is applied
inside that plan. Omitting both options keeps the safe `all` plus `auto`
defaults.

### List, Show, And Cancel Runs

```bash
mix favn.runs list
mix favn.runs list --status error --limit 20
mix favn.runs show RUN_ID
mix favn.runs cancel RUN_ID --wait
```

List options: `--status`, `--limit`, `--root-dir`.

Cancel options: `--wait`, `--timeout-ms`, `--wait-timeout-ms`,
`--poll-interval-ms`, `--root-dir`.

### Read Logs

Service logs:

```bash
mix favn.logs
mix favn.logs --service runner --tail 200
mix favn.logs --service all --follow
```

Run events:

```bash
mix favn.logs RUN_ID
mix favn.logs RUN_ID --tail 200
```

Options:

| Option | Meaning |
| --- | --- |
| `--service postgres|control-plane|runner|all` | Select service logs. |
| `--tail N` | Number of lines or events. Default is 100. |
| `--follow` | Follow service logs. Cannot be used with `RUN_ID`. |

### Inspect Relations And Query SQL

```bash
mix favn.inspect relation raw.sales.orders --connection important_lakehouse
mix favn.inspect partitions raw.sales.orders --connection important_lakehouse
mix favn.query "select * from mart.sales.order_summary" --connection important_lakehouse
```

`mix favn.inspect` relation forms:

- `name`
- `schema.name`
- `catalog.schema.name`

`mix favn.query` options:

| Option | Meaning |
| --- | --- |
| `--connection NAME` | Required when more than one SQL connection exists. |
| `--limit N` | Display row limit. Default is 50. |
| `--allow-write` | Allow local mutation. Without it, Favn applies a best-effort read-only guardrail. |

The read-only guardrail is not a security sandbox. It is a local safety check.

## Status, Diagnostics, Reload, Stop, Reset

```bash
mix favn.status
mix favn.diagnostics
mix favn.diagnostics --json
mix favn.reload
mix favn.stop
mix favn.reset
```

| Command | Use it for |
| --- | --- |
| `mix favn.status` | Show whether the local stack is running, stopped, partial, stale, or unknown. |
| `mix favn.diagnostics` | Show storage, scheduler, runner, and recent failure checks. |
| `mix favn.reload` | Publish a manifest change, or drain and select a newly customer-built runner image. |
| `mix favn.stop` | Stop the local stack. |
| `mix favn.reset` | Print the project-scoped deletion plan and refuse to mutate without `--yes`. |

`mix favn.stop` stops only the recorded Favn roles and preserves containers,
PostgreSQL state, consumer services, networks, volumes, and images. If a crash
occurred before runtime state was recorded, it discovers only contract-labeled
Favn containers in the derived Compose project and stops those roles.
`mix favn.reset --yes` removes generated `.favn/` state but preserves `.data`
after proving the known Favn roles are stopped.
It fails closed when partial-start roles cannot be inspected and preserves a
selected Compose template even when it is below `.favn/`. It does not run
`docker compose down` or delete the consumer Compose file, containers, networks,
volumes, services, data, runner images, or the official control-plane image.

## Backfills

Backfills are advanced local/operator workflows.

```bash
mix favn.backfill submit MyApp.Pipelines.Daily --from 2026-04-01 --to 2026-04-07 --kind day
mix favn.backfill submit MyApp.Pipelines.Daily --window day:2026-04-01..2026-04-07 --dry-run
mix favn.backfill missing-plan MyApp.Assets.Orders --plan-file coverage-plan.json
mix favn.backfill missing-submit MyApp.Assets.Orders --plan-file coverage-plan.json
mix favn.backfill windows RUN_ID
mix favn.backfill rerun-window RUN_ID --window-key day:2026-04-01
mix favn.backfill repair --all --apply
```

Common submit options:

| Option | Meaning |
| --- | --- |
| `--from`, `--to`, `--kind` | Explicit range input. |
| `--window KIND:FROM..TO` | Compact range input. Cannot be combined with `--from`, `--to`, or `--kind`. |
| `--timezone TZ` | Defaults to `Etc/UTC`. |
| `--dry-run` | Plan without creating runs. |
| `--refresh force` | Recompute selected windows. |
| `--wait` / `--no-wait` | Wait is default. |

Missing-window repair has a separate review and submit workflow. `missing-plan`
evaluates the active asset generation, prints every exact selected window and
the coverage checksum, and optionally writes the complete immutable plan to
`--plan-file`. `missing-submit` requires that file, prints the same selection
again, revalidates its manifest, generation, evaluation, and window keys, and
then submits it. A stale plan is rejected; the command never silently fills a
different gap. Use `--limit 1..500` and, when shown by an operator surface,
`--cursor CURSOR` to plan one bounded page instead of all missing windows.

### Rebuild An Incompatible Asset

An incompatible persisted SQL target blocks ordinary writes but leaves its
active generation readable. Rebuild it with a separate plan and approval:

```bash
mix favn.rebuild plan MyApp.Assets.Orders --reason "contract changed"
# Review the target, action/item counts, expiry, and complete plan in the UI.
mix favn.rebuild start PLAN_ID --plan-hash PLAN_HASH
mix favn.rebuild status OPERATION_ID
```

The plan pins the active manifest, target generations, physical inspections,
runtime-input expectations, downstream actions, and exact logical work items.
Each `plan` invocation creates a fresh expiring plan, even when target and reason
are unchanged; rerun it after expiry instead of replaying an old plan identity.
`start` revalidates those inputs and rejects a stale plan rather than rebuilding
something different. Use `mix favn.rebuild cancel OPERATION_ID --reason
"operator request"` to request safe cancellation. A failed operation can use
`mix favn.rebuild retry OPERATION_ID` only when the server proves retry is safe.
Use `mix favn.rebuild reconcile OPERATION_ID` when activation has an unknown
outcome; do not start a replacement rebuild while the marker is unresolved.

The authenticated UI at `/rebuilds` provides the same plan/review/start flow,
progress and item pages, and server-authorized cancel, retry, and reconcile
actions. Rebuild commands require the running local stack and use its private
orchestrator HTTP boundary; they never connect to PostgreSQL directly.

## Packaging Commands

These are deployment/operator commands, not needed for the first local run:

| Command | Use it for |
| --- | --- |
| `mix favn.init --target runner` | Create an editable starting Dockerfile for the customer runner. |
| `mix favn.build.manifest --runner-release-id ID` | Build a manifest bound to the operator-supplied runner release ID. |
| `mix favn.publish --manifest PATH` | Upload missing execution packages and stage the immutable manifest. |
| `mix favn.activate --manifest-version ID --workspace-id ID` | Activate an exact staged manifest after runner verification. |

`publish` and `activate` use `--orchestrator-url` or
`FAVN_ORCHESTRATOR_URL`. Set `FAVN_ORCHESTRATOR_SERVICE_TOKEN` in the process
environment; service tokens are deliberately rejected as command-line flags.

Manifest output lives at `.favn/dist/manifest/<manifest_version_id>/`.
The customer owns the runner Dockerfile and decides how to build and push it.
Use a new `rr_` ID for every executable image change. Favn validates exact
runner/manifest alignment but does not prove that an ID represents particular
source. See the production
[runner release guide](../../../docs/production/runner_releases.md).

## What You Should See In The UI

After `mix favn.dev`, the UI should open at the printed web URL. It should show
the local Favn operator view. After `mix favn.run`, you should see recent run
state move from pending/running to a final state such as ok or error.

Do not rely on UI loading state as final truth. Use `mix favn.runs`,
`mix favn.logs`, and `mix favn.diagnostics` when debugging.
