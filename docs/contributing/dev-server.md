# Running the Development Server

This is the canonical page for starting a Favn Phoenix server while working on
the repository itself. It covers two different servers that are easy to confuse.

| Server | Started from | Use it for |
| --- | --- | --- |
| Umbrella development server | the repository root | building UI: the design system, Tidewave, LiveDashboard, code reloading |
| `mix favn.dev` | a Favn *project* root, for example `examples/basic-workflow-tutorial` | verifying the UI against a real workspace, manifest, runner, and data plane |

The design system and Tidewave exist **only** on the umbrella development server.
The design system is compiled from `apps/favn_view/dev/`, which is excluded from
the release build, and Tidewave is a development dependency, so a separate project
that merely depends on `favn` receives neither. Use the example project to confirm
the UI behaves and looks right in practice; use the umbrella server to build it.

## Umbrella development server

```bash
FAVN_DATABASE_URL=<local-postgres-url> \
FAVN_DATABASE_MIGRATOR_URL=<local-postgres-migrator-url> \
FAVN_RUNTIME_INPUT_PIN_KEY=<32-byte-key> \
FAVN_RUNNER_BUILD_PROFILE=source \
FAVN_RUNNER_RELEASE_ID=rr_0000000000000000000000000000000000000000000000000000000000000001 \
mix phx.server
```

PostgreSQL must already be running and migrated. Run
`mix favn.postgres.upgrade` first if the schema is behind.

Once it is listening:

| Path | Purpose |
| --- | --- |
| `/` | the operator UI, on `FAVN_VIEW_PORT` (default `4173`) |
| `/design-system` | the component catalogue and its contract |
| `/design-system/render?id=…` | the components a query selects, and nothing else |
| `/tidewave/mcp` | the Tidewave MCP endpoint |
| `/dev/dashboard` | Phoenix LiveDashboard |

### Why the runner variables are required

`mix phx.server` from the umbrella root starts **every** application in the
umbrella, including `favn_runner`. A runner refuses to boot without a validated
release identity and has no development escape hatch, so without these two
variables the whole node dies at startup:

```
** (Mix) Could not start application favn_runner: ...
   ** (ArgumentError) runner release verification failed: :runner_release_id_missing
```

`FAVN_RUNNER_RELEASE_ID` must be `rr_` plus 64 lowercase hex characters; the
value above is a placeholder. `FAVN_RUNNER_BUILD_PROFILE=source` allows the
non-`linux/amd64` targets a workstation actually has. See
`FavnRunner.ReleaseVerifier`.

This node is for UI work only. Its scheduler and its private orchestrator API
listener are both off, and its runner shares the operator BEAM instead of being a
separate one. Do not use it to execute pipeline work — use `mix favn.dev` in a
project for that.

### Do not start from `apps/favn_view`

`favn_view` does not depend on `favn_storage_postgres`, so the orchestrator has
no persistence backend and the node dies with `persistence backend is
unavailable`. The umbrella root is the only working root.

### Slow requests on a bind mount

The Phoenix code reloader recompiles every umbrella app on each request. On a
native filesystem that sweep is milliseconds; through a dev-container or network
bind mount it is seconds per page. Scope it to the apps being edited:

```bash
FAVN_DEV_RELOADABLE_APPS=favn_view mix phx.server
```

The value is a comma-separated app list. Changes to apps outside it need a
server restart to be picked up. Unset, the whole umbrella reloads as usual.

## When `mix favn.dev` says the runner is not ready

`mix favn.dev` launches the runner as a second OS process, so its output arrives
here only as port data. It is written to the terminal and appended to
`.favn/local/runner.log` in the project, which is where to look when the stack
dies — the terminal goes with it.

Two failures are now told apart. `the runner exited before it was ready` means
the process is gone and the log holds its last words. `the runner did not become
ready in time` means it is still alive and did not register inside the startup
budget, which is 30 seconds by default and enough on a native filesystem. Through
a bind mount, where the runner loads the project's code paths across the mount, it
sometimes is not:

```bash
FAVN_DEV_RUNNER_START_TIMEOUT_MS=120000 mix favn.dev
```

## Design system

`/design-system` lists every function component in `FavnView.UI` and
`FavnView.Components`, discovered by reflection rather than registered by hand,
with its attrs, slots, examples, and — for anything with no curated example — a
note saying so.

`/design-system/render` renders only what the query selects, so a screenshot of
the page is a screenshot of the subject:

```
/design-system/render?id=button/button&example=variants&theme=dark&width=1200
/design-system/render?id=badge/badge&mode=matrix&axis=tone&scale=2&chrome=0
/design-system/render?group=page&format=json
```

The index documents every parameter. Two are worth knowing up front: `scale`
zooms the page so a screenshot has real pixels instead of an upscaled crop, and
`chrome=0` removes the viewer's own labels.

On a render page, one call returns both the verdicts and the geometry:

```javascript
window.favn.audit()      // per-example measurements, verdicts, and bounding boxes
window.favn.summary()    // just the failures and the boxes
```

The audit measures contrast against the element's used background, control target
sizes, accessible names on icon-only controls, and content clipped without an
ellipsis. Thresholds come from `FavnView.Dev.DesignSystem.Audit`, which is also
unit-tested, so the browser and the test suite cannot disagree about them. A
metric that could not be measured is reported as `skipped`, never as a pass.

Because the boxes come back with the verdicts, cropping a screenshot to one
component afterwards needs no second lookup. Convert a box to screenshot pixels
with `screenshot_width / viewport.inner_width` from the same call — the capture
size is not a fixed multiple of the viewport.

## Tidewave

One endpoint covers the whole node. Because the umbrella development server runs
`favn_view` and `favn_orchestrator` in the same BEAM, the View's Tidewave
endpoint inspects orchestrator, storage, and core source, docs, and runtime state
as well. There is no second endpoint to register.

Register it against the port the server is actually using:

```bash
claude mcp add --transport http tidewave "http://localhost:${FAVN_VIEW_PORT:-4173}/tidewave/mcp"
```

Tidewave is plugged in `dev` only. Do not expose it beyond the local interface.

## Related pages

- [`../structure/favn_view.md`](../structure/favn_view.md) — what `favn_view` owns.
- [`../design/component-patterns.md`](../design/component-patterns.md) — the design-system strategy.
- [`../../apps/favn/guides/local-development.md`](../../apps/favn/guides/local-development.md) — `mix favn.dev` for project users.
