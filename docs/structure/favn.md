# favn

Purpose: public package facade, public authoring entrypoint, public SQL client
entrypoint, and public `mix favn.*` task wrappers.

Code:
- `apps/favn/lib/favn.ex`
- `apps/favn/lib/favn/ai.ex`
- `apps/favn/lib/favn/sql_client.ex`
- `apps/favn/lib/mix/tasks/**/*.ex`

Tests:
- `apps/favn/test/`
- `apps/favn/test/favn_test.exs` doctests the public facade and depends on docs being emitted for `Favn`
- `apps/favn/test/consumer_dependency_install_test.exs` keeps one fast public
  package-boundary check while full fresh-project dependency resolution runs in
  the `:slow` tier
- public Mix task tests under `apps/favn/test/mix_tasks/`
- bootstrap and diagnostics task argument/default coverage in `apps/favn/test/mix_tasks/public_tasks_test.exs`

Use when changing public APIs, public docs breadcrumbs, public SQL client access,
or public Mix task argument/dispatch behavior, including `mix favn.runs cancel`,
`mix favn.bootstrap.single`, and `mix favn.diagnostics`.

Public SQL output-contract authoring is documented in
`apps/favn/guides/sql-output-contracts.md` and routed from `Favn.AI`.
`Favn.SQLAsset.contract/1` is the only public contract declaration; compiled
contract structs belong to `favn_core` and are not authored directly.

`Favn.SQLClient.with_connection/3` is the recommended public pattern for
asset-scoped SQL session reuse from Elixir assets and helpers. It opens one
session, passes it to the callback, and disconnects or returns it to the pool on
exit. Helpers that perform several SQL operations should accept an existing
session where practical so DuckDB/DuckLake bootstrap costs are paid once per asset
execution instead of once per helper call. Sessions are process-owned and should
not be shared concurrently across child tasks; child tasks should open their own
scoped session with `with_required_catalogs/2` or explicit `required_catalogs`.

The public HexDocs guide `guides/duckdb-session-scripts.md` is canonical for
native DuckDB startup/resources config, `@resources`, physical-session
lifecycle, and script safety.

The public HexDocs guide `guides/retries-and-replay.md` is canonical for node
retry precedence/safety, internal retries, replay input modes, runtime-input
pins, schedule interaction, cancellation, and recovery.

The public HexDocs guide `guides/runner-plugins.md` is canonical for isolated
runner lifecycle extensions, the simple supervised-children path, disposable
runner-local state, Azure credential-cache usage, and DuckDB token injection.
