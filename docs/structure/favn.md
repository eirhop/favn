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
- public Mix task tests under `apps/favn/test/mix_tasks/`
- bootstrap and diagnostics task argument/default coverage in `apps/favn/test/mix_tasks/public_tasks_test.exs`

Use when changing public APIs, public docs breadcrumbs, public SQL client access,
or public Mix task argument/dispatch behavior, including `mix favn.bootstrap.single` and `mix favn.diagnostics`.
