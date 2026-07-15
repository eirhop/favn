# AI Agent Development

Favn includes `Favn.AI`, a compiled-doc entry point for AI-assisted development.
Use it when an agent needs to decide which Favn modules, guides, or local
commands to inspect next.

## Start Here

From a project that depends on `:favn`, read the AI entry point before guessing
about APIs:

```bash
mix favn.read_doc Favn.AI
```

`Favn.AI` maps common tasks to the next docs to read, including asset authoring,
SQL assets, namespaces, pipelines, local commands, manifest functions, windows,
freshness, and `Favn.SQLClient`.

For transactional SQL asset checks, follow this path instead of inferring the
contract from runtime code:

1. Read `mix favn.read_doc Favn.SQLAsset` for placement and execution semantics.
2. Read `mix favn.read_doc Favn.SQLAsset check` for the macro's exact options.
3. Read `mix favn.read_doc Favn.SQL.CheckResult` when interpreting persisted run
   metadata and check outcomes.
4. Read [Transactional SQL Asset Checks](sql-asset-checks.md) for the full
   authoring workflow, result limits, and failure modes.

For behaviour-based SQL runtime inputs, follow this path instead of inventing
an inline resolver DSL:

1. Read `mix favn.read_doc Favn.SQLAsset` for placement and execution timing.
2. Read `mix favn.read_doc Favn.SQLAsset.RuntimeInputs` for `resolve/1`.
3. Read `mix favn.read_doc Favn.SQLAsset.RuntimeInputs.Result` and
   `mix favn.read_doc Favn.SQLAsset.RuntimeInputs.Error` for the only accepted
   outcomes.
4. Read [Runtime Inputs For SQL Assets](sql-runtime-inputs.md) for the full
   workflow, supported values, budgets, redaction, and retry boundary.

The canonical declaration is `@runtime_inputs MyApp.Inputs`. Anonymous
functions, captures, MFA tuples, and inline resolver blocks are unsupported.

For DuckDB session setup, do not invent structured extension, setting, secret,
or attach options. Read
[DuckDB Session Scripts And Resources](duckdb-session-scripts.md), then
`Favn.SQLAsset` and `Favn.Namespace`. Native trusted SQL files own DuckDB syntax;
SQL assets declare stable `@resources [...]` names. Both session scripts and
asset SQL use `@name` for values, but they have separate parameter sources.

## Recommended Workflow

- Use `:favn` as the public package surface for asset authoring, local commands,
  manifest helpers, and SQL client usage.
- Read `Favn.AI` first, then follow its module pointers with
  `mix favn.read_doc ModuleName`.
- Prefer the documented DSL modules and `mix favn.*` commands over direct calls
  into runtime, storage, orchestrator, runner, or UI implementation apps.
- Use the local guides in this package for user-facing behavior before reading
  source files.
- Keep project-specific instructions in the consumer project's `AGENTS.md` or
  equivalent agent instruction file.

## Consumer Agent Prompt

Add a short Favn section to a consumer project's agent instructions:

```md
## Favn

Favn is used to define business-oriented assets and pipelines in Elixir,
compile them into a manifest, and run or inspect them locally.

Before guessing about Favn APIs, read `mix favn.read_doc Favn.AI` and follow
the module pointers there. Prefer the recommended consumer shape unless this
project documents a stronger local convention.
```

## Follow-Up Reads

Useful follow-up commands:

```bash
mix favn.read_doc Favn
mix favn.read_doc Favn.Asset
mix favn.read_doc Favn.SQLAsset
mix favn.read_doc Favn.SQLAsset.RuntimeInputs
mix favn.read_doc Favn.SQLAsset.RuntimeInputs.Result
mix favn.read_doc Favn.SQLAsset.RuntimeInputs.Error
mix favn.read_doc Favn.SQLAsset check
mix favn.read_doc Favn.SQL.CheckResult
mix favn.read_doc Favn.Connection
mix favn.read_doc Favn.Pipeline
mix favn.read_doc Favn.Namespace
mix favn.read_doc Favn.SQLClient
mix favn.read_doc Favn.Dev
```

For repository contributors, the root `AGENTS.md` and `.opencode/skills/`
contain additional boundary-specific guidance. Those files are contributor
instructions for this repository, not consumer API documentation.
