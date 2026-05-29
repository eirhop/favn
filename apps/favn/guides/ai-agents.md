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
mix favn.read_doc Favn.Pipeline
mix favn.read_doc Favn.Namespace
mix favn.read_doc Favn.SQLClient
mix favn.read_doc Favn.Dev
```

For repository contributors, the root `AGENTS.md` and `.opencode/skills/`
contain additional boundary-specific guidance. Those files are contributor
instructions for this repository, not consumer API documentation.
