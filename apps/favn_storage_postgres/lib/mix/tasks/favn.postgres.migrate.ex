defmodule Mix.Tasks.Favn.Postgres.Migrate do
  @moduledoc "Applies the PostgreSQL Storage V2 migrations with the configured migrator URL."

  use Mix.Task

  alias FavnStoragePostgres.Release
  alias Mix.Tasks.Favn.Postgres.ReleaseHelpers

  @shortdoc "Applies PostgreSQL Storage V2 migrations"

  @impl true
  def run(_args) do
    Mix.Task.run("app.config")

    Release.migrate(ReleaseHelpers.migrator_env!())
    |> ReleaseHelpers.report("PostgreSQL Storage V2 schema is current")
  end
end

defmodule Mix.Tasks.Favn.Postgres.VerifyRestore do
  @moduledoc "Verifies Storage V2 schema and authoritative relationships after an isolated restore."

  use Mix.Task

  alias FavnStoragePostgres.Release
  alias Mix.Tasks.Favn.Postgres.ReleaseHelpers

  @shortdoc "Verifies an isolated PostgreSQL restore"

  @impl true
  def run(_args) do
    Mix.Task.run("app.config")

    Release.verify_restore()
    |> ReleaseHelpers.report("PostgreSQL Storage V2 restore is internally consistent")
  end
end

defmodule Mix.Tasks.Favn.Postgres.GrantRuntime do
  @moduledoc "Grants the least-privilege Storage V2 DML surface to a runtime role."

  use Mix.Task

  alias FavnStoragePostgres.Release
  alias Mix.Tasks.Favn.Postgres.ReleaseHelpers

  @shortdoc "Grants Storage V2 runtime privileges"

  @impl true
  def run(args) do
    Mix.Task.run("app.config")
    {options, positional, invalid} = OptionParser.parse(args, strict: [role: :string])

    if positional != [] or invalid != [] do
      Mix.raise("usage: mix favn.postgres.grant_runtime [--role ROLE]")
    end

    requested_role = Keyword.get(options, :role)
    env = ReleaseHelpers.migrator_env!()

    env =
      if requested_role, do: Map.put(env, "FAVN_DATABASE_RUNTIME_ROLE", requested_role), else: env

    Release.grant_runtime(env)
    |> ReleaseHelpers.report("Granted Storage V2 runtime privileges")
  end
end

defmodule Mix.Tasks.Favn.Postgres.Reset do
  @moduledoc "Explicitly destroys the local Storage V2 schema."

  use Mix.Task

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Repo
  alias Mix.Tasks.Favn.Postgres.ReleaseHelpers

  @shortdoc "Destroys the PostgreSQL Storage V2 schema"

  @impl true
  def run(["--yes-destroy-storage-v2"]) do
    Mix.Task.run("app.config")
    {:ok, _applications} = Application.ensure_all_started(:ecto_sql)
    {:ok, _applications} = Application.ensure_all_started(:postgrex)

    {:ok, options} = Config.repo_options_from_env(ReleaseHelpers.migrator_env!())
    {:ok, repo} = Repo.start_link(options)

    try do
      SQL.query!(Repo, "DROP SCHEMA IF EXISTS favn_control CASCADE", [])
      Mix.shell().info("Destroyed PostgreSQL Storage V2 schema")
    after
      GenServer.stop(repo)
    end
  end

  def run(_args) do
    Mix.raise("refusing destructive reset; pass --yes-destroy-storage-v2 explicitly")
  end
end
