defmodule Mix.Tasks.Favn.Postgres.Migrate do
  @moduledoc "Applies the PostgreSQL Storage V2 migrations with the configured migrator URL."

  use Mix.Task

  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.StorageV2.Migrations

  @shortdoc "Applies PostgreSQL Storage V2 migrations"

  @impl true
  def run(_args) do
    Mix.Task.run("app.config")
    {:ok, _applications} = Application.ensure_all_started(:ecto_sql)
    {:ok, _applications} = Application.ensure_all_started(:postgrex)

    options = repo_options!()
    {:ok, repo} = Repo.start_link(options)

    try do
      :ok = Migrations.migrate!(Repo)
      Mix.shell().info("PostgreSQL Storage V2 schema is current")
    after
      GenServer.stop(repo)
    end
  end

  defp repo_options! do
    case Config.repo_options() do
      {:ok, options} ->
        options

      {:error, reason} ->
        Mix.raise("invalid PostgreSQL migrator configuration: #{inspect(reason)}")
    end
  end
end

defmodule Mix.Tasks.Favn.Postgres.VerifyRestore do
  @moduledoc "Verifies Storage V2 schema and authoritative relationships after an isolated restore."

  use Mix.Task

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.StorageV2.Migrations

  @shortdoc "Verifies an isolated PostgreSQL restore"

  @impl true
  def run(_args) do
    Mix.Task.run("app.config")
    {:ok, _applications} = Application.ensure_all_started(:ecto_sql)
    {:ok, _applications} = Application.ensure_all_started(:postgrex)
    options = repo_options!()
    {:ok, repo} = Repo.start_link(options)

    try do
      Repo.checkout(fn ->
        SQL.query!(Repo, "SET statement_timeout = '10min'", [])
        verify_schema!()
        verify_authority!()
        Mix.shell().info("PostgreSQL Storage V2 restore is internally consistent")
      end)
    after
      GenServer.stop(repo)
    end
  end

  defp verify_schema! do
    case Migrations.diagnostics(Repo) do
      {:ok, %{ready?: true}} -> :ok
      {:ok, diagnostics} -> Mix.raise("restored schema is incompatible: #{inspect(diagnostics)}")
      {:error, reason} -> Mix.raise("restored schema diagnostics failed: #{inspect(reason)}")
    end
  end

  defp verify_authority! do
    %{rows: [[orphan_count]]} =
      SQL.query!(
        Repo,
        """
        SELECT
          (SELECT count(*) FROM favn_control.runs run
           LEFT JOIN favn_control.workspaces workspace USING (workspace_id)
           WHERE workspace.workspace_id IS NULL) +
          (SELECT count(*) FROM favn_control.run_events event
           LEFT JOIN favn_control.runs run USING (workspace_id, run_id)
           WHERE run.run_id IS NULL) +
          (SELECT count(*) FROM favn_control.workspace_deployments deployment
           LEFT JOIN favn_control.manifest_versions manifest USING (manifest_version_id)
           WHERE manifest.manifest_version_id IS NULL) +
          (SELECT count(*) FROM favn_control.run_targets target
           LEFT JOIN favn_control.workspace_deployment_targets catalog
             USING (workspace_id, deployment_id, target_kind, target_id)
           WHERE catalog.target_id IS NULL)
        """,
        []
      )

    if orphan_count != 0, do: Mix.raise("restored authority contains #{orphan_count} orphan rows")

    %{rows: [[invalid_cursor_count]]} =
      SQL.query!(
        Repo,
        """
        SELECT count(*)
        FROM favn_control.projection_cursors cursor
        WHERE cursor.last_publication_id >
          (SELECT last_publication_id FROM favn_control.outbox_publication_state WHERE singleton_id = 1)
        """,
        []
      )

    if invalid_cursor_count != 0,
      do: Mix.raise("restored projection cursor is ahead of durable publication state")
  end

  defp repo_options! do
    case Config.repo_options() do
      {:ok, options} ->
        options

      {:error, reason} ->
        Mix.raise("invalid PostgreSQL restore configuration: #{inspect(reason)}")
    end
  end
end

defmodule Mix.Tasks.Favn.Postgres.GrantRuntime do
  @moduledoc "Grants the least-privilege Storage V2 DML surface to a runtime role."

  use Mix.Task

  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Privileges
  alias FavnStoragePostgres.Repo

  @shortdoc "Grants Storage V2 runtime privileges"

  @impl true
  def run(args) do
    Mix.Task.run("app.config")
    {:ok, _applications} = Application.ensure_all_started(:ecto_sql)
    {:ok, _applications} = Application.ensure_all_started(:postgrex)
    {options, positional, invalid} = OptionParser.parse(args, strict: [role: :string])

    if positional != [] or invalid != [] do
      Mix.raise("usage: mix favn.postgres.grant_runtime [--role ROLE]")
    end

    role =
      Keyword.get(options, :role, System.get_env("FAVN_DATABASE_RUNTIME_ROLE", "favn_runtime"))

    repo_options = repo_options!()
    {:ok, repo} = Repo.start_link(repo_options)

    try do
      :ok = Privileges.grant_runtime!(Repo, role)
      Mix.shell().info("Granted Storage V2 runtime privileges to #{role}")
    after
      GenServer.stop(repo)
    end
  end

  defp repo_options! do
    case Config.repo_options() do
      {:ok, options} ->
        options

      {:error, reason} ->
        Mix.raise("invalid PostgreSQL migrator configuration: #{inspect(reason)}")
    end
  end
end

defmodule Mix.Tasks.Favn.Postgres.Reset do
  @moduledoc "Explicitly destroys the local Storage V2 schema."

  use Mix.Task

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Repo

  @shortdoc "Destroys the PostgreSQL Storage V2 schema"

  @impl true
  def run(["--yes-destroy-storage-v2"]) do
    Mix.Task.run("app.config")
    {:ok, _applications} = Application.ensure_all_started(:ecto_sql)
    {:ok, _applications} = Application.ensure_all_started(:postgrex)

    {:ok, options} = Config.repo_options()
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
