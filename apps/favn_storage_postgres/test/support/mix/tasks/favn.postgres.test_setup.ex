defmodule Mix.Tasks.Favn.Postgres.TestSetup do
  @moduledoc false

  use Mix.Task

  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RuntimePrivileges
  alias FavnStoragePostgres.StorageV2.Migrations
  alias Mix.Tasks.Favn.Postgres.ReleaseHelpers

  @shortdoc false

  @impl true
  def run(args) do
    Mix.Task.run("app.config")
    {options, positional, invalid} = OptionParser.parse(args, strict: [runtime_role: :string])

    if positional != [] or invalid != [] do
      Mix.raise("usage: mix favn.postgres.test_setup [--runtime-role ROLE]")
    end

    {:ok, _applications} = Application.ensure_all_started(:ecto_sql)
    {:ok, _applications} = Application.ensure_all_started(:postgrex)
    {:ok, repo_options} = Config.repo_options_from_env(ReleaseHelpers.migrator_env!())
    {:ok, repo} = Repo.start_link(repo_options)

    try do
      :ok = Migrations.migrate!(Repo)

      if runtime_role = Keyword.get(options, :runtime_role) do
        :ok = RuntimePrivileges.grant_runtime!(Repo, runtime_role)
      end
    after
      GenServer.stop(repo)
    end
  end
end
