defmodule FavnStoragePostgres.Supervisor do
  @moduledoc false

  use Supervisor

  alias FavnStoragePostgres.Migrations
  alias FavnStoragePostgres.Repo

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    repo_config = Keyword.fetch!(opts, :repo_config)
    migration_mode = Keyword.get(opts, :migration_mode, :manual)

    :ok = bootstrap_storage(repo_config, migration_mode)

    children = [{Repo, Keyword.put(repo_config, :name, Repo)}]
    Supervisor.init(children, strategy: :one_for_one)
  end

  defp bootstrap_storage(repo_config, :auto) do
    {:ok, pid} = Repo.start_link(Keyword.put(repo_config, :name, Repo))

    try do
      Migrations.migrate!(Repo)
    after
      GenServer.stop(pid)
    end

    :ok
  end

  defp bootstrap_storage(repo_config, :manual) do
    {:ok, pid} = Repo.start_link(Keyword.put(repo_config, :name, Repo))

    try do
      if Migrations.schema_ready?(Repo) do
        :ok
      else
        raise "favn postgres schema is not ready; run migrations or set migration_mode: :auto"
      end
    after
      GenServer.stop(pid)
    end

    :ok
  end

  defp bootstrap_storage(_repo_config, mode) do
    raise "invalid postgres migration mode: #{inspect(mode)}"
  end
end
