defmodule FavnStorageSqlite.Supervisor do
  @moduledoc false

  use Supervisor

  alias FavnStorageSqlite.Migrations
  alias FavnStorageSqlite.Repo

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    repo_name = Repo
    repo_opts = repo_opts(opts)

    if Keyword.get(opts, :migration_mode, :auto) == :auto do
      :ok = run_migrations(repo_name, repo_opts)
    end

    children = [{Repo, Keyword.put(repo_opts, :name, repo_name)}]
    Supervisor.init(children, strategy: :one_for_one)
  end

  defp run_migrations(repo_name, repo_opts) do
    {:ok, pid} = Repo.start_link(Keyword.put(repo_opts, :name, repo_name))

    try do
      Migrations.migrate!(repo_name)
    after
      GenServer.stop(pid)
    end

    :ok
  end

  defp repo_opts(opts) do
    [
      database: Keyword.fetch!(opts, :database),
      pool_size: Keyword.get(opts, :pool_size, 1),
      busy_timeout: Keyword.get(opts, :busy_timeout, 5_000)
    ]
  end
end
