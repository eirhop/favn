defmodule FavnStorageSqlite.Supervisor do
  @moduledoc false

  use Supervisor

  alias FavnStorageSqlite.Diagnostics
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
    policy_opts = policy_opts(opts)

    :ok = Diagnostics.validate_database_path!(opts)
    :ok = bootstrap_storage(repo_name, repo_opts, policy_opts)

    children = [{Repo, Keyword.put(repo_opts, :name, repo_name)}]
    Supervisor.init(children, strategy: :one_for_one)
  end

  defp bootstrap_storage(repo_name, repo_opts, %{migration_mode: :auto}) do
    run_approved_auto_migrations(repo_name, repo_opts)
  end

  defp bootstrap_storage(repo_name, repo_opts, %{migration_mode: :manual} = policy_opts) do
    with_bootstrap_repo(repo_name, repo_opts, fn ->
      case Migrations.schema_diagnostics(repo_name) do
        {:ok, %{status: :ready}} ->
          :ok

        {:ok, %{status: :empty_database}} when policy_opts.initialize_empty? ->
          :ok = Migrations.migrate!(repo_name)
          verify_schema_ready!(repo_name)

        {:ok, diagnostics} ->
          raise "favn sqlite schema is not ready; diagnostics: #{inspect(diagnostics)}"

        {:error, reason} ->
          raise "favn sqlite schema diagnostics failed: #{inspect(reason)}"
      end
    end)
  end

  defp bootstrap_storage(_repo_name, _repo_opts, %{migration_mode: mode}) do
    raise "invalid sqlite migration mode: #{inspect(mode)}"
  end

  defp run_approved_auto_migrations(repo_name, repo_opts) do
    with_bootstrap_repo(repo_name, repo_opts, fn ->
      case Migrations.schema_diagnostics(repo_name) do
        {:ok, %{status: :ready}} ->
          :ok

        {:ok, %{status: status}} when status in [:empty_database, :upgrade_required] ->
          :ok = Migrations.migrate!(repo_name)
          verify_schema_ready!(repo_name)

        {:ok, diagnostics} ->
          raise "favn sqlite schema is not ready before migrations; diagnostics: #{inspect(diagnostics)}"

        {:error, reason} ->
          raise "favn sqlite schema diagnostics failed before migrations: #{inspect(reason)}"
      end
    end)
  end

  defp verify_schema_ready!(repo_name) do
    case Migrations.schema_diagnostics(repo_name) do
      {:ok, %{status: :ready}} ->
        :ok

      {:ok, diagnostics} ->
        raise "favn sqlite schema is not ready after migrations; diagnostics: #{inspect(diagnostics)}"

      {:error, reason} ->
        raise "favn sqlite schema diagnostics failed after migrations: #{inspect(reason)}"
    end
  end

  defp with_bootstrap_repo(repo_name, repo_opts, fun) when is_function(fun, 0) do
    pid = start_bootstrap_repo!(repo_name, repo_opts)

    try do
      fun.()
    after
      GenServer.stop(pid)
    end

    :ok
  end

  defp start_bootstrap_repo!(repo_name, repo_opts) do
    case Repo.start_link(Keyword.put(bootstrap_repo_opts(repo_opts), :name, repo_name)) do
      {:ok, pid} -> pid
      {:error, reason} -> raise "favn sqlite database could not be opened: #{inspect(reason)}"
    end
  end

  defp repo_opts(opts) do
    [
      database: Keyword.fetch!(opts, :database),
      pool_size: Keyword.get(opts, :pool_size, 1),
      busy_timeout: Keyword.get(opts, :busy_timeout, 5_000)
    ]
  end

  defp policy_opts(opts) do
    %{
      migration_mode: Keyword.get(opts, :migration_mode, :auto),
      initialize_empty?: Keyword.get(opts, :initialize_empty?, false)
    }
  end

  defp bootstrap_repo_opts(repo_opts) do
    Keyword.put(repo_opts, :pool_size, 1)
  end
end
