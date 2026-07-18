defmodule Mix.Tasks.Favn.Postgres.CompactRuntimeInputKeys do
  @moduledoc """
  Removes unreferenced runtime-input key versions from the Storage V2 inventory.

  This task never reads, writes, or reports key material.
  """

  use Mix.Task

  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RuntimeInputKeyInventory

  @shortdoc "Removes unreferenced runtime-input key versions"

  @impl true
  def run(args) do
    if args != [], do: Mix.raise("usage: mix favn.postgres.compact_runtime_input_keys")

    Mix.Task.run("app.config")
    {:ok, _applications} = Application.ensure_all_started(:ecto_sql)
    {:ok, _applications} = Application.ensure_all_started(:postgrex)
    {:ok, repo} = Repo.start_link(repo_options!())

    try do
      case RuntimeInputKeyInventory.compact(Repo) do
        {:ok, []} ->
          Mix.shell().info("Runtime-input key inventory is already compact")

        {:ok, versions} ->
          Mix.shell().info(
            "Removed unreferenced runtime-input key versions: #{inspect(versions)}"
          )

        {:error, reason} ->
          Mix.raise("runtime-input key inventory compaction failed: #{inspect(reason)}")
      end
    after
      GenServer.stop(repo)
    end
  end

  defp repo_options! do
    case Config.repo_options() do
      {:ok, options} -> options
      {:error, reason} -> Mix.raise("invalid PostgreSQL configuration: #{inspect(reason)}")
    end
  end
end
