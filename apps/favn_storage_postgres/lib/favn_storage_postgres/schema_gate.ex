defmodule FavnStoragePostgres.SchemaGate do
  @moduledoc false

  alias FavnStoragePostgres.StorageV2.Migrations

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary
    }
  end

  @spec start_link(keyword()) :: :ignore | GenServer.on_start()
  def start_link(opts) do
    repo = Keyword.get(opts, :repo, FavnStoragePostgres.Repo)

    case Migrations.diagnostics(repo) do
      {:ok, %{ready?: true}} -> :ignore
      {:ok, diagnostics} -> {:error, {:postgres_schema_not_ready, diagnostics}}
      {:error, reason} -> {:error, {:postgres_schema_check_failed, reason}}
    end
  end
end
