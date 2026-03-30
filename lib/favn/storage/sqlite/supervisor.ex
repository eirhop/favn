defmodule Favn.Storage.SQLite.Supervisor do
  @moduledoc """
  Supervises the SQLite repo after running adapter migrations once at startup.

  NOTE: this bootstrap path currently uses a small custom migration step.
  Follow-up: evaluate replacing with an `Ecto.Migrator.start_link/1` child-based
  startup flow if it stays equally small and explicit for Favn.
  """

  use Supervisor

  alias Favn.Storage.SQLite.Migrations
  alias Favn.Storage.SQLite.Repo

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    :ok = run_bootstrap_migrations(opts)

    children = [
      {Repo, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp run_bootstrap_migrations(opts) do
    {:ok, pid} = Repo.start_link(opts)

    try do
      Migrations.migrate!(Repo)
    after
      GenServer.stop(pid)
    end

    :ok
  end
end
