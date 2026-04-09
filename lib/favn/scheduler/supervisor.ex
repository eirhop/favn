defmodule Favn.Scheduler.Supervisor do
  @moduledoc false
  use Supervisor

  alias Favn.Scheduler.Runtime
  alias Favn.Scheduler.Storage

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    case Storage.child_specs() do
      {:ok, scheduler_storage_children} ->
        children = scheduler_storage_children ++ [Runtime]
        Supervisor.init(children, strategy: :one_for_one)

      {:error, reason} ->
        {:stop, reason}
    end
  end
end
