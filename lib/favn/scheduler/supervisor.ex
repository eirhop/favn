defmodule Favn.Scheduler.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    case Favn.Scheduler.Storage.child_specs() do
      {:ok, scheduler_storage_children} ->
        children = scheduler_storage_children ++ [Favn.Scheduler.Runtime]
        Supervisor.init(children, strategy: :one_for_one)

      {:error, reason} ->
        {:stop, reason}
    end
  end
end
