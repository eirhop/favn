defmodule Favn.Scheduler.Storage do
  @moduledoc """
  Persistence facade for scheduler runtime state.
  """

  alias Favn.Scheduler.State

  @spec child_specs() :: [Supervisor.child_spec()]
  def child_specs do
    case impl_module() do
      Favn.Scheduler.Storage.Memory -> [Favn.Scheduler.Storage.Memory]
      _ -> []
    end
  end

  @spec get_state(module()) :: {:ok, State.t() | nil} | {:error, term()}
  def get_state(pipeline_module) when is_atom(pipeline_module) do
    impl_module().get_state(pipeline_module)
  end

  @spec put_state(State.t()) :: :ok | {:error, term()}
  def put_state(%State{} = state), do: impl_module().put_state(state)

  def impl_module do
    case Favn.Storage.adapter_module() do
      Favn.Storage.Adapter.SQLite -> Favn.Scheduler.Storage.SQLite
      _ -> Favn.Scheduler.Storage.Memory
    end
  end
end
