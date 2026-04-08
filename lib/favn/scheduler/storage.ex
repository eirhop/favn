defmodule Favn.Scheduler.Storage do
  @moduledoc """
  Scheduler-state storage facade delegated through the configured storage adapter.

  Adapters are responsible for both run persistence and scheduler-state persistence.
  """

  alias Favn.Scheduler.State

  @spec child_specs() :: {:ok, [Supervisor.child_spec()]} | {:error, term()}
  def child_specs do
    adapter = Favn.Storage.adapter_module()

    with :ok <- Favn.Storage.validate_adapter(adapter),
         {:ok, scheduler_child_spec} <- adapter.scheduler_child_spec(Favn.Storage.adapter_opts()) do
      {:ok, if(scheduler_child_spec == :none, do: [], else: [scheduler_child_spec])}
    else
      :none -> {:ok, []}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:store_error, other}}
    end
  end

  @spec get_state(module()) :: {:ok, State.t() | nil} | {:error, term()}
  def get_state(pipeline_module) when is_atom(pipeline_module) do
    adapter = Favn.Storage.adapter_module()

    with :ok <- Favn.Storage.validate_adapter(adapter) do
      adapter.get_scheduler_state(pipeline_module, Favn.Storage.adapter_opts())
    end
  end

  @spec put_state(State.t()) :: :ok | {:error, term()}
  def put_state(%State{} = state) do
    adapter = Favn.Storage.adapter_module()

    with :ok <- Favn.Storage.validate_adapter(adapter) do
      adapter.put_scheduler_state(state, Favn.Storage.adapter_opts())
    end
  end
end
