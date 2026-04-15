defmodule Favn.Storage.Adapter do
  @moduledoc """
  Behaviour for public Favn persistence adapters.
  """

  alias Favn.Run
  alias Favn.Scheduler.State, as: SchedulerState

  @type adapter_opts :: keyword()
  @type list_opts :: Favn.list_runs_opts()
  @type error :: :not_found | :invalid_opts | term()

  @callback child_spec(adapter_opts()) :: {:ok, Supervisor.child_spec()} | :none
  @callback scheduler_child_spec(adapter_opts()) :: {:ok, Supervisor.child_spec()} | :none
  @callback put_run(Run.t(), adapter_opts()) :: :ok | {:error, error()}
  @callback get_run(Favn.run_id(), adapter_opts()) :: {:ok, Run.t()} | {:error, error()}
  @callback list_runs(list_opts(), adapter_opts()) :: {:ok, [Run.t()]} | {:error, error()}
  @callback put_scheduler_state(SchedulerState.t(), adapter_opts()) :: :ok | {:error, error()}
  @callback get_scheduler_state(module(), atom() | nil, adapter_opts()) ::
              {:ok, SchedulerState.t() | nil} | {:error, error()}
end
