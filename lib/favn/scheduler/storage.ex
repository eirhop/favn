defmodule Favn.Scheduler.Storage do
  @moduledoc """
  Scheduler-state storage facade delegated through the configured storage adapter.

  Adapters are responsible for both run persistence and scheduler-state persistence.
  """

  alias Favn.Scheduler.State
  alias Favn.Storage

  @type error :: Storage.error()

  @spec child_specs() :: {:ok, [Supervisor.child_spec()]} | {:error, error()}
  def child_specs do
    adapter = Storage.adapter_module()

    with :ok <- Storage.validate_adapter(adapter),
         scheduler_child_spec <-
           safe_adapter_call(adapter, fn value, opts -> value.scheduler_child_spec(opts) end),
         {:ok, normalized} <- normalize_child_spec_result(scheduler_child_spec) do
      {:ok, maybe_child_to_list(normalized)}
    end
  end

  @spec get_state(module(), atom() | nil) :: {:ok, State.t() | nil} | {:error, error()}
  def get_state(pipeline_module, schedule_id \\ nil)

  def get_state(pipeline_module, schedule_id) when is_atom(pipeline_module) do
    adapter = Storage.adapter_module()

    with :ok <- Storage.validate_adapter(adapter),
         result <-
           safe_adapter_call(adapter, fn value, opts ->
             value.get_scheduler_state(pipeline_module, schedule_id, opts)
           end) do
      normalize_get_state_result(result)
    end
  end

  @spec put_state(State.t()) :: :ok | {:error, error()}
  def put_state(%State{} = state) do
    adapter = Storage.adapter_module()

    with :ok <- Storage.validate_adapter(adapter),
         result <-
           safe_adapter_call(adapter, fn value, opts -> value.put_scheduler_state(state, opts) end) do
      normalize_put_state_result(result)
    end
  end

  defp safe_adapter_call(adapter, fun) do
    fun.(adapter, Storage.adapter_opts())
  rescue
    error -> {:error, {:store_error, {:raised, error}}}
  catch
    :throw, reason -> {:error, {:store_error, {:thrown, reason}}}
    :exit, reason -> {:error, {:store_error, {:exited, reason}}}
  end

  defp normalize_child_spec_result(:none), do: {:ok, :none}
  defp normalize_child_spec_result({:ok, child_spec}), do: {:ok, child_spec}
  defp normalize_child_spec_result({:error, {:store_error, _} = reason}), do: {:error, reason}
  defp normalize_child_spec_result({:error, reason}), do: {:error, {:store_error, reason}}

  defp normalize_child_spec_result(other),
    do: {:error, {:store_error, {:invalid_child_spec_response, other}}}

  defp normalize_get_state_result({:ok, nil}), do: {:ok, nil}
  defp normalize_get_state_result({:ok, %State{} = state}), do: {:ok, state}
  defp normalize_get_state_result({:error, {:store_error, _} = reason}), do: {:error, reason}
  defp normalize_get_state_result({:error, :invalid_opts}), do: {:error, :invalid_opts}
  defp normalize_get_state_result({:error, reason}), do: {:error, {:store_error, reason}}

  defp normalize_get_state_result(other),
    do: {:error, {:store_error, {:invalid_scheduler_state_response, other}}}

  defp normalize_put_state_result(:ok), do: :ok
  defp normalize_put_state_result({:error, {:store_error, _} = reason}), do: {:error, reason}
  defp normalize_put_state_result({:error, :invalid_opts}), do: {:error, :invalid_opts}
  defp normalize_put_state_result({:error, reason}), do: {:error, {:store_error, reason}}

  defp normalize_put_state_result(other),
    do: {:error, {:store_error, {:invalid_put_state_response, other}}}

  defp maybe_child_to_list(:none), do: []
  defp maybe_child_to_list(value), do: [value]
end
