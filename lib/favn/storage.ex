defmodule Favn.Storage do
  @moduledoc """
  Storage facade that delegates run persistence to the configured storage adapter.

  This module is the canonical storage boundary used by `Favn` and
  `Favn.Runtime.Engine`. It validates adapter modules, normalizes adapter
  responses, and preserves stable error shapes for callers.
  """

  alias Favn.Run
  alias Favn.Runtime.Telemetry

  @default_adapter Favn.Storage.Adapter.Memory

  @type error :: :not_found | :invalid_opts | {:store_error, term()}

  @doc """
  Return child specs for the configured storage adapter.

  Adapters may return:

    * `{:ok, child_spec}` when a supervised process is required
    * `:none` when no supervised process is required

  The facade always returns a list to simplify `Supervisor.start_link/2`
  integration.
  """
  @spec child_specs() :: {:ok, [Supervisor.child_spec()]} | {:error, error()}
  def child_specs do
    adapter = adapter_module()

    with :ok <- validate_adapter(adapter),
         child_spec_result <- adapter.child_spec(adapter_opts()),
         {:ok, child_spec} <- normalize_child_spec_result(child_spec_result) do
      {:ok, maybe_child_to_list(child_spec)}
    else
      {:error, {:store_error, _reason}} = error -> error
      {:error, reason} -> {:error, {:store_error, reason}}
    end
  end

  @doc """
  Persist one `%Favn.Run{}` value through the configured adapter.

  Returns `:ok` on success, otherwise a normalized storage error.
  """
  @spec put_run(Run.t()) :: :ok | {:error, error()}
  def put_run(%Run{} = run) do
    adapter_call(:put_run, %{run_id: run.id}, fn adapter, opts -> adapter.put_run(run, opts) end)
  end

  @doc """
  Fetch one run by ID from the configured adapter.

  Returns `{:error, :not_found}` when the run ID does not exist.
  """
  @spec get_run(Favn.run_id()) :: {:ok, Run.t()} | {:error, error()}
  def get_run(run_id) do
    adapter_call(:get_run, %{run_id: run_id}, fn adapter, opts ->
      adapter.get_run(run_id, opts)
    end)
  end

  @doc """
  List runs from storage.

  Supported filters:

    * `:status` - one of `:queued | :running | :ok | :error | :cancelled | :timed_out`
    * `:limit` - positive integer max result count
  """
  @spec list_runs(Favn.list_runs_opts()) :: {:ok, [Run.t()]} | {:error, error()}
  def list_runs(opts \\ []) when is_list(opts) do
    with :ok <- validate_list_opts(opts) do
      adapter_call(:list_runs, %{run_id: :all}, fn adapter, adapter_opts ->
        adapter.list_runs(opts, adapter_opts)
      end)
    end
  end

  @doc false
  @spec list_queued_runs(keyword()) :: {:ok, [Run.t()]} | {:error, error()}
  def list_queued_runs(opts \\ []) when is_list(opts) do
    limit = Keyword.get(opts, :limit)

    if is_nil(limit) or (is_integer(limit) and limit > 0) do
      adapter_call(:list_queued_runs, %{run_id: :all}, fn adapter, adapter_opts ->
        adapter.list_queued_runs(opts, adapter_opts)
      end)
    else
      {:error, :invalid_opts}
    end
  end

  @doc false
  @spec allocate_queue_seq() :: {:ok, pos_integer()} | {:error, error()}
  def allocate_queue_seq do
    adapter_call(:allocate_queue_seq, %{run_id: :queue_seq}, fn adapter, adapter_opts ->
      adapter.allocate_queue_seq(adapter_opts)
    end)
  end

  @doc """
  Return the configured storage adapter module.

  Defaults to `Favn.Storage.Adapter.Memory`.
  """
  @spec adapter_module() :: module()
  def adapter_module do
    Application.get_env(:favn, :storage_adapter, @default_adapter)
  end

  @doc """
  Return adapter options passed through on each adapter call.
  """
  @spec adapter_opts() :: keyword()
  def adapter_opts do
    Application.get_env(:favn, :storage_adapter_opts, [])
  end

  @doc """
  Validate that `adapter` is loadable and exports required callbacks.

  This verifies callback presence at runtime to keep misconfiguration errors
  explicit and early.
  """
  @spec validate_adapter(module()) :: :ok | {:error, error()}
  def validate_adapter(adapter) when is_atom(adapter) do
    required_callbacks = [
      {:child_spec, 1},
      {:scheduler_child_spec, 1},
      {:put_run, 2},
      {:get_run, 2},
      {:list_runs, 2},
      {:list_queued_runs, 2},
      {:allocate_queue_seq, 1},
      {:put_scheduler_state, 2},
      {:get_scheduler_state, 3}
    ]

    with {:module, ^adapter} <- Code.ensure_loaded(adapter),
         true <-
           Enum.all?(required_callbacks, fn {name, arity} ->
             function_exported?(adapter, name, arity)
           end) do
      :ok
    else
      _ -> {:error, {:store_error, {:invalid_storage_adapter, adapter}}}
    end
  end

  defp validate_list_opts(opts) do
    status = Keyword.get(opts, :status)
    limit = Keyword.get(opts, :limit)

    cond do
      not is_nil(status) and
          status not in [:queued, :running, :ok, :error, :cancelled, :timed_out] ->
        {:error, :invalid_opts}

      not is_nil(limit) and (not is_integer(limit) or limit <= 0) ->
        {:error, :invalid_opts}

      true ->
        :ok
    end
  end

  defp adapter_call(operation, metadata, fun) do
    adapter = adapter_module()
    started = System.monotonic_time(:millisecond)

    result =
      case validate_adapter(adapter) do
        :ok ->
          safe_adapter_call(adapter, operation, fun)

        {:error, reason} ->
          {:error, reason}
      end

    duration_ms = System.monotonic_time(:millisecond) - started

    _ =
      Telemetry.emit_operation(:storage, operation, duration_ms, %{
        run_id: Map.get(metadata, :run_id, :unknown),
        operation: operation,
        adapter: adapter,
        result: storage_result_status(result),
        error_kind: storage_error_kind(result),
        error_class: storage_error_class(result)
      })

    result
  end

  defp storage_result_status(:ok), do: :ok
  defp storage_result_status({:ok, _}), do: :ok
  defp storage_result_status({:error, _}), do: :error

  defp storage_error_kind({:error, _}), do: :error
  defp storage_error_kind(_), do: nil

  defp storage_error_class({:error, {:store_error, {:invalid_storage_adapter, _}}}),
    do: :invalid_adapter

  defp storage_error_class({:error, {:store_error, {:raised, _}}}), do: :adapter_raise
  defp storage_error_class({:error, {:store_error, {:thrown, _}}}), do: :adapter_throw
  defp storage_error_class({:error, {:store_error, {:exited, _}}}), do: :adapter_exit
  defp storage_error_class({:error, {:store_error, _}}), do: :adapter_error
  defp storage_error_class({:error, :not_found}), do: :not_found
  defp storage_error_class({:error, :invalid_opts}), do: :invalid_opts
  defp storage_error_class(_), do: nil

  defp safe_adapter_call(adapter, _operation, fun) do
    adapter
    |> fun.(adapter_opts())
    |> normalize_result()
  rescue
    error -> {:error, {:store_error, {:raised, error}}}
  catch
    :throw, reason -> {:error, {:store_error, {:thrown, reason}}}
    :exit, reason -> {:error, {:store_error, {:exited, reason}}}
  end

  defp normalize_result(:ok), do: :ok
  defp normalize_result({:ok, _value} = ok), do: ok
  defp normalize_result({:error, :not_found}), do: {:error, :not_found}
  defp normalize_result({:error, :invalid_opts}), do: {:error, :invalid_opts}
  defp normalize_result({:error, {:store_error, _reason}} = error), do: error
  defp normalize_result({:error, reason}), do: {:error, {:store_error, reason}}

  defp maybe_child_to_list(:none), do: []
  defp maybe_child_to_list(value), do: [value]

  defp normalize_child_spec_result(:none), do: {:ok, :none}
  defp normalize_child_spec_result({:ok, child_spec}), do: {:ok, child_spec}
  defp normalize_child_spec_result({:error, reason}), do: {:error, reason}
  defp normalize_child_spec_result(other), do: {:error, {:invalid_child_spec_response, other}}
end
