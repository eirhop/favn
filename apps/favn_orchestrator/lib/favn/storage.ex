defmodule Favn.Storage do
  @moduledoc """
  Public storage facade for run and scheduler state persistence.
  """

  alias Favn.Run

  @default_adapter Favn.Storage.Adapter.Memory

  @type error :: :not_found | :invalid_opts | {:store_error, term()}

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

  @spec put_run(Run.t()) :: :ok | {:error, error()}
  def put_run(%Run{} = run) do
    adapter_call(fn adapter, opts -> adapter.put_run(run, opts) end)
  end

  @spec get_run(term()) :: {:ok, Run.t()} | {:error, error()}
  def get_run(run_id) do
    adapter_call(fn adapter, opts -> adapter.get_run(run_id, opts) end)
  end

  @spec list_runs(Favn.list_runs_opts()) :: {:ok, [Run.t()]} | {:error, error()}
  def list_runs(opts \\ []) when is_list(opts) do
    with :ok <- validate_list_opts(opts) do
      adapter_call(fn adapter, adapter_opts -> adapter.list_runs(opts, adapter_opts) end)
    end
  end

  @spec adapter_module() :: module()
  def adapter_module do
    Application.get_env(:favn, :storage_adapter, @default_adapter)
  end

  @spec adapter_opts() :: keyword()
  def adapter_opts do
    Application.get_env(:favn, :storage_adapter_opts, [])
  end

  @spec validate_adapter(module()) :: :ok | {:error, error()}
  def validate_adapter(adapter) when is_atom(adapter) do
    required_callbacks = [
      {:child_spec, 1},
      {:scheduler_child_spec, 1},
      {:put_run, 2},
      {:get_run, 2},
      {:list_runs, 2},
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
      not is_nil(status) and status not in [:running, :ok, :error, :cancelled, :timed_out] ->
        {:error, :invalid_opts}

      not is_nil(limit) and (not is_integer(limit) or limit <= 0) ->
        {:error, :invalid_opts}

      true ->
        :ok
    end
  end

  defp adapter_call(fun) when is_function(fun, 2) do
    adapter = adapter_module()

    case validate_adapter(adapter) do
      :ok -> safe_adapter_call(adapter, fun)
      {:error, reason} -> {:error, reason}
    end
  end

  defp safe_adapter_call(adapter, fun) do
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
