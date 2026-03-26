defmodule Flux.Storage do
  @moduledoc """
  Storage facade that delegates run persistence to the configured storage adapter.

  This module is the canonical storage boundary used by `Flux` and
  `Flux.Runtime.Runner`. It validates adapter modules, normalizes adapter
  responses, and preserves stable error shapes for callers.
  """

  alias Flux.Run

  @default_adapter Flux.Storage.Adapter.Memory

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
         {:ok, child_spec} <- adapter.child_spec(adapter_opts()) do
      {:ok, [child_spec]}
    else
      :none -> {:ok, []}
      {:error, {:store_error, _reason}} = error -> error
      {:error, reason} -> {:error, {:store_error, reason}}
    end
  end

  @doc """
  Persist one `%Flux.Run{}` value through the configured adapter.

  Returns `:ok` on success, otherwise a normalized storage error.
  """
  @spec put_run(Run.t()) :: :ok | {:error, error()}
  def put_run(%Run{} = run) do
    adapter_call(fn adapter, opts -> adapter.put_run(run, opts) end)
  end

  @doc """
  Fetch one run by ID from the configured adapter.

  Returns `{:error, :not_found}` when the run ID does not exist.
  """
  @spec get_run(Flux.run_id()) :: {:ok, Run.t()} | {:error, error()}
  def get_run(run_id) do
    adapter_call(fn adapter, opts -> adapter.get_run(run_id, opts) end)
  end

  @doc """
  List runs from storage.

  Supported filters:

    * `:status` - one of `:running | :ok | :error`
    * `:limit` - positive integer max result count
  """
  @spec list_runs(Flux.list_runs_opts()) :: {:ok, [Run.t()]} | {:error, error()}
  def list_runs(opts \\ []) when is_list(opts) do
    with :ok <- validate_list_opts(opts) do
      adapter_call(fn adapter, adapter_opts -> adapter.list_runs(opts, adapter_opts) end)
    end
  end

  @doc """
  Return the configured storage adapter module.

  Defaults to `Flux.Storage.Adapter.Memory`.
  """
  @spec adapter_module() :: module()
  def adapter_module do
    Application.get_env(:flux, :storage_adapter, @default_adapter)
  end

  @doc """
  Return adapter options passed through on each adapter call.
  """
  @spec adapter_opts() :: keyword()
  def adapter_opts do
    Application.get_env(:flux, :storage_adapter_opts, [])
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
      {:put_run, 2},
      {:get_run, 2},
      {:list_runs, 2}
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
      not is_nil(status) and status not in [:running, :ok, :error] ->
        {:error, :invalid_opts}

      not is_nil(limit) and (not is_integer(limit) or limit <= 0) ->
        {:error, :invalid_opts}

      true ->
        :ok
    end
  end

  defp adapter_call(fun) do
    adapter = adapter_module()

    with :ok <- validate_adapter(adapter) do
      adapter
      |> fun.(adapter_opts())
      |> normalize_result()
    end
  end

  defp normalize_result(:ok), do: :ok
  defp normalize_result({:ok, _value} = ok), do: ok
  defp normalize_result({:error, :not_found}), do: {:error, :not_found}
  defp normalize_result({:error, :invalid_opts}), do: {:error, :invalid_opts}
  defp normalize_result({:error, {:store_error, _reason}} = error), do: error
  defp normalize_result({:error, reason}), do: {:error, {:store_error, reason}}
end
