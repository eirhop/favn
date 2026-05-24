defmodule FavnOrchestrator.BoundedDispatcher do
  @moduledoc """
  Supervised bounded task dispatch for orchestrator control-plane submissions.

  The dispatcher is intentionally small: callers own durable intent and lifecycle
  state, while this module provides a supervised execution boundary with explicit
  concurrency limits. Actual run creation must still go through the public
  orchestrator submission APIs.
  """

  @default_max_concurrency 4
  @default_timeout_ms 15_000

  @type dispatch_result(value) :: {:ok, [value]} | {:error, term()}
  @type item_result(value) ::
          %{item: term(), status: :ok, value: value}
          | %{item: term(), status: :error, reason: term()}

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    name = Keyword.get(opts, :name, task_supervisor_name())

    Supervisor.child_spec(
      {Task.Supervisor, name: name},
      id: __MODULE__,
      restart: :permanent
    )
  end

  @doc """
  Runs one dispatched task under the bounded task supervisor.
  """
  @spec run((-> {:ok, value} | {:error, term()} | value), keyword()) ::
          {:ok, value} | {:error, term()}
        when value: term()
  def run(fun, opts \\ []) when is_function(fun, 0) and is_list(opts) do
    case run_many([:job], fn :job -> fun.() end, Keyword.put(opts, :max_concurrency, 1)) do
      {:ok, [value]} -> {:ok, value}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Runs a collection through supervised tasks with a bounded concurrency limit.

  The worker may return `:ok`, `{:ok, value}`, or `{:error, reason}`. The first
  error is returned to the caller.
  """
  @spec run_many(Enumerable.t(), (term() -> {:ok, value} | {:error, term()} | value), keyword()) ::
          dispatch_result(value | :ok)
        when value: term()
  def run_many(items, worker, opts \\ []) when is_function(worker, 1) and is_list(opts) do
    {:ok, results} = run_many_results(items, worker, opts)
    collect_results(results)
  end

  @doc """
  Runs a collection and returns one result per input item.

  This is useful for callers that need deterministic compensation after partial
  failure. Timeouts are finite by default and are reported as
  `{:dispatcher_timeout, timeout_ms}` for the affected item.
  """
  @spec run_many_results(
          Enumerable.t(),
          (term() -> {:ok, value} | {:error, term()} | value),
          keyword()
        ) :: {:ok, [item_result(value | :ok)]}
        when value: term()
  def run_many_results(items, worker, opts \\ []) when is_function(worker, 1) and is_list(opts) do
    items = Enum.to_list(items)

    if items == [] do
      {:ok, []}
    else
      supervisor = Keyword.get(opts, :supervisor, task_supervisor_name())
      max_concurrency = max_concurrency(opts)
      timeout_ms = timeout_ms(opts)

      results =
        supervisor
        |> Task.Supervisor.async_stream_nolink(items, worker,
          max_concurrency: max_concurrency,
          ordered: true,
          timeout: timeout_ms,
          on_timeout: :kill_task
        )
        |> Stream.zip(items)
        |> Enum.map(fn {result, item} -> item_result(item, result, timeout_ms) end)

      {:ok, results}
    end
  end

  @doc "Returns the configured default dispatcher concurrency."
  @spec configured_max_concurrency() :: pos_integer()
  def configured_max_concurrency do
    case Application.get_env(:favn_orchestrator, :dispatcher, []) do
      opts when is_list(opts) ->
        positive_integer(opts[:max_concurrency], @default_max_concurrency)

      _other ->
        @default_max_concurrency
    end
  end

  @doc "Returns the configured default dispatcher timeout in milliseconds."
  @spec configured_timeout_ms() :: pos_integer()
  def configured_timeout_ms do
    case Application.get_env(:favn_orchestrator, :dispatcher, []) do
      opts when is_list(opts) ->
        positive_integer(opts[:timeout_ms], @default_timeout_ms)

      _other ->
        @default_timeout_ms
    end
  end

  defp collect_results(results) do
    results
    |> Enum.reduce_while({:ok, []}, fn
      %{status: :ok, value: value}, {:ok, acc} ->
        {:cont, {:ok, [value | acc]}}

      %{status: :error, reason: reason}, {:ok, _acc} ->
        {:halt, {:error, reason}}
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      {:error, _reason} = error -> error
    end
  end

  defp item_result(item, {:ok, {:ok, value}}, _timeout_ms),
    do: %{item: item, status: :ok, value: value}

  defp item_result(item, {:ok, :ok}, _timeout_ms), do: %{item: item, status: :ok, value: :ok}

  defp item_result(item, {:ok, {:error, reason}}, _timeout_ms),
    do: %{item: item, status: :error, reason: reason}

  defp item_result(item, {:ok, value}, _timeout_ms), do: %{item: item, status: :ok, value: value}

  defp item_result(item, {:exit, :timeout}, timeout_ms),
    do: %{item: item, status: :error, reason: {:dispatcher_timeout, timeout_ms}}

  defp item_result(item, {:exit, reason}, _timeout_ms),
    do: %{item: item, status: :error, reason: {:dispatcher_task_exit, reason}}

  defp max_concurrency(opts) do
    opts
    |> Keyword.get(:max_concurrency, configured_max_concurrency())
    |> positive_integer(@default_max_concurrency)
  end

  defp timeout_ms(opts) do
    opts
    |> Keyword.get(:timeout_ms, configured_timeout_ms())
    |> positive_integer(@default_timeout_ms)
  end

  defp positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, default), do: default

  defp task_supervisor_name, do: Module.concat(__MODULE__, TaskSupervisor)
end
