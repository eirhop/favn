defmodule FavnOrchestrator.BoundedDispatcher do
  @moduledoc """
  Supervised bounded task dispatch for orchestrator control-plane submissions.

  The dispatcher is intentionally small: callers own durable intent and lifecycle
  state, while this module provides a supervised execution boundary with explicit
  concurrency limits. Actual run creation must still go through the public
  orchestrator submission APIs.
  """

  @default_max_concurrency 4
  @default_timeout :infinity

  @type dispatch_result(value) :: {:ok, [value]} | {:error, term()}

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
  error stops result collection and is returned to the caller. Already-started
  tasks are still owned by the task supervisor.
  """
  @spec run_many(Enumerable.t(), (term() -> {:ok, value} | {:error, term()} | value), keyword()) ::
          dispatch_result(value | :ok)
        when value: term()
  def run_many(items, worker, opts \\ []) when is_function(worker, 1) and is_list(opts) do
    items = Enum.to_list(items)

    if items == [] do
      {:ok, []}
    else
      supervisor = Keyword.get(opts, :supervisor, task_supervisor_name())
      max_concurrency = max_concurrency(opts)
      timeout = Keyword.get(opts, :timeout, @default_timeout)

      supervisor
      |> Task.Supervisor.async_stream_nolink(items, worker,
        max_concurrency: max_concurrency,
        ordered: true,
        timeout: timeout,
        on_timeout: :kill_task
      )
      |> Enum.reduce_while({:ok, []}, &collect_result/2)
      |> case do
        {:ok, values} -> {:ok, Enum.reverse(values)}
        {:error, _reason} = error -> error
      end
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

  defp collect_result({:ok, {:ok, value}}, {:ok, acc}), do: {:cont, {:ok, [value | acc]}}
  defp collect_result({:ok, :ok}, {:ok, acc}), do: {:cont, {:ok, [:ok | acc]}}
  defp collect_result({:ok, {:error, reason}}, {:ok, _acc}), do: {:halt, {:error, reason}}
  defp collect_result({:ok, value}, {:ok, acc}), do: {:cont, {:ok, [value | acc]}}

  defp collect_result({:exit, reason}, {:ok, _acc}),
    do: {:halt, {:error, {:dispatcher_task_exit, reason}}}

  defp max_concurrency(opts) do
    opts
    |> Keyword.get(:max_concurrency, configured_max_concurrency())
    |> positive_integer(@default_max_concurrency)
  end

  defp positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, default), do: default

  defp task_supervisor_name, do: Module.concat(__MODULE__, TaskSupervisor)
end
