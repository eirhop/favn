defmodule FavnOrchestrator.RunnerHealth do
  @moduledoc """
  Maintains one bounded, reusable runner diagnostic snapshot for readiness.

  Expensive adapter preflights run in a supervised task, never in an HTTP
  readiness request. Failed, timed-out, and stale snapshots fail closed.
  """

  use GenServer

  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.RunnerClientValidator
  alias FavnOrchestrator.RuntimeConfig

  @default_interval_ms 5_000
  @minimum_stale_after_ms 15_000

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc "Returns the latest bounded runner diagnostic result."
  @spec snapshot(GenServer.server()) :: {:ok, map()} | {:error, term()}
  def snapshot(server \\ __MODULE__) do
    GenServer.call(server, :snapshot, 250)
  catch
    :exit, _reason -> {:error, :runner_health_unavailable}
  end

  @doc "Invalidates the current result and schedules an immediate probe."
  @spec refresh(GenServer.server()) :: :ok
  def refresh(server \\ __MODULE__) do
    GenServer.call(server, :refresh, 250)
  catch
    :exit, _reason -> :ok
  end

  @impl true
  def init(opts) do
    runtime_config = Keyword.get_lazy(opts, :runtime_config, &RuntimeConfig.current/0)
    timeout_ms = diagnostics_timeout_ms(runtime_config.runner_client_opts)

    state = %{
      runner_client: Keyword.get(opts, :runner_client, runtime_config.runner_client),
      runner_opts: Keyword.get(opts, :runner_opts, runtime_config.runner_client_opts),
      lifecycle: Keyword.get(opts, :lifecycle, Lifecycle),
      task_supervisor:
        Keyword.get(opts, :task_supervisor, FavnOrchestrator.RunManagerTaskSupervisor),
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      timeout_ms: Keyword.get(opts, :timeout_ms, timeout_ms),
      stale_after_ms:
        Keyword.get(opts, :stale_after_ms, max(timeout_ms * 3, @minimum_stale_after_ms)),
      task: nil,
      result: {:error, :runner_diagnostics_pending},
      checked_at_ms: nil
    }

    send(self(), :probe)
    {:ok, state}
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    result =
      cond do
        is_nil(state.checked_at_ms) ->
          {:error, :runner_diagnostics_pending}

        System.monotonic_time(:millisecond) - state.checked_at_ms > state.stale_after_ms ->
          {:error, :runner_diagnostics_stale}

        true ->
          state.result
      end

    {:reply, result, state}
  end

  def handle_call(:refresh, _from, state) do
    state = cancel_task(state)
    send(self(), :probe)

    {:reply, :ok, %{state | result: {:error, :runner_diagnostics_pending}, checked_at_ms: nil}}
  end

  @impl true
  def handle_info(:probe, %{task: nil} = state) do
    if Lifecycle.ensure_accepting(state.lifecycle) == :ok do
      start_probe(state)
    else
      {:noreply, schedule(state)}
    end
  end

  def handle_info(:probe, state), do: {:noreply, state}

  def handle_info({:probe_result, token, result}, %{task: %{token: token} = task} = state) do
    _ = Process.cancel_timer(task.timeout)
    Process.demonitor(task.monitor, [:flush])
    result = normalize_result(result)
    emit_probe(result, task.started_at_ms)

    {:noreply,
     state
     |> Map.merge(%{
       task: nil,
       result: result,
       checked_at_ms: System.monotonic_time(:millisecond)
     })
     |> schedule()}
  end

  def handle_info({:probe_timeout, token}, %{task: %{token: token} = task} = state) do
    Process.exit(task.pid, :kill)
    Process.demonitor(task.monitor, [:flush])
    result = {:error, :runner_diagnostics_timeout}
    emit_probe(result, task.started_at_ms)

    {:noreply,
     state
     |> Map.merge(%{
       task: nil,
       result: result,
       checked_at_ms: System.monotonic_time(:millisecond)
     })
     |> schedule()}
  end

  def handle_info(
        {:DOWN, monitor, :process, _pid, _reason},
        %{task: %{monitor: monitor} = task} = state
      ) do
    result = {:error, :runner_diagnostics_failed}
    emit_probe(result, task.started_at_ms)

    {:noreply,
     state
     |> Map.merge(%{
       task: nil,
       result: result,
       checked_at_ms: System.monotonic_time(:millisecond)
     })
     |> schedule()}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp start_probe(state) do
    parent = self()
    token = make_ref()

    case Task.Supervisor.start_child(state.task_supervisor, fn ->
           send(parent, {:probe_result, token, probe(state.runner_client, state.runner_opts)})
         end) do
      {:ok, pid} ->
        monitor = Process.monitor(pid)
        timeout = Process.send_after(self(), {:probe_timeout, token}, state.timeout_ms)

        {:noreply,
         %{
           state
           | task: %{
               pid: pid,
               monitor: monitor,
               timeout: timeout,
               token: token,
               started_at_ms: System.monotonic_time(:millisecond)
             }
         }}

      {:error, _reason} ->
        {:noreply,
         state
         |> Map.merge(%{
           result: {:error, :runner_diagnostics_failed},
           checked_at_ms: System.monotonic_time(:millisecond)
         })
         |> schedule()}
    end
  end

  defp probe(runner_client, runner_opts) do
    with :ok <- RunnerClientValidator.validate(runner_client),
         true <- function_exported?(runner_client, :diagnostics, 1),
         {:ok, diagnostics} when is_map(diagnostics) <- runner_client.diagnostics(runner_opts) do
      {:ok, diagnostics}
    else
      false -> {:error, :runner_diagnostics_not_supported}
      {:error, reason} -> {:error, reason}
      _invalid -> {:error, :runner_client_not_available}
    end
  end

  defp normalize_result({:ok, diagnostics}) when is_map(diagnostics), do: {:ok, diagnostics}
  defp normalize_result({:error, reason}), do: {:error, reason}
  defp normalize_result(_invalid), do: {:error, :runner_diagnostics_failed}

  defp emit_probe(result, started_at_ms) do
    status = if match?({:ok, _diagnostics}, result), do: :ok, else: :error

    OperationalEvents.emit(
      :runner_diagnostic_completed,
      %{duration_ms: max(System.monotonic_time(:millisecond) - started_at_ms, 0)},
      %{status: status, result: diagnostic_result(result)}
    )
  end

  defp diagnostic_result({:ok, _diagnostics}), do: :ready_snapshot
  defp diagnostic_result({:error, reason}) when is_atom(reason), do: reason
  defp diagnostic_result({:error, _reason}), do: :runner_diagnostics_failed

  defp schedule(state) do
    Process.send_after(self(), :probe, state.interval_ms)
    state
  end

  defp cancel_task(%{task: nil} = state), do: state

  defp cancel_task(%{task: task} = state) do
    _ = Process.cancel_timer(task.timeout)
    Process.exit(task.pid, :kill)
    Process.demonitor(task.monitor, [:flush])
    %{state | task: nil}
  end

  defp diagnostics_timeout_ms(opts) do
    case Keyword.get(opts, :runner_diagnostics_timeout_ms, 5_000) do
      timeout_ms when is_integer(timeout_ms) and timeout_ms > 0 -> timeout_ms + 250
      _invalid -> 5_250
    end
  end
end
