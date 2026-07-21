defmodule FavnOrchestrator.ActiveManifestReconciler do
  @moduledoc """
  Re-registers persisted active manifests after a runner cache restart.

  Reconciliation is an admitted background mutation, never part of a readiness
  request. Each pass has one total deadline and is retried periodically, so a
  missing or restarting runner cannot block the orchestrator process.
  """

  use GenServer

  alias Favn.Manifest.Version
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunnerManifestRegistration
  alias FavnOrchestrator.RuntimeConfig

  @default_interval_ms 5_000
  @default_timeout_ms 5_000
  @minimum_stale_after_ms 15_000

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc "Returns the latest reconciled active-manifest registration snapshot."
  @spec snapshot(GenServer.server()) :: {:ok, map()} | {:error, term()}
  def snapshot(server \\ __MODULE__) do
    GenServer.call(server, :snapshot, 250)
  catch
    :exit, _reason -> {:error, :active_manifest_reconciliation_unavailable}
  end

  @doc "Invalidates the snapshot and schedules reconciliation after activation."
  @spec refresh(GenServer.server()) :: :ok
  def refresh(server \\ __MODULE__) do
    GenServer.call(server, :refresh, 250)
  catch
    :exit, _reason -> :ok
  end

  @impl true
  def init(opts) do
    runtime_config = Keyword.get_lazy(opts, :runtime_config, &RuntimeConfig.current/0)

    state = %{
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      timeout_ms: Keyword.get(opts, :timeout_ms, @default_timeout_ms),
      lifecycle: Keyword.get(opts, :lifecycle, Lifecycle),
      task_supervisor:
        Keyword.get(opts, :task_supervisor, FavnOrchestrator.RunManagerTaskSupervisor),
      workspace_ids: Keyword.get(opts, :workspace_ids, runtime_config.workspace_ids),
      runner_client: Keyword.get(opts, :runner_client, runtime_config.runner_client),
      runner_opts: Keyword.get(opts, :runner_opts, runtime_config.runner_client_opts),
      load_manifest: Keyword.get(opts, :load_manifest, &load_active_manifest/1),
      task: nil,
      result: {:error, :active_manifest_reconciliation_pending},
      checked_at_ms: nil,
      stale_after_ms:
        Keyword.get(
          opts,
          :stale_after_ms,
          max(Keyword.get(opts, :interval_ms, @default_interval_ms) * 3, @minimum_stale_after_ms)
        )
    }

    send(self(), :reconcile)
    {:ok, state}
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    result =
      cond do
        is_nil(state.checked_at_ms) ->
          {:error, :active_manifest_reconciliation_pending}

        System.monotonic_time(:millisecond) - state.checked_at_ms > state.stale_after_ms ->
          {:error, :active_manifest_reconciliation_stale}

        true ->
          state.result
      end

    {:reply, result, state}
  end

  def handle_call(:refresh, _from, state) do
    state = cancel_task(state)
    send(self(), :reconcile)

    {:reply, :ok,
     %{
       state
       | result: {:error, :active_manifest_reconciliation_pending},
         checked_at_ms: nil
     }}
  end

  @impl true
  def handle_info(:reconcile, %{task: nil} = state) do
    parent = self()
    token = make_ref()

    case Task.Supervisor.start_child(state.task_supervisor, fn ->
           result =
             Lifecycle.with_admission(
               fn -> reconcile(state) end,
               state.lifecycle
             )

           send(parent, {:reconcile_result, token, result})
         end) do
      {:ok, pid} ->
        monitor = Process.monitor(pid)
        timeout = Process.send_after(self(), {:reconcile_timeout, token}, state.timeout_ms)
        {:noreply, %{state | task: %{pid: pid, monitor: monitor, timeout: timeout, token: token}}}

      {:error, reason} ->
        _ = reason
        result = {:error, :active_manifest_reconciliation_task_unavailable}
        emit_completed(result, 0)

        {:noreply,
         state
         |> Map.merge(%{
           result: result,
           checked_at_ms: System.monotonic_time(:millisecond)
         })
         |> schedule()}
    end
  end

  def handle_info(:reconcile, state), do: {:noreply, state}

  def handle_info(
        {:reconcile_result, token, result},
        %{task: %{token: token} = task} = state
      ) do
    _ = Process.cancel_timer(task.timeout)
    Process.demonitor(task.monitor, [:flush])
    emit_completed(result, state.timeout_ms)

    {:noreply,
     state
     |> Map.merge(%{
       task: nil,
       result: result,
       checked_at_ms: System.monotonic_time(:millisecond)
     })
     |> schedule()}
  end

  def handle_info({:reconcile_timeout, token}, %{task: %{token: token} = task} = state) do
    Process.exit(task.pid, :kill)
    Process.demonitor(task.monitor, [:flush])
    result = {:error, :active_manifest_reconciliation_timeout}
    emit_completed(result, state.timeout_ms)

    {:noreply,
     state
     |> Map.merge(%{
       task: nil,
       result: result,
       checked_at_ms: System.monotonic_time(:millisecond)
     })
     |> schedule()}
  end

  def handle_info({:DOWN, monitor, :process, _pid, reason}, %{task: %{monitor: monitor}} = state) do
    _ = reason
    result = {:error, :active_manifest_reconciliation_failed}
    emit_completed(result, state.timeout_ms)

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

  defp reconcile(state) do
    summary =
      Enum.reduce(
        state.workspace_ids,
        %{checked: 0, aligned: 0, inactive: 0, failed: 0, manifests: []},
        fn
          workspace_id, summary ->
            reconcile_workspace(workspace_id, state, summary)
        end
      )

    summary = Map.update!(summary, :manifests, &Enum.reverse/1)
    if summary.failed == 0, do: {:ok, summary}, else: {:error, summary}
  end

  defp reconcile_workspace(workspace_id, state, summary) do
    summary = Map.update!(summary, :checked, &(&1 + 1))

    case state.load_manifest.(workspace_id) do
      {:ok, %Version{} = version} ->
        case RunnerManifestRegistration.ensure(state.runner_client, version, state.runner_opts) do
          :ok ->
            summary
            |> Map.update!(:aligned, &(&1 + 1))
            |> Map.update!(:manifests, fn manifests ->
              [
                %{
                  workspace_id: workspace_id,
                  manifest_version_id: version.manifest_version_id,
                  required_runner_release_id: version.required_runner_release_id,
                  runner_cache: :registered
                }
                | manifests
              ]
            end)

          {:error, _reason} ->
            Map.update!(summary, :failed, &(&1 + 1))
        end

      {:error, %Error{kind: :not_found}} ->
        Map.update!(summary, :inactive, &(&1 + 1))

      {:error, _reason} ->
        Map.update!(summary, :failed, &(&1 + 1))
    end
  end

  defp load_active_manifest(workspace_id) do
    workspace_id
    |> SystemContext.workspace(:active_manifest_reconciliation)
    |> ManifestStore.get_active_manifest()
  end

  defp schedule(state) do
    Process.send_after(self(), :reconcile, state.interval_ms)
    state
  end

  defp cancel_task(%{task: nil} = state), do: state

  defp cancel_task(%{task: task} = state) do
    _ = Process.cancel_timer(task.timeout)
    Process.exit(task.pid, :kill)
    Process.demonitor(task.monitor, [:flush])
    %{state | task: nil}
  end

  defp emit_completed(result, timeout_ms) do
    {status, summary} =
      case result do
        {:ok, summary} when is_map(summary) -> {:ok, summary}
        {:error, summary} when is_map(summary) -> {:error, summary}
        {:error, reason} -> {:error, %{reason: reason}}
      end

    measurements = %{
      checked: Map.get(summary, :checked, 0),
      aligned: Map.get(summary, :aligned, 0),
      failed: Map.get(summary, :failed, 0)
    }

    OperationalEvents.emit(
      :active_manifest_reconciliation_completed,
      measurements,
      %{status: status, timeout_ms: timeout_ms}
    )
  end
end
