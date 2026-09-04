defmodule FavnLocal.DevelopmentRuntime do
  @moduledoc """
  Owns source-development runner processes using the production registration,
  pull, result, and drain protocol.

  Unchanged reloads skip deployment; manifest-only reloads keep the runner.
  Changed releases overlap: the candidate registers before activation and the
  previous runner remains available until its exact release has no work.
  """

  use GenServer

  alias Favn.Manifest.Publication
  alias FavnLocal.Config
  alias FavnLocal.Locator
  alias FavnLocal.Publication, as: LocalPublication
  alias FavnLocal.ReloadResult
  alias FavnLocal.RunnerProcessLauncher
  alias FavnOrchestrator.Persistence.Queries, as: Q
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunnerRegistry

  @probe_interval_ms 100
  @runner_drain_probe_interval_ms 50
  @runner_drain_timeout_ms 60_000
  @default_runner_start_timeout_ms 30_000
  @runner_stop_timeout_ms 15_000
  @request_timeout_ms 120_000
  @runner_crash_window_ms 60_000
  @runner_crash_budget 5

  @type status :: :starting | :ready | :reloading | :stopping | :failed

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def await_ready(timeout_ms \\ @request_timeout_ms)
      when is_integer(timeout_ms) and timeout_ms > 0 do
    GenServer.call(__MODULE__, {:await_ready, now_ms() + timeout_ms}, timeout_ms + 250)
  catch
    :exit, {:timeout, _call} -> {:error, {:startup_timeout, :coordinator}}
    :exit, _reason -> {:error, {:startup_unavailable, :coordinator}}
  end

  @doc "Reloads within a bounded caller wait; a timeout does not cancel activation."
  @spec reload(Publication.t(), String.t(), timeout()) ::
          {:ok, ReloadResult.t()} | {:error, term()}
  def reload(%Publication{} = publication, runner_release_id, timeout_ms \\ @request_timeout_ms)
      when is_binary(runner_release_id),
      do: GenServer.call(__MODULE__, {:reload, publication, runner_release_id}, timeout_ms)

  def stop(timeout_ms \\ @request_timeout_ms),
    do: GenServer.call(__MODULE__, :stop, timeout_ms)

  def status, do: GenServer.call(__MODULE__, :status, 1_000)

  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    publication = Keyword.fetch!(opts, :publication)

    with :ok <- ensure_local_capacity_partition(config.runner_release_id),
         :ok <- Locator.write(config, config.runner_release_id),
         {:ok, runner} <- RunnerProcessLauncher.start(config, config.runner_release_id) do
      schedule(:probe_runner, @probe_interval_ms)
      schedule(:startup_deadline, runner_start_timeout_ms())

      {:ok,
       %{
         config: config,
         runner: runner,
         candidate: nil,
         retiring: nil,
         publication: publication,
         deployment: nil,
         status: :starting,
         startup_action: :deploy,
         deadline: now_ms() + runner_start_timeout_ms(),
         ready_waiters: [],
         request: nil,
         task: nil,
         pending_deployment: nil,
         stopping_ports: MapSet.new(),
         ignored_ports: MapSet.new(),
         runner_exits: [],
         failure: nil
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:await_ready, _deadline}, _from, %{status: :ready} = state),
    do: {:reply, {:ok, summary(state)}, state}

  def handle_call({:await_ready, deadline}, from, %{status: status} = state)
      when status in [:starting, :reloading] do
    deadline = min(deadline, state.deadline)
    schedule(:startup_deadline, max(deadline - now_ms(), 0))
    {:noreply, %{state | deadline: deadline, ready_waiters: [from | state.ready_waiters]}}
  end

  def handle_call({:await_ready, _deadline}, _from, state),
    do: {:reply, {:error, state.failure || :not_ready}, state}

  def handle_call({:reload, _publication, _release_id}, _from, %{retiring: retiring} = state)
      when not is_nil(retiring),
      do: {:reply, {:error, :runner_still_draining}, state}

  def handle_call(
        {:reload, publication, release_id},
        from,
        %{status: :ready, runner: %{release_id: release_id}} = state
      ) do
    task =
      Task.Supervisor.async_nolink(FavnLocal.TaskSupervisor, fn ->
        LocalPublication.reload(
          publication,
          state.publication,
          state.deployment,
          state.config.workspace_id
        )
      end)

    {:noreply,
     %{state | status: :reloading, request: {from, publication, release_id}, task: task}}
  end

  def handle_call({:reload, publication, release_id}, from, %{status: :ready} = state) do
    with :ok <- ensure_local_capacity_partition(release_id),
         {:ok, candidate} <- RunnerProcessLauncher.start(state.config, release_id) do
      schedule(:probe_runner, @probe_interval_ms)

      {:noreply,
       %{
         state
         | candidate: candidate,
           status: :reloading,
           request: {from, publication, release_id},
           deadline: now_ms() + runner_start_timeout_ms()
       }}
    else
      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:reload, _publication, _release_id}, _from, state),
    do: {:reply, {:error, {:lifecycle_not_ready, state.status}}, state}

  def handle_call(:stop, _from, %{status: :stopping} = state), do: {:reply, :ok, state}

  def handle_call(:stop, from, state) do
    reason =
      if state.task, do: {:reload_outcome_unknown, :reload_interrupted}, else: :reload_interrupted

    reply_request(state, {:error, reason})
    state = detach_task(state)
    _ = FavnOrchestrator.drain()
    runners = [state.runner, state.candidate, state.retiring] |> Enum.reject(&is_nil/1)
    Enum.each(runners, &stop_runner/1)
    ports = MapSet.new(runners, & &1.port)

    if MapSet.size(ports) == 0 do
      start_shutdown(%{state | status: :stopping, request: {:stop, from}})
    else
      {:noreply, %{state | status: :stopping, request: {:stop, from}, stopping_ports: ports}}
    end
  end

  def handle_call(:status, _from, state), do: {:reply, summary(state), state}

  @impl true
  def handle_info(:startup_deadline, %{status: status, retiring: nil} = state)
      when status == :starting do
    if now_ms() >= state.deadline do
      phase = if state.task, do: :deployment, else: :registration
      fail(state, {:startup_timeout, phase})
    else
      {:noreply, state}
    end
  end

  def handle_info(:probe_runner, %{status: :starting} = state) do
    case RunnerProcessLauncher.refresh_registration(state.runner) do
      {:ok, runner} when state.startup_action == :deploy ->
        start_deployment(%{state | runner: runner})

      {:ok, runner} ->
        ready_after_recovery(%{state | runner: runner})

      :not_ready ->
        if now_ms() >= state.deadline do
          fail(state, start_failure(state, state.runner))
        else
          schedule(:probe_runner, @probe_interval_ms)
          {:noreply, state}
        end
    end
  end

  def handle_info(:probe_runner, %{status: :reloading, candidate: candidate} = state)
      when not is_nil(candidate) do
    case RunnerProcessLauncher.refresh_registration(candidate) do
      {:ok, candidate} ->
        start_deployment(%{state | candidate: candidate})

      :not_ready ->
        if now_ms() >= state.deadline do
          abort_candidate(state, start_failure(state, candidate))
        else
          schedule(:probe_runner, @probe_interval_ms)
          {:noreply, state}
        end
    end
  end

  def handle_info(:probe_retiring, %{status: :reloading, retiring: retiring} = state)
      when not is_nil(retiring) do
    cond do
      safe_to_stop?(retiring) ->
        stop_runner(retiring)
        {:noreply, state}

      now_ms() >= state.deadline ->
        complete_reload(state, {:warning, :old_runner_drain_timeout})

      true ->
        schedule(:probe_retiring, @runner_drain_probe_interval_ms)
        {:noreply, state}
    end
  end

  def handle_info(:probe_retiring, %{status: :ready, retiring: retiring} = state)
      when not is_nil(retiring) do
    if safe_to_stop?(retiring) do
      stop_runner(retiring)
    else
      schedule(:probe_retiring, @runner_drain_probe_interval_ms)
    end

    {:noreply, state}
  end

  # Runner output is written twice on purpose. The terminal is where an operator
  # is looking; the log file is what survives, and a runner that dies during boot
  # takes the terminal down with it. Output is never dropped for being from a port
  # we no longer track — that output is exactly the interesting kind.
  def handle_info({port, {:data, bytes}}, state) when is_port(port) do
    IO.write(bytes)
    append_runner_log(state, bytes)
    {:noreply, state}
  end

  def handle_info({port, {:exit_status, status}}, state) when is_port(port),
    do: runner_exited(state, port, status)

  def handle_info({:runner_stop_timeout, port}, %{status: :stopping} = state) do
    if MapSet.member?(state.stopping_ports, port) do
      close_port(port)
      runner_exited(state, port, :timeout)
    else
      {:noreply, state}
    end
  end

  def handle_info({:runner_stop_timeout, port}, state) do
    cond do
      MapSet.member?(state.ignored_ports, port) ->
        close_port(port)
        {:noreply, state}

      managed_port?(state, port) ->
        close_port(port)

        state
        |> runner_exited(port, :timeout)
        |> ignore_late_exit(port)

      true ->
        {:noreply, state}
    end
  end

  def handle_info({ref, result}, %{task: %{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    deployment_finished(%{state | task: nil}, result)
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %{status: :reloading, task: %{ref: ref}} = state
      ),
      do: abort_candidate(state, {:deployment_task_failed, reason})

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task: %{ref: ref}} = state),
    do: deployment_finished(%{state | task: nil}, {:error, {:deployment_task_failed, reason}})

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    if state.status == :starting, do: stop_deployment(state)

    [state.runner, state.candidate, state.retiring]
    |> Enum.reject(&is_nil/1)
    |> Enum.each(&RunnerProcessLauncher.stop/1)

    Config.clear_source_development_auth()
    Locator.delete(state.config.root_dir)
    :ok
  end

  defp start_deployment(state) do
    publication =
      case state.request do
        {_from, publication, _release_id} -> publication
        _none -> state.publication
      end

    schedule(:startup_deadline, max(state.deadline - now_ms(), 0))

    task =
      Task.Supervisor.async_nolink(FavnLocal.TaskSupervisor, fn ->
        LocalPublication.deploy(publication, state.config.workspace_id)
      end)

    {:noreply, %{state | task: task}}
  end

  defp deployment_finished(%{status: :starting} = state, {:ok, deployment}) do
    :ok = Locator.write(state.config, state.runner.release_id)
    ready_state = ready_state(%{state | deployment: deployment})
    reply_waiters(state.ready_waiters, {:ok, Map.merge(deployment, summary(ready_state))})
    {:noreply, ready_state}
  end

  defp deployment_finished(
         %{status: :reloading, candidate: nil} = state,
         {:ok, %{reload_status: classification} = deployment}
       ) do
    ready = state |> commit_deployment(deployment) |> ready_state()
    response = ReloadResult.new(classification, deployment, summary(ready))
    reply_request(state, {:ok, response})
    reply_waiters(state.ready_waiters, {:ok, response})
    {:noreply, ready}
  end

  defp deployment_finished(
         %{status: :reloading, candidate: candidate, runner: old_runner} = state,
         {:ok, deployment}
       )
       when not is_nil(candidate) do
    state = commit_deployment(state, deployment)

    state = %{
      state
      | runner: candidate,
        candidate: nil,
        retiring: old_runner,
        pending_deployment: deployment,
        deadline: now_ms() + @runner_drain_timeout_ms
    }

    schedule(:probe_retiring, 0)
    {:noreply, state}
  end

  defp deployment_finished(%{status: :reloading} = state, {:error, reason}),
    do: abort_candidate(state, reason)

  defp deployment_finished(%{status: :stopping, request: {:stop, from}} = state, :ok) do
    Locator.delete(state.config.root_dir)
    GenServer.reply(from, :ok)
    {:stop, :normal, %{state | task: nil}}
  end

  defp deployment_finished(state, {:error, reason}), do: fail(state, reason)

  defp runner_exited(%{status: :stopping} = state, port, _status) do
    if MapSet.member?(state.stopping_ports, port) do
      ports = MapSet.delete(state.stopping_ports, port)

      if MapSet.size(ports) == 0,
        do: start_shutdown(%{state | stopping_ports: ports}),
        else: {:noreply, %{state | stopping_ports: ports}}
    else
      {:noreply, state}
    end
  end

  defp runner_exited(state, port, status) when is_port(port) do
    if MapSet.member?(state.ignored_ports, port) do
      {:noreply, %{state | ignored_ports: MapSet.delete(state.ignored_ports, port)}}
    else
      runner_exited_managed(state, port, status)
    end
  end

  defp runner_exited_managed(
         %{status: :reloading, retiring: %{port: port}} = state,
         port,
         status
       ) do
    retiring = state.retiring
    state = %{state | retiring: nil}

    if durable_release_drained?(retiring) do
      complete_reload(state, :drained)
    else
      restart_retiring(state, retiring, status)
    end
  end

  defp runner_exited_managed(
         %{status: :ready, retiring: %{port: port}} = state,
         port,
         status
       ) do
    retiring = state.retiring
    state = %{state | retiring: nil}

    if durable_release_drained?(retiring) do
      {:noreply, state}
    else
      restart_retiring(state, retiring, status)
    end
  end

  defp runner_exited_managed(%{candidate: %{port: port}} = state, port, _status),
    do: abort_candidate(%{state | candidate: nil}, :candidate_runner_exited)

  # A runner that keeps dying will not be fixed by another silent relaunch.
  # Budgeting the restarts turns an invisible crash loop into an explicit
  # failure that points the operator at the runner log.
  defp runner_exited_managed(%{runner: %{port: port}, status: :ready} = state, port, status) do
    case runner_crash_budget_state(state.runner_exits, now_ms()) do
      {:fail, exits} ->
        fail(
          %{state | runner_exits: exits},
          {:runner_crash_loop,
           %{
             exits_in_window: length(exits) + 1,
             window_ms: @runner_crash_window_ms,
             last_exit_status: status,
             runner_log: runner_log_path(state)
           }}
        )

      {:continue, exits} ->
        case RunnerProcessLauncher.start(state.config, state.runner.release_id) do
          {:ok, runner} ->
            schedule(:probe_runner, @probe_interval_ms)

            {:noreply,
             %{
               state
               | runner: runner,
                 status: :starting,
                 startup_action: :recover,
                 deadline: now_ms() + runner_start_timeout_ms(),
                 runner_exits: exits
             }}

          {:error, reason} ->
            fail(state, {:runner_crashed, reason})
        end
    end
  end

  defp runner_exited_managed(state, _port, status),
    do: fail(state, {:runner_exited, status})

  @doc false
  @spec runner_crash_budget_state([integer()], integer()) ::
          {:continue | :fail, [integer()]}
  def runner_crash_budget_state(exits, now_ms) do
    recent = Enum.filter(exits, &(&1 > now_ms - @runner_crash_window_ms))

    if length(recent) >= @runner_crash_budget,
      do: {:fail, recent},
      else: {:continue, [now_ms | recent]}
  end

  defp restart_retiring(state, retiring, exit_status) do
    case RunnerProcessLauncher.start(state.config, retiring.release_id) do
      {:ok, restarted} ->
        schedule(:probe_retiring, @probe_interval_ms)
        {:noreply, %{state | retiring: restarted}}

      {:error, reason} ->
        fail(state, {:retiring_runner_restart_failed, exit_status, reason})
    end
  end

  defp complete_reload(state, drain_status) do
    :ok = Locator.write(state.config, state.runner.release_id)
    ready = ready_state(state)

    response =
      ReloadResult.new(:runner_replaced, state.pending_deployment, summary(ready), drain_status)

    case state.request do
      {from, %Publication{}, _release_id} -> GenServer.reply(from, {:ok, response})
      _none -> :ok
    end

    if drain_status == {:warning, :old_runner_drain_timeout},
      do: schedule(:probe_retiring, @runner_drain_probe_interval_ms)

    reply_waiters(state.ready_waiters, {:ok, response})
    {:noreply, ready}
  end

  defp abort_candidate(state, reason) do
    if state.candidate, do: stop_runner(state.candidate)

    ignored_ports =
      if state.candidate,
        do: MapSet.put(state.ignored_ports, state.candidate.port),
        else: state.ignored_ports

    unknown? = not is_nil(state.task) or match?({:reload_outcome_unknown, _}, reason)

    reason =
      if unknown? and not match?({:reload_outcome_unknown, _}, reason),
        do: {:reload_outcome_unknown, reason},
        else: reason

    reply_request(state, {:error, reason})
    reply_waiters(state.ready_waiters, {:error, reason})
    state = detach_task(state)

    {:noreply,
     %{
       state
       | candidate: nil,
         status: if(unknown?, do: :failed, else: :ready),
         ignored_ports: ignored_ports,
         request: nil,
         deadline: nil,
         ready_waiters: [],
         failure: if(unknown?, do: reason)
     }}
  end

  defp commit_deployment(%{request: {_from, publication, _release_id}} = state, deployment),
    do: %{state | publication: publication, deployment: deployment}

  defp detach_task(%{task: nil} = state), do: state

  defp detach_task(state) do
    Process.demonitor(state.task.ref, [:flush])
    %{state | task: nil}
  end

  defp reply_request(%{request: {from, %Publication{}, _release_id}}, reply),
    do: GenServer.reply(from, reply)

  defp reply_request(_state, _reply), do: :ok

  defp ready_after_recovery(state) do
    ready = ready_state(state)
    reply_waiters(state.ready_waiters, {:ok, summary(ready)})
    {:noreply, ready}
  end

  defp ready_state(state) do
    %{
      state
      | status: :ready,
        startup_action: :deploy,
        request: nil,
        task: nil,
        pending_deployment: nil,
        deadline: nil,
        ready_waiters: [],
        failure: nil
    }
  end

  defp safe_to_stop?(runner) do
    context = SystemContext.platform(:favn_local_runner_drain, roles: [:platform_operator])

    drain =
      Persistence.stores().runner_tasks.release_drain(%Q.GetRunnerReleaseDrain{
        platform_context: context,
        runner_pool: "default",
        required_runner_release_id: runner.release_id
      })

    session_idle? =
      case RunnerRegistry.fetch(runner.runner_instance_id) do
        {:ok, %{status: :idle}} -> true
        {:error, :runner_session_not_found} -> true
        _other -> false
      end

    match?({:ok, %{durable_drained?: true}}, drain) and session_idle?
  end

  defp durable_release_drained?(runner) do
    context = SystemContext.platform(:favn_local_runner_drain, roles: [:platform_operator])

    match?(
      {:ok, %{durable_drained?: true}},
      Persistence.stores().runner_tasks.release_drain(%Q.GetRunnerReleaseDrain{
        platform_context: context,
        runner_pool: "default",
        required_runner_release_id: runner.release_id
      })
    )
  end

  defp ensure_local_capacity_partition(release_id) do
    context = SystemContext.platform(:favn_local_runner_partition, roles: [:platform_operator])

    case Persistence.stores().runner_tasks.ensure_demand(%C.EnsureRunnerCapacityDemand{
           platform_context: context,
           runner_pool: "default",
           required_runner_release_id: release_id,
           occurred_at: DateTime.utc_now()
         }) do
      {:ok, _demand} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp stop_runner(runner) do
    runner =
      case RunnerProcessLauncher.refresh_registration(runner) do
        {:ok, registered} -> registered
        :not_ready -> runner
      end

    :ok = RunnerProcessLauncher.stop(runner)
    schedule({:runner_stop_timeout, runner.port}, @runner_stop_timeout_ms)
  end

  defp start_shutdown(%{request: {:stop, from}} = state) do
    task =
      Task.Supervisor.async_nolink(FavnLocal.TaskSupervisor, fn ->
        _ = Application.stop(:favn_view)
        _ = Application.stop(:favn_orchestrator)
        :ok
      end)

    {:noreply, %{state | task: task, request: {:stop, from}}}
  end

  defp stop_deployment(%{task: %Task{} = task} = state) do
    Task.shutdown(task, :brutal_kill)
    %{state | task: nil}
  end

  defp stop_deployment(state), do: state

  defp fail(state, reason) do
    reason =
      if state.status == :reloading and state.task,
        do: {:reload_outcome_unknown, reason},
        else: reason

    state =
      if state.status == :starting do
        state = stop_deployment(state)
        Enum.each([state.runner, state.candidate] |> Enum.reject(&is_nil/1), &stop_runner/1)
        state
      else
        detach_task(state)
      end

    reply_waiters(state.ready_waiters, {:error, reason})

    case state.request do
      {:stop, from} -> GenServer.reply(from, {:error, reason})
      {from, %Publication{}, _release_id} -> GenServer.reply(from, {:error, reason})
      _none -> :ok
    end

    {:noreply,
     %{
       state
       | status: :failed,
         failure: reason,
         ready_waiters: [],
         request: nil,
         task: nil
     }}
  end

  # Starting the runner means booting a second BEAM and loading the project's code
  # paths. Thirty seconds is generous on a native filesystem and not always enough
  # through a bind mount, where the whole stack then tears itself down. Overridable
  # so a slow environment can say so rather than be told it is broken.
  defp runner_start_timeout_ms do
    case Integer.parse(System.get_env("FAVN_DEV_RUNNER_START_TIMEOUT_MS", "")) do
      {milliseconds, ""} when milliseconds > 0 -> milliseconds
      _unset_or_invalid -> @default_runner_start_timeout_ms
    end
  end

  # "The runner never registered" has two causes that need different answers, and
  # reporting both as a timeout sent an operator looking for a slow machine when
  # the runner had already died. A closed port means the OS process is gone.
  defp start_failure(state, %{port: port}) do
    if is_nil(Port.info(port)),
      do: {:runner_exited_before_ready, runner_log_path(state)},
      else: {:runner_start_timeout, runner_log_path(state)}
  end

  defp start_failure(state, _runner), do: {:runner_start_timeout, runner_log_path(state)}

  defp runner_log_path(%{config: config}), do: Locator.runner_log_path(config.root_dir)

  defp append_runner_log(state, bytes) do
    path = runner_log_path(state)

    with :ok <- File.mkdir_p(Path.dirname(path)) do
      File.write(path, bytes, [:append])
    end
  catch
    _kind, _reason -> :ok
  end

  defp managed_port?(state, port) do
    MapSet.member?(state.ignored_ports, port) or
      Enum.any?([state.runner, state.candidate, state.retiring], fn
        %{port: ^port} -> true
        _other -> false
      end)
  end

  defp close_port(port) do
    Port.close(port)
  catch
    :error, :badarg -> :ok
  end

  defp ignore_late_exit({:noreply, state}, port),
    do: {:noreply, %{state | ignored_ports: MapSet.put(state.ignored_ports, port)}}

  defp reply_waiters(waiters, reply), do: Enum.each(waiters, &GenServer.reply(&1, reply))
  defp schedule(message, delay), do: Process.send_after(self(), message, delay)

  defp summary(state) do
    %{
      status: state.status,
      operator_node: state.config.operator_node,
      runner_node: state.runner.node,
      runner_release_id: state.runner.release_id,
      workspace_id: state.config.workspace_id,
      view_url: "http://127.0.0.1:#{state.config.view_port}",
      orchestrator_url: "http://127.0.0.1:#{state.config.orchestrator_port}"
    }
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
