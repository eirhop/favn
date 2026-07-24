defmodule FavnLocal.Lifecycle do
  @moduledoc """
  Owns the Docker-free source-development runner lifecycle.

  The lifecycle owns exactly one runner OS process. Compilation and manifest
  construction happen in the invoking Mix command before a reload reaches this
  process.
  """

  use GenServer

  alias Favn.Manifest.Publication
  alias FavnLocal.Locator
  alias FavnLocal.Publication, as: LocalPublication
  alias FavnLocal.RunnerChild

  @probe_interval_ms 100
  @runner_start_timeout_ms 30_000
  @runner_stop_timeout_ms 15_000
  @request_timeout_ms 60_000

  @type status :: :starting | :ready | :reloading | :stopping | :failed

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec await_ready(timeout()) :: {:ok, map()} | {:error, term()}
  def await_ready(timeout_ms \\ @request_timeout_ms) do
    GenServer.call(__MODULE__, :await_ready, timeout_ms)
  end

  @spec reload(Publication.t(), String.t(), timeout()) :: {:ok, map()} | {:error, term()}
  def reload(%Publication{} = publication, runner_release_id, timeout_ms \\ @request_timeout_ms)
      when is_binary(runner_release_id) do
    GenServer.call(__MODULE__, {:reload, publication, runner_release_id}, timeout_ms)
  end

  @spec stop(timeout()) :: :ok | {:error, term()}
  def stop(timeout_ms \\ @request_timeout_ms) do
    GenServer.call(__MODULE__, :stop, timeout_ms)
  end

  @spec status() :: map()
  def status, do: GenServer.call(__MODULE__, :status, 1_000)

  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    publication = Keyword.fetch!(opts, :publication)

    with :ok <- Locator.write(config, config.runner_release_id),
         {:ok, runner} <- RunnerChild.start(config, config.runner_release_id) do
      deadline = now_ms() + @runner_start_timeout_ms
      Process.send_after(self(), :probe_runner, @probe_interval_ms)

      {:ok,
       %{
         config: config,
         runner: runner,
         publication: publication,
         status: :starting,
         deadline: deadline,
         ready_waiters: [],
         request: nil,
         maintenance_token: nil,
         task: nil,
         failure: nil
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:await_ready, _from, %{status: :ready} = state) do
    {:reply, {:ok, summary(state)}, state}
  end

  def handle_call(:await_ready, from, %{status: status} = state)
      when status in [:starting, :reloading] do
    {:noreply, %{state | ready_waiters: [from | state.ready_waiters]}}
  end

  def handle_call(:await_ready, _from, state),
    do: {:reply, {:error, state.failure || :not_ready}, state}

  def handle_call({:reload, publication, runner_release_id}, from, %{status: :ready} = state) do
    token = random_token()

    with {:ok, ^token} <- FavnOrchestrator.begin_runner_replacement(token),
         %{active_admissions: 0} <- FavnOrchestrator.runner_replacement_status() do
      :ok = RunnerChild.stop(state.runner)

      Process.send_after(
        self(),
        {:runner_stop_timeout, state.runner.port},
        @runner_stop_timeout_ms
      )

      {:noreply,
       %{
         state
         | status: :reloading,
           request: {from, publication, runner_release_id},
           maintenance_token: token,
           deadline: now_ms() + @runner_start_timeout_ms
       }}
    else
      %{active_admissions: count} when is_integer(count) and count > 0 ->
        _ = FavnOrchestrator.finish_runner_replacement(token)
        {:reply, {:error, {:runs_in_flight, count}}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:reload, _publication, _runner_release_id}, _from, state),
    do: {:reply, {:error, {:lifecycle_not_ready, state.status}}, state}

  def handle_call(:stop, _from, %{status: :stopping} = state),
    do: {:reply, :ok, state}

  def handle_call(:stop, from, state) do
    _ = FavnOrchestrator.drain()
    :ok = RunnerChild.stop(state.runner)
    Process.send_after(self(), {:runner_stop_timeout, state.runner.port}, @runner_stop_timeout_ms)
    {:noreply, %{state | status: :stopping, request: {:stop, from}}}
  end

  def handle_call(:status, _from, state), do: {:reply, summary(state), state}

  @impl true
  def handle_info(:probe_runner, %{status: status} = state)
      when status in [:starting, :reloading] do
    cond do
      RunnerChild.ready?(state.runner) ->
        start_deployment(state)

      now_ms() >= state.deadline ->
        fail(state, :runner_start_timeout)

      true ->
        Process.send_after(self(), :probe_runner, @probe_interval_ms)
        {:noreply, state}
    end
  end

  def handle_info({port, {:data, bytes}}, %{runner: %{port: port}} = state) do
    IO.write(bytes)
    {:noreply, state}
  end

  def handle_info({port, {:exit_status, status}}, %{runner: %{port: port}} = state) do
    runner_exited(state, status)
  end

  def handle_info({:runner_stop_timeout, port}, %{runner: %{port: port}} = state) do
    _ = Port.close(port)
    runner_exited(state, :timeout)
  catch
    :error, :badarg -> runner_exited(state, :timeout)
  end

  def handle_info({ref, result}, %{task: %{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    deployment_finished(state, result)
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task: %{ref: ref}} = state) do
    deployment_finished(state, {:error, {:deployment_task_failed, reason}})
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    finish_runner_replacement(state.maintenance_token)
    Locator.delete(state.config.root_dir)
    :ok
  end

  defp runner_exited(
         %{status: :reloading, request: {from, publication, release_id}} = state,
         _status
       ) do
    case RunnerChild.start(state.config, release_id) do
      {:ok, runner} ->
        Process.send_after(self(), :probe_runner, @probe_interval_ms)

        {:noreply,
         %{
           state
           | runner: runner,
             publication: publication,
             request: {from, publication, release_id},
             deadline: now_ms() + @runner_start_timeout_ms
         }}

      {:error, reason} ->
        fail(state, reason)
    end
  end

  defp runner_exited(%{status: :stopping} = state, _status), do: start_shutdown(state)

  defp runner_exited(%{status: :ready} = state, status) do
    case RunnerChild.start(state.config, state.runner.release_id) do
      {:ok, runner} ->
        Process.send_after(self(), :probe_runner, @probe_interval_ms)

        {:noreply,
         %{
           state
           | runner: runner,
             status: :starting,
             deadline: now_ms() + @runner_start_timeout_ms
         }}

      {:error, reason} ->
        fail(state, {:runner_crashed, status, reason})
    end
  end

  defp runner_exited(state, status), do: fail(state, {:runner_exited, status})

  defp start_deployment(state) do
    token = state.maintenance_token
    workspace_id = state.config.workspace_id
    publication = state.publication

    task =
      Task.Supervisor.async_nolink(FavnLocal.TaskSupervisor, fn ->
        LocalPublication.deploy(publication, workspace_id, token)
      end)

    {:noreply, %{state | task: task}}
  end

  defp deployment_finished(%{status: :starting} = state, {:ok, deployment}) do
    :ok = Locator.write(state.config, state.runner.release_id)

    ready_state = %{
      state
      | status: :ready,
        task: nil,
        ready_waiters: [],
        failure: nil
    }

    reply_waiters(state.ready_waiters, {:ok, Map.merge(summary(ready_state), deployment)})
    {:noreply, ready_state}
  end

  defp deployment_finished(
         %{status: :reloading, request: {from, _publication, _release_id}} = state,
         {:ok, deployment}
       ) do
    :ok = Locator.write(state.config, state.runner.release_id)
    :ok = FavnOrchestrator.finish_runner_replacement(state.maintenance_token)
    GenServer.reply(from, {:ok, deployment})
    reply_waiters(state.ready_waiters, {:ok, Map.merge(summary(state), deployment)})

    {:noreply,
     %{
       state
       | status: :ready,
         request: nil,
         maintenance_token: nil,
         task: nil,
         ready_waiters: [],
         failure: nil
     }}
  end

  defp deployment_finished(%{status: :stopping, request: {:stop, from}} = state, :ok) do
    Locator.delete(state.config.root_dir)
    GenServer.reply(from, :ok)
    {:stop, :normal, %{state | task: nil}}
  end

  defp deployment_finished(state, {:error, reason}), do: fail(%{state | task: nil}, reason)

  defp start_shutdown(%{request: {:stop, from}} = state) do
    task =
      Task.Supervisor.async_nolink(FavnLocal.TaskSupervisor, fn ->
        _ = Application.stop(:favn_view)
        _ = Application.stop(:favn_orchestrator)
        :ok
      end)

    {:noreply, %{state | task: task, request: {:stop, from}}}
  end

  defp fail(%{status: :reloading} = state, reason) do
    finish_runner_replacement(state.maintenance_token)
    reply_waiters(state.ready_waiters, {:error, reason})

    case state.request do
      {from, %Publication{}, _release_id} -> GenServer.reply(from, {:error, reason})
      _none -> :ok
    end

    _ = RunnerChild.stop(state.runner)
    _ = Application.stop(:favn_view)
    _ = Application.stop(:favn_orchestrator)
    Locator.delete(state.config.root_dir)

    {:stop, {:shutdown, {:reload_failed, reason}},
     %{
       state
       | status: :failed,
         failure: reason,
         ready_waiters: [],
         request: nil,
         maintenance_token: nil,
         task: nil
     }}
  end

  defp fail(state, reason) do
    reply_waiters(state.ready_waiters, {:error, reason})

    case state.request do
      {:stop, from} -> GenServer.reply(from, {:error, reason})
      _none -> :ok
    end

    {:noreply,
     %{state | status: :failed, failure: reason, ready_waiters: [], request: nil, task: nil}}
  end

  defp finish_runner_replacement(nil), do: :ok

  defp finish_runner_replacement(token) when is_binary(token) do
    _ = FavnOrchestrator.finish_runner_replacement(token)
    :ok
  end

  defp reply_waiters(waiters, reply), do: Enum.each(waiters, &GenServer.reply(&1, reply))

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

  defp random_token, do: 32 |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)
  defp now_ms, do: System.monotonic_time(:millisecond)
end
