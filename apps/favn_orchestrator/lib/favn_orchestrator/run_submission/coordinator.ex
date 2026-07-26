defmodule FavnOrchestrator.RunSubmission.Coordinator do
  @moduledoc false

  use GenServer

  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Persistence.Queries.PageClaimableRunSubmissionWorkspaces
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunSubmission.Worker
  alias FavnOrchestrator.RuntimeConfig

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc false
  @spec diagnostics(GenServer.server()) :: map()
  def diagnostics(server \\ __MODULE__), do: GenServer.call(server, :diagnostics)

  @impl true
  def init(opts) do
    state = %{
      task_supervisor: Keyword.fetch!(opts, :task_supervisor),
      store: Keyword.fetch!(opts, :store),
      lifecycle: Keyword.get(opts, :lifecycle, Lifecycle),
      worker: Keyword.get(opts, :worker, Worker),
      worker_options: Keyword.get(opts, :worker_options, []),
      global_concurrency: Keyword.fetch!(opts, :global_concurrency),
      per_workspace_concurrency: Keyword.fetch!(opts, :per_workspace_concurrency),
      workspace_page_size: Keyword.fetch!(opts, :workspace_page_size),
      poll_interval_ms: Keyword.fetch!(opts, :poll_interval_ms),
      tasks: %{},
      workspace_counts: %{},
      workspace_cursor: nil,
      incarnation: new_incarnation(),
      sequence: 0,
      poll_timer: nil
    }

    {:ok, state, {:continue, :dispatch}}
  end

  @impl true
  def handle_continue(:dispatch, state), do: {:noreply, dispatch(state)}

  @impl true
  def handle_call(:diagnostics, _from, state) do
    {:reply,
     %{
       active: map_size(state.tasks),
       global_concurrency: state.global_concurrency,
       per_workspace_concurrency: state.per_workspace_concurrency,
       workspace_counts: state.workspace_counts,
       workspace_cursor: state.workspace_cursor
     }, state}
  end

  @impl true
  def handle_info(:dispatch, state), do: {:noreply, dispatch(%{state | poll_timer: nil})}

  def handle_info({reference, _result}, state) when is_reference(reference) do
    Process.demonitor(reference, [:flush])
    {:noreply, state |> complete_task(reference) |> request_dispatch()}
  end

  def handle_info({:DOWN, reference, :process, _pid, _reason}, state) do
    {:noreply, state |> complete_task(reference) |> request_dispatch()}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp dispatch(state) do
    cond do
      Lifecycle.ensure_accepting(state.lifecycle) != :ok ->
        schedule_poll(state)

      available_slots(state) == 0 ->
        state

      true ->
        dispatch_page(state)
    end
  end

  defp dispatch_page(state) do
    query = %PageClaimableRunSubmissionWorkspaces{
      platform_context: SystemContext.platform(:run_submission_workspace_discovery),
      after: state.workspace_cursor,
      limit: state.workspace_page_size
    }

    case state.store.page_claimable_workspaces(query) do
      {:ok, page} ->
        active_before = map_size(state.tasks)

        {state, last_considered} =
          Enum.reduce_while(page.workspace_ids, {state, nil}, fn workspace_id, {current, last} ->
            if available_slots(current) == 0 do
              {:halt, {current, last}}
            else
              {:cont, {maybe_start_worker(current, workspace_id), workspace_id}}
            end
          end)

        fully_considered? =
          page.workspace_ids == [] or last_considered == List.last(page.workspace_ids)

        next_cursor =
          cond do
            not fully_considered? -> last_considered
            page.has_more? -> page.next
            true -> nil
          end

        state = %{state | workspace_cursor: next_cursor}
        started? = map_size(state.tasks) > active_before

        cond do
          available_slots(state) == 0 -> state
          not is_nil(next_cursor) or started? -> request_dispatch(state)
          true -> schedule_poll(state)
        end

      {:error, _reason} ->
        schedule_poll(state)
    end
  end

  defp maybe_start_worker(state, workspace_id) do
    if Map.get(state.workspace_counts, workspace_id, 0) >= state.per_workspace_concurrency do
      state
    else
      start_worker(state, workspace_id)
    end
  end

  defp start_worker(state, workspace_id) do
    sequence = state.sequence + 1
    owner_id = owner_id(workspace_id, state.incarnation, sequence)

    opts =
      state.worker_options
      |> Keyword.put(:store, state.store)
      |> Keyword.put(:lifecycle, state.lifecycle)
      |> Keyword.put(:owner_id, owner_id)

    task =
      Task.Supervisor.async_nolink(
        state.task_supervisor,
        state.worker,
        :run,
        [workspace_id, opts]
      )

    %{
      state
      | sequence: sequence,
        tasks: Map.put(state.tasks, task.ref, {workspace_id, task.pid}),
        workspace_counts: Map.update(state.workspace_counts, workspace_id, 1, &(&1 + 1))
    }
  catch
    :exit, _reason -> state
  end

  defp complete_task(state, reference) do
    case Map.pop(state.tasks, reference) do
      {nil, tasks} ->
        %{state | tasks: tasks}

      {{workspace_id, _pid}, tasks} ->
        count = Map.fetch!(state.workspace_counts, workspace_id) - 1

        workspace_counts =
          if count == 0,
            do: Map.delete(state.workspace_counts, workspace_id),
            else: Map.put(state.workspace_counts, workspace_id, count)

        %{state | tasks: tasks, workspace_counts: workspace_counts}
    end
  end

  defp request_dispatch(%{poll_timer: nil} = state) do
    send(self(), :dispatch)
    state
  end

  defp request_dispatch(state), do: state

  defp schedule_poll(%{poll_timer: nil} = state) do
    timer = Process.send_after(self(), :dispatch, state.poll_interval_ms)
    %{state | poll_timer: timer}
  end

  defp schedule_poll(state), do: state

  defp available_slots(state), do: state.global_concurrency - map_size(state.tasks)

  defp owner_id(workspace_id, incarnation, sequence) do
    instance = RuntimeConfig.instance_id()

    digest =
      {instance, incarnation, workspace_id, sequence}
      |> :erlang.term_to_binary([:deterministic])
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.url_encode64(padding: false)

    "#{String.slice(instance, 0, 96)}:run-submission:#{digest}"
  end

  defp new_incarnation do
    16
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end
end
