defmodule FavnStoragePostgres.Bootstrap.ConnectionGuard do
  @moduledoc false

  alias FavnStoragePostgres.Bootstrap.WorkflowRunner

  defmodule State do
    @moduledoc false

    @enforce_keys [
      :connection,
      :connection_monitor,
      :authentication,
      :authentication_monitor,
      :reference,
      :worker,
      :worker_monitor,
      :workflow_reference,
      :classifier
    ]
    defstruct @enforce_keys
  end

  @type resource :: :connection | :authentication

  @spec run(pid(), pid() | nil, (resource(), term() -> result), (-> result)) :: result
        when result: term()
  def run(connection, authentication, classifier, function)
      when is_pid(connection) and (is_pid(authentication) or is_nil(authentication)) and
             is_function(classifier, 2) and is_function(function, 0) do
    caller = self()
    reference = make_ref()
    workflow_context = WorkflowRunner.current_context()
    connection_monitor = Process.monitor(connection)
    authentication_monitor = monitor(authentication)

    {worker, worker_monitor} =
      spawn_monitor(fn ->
        case workflow_context do
          nil ->
            send(caller, {reference, :result, function.()})

          workflow_context ->
            case WorkflowRunner.guarded_result(workflow_context, function) do
              {:ok, result} -> send(caller, {reference, :result, result})
              {:error, failure} -> send(caller, {reference, :failure, failure})
            end
        end
      end)

    await(%State{
      connection: connection,
      connection_monitor: connection_monitor,
      authentication: authentication,
      authentication_monitor: authentication_monitor,
      reference: reference,
      worker: worker,
      worker_monitor: worker_monitor,
      workflow_reference: WorkflowRunner.context_reference(workflow_context),
      classifier: classifier
    })
  end

  @doc false
  @spec run_unknown_outcome([pid()], (-> result)) :: result when result: term()
  def run_unknown_outcome(resources, function)
      when is_list(resources) and is_function(function, 0) do
    watcher = start_resource_watcher(resources)

    try do
      function.()
    after
      stop_resource_watcher(watcher)
    end
  end

  defp await(%State{} = state) do
    receive do
      {reference, :result, result} when reference == state.reference ->
        demonitor_all(state)
        result

      {reference, :failure, failure} when reference == state.reference ->
        demonitor_all(state)
        WorkflowRunner.propagate_failure(failure)

      {:favn_workflow_context, workflow_reference, event}
      when workflow_reference == state.workflow_reference and is_reference(workflow_reference) ->
        WorkflowRunner.absorb_context_event(workflow_reference, event)
        await(state)

      {:DOWN, monitor, :process, connection, reason}
      when monitor == state.connection_monitor and connection == state.connection ->
        stop_worker(state)
        demonitor(state.authentication_monitor)
        state.classifier.(:connection, reason)

      {:DOWN, monitor, :process, authentication, reason}
      when monitor == state.authentication_monitor and authentication == state.authentication ->
        stop_worker(state)
        demonitor(state.connection_monitor)
        state.classifier.(:authentication, reason)

      {:DOWN, monitor, :process, worker, reason}
      when monitor == state.worker_monitor and worker == state.worker ->
        absorb_pending_context_events(state.workflow_reference)
        classify_worker_down(state, reason)
    end
  end

  defp classify_worker_down(%State{} = state, reason) do
    cond do
      unavailable?(state.authentication) ->
        demonitor(state.authentication_monitor)
        demonitor(state.connection_monitor)
        state.classifier.(:authentication, reason)

      not Process.alive?(state.connection) ->
        demonitor(state.authentication_monitor)
        demonitor(state.connection_monitor)
        state.classifier.(:connection, reason)

      true ->
        demonitor(state.authentication_monitor)
        demonitor(state.connection_monitor)
        exit(reason)
    end
  end

  defp stop_worker(%State{} = state) do
    Process.exit(state.worker, :kill)
    await_worker_down(state.worker_monitor, state.worker)
    absorb_pending_context_events(state.workflow_reference)
  end

  defp await_worker_down(monitor, worker) do
    receive do
      {:DOWN, ^monitor, :process, ^worker, _reason} -> :ok
    end
  end

  defp absorb_pending_context_events(workflow_reference) do
    receive do
      {:favn_workflow_context, ^workflow_reference, event}
      when is_reference(workflow_reference) ->
        WorkflowRunner.absorb_context_event(workflow_reference, event)
        absorb_pending_context_events(workflow_reference)
    after
      0 -> :ok
    end
  end

  defp unavailable?(nil), do: false
  defp unavailable?(process), do: not Process.alive?(process)

  defp start_resource_watcher(resources) do
    caller = self()
    ready_reference = make_ref()

    watcher =
      spawn(fn ->
        resource_monitors =
          Map.new(resources, fn resource -> {Process.monitor(resource), resource} end)

        caller_monitor = Process.monitor(caller)

        await_resource_guard_ready(
          resources,
          resource_monitors,
          caller,
          caller_monitor,
          ready_reference
        )
      end)

    receive do
      {:resource_watcher_ready, ^ready_reference, ^watcher} -> watcher
    end
  end

  defp await_resource_guard_ready(
         resources,
         resource_monitors,
         caller,
         caller_monitor,
         ready_reference
       ) do
    receive do
      {:DOWN, ^caller_monitor, :process, ^caller, _reason} ->
        stop_resources(resources)

      {:DOWN, monitor, :process, _resource, _reason}
      when is_map_key(resource_monitors, monitor) ->
        stop_resources(resources)
        Process.exit(caller, :kill)
    after
      0 ->
        send(caller, {:resource_watcher_ready, ready_reference, self()})
        watch_resources(resources, resource_monitors, caller, caller_monitor)
    end
  end

  defp watch_resources(resources, resource_monitors, caller, caller_monitor) do
    receive do
      {:stop_resource_watcher, reference, ^caller} ->
        Enum.each(Map.keys(resource_monitors), &Process.demonitor(&1, [:flush]))
        Process.demonitor(caller_monitor, [:flush])
        send(caller, {:resource_watcher_stopped, reference, self()})

      {:DOWN, ^caller_monitor, :process, ^caller, _reason} ->
        stop_resources(resources)

      {:DOWN, monitor, :process, _resource, _reason}
      when is_map_key(resource_monitors, monitor) ->
        stop_resources(resources)
        Process.exit(caller, :kill)
    end
  end

  defp stop_resources(resources), do: Enum.each(resources, &Process.exit(&1, :kill))

  defp stop_resource_watcher(watcher) do
    reference = make_ref()
    monitor = Process.monitor(watcher)
    send(watcher, {:stop_resource_watcher, reference, self()})

    receive do
      {:resource_watcher_stopped, ^reference, ^watcher} ->
        Process.demonitor(monitor, [:flush])
        :ok

      {:DOWN, ^monitor, :process, ^watcher, _reason} ->
        :ok
    end
  end

  defp monitor(nil), do: nil
  defp monitor(process), do: Process.monitor(process)

  defp demonitor_all(%State{} = state) do
    demonitor(state.worker_monitor)
    demonitor(state.connection_monitor)
    demonitor(state.authentication_monitor)
  end

  defp demonitor(nil), do: :ok
  defp demonitor(monitor), do: Process.demonitor(monitor, [:flush])
end
