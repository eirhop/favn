defmodule FavnRunner.TaskExecutor do
  @moduledoc "Executes exactly one prepared durable runner-task assignment."
  use GenServer

  alias Favn.Contracts.GenerationCapabilitiesRequest
  alias Favn.Contracts.GenerationCapabilitiesResult
  alias Favn.Contracts.GenerationActivationRequest
  alias Favn.Contracts.GenerationDiscardRequest
  alias Favn.Contracts.GenerationMarkerInitializationRequest
  alias Favn.Contracts.GenerationMarkerReadRequest
  alias Favn.Contracts.GenerationMarkerReadResult
  alias Favn.Contracts.GenerationReconciliationRequest
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerTask.Assignment
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias FavnRunner.GenerationOperations
  alias FavnRunner.Inspection
  alias FavnRunner.Lifecycle
  alias FavnRunner.ManifestResolver
  alias FavnRunner.ManifestStore
  alias FavnRunner.ReleaseVerifier
  alias FavnRunner.TaskResultBuffer
  alias FavnRunner.Worker

  defmodule Result do
    @moduledoc false
    @enforce_keys [:outcome, :retry_class]
    defstruct [:outcome, :retry_class, :result, :error]
  end

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :assignment).task_id},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      shutdown: 5_000,
      type: :worker
    }
  end

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc "Stops the active worker and reports a typed cancelled result."
  @spec cancel(pid(), term()) :: :ok | {:error, term()}
  def cancel(executor, reason), do: GenServer.call(executor, {:cancel, reason}, 10_000)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    assignment = Keyword.fetch!(opts, :assignment)
    payload = Keyword.fetch!(opts, :payload)
    owner = Keyword.fetch!(opts, :owner)

    case {assignment, payload} do
      {%Assignment{task_kind: :asset_attempt}, %RunnerWork{} = work} ->
        init_asset(assignment, work, owner)

      {%Assignment{}, payload} ->
        {:ok,
         %{
           assignment: assignment,
           payload: payload,
           owner: owner,
           task_mode: :operation,
           worker: nil,
           monitor: nil,
           result_sent?: false
         }, {:continue, :start_operation}}

      _other ->
        {:stop, :invalid_task_executor_options}
    end
  end

  defp init_asset(assignment, work, owner) do
    if runtime_input_resolution_only?(work) do
      {:ok,
       %{
         assignment: assignment,
         work: work,
         owner: owner,
         task_mode: :asset,
         worker: nil,
         monitor: nil,
         result_sent?: false
       }, {:continue, :complete_runtime_input_resolution}}
    else
      with {:ok, asset_ref} <- ManifestResolver.resolve_target_ref(work),
           {:ok, manifest, asset, relations} <-
             ManifestStore.fetch_execution_bundle(
               work.manifest_lease_id,
               work.manifest_version_id,
               work.manifest_content_hash,
               asset_ref,
               work.execution_package
             ),
           {:ok, worker} <- start_worker(assignment, work, manifest, asset, relations) do
        {:ok,
         %{
           assignment: assignment,
           work: work,
           owner: owner,
           task_mode: :asset,
           worker: worker,
           monitor: Process.monitor(worker),
           result_sent?: false
         }}
      else
        {:error, reason} -> {:stop, reason}
        _other -> {:stop, :invalid_task_executor_options}
      end
    end
  end

  @impl true
  def handle_continue(:complete_runtime_input_resolution, state) do
    result = runtime_input_resolution_result(state.work)
    send(state.owner, {:runner_task_finished, self(), result})
    {:stop, :normal, %{state | result_sent?: true}}
  end

  @impl true
  def handle_continue(:start_operation, state) do
    executor = self()

    {worker, monitor} =
      :erlang.spawn_opt(
        fn ->
          result = guarded_operation(state.assignment, state.payload)
          send(executor, {:operation_finished, self(), result})
        end,
        [:link, :monitor]
      )

    {:noreply, %{state | worker: worker, monitor: monitor}}
  end

  @impl true
  def handle_call(
        {:cancel, reason},
        _from,
        %{task_mode: :asset, worker: worker} = state
      )
      when is_pid(worker) do
    case DynamicSupervisor.terminate_child(FavnRunner.WorkerSupervisor, state.worker) do
      :ok ->
        send(state.owner, {:runner_task_finished, self(), cancelled_result(state.work, reason)})
        {:stop, :normal, :ok, %{state | result_sent?: true}}

      {:error, :not_found} ->
        {:reply, {:error, :already_finished}, state}
    end
  end

  def handle_call(
        {:cancel, reason},
        _from,
        %{task_mode: :operation, worker: worker} = state
      )
      when is_pid(worker) do
    Process.exit(worker, :kill)
    Process.demonitor(state.monitor, [:flush])

    send(
      state.owner,
      {:runner_task_finished, self(),
       interrupted_operation_result(state.assignment.task_kind, {:cancelled, reason}, true)}
    )

    {:stop, :normal, :ok, %{state | result_sent?: true, worker: nil, monitor: nil}}
  end

  def handle_call({:cancel, _reason}, _from, state),
    do: {:reply, {:error, :already_finished}, state}

  def handle_call({:runner_log_entry, _execution_id, entry}, _from, state) do
    if TaskResultBuffer.append(entry) == :flush do
      send(state.owner, {:runner_task_logs_ready, self()})
    end

    {:reply, :ok, state}
  end

  def handle_call({:runner_event, _execution_id, event}, _from, state) do
    if TaskResultBuffer.append(%{type: :runner_event, event: event}) == :flush do
      send(state.owner, {:runner_task_logs_ready, self()})
    end

    {:reply, :ok, state}
  end

  def handle_call({:runner_result, _execution_id, %RunnerResult{} = result}, _from, state) do
    send(state.owner, {:runner_task_finished, self(), result})
    {:stop, :normal, :ok, %{state | result_sent?: true}}
  end

  @impl true
  def handle_info({:runner_log_entry, _execution_id, entry}, state) do
    send(state.owner, {:runner_task_log, self(), entry})
    {:noreply, state}
  end

  def handle_info({:runner_event, _execution_id, event}, state) do
    send(state.owner, {:runner_task_event, self(), event})
    {:noreply, state}
  end

  def handle_info({:runner_result, _execution_id, %RunnerResult{} = result}, state) do
    send(state.owner, {:runner_task_finished, self(), result})
    {:stop, :normal, %{state | result_sent?: true}}
  end

  def handle_info(
        {:operation_finished, worker, %Result{} = result},
        %{task_mode: :operation, worker: worker} = state
      ) do
    Process.demonitor(state.monitor, [:flush])
    send(state.owner, {:runner_task_finished, self(), result})
    {:stop, :normal, %{state | result_sent?: true, worker: nil, monitor: nil}}
  end

  def handle_info(
        {:DOWN, ref, :process, worker, reason},
        %{task_mode: :operation, monitor: ref, worker: worker} = state
      ) do
    send(
      state.owner,
      {:runner_task_finished, self(),
       interrupted_operation_result(state.assignment.task_kind, reason, false)}
    )

    {:stop, :normal, %{state | result_sent?: true, worker: nil, monitor: nil}}
  end

  def handle_info(
        {:EXIT, worker, _reason},
        %{task_mode: :operation, worker: worker} = state
      ),
      do: {:noreply, state}

  def handle_info(
        {:EXIT, worker, _reason},
        %{task_mode: :asset, worker: worker} = state
      ),
      do: {:noreply, state}

  def handle_info(
        {:DOWN, ref, :process, worker, reason},
        %{task_mode: :asset, monitor: ref, worker: worker} = state
      ) do
    if state.result_sent? do
      {:stop, :normal, state}
    else
      send(state.owner, {:runner_task_finished, self(), crashed_result(state.work, reason)})
      {:stop, :normal, state}
    end
  end

  defp start_worker(assignment, work, manifest, asset, relations) do
    child_spec = %{
      id: {Worker, assignment.task_id, assignment.assignment_generation},
      start:
        {Worker, :start_link,
         [
           %{
             server: {:bounded, self()},
             execution_id: assignment.task_id,
             work: work,
             manifest: manifest,
             asset: asset,
             relation_by_module: relations,
             result_max_bytes: 768 * 1_024
           }
         ]},
      restart: :temporary,
      shutdown: 5_000
    }

    case DynamicSupervisor.start_child(FavnRunner.WorkerSupervisor, child_spec) do
      {:ok, worker} = started ->
        Process.link(worker)
        started

      other ->
        other
    end
  end

  defp crashed_result(work, reason) do
    %RunnerResult{
      run_id: work.run_id,
      manifest_version_id: work.manifest_version_id,
      manifest_content_hash: work.manifest_content_hash,
      required_runner_release_id: work.required_runner_release_id,
      status: :error,
      asset_results: [],
      error:
        RunnerError.normalize(reason,
          kind: :exit,
          type: :worker_crash,
          retryable?: false,
          outcome: :unknown
        ),
      metadata: RunnerWork.lifecycle_metadata(work)
    }
  end

  defp cancelled_result(work, reason) do
    %RunnerResult{
      run_id: work.run_id,
      manifest_version_id: work.manifest_version_id,
      manifest_content_hash: work.manifest_content_hash,
      required_runner_release_id: work.required_runner_release_id,
      status: :cancelled,
      asset_results: [],
      error: RunnerError.cancelled(reason),
      metadata: RunnerWork.lifecycle_metadata(work)
    }
  end

  defp runtime_input_resolution_result(work) do
    %RunnerResult{
      run_id: work.run_id,
      manifest_version_id: work.manifest_version_id,
      manifest_content_hash: work.manifest_content_hash,
      required_runner_release_id: work.required_runner_release_id,
      status: :ok,
      asset_results: [],
      metadata: RunnerWork.lifecycle_metadata(work)
    }
  end

  defp runtime_input_resolution_only?(%RunnerWork{metadata: metadata}) do
    Map.get(metadata, :runner_task_mode, Map.get(metadata, "runner_task_mode")) in [
      :runtime_input_resolution,
      "runtime_input_resolution"
    ]
  end

  defp guarded_operation(%Assignment{} = assignment, payload) do
    Lifecycle.with_admission(fn -> execute_operation(assignment, payload) end)
  rescue
    exception ->
      operation_error(
        assignment.task_kind,
        {:exception, exception.__struct__, Exception.message(exception)}
      )
  catch
    kind, reason ->
      operation_error(assignment.task_kind, {kind, reason})
  end

  defp execute_operation(
         %Assignment{task_kind: :relation_inspection} = assignment,
         request
       ) do
    with_operation_version(assignment, request, fn version ->
      operation_result(
        :relation_inspection,
        Inspection.inspect_relation(
          request,
          version,
          assignment.required_runner_release_id
        )
      )
    end)
  end

  defp execute_operation(
         %Assignment{task_kind: :generation_capabilities} = assignment,
         %GenerationCapabilitiesRequest{manifest: version, asset_ref: asset_ref}
       ) do
    with_operation_version(assignment, version, fn installed_version ->
      with {:ok, asset} <- ManifestResolver.resolve_asset(installed_version, asset_ref),
           {:ok, capabilities} <- GenerationOperations.capabilities(asset) do
        operation_result(
          :generation_capabilities,
          {:ok, %GenerationCapabilitiesResult{capabilities: capabilities}}
        )
      else
        {:error, reason} -> operation_error(:generation_capabilities, reason)
      end
    end)
  end

  defp execute_operation(
         %Assignment{task_kind: :generation_marker_read} = assignment,
         %GenerationMarkerReadRequest{manifest: version, asset_ref: asset_ref}
       ) do
    with_operation_version(assignment, version, fn installed_version ->
      with {:ok, asset} <- ManifestResolver.resolve_asset(installed_version, asset_ref),
           {:ok, marker} <- GenerationOperations.marker(asset) do
        operation_result(
          :generation_marker_read,
          {:ok, %GenerationMarkerReadResult{marker: marker}}
        )
      else
        {:error, reason} -> operation_error(:generation_marker_read, reason)
      end
    end)
  end

  defp execute_operation(
         %Assignment{task_kind: :generation_marker_initialize} = assignment,
         %GenerationMarkerInitializationRequest{} = request
       ) do
    with_operation_version(assignment, request, fn version ->
      operation_result(
        :generation_marker_initialize,
        GenerationOperations.initialize_marker(request, version)
      )
    end)
  end

  defp execute_operation(
         %Assignment{task_kind: :generation_activate} = assignment,
         %GenerationActivationRequest{} = request
       ) do
    with_operation_version(assignment, request, fn version ->
      operation_result(:generation_activate, GenerationOperations.activate(request, version))
    end)
  end

  defp execute_operation(
         %Assignment{task_kind: :generation_reconcile} = assignment,
         %GenerationReconciliationRequest{} = request
       ) do
    with_operation_version(assignment, request.activation, fn version ->
      operation_result(
        :generation_reconcile,
        GenerationOperations.reconcile(request, version)
      )
    end)
  end

  defp execute_operation(
         %Assignment{task_kind: :generation_discard} = assignment,
         %GenerationDiscardRequest{} = request
       ) do
    with_operation_version(assignment, request, fn version ->
      operation_result(:generation_discard, GenerationOperations.discard(request, version))
    end)
  end

  defp execute_operation(%Assignment{task_kind: kind}, payload) do
    operation_error(
      kind,
      {:invalid_runner_task_operation_payload, kind, payload}
    )
  end

  defp with_operation_version(%Assignment{} = assignment, identity, execute) do
    kind = assignment.task_kind

    with {:ok, manifest_version_id, content_hash, payload_release} <-
           manifest_identity(identity),
         :ok <- exact_payload_release(payload_release, assignment.required_runner_release_id),
         :ok <- ReleaseVerifier.verify_required_release(assignment.required_runner_release_id),
         {:ok, version} <- ManifestStore.fetch(manifest_version_id, content_hash),
         {:ok, release_id} <- Version.release_for_pool(version, assignment.runner_pool),
         true <-
           release_id == assignment.required_runner_release_id or
             {:error, :runner_release_mismatch} do
      execute.(version)
    else
      {:error, reason} -> operation_error(kind, reason)
    end
  end

  defp manifest_identity(%Version{} = version),
    do: {:ok, version.manifest_version_id, version.content_hash, nil}

  defp manifest_identity(%{
         manifest_version_id: manifest_version_id,
         manifest_content_hash: content_hash,
         required_runner_release_id: release_id
       }),
       do: {:ok, manifest_version_id, content_hash, release_id}

  defp manifest_identity(_identity), do: {:error, :invalid_runner_task_manifest_identity}

  defp exact_payload_release(nil, _expected), do: :ok
  defp exact_payload_release(release, release), do: :ok
  defp exact_payload_release(_actual, _expected), do: {:error, :runner_release_mismatch}

  defp interrupted_operation_result(kind, reason, cancelled?) do
    if safe_operation?(kind) do
      if cancelled? do
        %Result{
          outcome: :cancelled,
          retry_class: :terminal,
          error: RunnerError.cancelled(reason)
        }
      else
        operation_error(kind, {:operation_process_stopped, reason})
      end
    else
      operation_error(kind, {:operation_outcome_unknown, reason})
    end
  end

  defp safe_operation?(kind),
    do: kind in [:relation_inspection, :generation_capabilities, :generation_marker_read]

  defp operation_result(
         _kind,
         {:ok, %{outcome: :safe_failure, error: %RunnerError{} = error}}
       ),
       do: %Result{
         outcome: :failed,
         retry_class: :safe_to_retry,
         error: %{error | retryable?: true}
       }

  defp operation_result(
         _kind,
         {:ok, %{outcome: :outcome_unknown, error: %RunnerError{} = error}}
       ),
       do: %Result{outcome: :unknown, retry_class: :reconcile_before_retry, error: error}

  defp operation_result(_kind, {:ok, result}),
    do: %Result{outcome: :succeeded, retry_class: :terminal, result: result}

  defp operation_result(kind, {:error, reason}), do: operation_error(kind, reason)

  defp operation_error(kind, reason)
       when kind in [:relation_inspection, :generation_capabilities, :generation_marker_read] do
    error =
      RunnerError.normalize(reason,
        phase: :runner_task_execution,
        retryable?: true,
        outcome: :safe_failure
      )

    %Result{
      outcome: :failed,
      retry_class: :safe_to_retry,
      error: error
    }
  end

  defp operation_error(_kind, reason) do
    error =
      RunnerError.normalize(reason,
        phase: :runner_task_execution,
        retryable?: false,
        outcome: :unknown
      )

    %Result{
      outcome: :unknown,
      retry_class: :reconcile_before_retry,
      error: error
    }
  end
end
