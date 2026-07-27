defmodule FavnRunner.TaskExecutor do
  @moduledoc "Executes exactly one prepared asset assignment under the runner worker supervisor."
  use GenServer

  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerTask.Assignment
  alias Favn.Contracts.RunnerWork
  alias FavnRunner.ManifestResolver
  alias FavnRunner.ManifestStore
  alias FavnRunner.TaskResultBuffer
  alias FavnRunner.Worker

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc "Stops the active worker and reports a typed cancelled result."
  @spec cancel(pid(), term()) :: :ok | {:error, term()}
  def cancel(executor, reason), do: GenServer.call(executor, {:cancel, reason}, 10_000)

  @impl true
  def init(opts) do
    assignment = Keyword.fetch!(opts, :assignment)
    work = Keyword.fetch!(opts, :work)
    owner = Keyword.fetch!(opts, :owner)

    with %Assignment{} <- assignment,
         %RunnerWork{} <- work,
         {:ok, asset_ref} <- ManifestResolver.resolve_target_ref(work),
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
         worker: worker,
         monitor: Process.monitor(worker),
         result_sent?: false
       }}
    else
      {:error, reason} -> {:stop, reason}
      _other -> {:stop, :invalid_task_executor_options}
    end
  end

  @impl true
  def handle_call({:cancel, reason}, _from, state) do
    case DynamicSupervisor.terminate_child(FavnRunner.WorkerSupervisor, state.worker) do
      :ok ->
        send(state.owner, {:runner_task_finished, self(), cancelled_result(state.work, reason)})
        {:stop, :normal, :ok, %{state | result_sent?: true}}

      {:error, :not_found} ->
        {:reply, {:error, :already_finished}, state}
    end
  end

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

  def handle_info({:DOWN, ref, :process, worker, reason}, %{monitor: ref, worker: worker} = state) do
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

    DynamicSupervisor.start_child(FavnRunner.WorkerSupervisor, child_spec)
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
end
