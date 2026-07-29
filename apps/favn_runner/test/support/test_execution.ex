defmodule FavnRunner.TestExecution do
  @moduledoc false

  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerTask.Assignment
  alias Favn.Contracts.RunnerWork
  alias FavnRunner.Lifecycle
  alias FavnRunner.ManifestHandle
  alias FavnRunner.ManifestStore
  alias FavnRunner.ReleaseVerifier
  alias FavnRunner.SQLRuntimePreflight
  alias FavnRunner.TaskExecutor

  def run(%RunnerWork{} = work, opts \\ []) when is_list(opts) do
    preflight? = is_nil(work.manifest_lease_id)

    Lifecycle.with_admission(
      fn ->
        with :ok <- ReleaseVerifier.verify_required_release(work.required_runner_release_id) do
          with_scoped_manifest_lease(work, opts, fn leased_work ->
            if preflight? do
              case preflight_work_scope(leased_work, opts) do
                :ok ->
                  execute_task(leased_work, opts)

                {:error, {%ManifestHandle{} = handle, diagnostic}} ->
                  {:ok, preflight_failed_result(leased_work, handle, diagnostic)}

                {:error, reason} ->
                  {:error, reason}
              end
            else
              execute_task(leased_work, opts)
            end
          end)
        end
      end,
      Keyword.get(opts, :lifecycle, Lifecycle)
    )
  end

  defp execute_task(work, opts) do
    task_id = "test_" <> Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)

    assignment = %Assignment{
      command_id: task_id,
      workspace_id: "test",
      task_id: task_id,
      task_kind: :asset_attempt,
      runner_instance_id: "runner-test",
      runner_pool: Atom.to_string(RunnerWork.runner_pool(work)),
      required_runner_release_id: work.required_runner_release_id,
      assigned_at: DateTime.utc_now(),
      lease_expires_at: DateTime.add(DateTime.utc_now(), 60, :second),
      retry_class: :safe_to_retry,
      payload: work
    }

    child = {TaskExecutor, assignment: assignment, payload: work, owner: self()}

    with {:ok, executor} <-
           DynamicSupervisor.start_child(FavnRunner.TaskExecutorSupervisor, child) do
      receive do
        {:runner_task_finished, ^executor, %RunnerResult{} = result} -> {:ok, result}
      after
        Keyword.get(opts, :timeout, 5_000) -> {:error, :timeout}
      end
    end
  end

  defp preflight_work_scope(%RunnerWork{} = work, opts) do
    manifest_store = Keyword.get(opts, :manifest_store, ManifestStore)

    with {:ok, handle} <-
           ManifestStore.fetch_handle(
             work.manifest_version_id,
             work.manifest_content_hash,
             server: manifest_store
           ) do
      case SQLRuntimePreflight.run(handle, RunnerWork.planned_asset_refs(work),
             server: manifest_store
           ) do
        :ok -> :ok
        {:error, diagnostic} -> {:error, {handle, diagnostic}}
      end
    end
  end

  defp preflight_failed_result(work, manifest, diagnostic) do
    %RunnerResult{
      run_id: work.run_id,
      manifest_version_id: manifest.manifest_version_id,
      manifest_content_hash: manifest.content_hash,
      required_runner_release_id: manifest.required_runner_release_id,
      status: :error,
      asset_results: [],
      error:
        RunnerError.normalize(diagnostic,
          kind: :preflight,
          type: :missing_runtime_config,
          message: Map.get(diagnostic, :message, "runner preflight failed"),
          details: Map.get(diagnostic, :details, %{}),
          retryable?: false
        ),
      metadata: Map.put(RunnerWork.lifecycle_metadata(work), :preflight, :sql_runtime_config)
    }
  end

  defp with_scoped_manifest_lease(%RunnerWork{manifest_lease_id: lease_id} = work, _opts, fun)
       when is_binary(lease_id),
       do: fun.(work)

  defp with_scoped_manifest_lease(%RunnerWork{} = work, opts, fun) do
    lease_id = "test:" <> Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
    expires_at = DateTime.add(DateTime.utc_now(), 60, :second)
    manifest_store = Keyword.get(opts, :manifest_store, ManifestStore)

    with :ok <-
           ManifestStore.acquire_registered(
             work.manifest_version_id,
             work.manifest_content_hash,
             lease_id,
             expires_at,
             server: manifest_store
           ) do
      try do
        fun.(%{work | manifest_lease_id: lease_id})
      after
        ManifestStore.release(lease_id, server: manifest_store)
      end
    end
  end
end
