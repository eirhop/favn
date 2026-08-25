defmodule FavnOrchestrator.Rebuild.RuntimeInputs do
  @moduledoc false

  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias Favn.RuntimeInput.Pin
  alias FavnOrchestrator.AssetRunnerTasks
  alias FavnOrchestrator.ExecutionPackages
  alias FavnOrchestrator.OperationRunnerTasks
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunManager.Submission
  alias FavnOrchestrator.RunManager.SubmissionBuilder
  alias FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle
  alias FavnOrchestrator.Runs

  @spec freeze(
          WorkspaceContext.t(),
          Version.t(),
          Index.t(),
          String.t(),
          [map()]
        ) :: {:ok, [term()]} | {:error, term()}
  def freeze(context, version, index, deployment_id, specs) when is_list(specs) do
    Enum.reduce_while(specs, {:ok, []}, fn spec, {:ok, items} ->
      case freeze_one(context, version, index, deployment_id, spec) do
        {:ok, item} -> {:cont, {:ok, [item | items]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> then(fn
      {:ok, items} -> {:ok, Enum.reverse(items)}
      {:error, _reason} = error -> error
    end)
  end

  defp freeze_one(context, version, index, deployment_id, spec) do
    opts =
      [
        run_id: spec.run_id,
        manifest_version_id: version.manifest_version_id,
        dependencies: :none,
        rebuild: spec.rebuild,
        combine_windows: spec.combine_windows,
        refresh: :force,
        metadata: %{
          rebuild_operation_id: spec.rebuild.operation_id,
          rebuild_action_id: spec.rebuild.action_id,
          rebuild_item_id: spec.rebuild.item_id,
          rebuild_evaluated_at: spec.evaluated_at
        }
      ]
      |> maybe_put_selection(spec.window_selection)

    with {:ok, %Submission{} = submission} <-
           SubmissionBuilder.persisted_target(
             context,
             :asset,
             spec.asset.ref,
             deployment_id,
             version.manifest_version_id,
             spec.run_id,
             opts
           ),
         run <- %{submission.run_state | inserted_at: spec.evaluated_at},
         node_key when not is_nil(node_key) <- List.first(run.plan.target_node_keys),
         node <- Map.fetch!(run.plan.nodes, node_key),
         {:ok, lifecycle} <-
           run
           |> StepAttemptLifecycle.new(version, node_key, Map.get(node, :stage, 0), 1)
           |> StepAttemptLifecycle.build_work(index),
         {:ok, work} <-
           ExecutionPackages.attach(
             context,
             deployment_id,
             lifecycle.work,
             version,
             index
           ),
         :ok <- validate_runner_binding(work, spec.runner_binding),
         {:ok, expectation} <-
           resolve_expectation(
             context,
             run,
             node_key,
             Map.get(node, :stage, 0),
             work,
             spec
           ) do
      {:ok, Map.put(spec.item, :runtime_input_expectation, expectation)}
    else
      nil -> {:error, :rebuild_runtime_input_node_missing}
      {:error, _reason} = error -> error
    end
  end

  defp resolve_expectation(
         _context,
         _run,
         _node_key,
         _stage,
         %{execution_package: %ExecutionPackage{sql_execution: %{runtime_inputs: nil}}},
         _spec
       ),
       do: {:ok, nil}

  defp resolve_expectation(context, run, node_key, stage, work, spec) do
    metadata = Map.put(work.metadata, :runner_task_mode, :runtime_input_resolution)
    work = %{work | metadata: metadata}

    continuation = %{
      rebuild_operation_id: spec.rebuild.operation_id,
      rebuild_action_id: spec.rebuild.action_id,
      rebuild_item_id: spec.rebuild.item_id,
      purpose: :runtime_input_resolution
    }

    with {:ok, task, _work} <-
           AssetRunnerTasks.enqueue(run, work, node_key, stage, 1, continuation),
         {:ok, _result} <-
           OperationRunnerTasks.await(context, task.task_id),
         {:ok, [%Pin{} = pin]} <- Runs.get_runtime_inputs(context, run.id, [node_key]) do
      {:ok,
       %{
         resolver: Atom.to_string(pin.resolver),
         input_identity: pin.input_identity,
         payload_fingerprint: pin.payload_fingerprint
       }}
    else
      {:ok, []} -> {:error, :rebuild_runtime_input_resolution_missing}
      {:error, _reason} = error -> error
      invalid -> {:error, {:invalid_rebuild_runtime_input_resolution, invalid}}
    end
  end

  defp maybe_put_selection(opts, nil), do: opts
  defp maybe_put_selection(opts, selection), do: Keyword.put(opts, :window_selection, selection)

  defp validate_runner_binding(work, binding) do
    with {:ok, runner_pool} <- Favn.RunnerPool.encode(work.runner_pool),
         true <- runner_pool == binding.runner_pool,
         true <- work.required_runner_release_id == binding.required_runner_release_id do
      :ok
    else
      _mismatch -> {:error, :rebuild_runner_binding_mismatch}
    end
  end
end
