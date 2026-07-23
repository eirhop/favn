defmodule FavnOrchestrator.Rebuild.RuntimeInputs do
  @moduledoc false

  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias Favn.RuntimeInput.Resolution
  alias FavnOrchestrator.ExecutionPackages
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunManager.Submission
  alias FavnOrchestrator.RunManager.SubmissionBuilder
  alias FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle
  alias FavnOrchestrator.RunnerDispatch

  @spec freeze(
          WorkspaceContext.t(),
          Version.t(),
          Index.t(),
          String.t(),
          [map()],
          module(),
          keyword()
        ) :: {:ok, [term()]} | {:error, term()}
  def freeze(context, version, index, deployment_id, specs, runner_client, runner_opts)
      when is_list(specs) do
    Enum.reduce_while(specs, {:ok, []}, fn spec, {:ok, items} ->
      case freeze_one(context, version, index, deployment_id, spec, runner_client, runner_opts) do
        {:ok, item} -> {:cont, {:ok, [item | items]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> then(fn
      {:ok, items} -> {:ok, Enum.reverse(items)}
      {:error, _reason} = error -> error
    end)
  end

  defp freeze_one(context, version, index, deployment_id, spec, runner_client, runner_opts) do
    opts =
      [
        run_id: spec.run_id,
        manifest_version_id: version.manifest_version_id,
        dependencies: :none,
        rebuild: spec.rebuild,
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
           SubmissionBuilder.asset(context, spec.asset.ref, opts),
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
         {:ok, expectation} <- resolve_expectation(work, runner_client, runner_opts) do
      {:ok, Map.put(spec.item, :runtime_input_expectation, expectation)}
    else
      nil -> {:error, :rebuild_runtime_input_node_missing}
      {:error, _reason} = error -> error
    end
  end

  defp resolve_expectation(
         %{execution_package: %ExecutionPackage{sql_execution: %{runtime_inputs: nil}}},
         _runner_client,
         _runner_opts
       ),
       do: {:ok, nil}

  defp resolve_expectation(work, runner_client, runner_opts) do
    case RunnerDispatch.resolve_runtime_inputs(runner_client, work, runner_opts) do
      {:ok, %Resolution{} = resolution} ->
        {:ok,
         %{
           resolver: Atom.to_string(resolution.resolver),
           input_identity: resolution.input_identity,
           payload_fingerprint: resolution.payload_fingerprint
         }}

      {:ok, nil} ->
        {:error, :rebuild_runtime_input_resolution_missing}

      {:error, _reason} = error ->
        error

      invalid ->
        {:error, {:invalid_rebuild_runtime_input_resolution, invalid}}
    end
  end

  defp maybe_put_selection(opts, nil), do: opts
  defp maybe_put_selection(opts, selection), do: Keyword.put(opts, :window_selection, selection)
end
