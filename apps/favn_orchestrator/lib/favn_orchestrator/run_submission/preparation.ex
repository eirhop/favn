defmodule FavnOrchestrator.RunSubmission.Preparation do
  @moduledoc false

  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.Persistence.Results.RunSubmission
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunManager.Submission
  alias FavnOrchestrator.RunManager.SubmissionBuilder
  alias FavnOrchestrator.RunSubmission.Intent

  @format "favn.run_submission.preparation.v1"

  @spec prepare(WorkspaceContext.t(), RunSubmission.t(), keyword()) ::
          {:ok, Submission.t(), map()} | {:error, term()}
  def prepare(%WorkspaceContext{} = context, %RunSubmission{} = submission, runtime_opts \\ []) do
    with true <- context.workspace_id == submission.workspace_id,
         {:ok, {operation, selector, opts}} <- Intent.decode(submission.intent) do
      opts = put_memory_capacity_token(opts, runtime_opts)

      ManifestStore.with_deployment_manifest(
        context,
        submission.deployment_id,
        submission.manifest_version_id,
        capacity_opts(runtime_opts),
        fn version ->
          with {:ok, prepared} <-
                 prepare_operation(
                   context,
                   version,
                   submission,
                   operation,
                   selector,
                   opts
                 ) do
            {:ok, prepared, summary(prepared, submission)}
          end
        end
      )
    else
      false -> {:error, :run_submission_workspace_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp put_memory_capacity_token(opts, runtime_opts) do
    case Keyword.get(runtime_opts, :memory_capacity_token) do
      nil -> opts
      token -> Keyword.put(opts, :_memory_capacity_token, token)
    end
  end

  defp capacity_opts(runtime_opts) do
    case Keyword.get(runtime_opts, :memory_capacity_token) do
      nil -> []
      token -> [memory_capacity_token: token]
    end
  end

  defp prepare_operation(context, version, submission, :asset, target_id, opts)
       when is_binary(target_id) do
    with true <- submission.target_kind == "asset" and submission.target_id == target_id,
         {:ok, asset} <- ManifestTarget.resolve_asset(version, target_id) do
      SubmissionBuilder.persisted_target(
        context,
        :asset,
        asset.ref,
        submission.deployment_id,
        submission.manifest_version_id,
        submission.run_id,
        opts
      )
    else
      false -> {:error, :run_submission_target_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_operation(context, version, submission, :pipeline, target_id, opts)
       when is_binary(target_id) do
    with true <- submission.target_kind == "pipeline" and submission.target_id == target_id,
         {:ok, pipeline} <- ManifestTarget.resolve_pipeline(version, target_id) do
      SubmissionBuilder.persisted_target(
        context,
        :pipeline,
        {pipeline.module, pipeline.name},
        submission.deployment_id,
        submission.manifest_version_id,
        submission.run_id,
        opts
      )
    else
      false -> {:error, :run_submission_target_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_operation(
         context,
         version,
         submission,
         :pipeline_assets,
         [_ | _] = target_refs,
         opts
       ) do
    with true <-
           submission.target_kind == "asset" and
             submission.target_id == ManifestTarget.asset_id(hd(target_refs)),
         :ok <- validate_asset_refs(version, target_refs) do
      SubmissionBuilder.persisted_pipeline_targets(
        context,
        target_refs,
        submission.deployment_id,
        submission.manifest_version_id,
        submission.run_id,
        opts
      )
    else
      false -> {:error, :run_submission_target_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_operation(
         context,
         _version,
         submission,
         :rerun,
         source_run_id,
         opts
       )
       when is_binary(source_run_id) do
    SubmissionBuilder.persisted_rerun(
      context,
      source_run_id,
      submission.deployment_id,
      submission.manifest_version_id,
      submission.run_id,
      opts
    )
  end

  defp prepare_operation(
         _context,
         _version,
         _submission,
         _operation,
         _selector,
         _opts
       ),
       do: {:error, :invalid_run_submission_target}

  defp validate_asset_refs(version, refs) do
    Enum.reduce_while(refs, :ok, fn
      {module, name} = ref, :ok when is_atom(module) and is_atom(name) ->
        case ManifestTarget.resolve_asset(version, ManifestTarget.asset_id(ref)) do
          {:ok, _asset} -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end

      _invalid, :ok ->
        {:halt, {:error, :invalid_run_submission_target}}
    end)
  end

  defp summary(%Submission{run_state: run}, %RunSubmission{} = submission) do
    %{
      "format" => @format,
      "workspace_id" => submission.workspace_id,
      "run_id" => run.id,
      "deployment_id" => run.deployment_id,
      "manifest_version_id" => run.manifest_version_id,
      "target_kind" => submission.target_kind,
      "target_id" => submission.target_id,
      "plan_hash" => run.plan_hash
    }
  end
end
