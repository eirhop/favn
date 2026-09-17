defmodule FavnMaintenance.InitialRegistration do
  @moduledoc """
  Checked operator-console repair for successful first writes whose registration failed.

  Load in the existing control-plane console, then call `repair/2` with an
  administrator workspace context and the original successful asset task ID.
  Quiesce this target for the entire call: stop new submissions and wait for all
  existing asset writes and other target operations to stop. Keep operation
  runners available. A timeout can leave tasks active: maintain quiescence until
  registration succeeds or all dispatched tasks are settled/reconciled. This script does not acquire a maintenance lock; its
  preflight alone cannot exclude future concurrent writes.
  This finishes registration only; it never executes an asset or reopens a run.
  """

  alias FavnOrchestrator.InitialTargetGenerationReconciler
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.MaterializationClaims
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries.GetInitialTargetRecoveryCandidate
  alias FavnOrchestrator.Persistence.Queries.GetRun
  alias FavnOrchestrator.Persistence.Queries.GetRunnerTask
  alias FavnOrchestrator.Persistence.Queries.GetTargetBinding
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @spec repair(WorkspaceContext.t(), String.t()) :: :ok | {:error, term()}
  def repair(%WorkspaceContext{} = context, task_id) when is_binary(task_id) do
    with true <- WorkspaceContext.valid?(context) and :workspace_admin in context.roles,
         {:ok, %{task_kind: :asset_attempt, status: :succeeded, data_state: :available} = task} <-
           Persistence.stores().runner_tasks.get(%GetRunnerTask{
             workspace_context: context,
             task_id: task_id
           }),
         %{materialization_claim: claim} when is_map(claim) <- task.orchestration_context,
         true <-
           claim.workspace_id == context.workspace_id and claim.claim_key == task.write_claim_key,
         true <- claim.target_generation_id == task.payload.target_generation_id,
         {:ok, %{status: :error}} <-
           Persistence.stores().runs.get_run(%GetRun{
             workspace_context: context,
             run_id: task.run_id
           }),
         {:ok, version} <- ManifestStore.get_manifest(context, task.manifest_version_id),
         true <- version.content_hash == task.manifest_content_hash,
         {:ok, index} <- Favn.Manifest.Index.build_from_version(version),
         {:ok, binding} <-
           Persistence.stores().target_generations.get_binding(%GetTargetBinding{
             workspace_context: context,
             target_id: task.write_target_id
           }) do
      if binding.active_generation_id == task.payload.target_generation_id and
           is_binary(binding.active_generation_id) do
        :ok
      else
        with {:ok, candidate} <-
               Persistence.stores().target_recovery.get_initial_candidate(
                 %GetInitialTargetRecoveryCandidate{
                   workspace_context: context,
                   target_id: task.write_target_id
                 }
               ),
             true <- candidate.binding.compatibility_status == :uninitialized,
             true <- candidate.generation.creating_manifest_id == task.manifest_version_id,
             true <- candidate.binding.desired_manifest_id == task.manifest_version_id,
             true <-
               candidate.generation.target_generation_id == task.payload.target_generation_id,
             true <-
               candidate.generation.creating_descriptor_hash ==
                 candidate.binding.desired_descriptor_hash,
             true <-
               candidate.materialization_id == MaterializationClaims.materialization_id(claim),
             :ok <- no_unresolved_write(context.workspace_id, task.write_target_id) do
          InitialTargetGenerationReconciler.reconcile(%{
            asset_ref: task.payload.asset_ref,
            version: version,
            manifest_index: index,
            materialization_claim: claim
          })
        else
          {:error, _} = error -> error
          _ -> {:error, :initial_registration_evidence_mismatch}
        end
      end
    else
      {:error, _} = error -> error
      _ -> {:error, :initial_registration_repair_not_eligible}
    end
  end

  defp no_unresolved_write(workspace, target) do
    # This administrative script is PostgreSQL-specific. Existing write fences
    # still recheck authority during enqueue/start; this read never releases a hold.
    case Ecto.Adapters.SQL.query(
           FavnStoragePostgres.Repo,
           """
           SELECT EXISTS (
             SELECT 1 FROM favn_control.target_operation_locks
             WHERE workspace_id=$1 AND target_id=$2 AND effect_state IN ('in_flight','outcome_unknown')
             UNION ALL
             SELECT 1 FROM favn_control.materialization_claims
             WHERE workspace_id=$1 AND target_id=$2 AND effect_state IN ('in_flight','outcome_unknown')
           )
           """,
           [workspace, target]
         ) do
      {:ok, %{rows: [[false]]}} -> :ok
      {:ok, %{rows: [[true]]}} -> {:error, :target_write_requires_reconciliation}
      {:error, reason} -> {:error, reason}
    end
  end
end
