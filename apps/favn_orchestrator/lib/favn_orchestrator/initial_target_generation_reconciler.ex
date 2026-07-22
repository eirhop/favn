defmodule FavnOrchestrator.InitialTargetGenerationReconciler do
  @moduledoc """
  Activates an initial persisted target generation after its first successful write.

  The successful materialization ledger remains the authority for the write.
  This boundary performs a read-only physical inspection and asks the generation
  store to atomically record the fingerprint and active binding.
  """

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Manifest.Version
  alias Favn.TargetCompatibility.PhysicalFingerprint
  alias FavnOrchestrator.MaterializationClaims
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.ReconcileInitialTargetGeneration
  alias FavnOrchestrator.Persistence.Queries.GetTargetBinding
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunnerDispatch
  alias FavnOrchestrator.RunnerReleaseCompatibility
  alias FavnOrchestrator.RuntimeConfig

  @doc "Reconciles an uninitialized persisted target, or returns `:ok` when none is pending."
  @spec reconcile(map()) :: :ok | {:error, term()}
  def reconcile(%{materialization_claim: nil}), do: :ok

  def reconcile(%{materialization_claim: claim} = entry)
      when is_map(claim) do
    case field(claim, :target_generation_id) do
      generation_id when is_binary(generation_id) ->
        reconcile_persisted(entry, claim, generation_id)

      nil ->
        :ok
    end
  end

  def reconcile(_entry), do: :ok

  defp reconcile_persisted(entry, claim, generation_id) do
    workspace_id = field(claim, :workspace_id)
    target_id = Favn.TargetIdentity.for_asset(field(entry, :asset_ref))
    context = SystemContext.workspace(workspace_id, :initial_target_generation_reconcile)

    case Persistence.stores().target_generations.get_binding(%GetTargetBinding{
           workspace_context: context,
           target_id: target_id
         }) do
      {:ok, %{active_generation_id: ^generation_id}} ->
        :ok

      {:ok,
       %{
         active_generation_id: nil,
         compatibility_status: :uninitialized,
         desired_manifest_id: manifest_id
       }} ->
        inspect_and_reconcile(entry, claim, generation_id, target_id, manifest_id, context)

      {:ok, binding} ->
        {:error, {:initial_target_generation_binding_mismatch, binding_state(binding)}}

      {:error, reason} ->
        {:error, {:initial_target_generation_binding_failed, reason}}
    end
  end

  defp inspect_and_reconcile(entry, claim, generation_id, target_id, manifest_id, context) do
    version = field(entry, :version)
    asset_ref = field(entry, :asset_ref)

    with %Version{manifest_version_id: ^manifest_id} <- version,
         {:ok, fingerprint} <- inspect_physical(version, asset_ref),
         {:ok, _result} <-
           Persistence.stores().target_generations.reconcile_initial(
             reconciliation_command(
               claim,
               generation_id,
               target_id,
               manifest_id,
               fingerprint,
               context
             )
           ) do
      :ok
    else
      %Version{} -> {:error, :initial_target_generation_manifest_mismatch}
      nil -> {:error, :initial_target_generation_manifest_missing}
      {:error, reason} -> {:error, {:initial_target_generation_reconciliation_failed, reason}}
      _invalid -> {:error, :initial_target_generation_manifest_invalid}
    end
  end

  defp inspect_physical(version, asset_ref) do
    runtime = RuntimeConfig.current()

    request = %RelationInspectionRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: version.required_runner_release_id,
      asset_ref: asset_ref,
      include: [:relation, :columns, :table_metadata],
      sample_limit: 0
    }

    with runner when is_atom(runner) and not is_nil(runner) <- runtime.runner_client,
         {:ok, %RelationInspectionResult{} = result} <-
           RunnerDispatch.inspect_relation(runner, request, runtime.runner_client_opts),
         :ok <-
           RunnerReleaseCompatibility.verify_inspection_result(
             version.required_runner_release_id,
             result
           ),
         {:ok, %PhysicalFingerprint{} = fingerprint} <-
           PhysicalFingerprint.from_inspection(result),
         {:ok, asset} <-
           ManifestTarget.resolve_asset(version, Favn.TargetIdentity.for_asset(asset_ref)),
         [] <- PhysicalFingerprint.identity_diff(asset.target_descriptor, fingerprint) do
      {:ok, fingerprint}
    else
      nil -> {:error, :runner_client_unavailable}
      {:ok, :not_found} -> {:error, :materialized_relation_not_found}
      {:ok, _invalid} -> {:error, :invalid_runner_inspection_result}
      [_ | _] = diff -> {:error, {:physical_identity_mismatch, diff}}
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_runner_inspection_result}
    end
  end

  defp reconciliation_command(
         claim,
         generation_id,
         target_id,
         manifest_id,
         fingerprint,
         context
       ) do
    materialization_id = MaterializationClaims.materialization_id(claim)

    %ReconcileInitialTargetGeneration{
      workspace_context: context,
      command_id: command_id(materialization_id, generation_id, fingerprint.fingerprint),
      target_id: target_id,
      manifest_version_id: manifest_id,
      target_generation_id: generation_id,
      materialization_id: materialization_id,
      physical_schema_fingerprint: fingerprint.fingerprint,
      data_plane_marker: nil,
      occurred_at: DateTime.utc_now()
    }
  end

  defp command_id(materialization_id, generation_id, fingerprint) do
    digest =
      :crypto.hash(:sha256, [materialization_id, 0, generation_id, 0, fingerprint])
      |> Base.url_encode64(padding: false)

    "target-generation:reconcile-initial:" <> digest
  end

  defp binding_state(nil), do: %{status: :missing}

  defp binding_state(binding) do
    %{
      status: Map.get(binding, :compatibility_status),
      active_generation_id: Map.get(binding, :active_generation_id)
    }
  end

  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end
