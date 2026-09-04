defmodule FavnOrchestrator.TargetRecovery.WriteResolution do
  @moduledoc false
  alias Favn.Contracts
  alias Favn.Manifest.Version
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.OperationRunnerTasks
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.ResolveRunnerTaskWrite
  alias FavnOrchestrator.Persistence.SystemContext

  @proof_keys ~w(assignment_generation owner_fence stopped_at stop_mechanism runner_stopped
    backend_stopped evidence_reference reason disposition)a
  @mechanisms [:backend_session_terminated, :infrastructure_removed, :adapter_stop_verified]

  def resolve(context, task_id, proof, opts) do
    with :ok <- validate(proof),
         {:ok, command_id} <- Keyword.fetch(opts, :command_id),
         {:ok, issued_at} <- Keyword.fetch(opts, :issued_at),
         true <- is_struct(issued_at, DateTime) do
      command = %ResolveRunnerTaskWrite{
        workspace_context: context,
        command_id: command_id,
        task_id: task_id,
        expected_assignment_generation: proof.assignment_generation,
        expected_owner_fence: proof.owner_fence,
        stopped_at: proof.stopped_at,
        stop_mechanism: proof.stop_mechanism,
        runner_stopped: proof.runner_stopped,
        backend_stopped: proof.backend_stopped,
        evidence_reference: proof.evidence_reference,
        reason: proof.reason,
        disposition: proof.disposition,
        observation_task_ids: [],
        issued_at: issued_at,
        occurred_at: DateTime.utc_now()
      }

      case Persistence.stores().runner_tasks.get_write_resolution(command) do
        {:ok, nil} ->
          with {:ok, task} <- OperationRunnerTasks.fetch(context, task_id),
               {:ok, observations} <- observations(context, task, proof, command_id) do
            Persistence.stores().runner_tasks.resolve_write(%{
              command
              | observation_task_ids: observations
            })
          end

        result ->
          result
      end
    else
      invalid when invalid in [:error, false] ->
        {:error, :write_resolution_command_identity_required}

      {:error, _reason} = error ->
        error
    end
  end

  def validate(proof) when is_map(proof) do
    if Enum.sort(Map.keys(proof)) == Enum.sort(@proof_keys) and
         is_integer(proof.assignment_generation) and proof.assignment_generation > 0 and
         is_integer(proof.owner_fence) and proof.owner_fence > 0 and
         is_struct(proof.stopped_at, DateTime) and
         DateTime.compare(proof.stopped_at, DateTime.utc_now()) != :gt and
         proof.runner_stopped == true and proof.backend_stopped == true and
         proof.stop_mechanism in @mechanisms and
         proof.disposition in [:verified_no_effect, :observe_generation] and
         bounded?(proof.evidence_reference, 2_048) and bounded?(proof.reason, 4_096),
       do: :ok,
       else: {:error, :invalid_write_resolution_proof}
  end

  def validate(_proof), do: {:error, :invalid_write_resolution_proof}

  defp observations(
         _context,
         %{task_kind: :asset_attempt},
         %{disposition: :verified_no_effect},
         _command
       ),
       do: {:ok, []}

  defp observations(
         context,
         %{data_state: :available, status: status} = task,
         %{disposition: :observe_generation} = proof,
         command
       )
       when status in [:unknown, :failed, :cancelled] do
    with {:ok, version} <-
           ManifestStore.get_manifest(
             SystemContext.platform(:task_write_resolution),
             task.manifest_version_id
           ),
         %{} = asset <-
           Enum.find(
             version.manifest.assets,
             &(Favn.TargetIdentity.for_asset(&1.ref) == task.write_target_id)
           ),
         {:ok, requests} <- observation_requests(task, version, asset.ref) do
      Enum.reduce_while(requests, {:ok, []}, fn {kind, payload}, {:ok, ids} ->
        identity = {:write_resolution, command, task.task_id, proof.assignment_generation, kind}

        with {:ok, observation} <-
               OperationRunnerTasks.ensure(context, version, asset.ref, kind, payload, identity),
             {:ok, _result} <- OperationRunnerTasks.await(context, observation.task_id) do
          {:cont, {:ok, ids ++ [observation.task_id]}}
        else
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    else
      nil -> {:error, :write_resolution_target_not_in_manifest}
      {:error, _reason} = error -> error
    end
  end

  defp observations(_context, _task, _proof, _command),
    do: {:error, :write_resolution_evidence_unavailable}

  defp observation_requests(%{task_kind: :generation_activate, payload: request}, _version, _ref),
    do:
      {:ok,
       [generation_reconcile: %Contracts.GenerationReconciliationRequest{activation: request}]}

  defp observation_requests(%{task_kind: kind, payload: request}, version, ref)
       when kind in [:generation_marker_initialize, :generation_discard] do
    relation =
      if kind == :generation_discard,
        do: request.candidate_relation,
        else: request.active_relation

    {:ok,
     [
       generation_marker_read: %Contracts.GenerationMarkerReadRequest{
         manifest: Version.identity(version),
         asset_ref: ref
       },
       relation_inspection: %Contracts.RelationInspectionRequest{
         manifest_version_id: version.manifest_version_id,
         manifest_content_hash: version.content_hash,
         required_runner_release_id: request.required_runner_release_id,
         relation: relation,
         include: if(kind == :generation_discard, do: [:relation], else: [:relation, :columns]),
         sample_limit: 0
       }
     ]}
  end

  defp observation_requests(_task, _version, _ref), do: {:error, :write_resolution_not_supported}
  defp bounded?(value, limit), do: is_binary(value) and byte_size(value) in 1..limit
end
