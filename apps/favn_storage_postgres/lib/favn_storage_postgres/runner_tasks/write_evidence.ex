defmodule FavnStoragePostgres.RunnerTasks.WriteEvidence do
  @moduledoc false
  alias Favn.Contracts, as: C
  alias Favn.TargetCompatibility.PhysicalFingerprint

  def validate(%{task_kind: :asset_attempt}, %{disposition: :verified_no_effect}, []),
    do: {:ok, %{"disposition" => "administrator_verified_no_effect"}}

  def validate(%{task_kind: :generation_activate, payload: request}, command, [observation]) do
    with true <- fresh?(observation, command),
         %{
           task_kind: :generation_reconcile,
           payload: %C.GenerationReconciliationRequest{activation: ^request},
           result: result
         } <- observation,
         :ok <- C.GenerationReconciliationResult.validate(result, observation.payload),
         true <- result.disposition in [:candidate_active, :previous_active] do
      {:ok, %{"disposition" => Atom.to_string(result.disposition)}}
    else
      _invalid -> {:error, :write_resolution_outcome_unproved}
    end
  end

  def validate(%{task_kind: kind, payload: request} = task, command, [marker_task, inspection])
      when kind in [:generation_marker_initialize, :generation_discard] do
    relation =
      if kind == :generation_discard,
        do: request.candidate_relation,
        else: request.active_relation

    with true <- fresh?(marker_task, command) and fresh?(inspection, command),
         %{
           task_kind: :generation_marker_read,
           payload: %{asset_ref: ref},
           result: %C.GenerationMarkerReadResult{marker: marker}
         } <- marker_task,
         true <- Favn.TargetIdentity.for_asset(ref) == task.write_target_id,
         %{
           task_kind: :relation_inspection,
           payload: %{relation: ^relation, asset_ref: nil, include: include},
           result: %C.RelationInspectionResult{relation_ref: ^relation}
         } <- inspection,
         true <- :relation in include,
         :ok <- domain_outcome(kind, request, marker, inspection.result, inspection.terminal_at) do
      {:ok,
       %{
         "disposition" =>
           if(kind == :generation_discard, do: "candidate_absent", else: "marker_initialized")
       }}
    else
      _invalid -> {:error, :write_resolution_outcome_unproved}
    end
  end

  def validate(_task, _command, _observations), do: {:error, :write_resolution_outcome_unproved}

  defp fresh?(%{status: :succeeded, data_state: :available} = observation, command) do
    command.disposition == :observe_generation and
      DateTime.compare(observation.enqueued_at, command.stopped_at) != :lt and
      DateTime.compare(observation.terminal_at, command.stopped_at) != :lt
  end

  defp fresh?(_observation, _command), do: false

  defp domain_outcome(:generation_marker_initialize, request, marker, inspection, observed_at) do
    with {:ok, %PhysicalFingerprint{} = fingerprint} <-
           PhysicalFingerprint.from_inspection(inspection) do
      C.GenerationMarkerInitializationResult.validate(
        %C.GenerationMarkerInitializationResult{
          required_runner_release_id: request.required_runner_release_id,
          target_id: request.target_id,
          target_generation_id: request.target_generation_id,
          initialization_token: request.initialization_token,
          outcome: :succeeded,
          observed_marker: marker,
          physical_fingerprint: fingerprint.fingerprint,
          completed_at: observed_at
        },
        request
      )
    end
  end

  defp domain_outcome(:generation_discard, request, marker, inspection, observed_at) do
    if absent?(inspection) do
      C.GenerationDiscardResult.validate(
        %C.GenerationDiscardResult{
          required_runner_release_id: request.required_runner_release_id,
          target_id: request.target_id,
          candidate_generation_id: request.candidate_generation_id,
          discard_token: request.discard_token,
          outcome: :already_absent,
          observed_marker: marker,
          candidate_present: false,
          completed_at: observed_at
        },
        request
      )
    else
      {:error, :candidate_absence_unproved}
    end
  end

  defp absent?(%C.RelationInspectionResult{relation: nil, error: nil, warnings: warnings}),
    do:
      is_list(warnings) and
        Enum.all?(
          warnings,
          &match?(
            %{code: code, message: message}
            when code in [
                   :columns_failed,
                   :row_count_failed,
                   :sample_failed,
                   :table_metadata_failed
                 ] and is_binary(message),
            &1
          )
        )

  defp absent?(_result), do: false
end
