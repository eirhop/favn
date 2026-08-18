defmodule FavnOrchestrator.RunServer.Execution.RecoveryPosition do
  @moduledoc """
  Durable pipeline position needed to make active-task recovery fail closed.

  Node results are accumulated only in the live run process until a stage is
  drained. This marker records that the current stage attempt has already
  persisted at least one terminal node outcome, so recovery cannot reconstruct
  the remaining stage without risking duplicate work.
  """

  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState

  @outcome_key :pipeline_active_stage_outcome
  @outcome_string_key Atom.to_string(@outcome_key)

  @doc "Records that one outcome in the active pipeline stage is durable."
  @spec record_outcome(RunState.t(), non_neg_integer(), pos_integer()) :: RunState.t()
  def record_outcome(%RunState{} = run, stage, attempt)
      when is_integer(stage) and stage >= 0 and is_integer(attempt) and attempt > 0 do
    metadata =
      run.metadata
      |> Map.delete(@outcome_string_key)
      |> Map.put(@outcome_key, %{stage: stage, attempt: attempt})

    Snapshots.snapshot_update(run, metadata: metadata)
  end

  @doc "Clears the prior stage marker before a new pipeline stage attempt begins."
  @spec clear_outcome(RunState.t()) :: RunState.t()
  def clear_outcome(%RunState{} = run) do
    metadata = run.metadata |> Map.delete(@outcome_key) |> Map.delete(@outcome_string_key)
    Snapshots.snapshot_update(run, metadata: metadata)
  end

  @doc "Returns whether the durable snapshot contains active-stage outcome evidence."
  @spec outcome_recorded?(RunState.t()) :: boolean()
  def outcome_recorded?(%RunState{metadata: metadata}) when is_map(metadata) do
    Map.has_key?(metadata, @outcome_key) or Map.has_key?(metadata, @outcome_string_key)
  end
end
