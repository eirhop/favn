defmodule FavnOrchestrator.Backfill.CoverageProjector do
  @moduledoc """
  Projects validated coverage evidence from successful runs.

  Coverage is optional derived state. Invalid or absent evidence must not make
  the authoritative run transition fail.
  """

  alias FavnOrchestrator.Backfill.CoverageEvidence
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  @spec project_transition(RunState.t(), atom(), map()) :: :ok | {:error, term()}
  def project_transition(%RunState{} = run, :run_finished, _data) do
    case CoverageEvidence.from_run(run) do
      {:ok, baseline} -> Storage.put_coverage_baseline(baseline)
      :ignore -> :ok
      {:error, _reason} -> :ok
    end
  end

  def project_transition(%RunState{}, _event_type, _data), do: :ok
end
