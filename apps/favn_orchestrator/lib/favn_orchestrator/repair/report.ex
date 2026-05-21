defmodule FavnOrchestrator.Repair.Report do
  @moduledoc """
  Summary returned by runtime-state repair workflows.
  """

  @type mode :: :dry_run | :apply

  @type t :: %__MODULE__{
          mode: mode(),
          runs_scanned: non_neg_integer(),
          runs_terminalized: non_neg_integer(),
          steps_terminalized: non_neg_integer(),
          execution_leases_expired: non_neg_integer(),
          materialization_claims_expired: non_neg_integer(),
          backfill_parents_reprojected: non_neg_integer(),
          freshness_states_rebuilt: non_neg_integer(),
          freshness_states_skipped: non_neg_integer(),
          errors: [term()]
        }

  defstruct mode: :dry_run,
            runs_scanned: 0,
            runs_terminalized: 0,
            steps_terminalized: 0,
            execution_leases_expired: 0,
            materialization_claims_expired: 0,
            backfill_parents_reprojected: 0,
            freshness_states_rebuilt: 0,
            freshness_states_skipped: 0,
            errors: []

  @doc "Returns a new empty report for the repair mode."
  @spec new(mode()) :: t()
  def new(mode) when mode in [:dry_run, :apply], do: %__MODULE__{mode: mode}

  @doc "Adds one count to a report counter."
  @spec bump(t(), atom(), non_neg_integer()) :: t()
  def bump(%__MODULE__{} = report, key, count \\ 1) when is_atom(key) and count >= 0 do
    Map.update!(report, key, &(&1 + count))
  end

  @doc "Adds an error to the report without raising."
  @spec error(t(), term()) :: t()
  def error(%__MODULE__{} = report, reason), do: %{report | errors: report.errors ++ [reason]}
end
