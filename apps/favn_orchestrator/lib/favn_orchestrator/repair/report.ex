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
          backfill_windows_reconciled: non_neg_integer(),
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
            backfill_windows_reconciled: 0,
            backfill_parents_reprojected: 0,
            freshness_states_rebuilt: 0,
            freshness_states_skipped: 0,
            errors: []

  @doc "Returns a new empty report for the repair mode."
  @spec new(mode()) :: t()
  def new(mode) when mode in [:dry_run, :apply], do: %__MODULE__{mode: mode}

  @doc "Adds one count to a report counter."
  @spec bump(t(), atom(), non_neg_integer()) :: t()
  def bump(report, key, count \\ 1)

  def bump(%__MODULE__{} = report, :runs_scanned, count)
      when is_integer(count) and count >= 0,
      do: %{report | runs_scanned: report.runs_scanned + count}

  def bump(%__MODULE__{} = report, :runs_terminalized, count)
      when is_integer(count) and count >= 0,
      do: %{report | runs_terminalized: report.runs_terminalized + count}

  def bump(%__MODULE__{} = report, :steps_terminalized, count)
      when is_integer(count) and count >= 0,
      do: %{report | steps_terminalized: report.steps_terminalized + count}

  def bump(%__MODULE__{} = report, :execution_leases_expired, count)
      when is_integer(count) and count >= 0,
      do: %{report | execution_leases_expired: report.execution_leases_expired + count}

  def bump(%__MODULE__{} = report, :materialization_claims_expired, count)
      when is_integer(count) and count >= 0,
      do: %{
        report
        | materialization_claims_expired: report.materialization_claims_expired + count
      }

  def bump(%__MODULE__{} = report, :backfill_windows_reconciled, count)
      when is_integer(count) and count >= 0,
      do: %{report | backfill_windows_reconciled: report.backfill_windows_reconciled + count}

  def bump(%__MODULE__{} = report, :backfill_parents_reprojected, count)
      when is_integer(count) and count >= 0,
      do: %{report | backfill_parents_reprojected: report.backfill_parents_reprojected + count}

  def bump(%__MODULE__{} = report, :freshness_states_rebuilt, count)
      when is_integer(count) and count >= 0,
      do: %{report | freshness_states_rebuilt: report.freshness_states_rebuilt + count}

  def bump(%__MODULE__{} = report, :freshness_states_skipped, count)
      when is_integer(count) and count >= 0,
      do: %{report | freshness_states_skipped: report.freshness_states_skipped + count}

  def bump(%__MODULE__{}, key, count) do
    raise ArgumentError,
          "invalid repair report counter #{inspect(key)} or count #{inspect(count)}"
  end

  @doc "Adds an error to the report without raising."
  @spec error(t(), term()) :: t()
  def error(%__MODULE__{} = report, reason), do: %{report | errors: report.errors ++ [reason]}
end
