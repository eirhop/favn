defmodule Favn.Coverage.Summary do
  @moduledoc """
  Bounded coverage result for one asset evidence generation.

  Coverage is independent of freshness. A complete summary means every window
  expected at `evaluated_at` has authoritative success evidence for the pinned
  generation, including the valid zero-window case.
  """

  alias Favn.Window.Anchor

  @type status :: :complete | :incomplete | :unknown
  @type unknown_reason ::
          :coverage_not_declared
          | :non_windowed_asset
          | :target_generation_uninitialized
          | :authoritative_state_unavailable

  @enforce_keys [:status, :evaluated_at, :manifest_version_id, :target_id]
  defstruct [
    :status,
    :unknown_reason,
    :evaluated_at,
    :manifest_version_id,
    :target_id,
    :first_window,
    :last_expected_window,
    :expected_count,
    :covered_count,
    :missing_count,
    :evidence_generation_id,
    :active_target_generation_id,
    :evaluation_checksum
  ]

  @type t :: %__MODULE__{
          status: status(),
          unknown_reason: unknown_reason() | nil,
          evaluated_at: DateTime.t(),
          manifest_version_id: String.t(),
          target_id: String.t(),
          first_window: Anchor.t() | nil,
          last_expected_window: Anchor.t() | nil,
          expected_count: non_neg_integer() | nil,
          covered_count: non_neg_integer() | nil,
          missing_count: non_neg_integer() | nil,
          evidence_generation_id: String.t() | nil,
          active_target_generation_id: String.t() | nil,
          evaluation_checksum: String.t() | nil
        }

  @unknown_reasons [
    :coverage_not_declared,
    :non_windowed_asset,
    :target_generation_uninitialized,
    :authoritative_state_unavailable
  ]

  @doc "Builds and validates a coverage summary."
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    summary = struct(__MODULE__, Map.new(attrs))

    with :ok <- validate_identity(summary),
         :ok <- validate_status(summary),
         :ok <- validate_counts(summary) do
      {:ok, summary}
    end
  rescue
    KeyError -> {:error, :invalid_coverage_summary}
  end

  def new(_attrs), do: {:error, :invalid_coverage_summary}

  defp validate_identity(%__MODULE__{} = summary) do
    if match?(%DateTime{}, summary.evaluated_at) and valid_id?(summary.manifest_version_id) and
         valid_id?(summary.target_id),
       do: :ok,
       else: {:error, :invalid_coverage_summary_identity}
  end

  defp validate_status(%__MODULE__{status: :unknown, unknown_reason: reason})
       when reason in @unknown_reasons,
       do: :ok

  defp validate_status(%__MODULE__{status: status, unknown_reason: nil})
       when status in [:complete, :incomplete],
       do: :ok

  defp validate_status(_summary), do: {:error, :invalid_coverage_summary_status}

  defp validate_counts(%__MODULE__{status: :unknown}), do: :ok

  defp validate_counts(%__MODULE__{} = summary) do
    counts = [summary.expected_count, summary.covered_count, summary.missing_count]

    if Enum.all?(counts, &(is_integer(&1) and &1 >= 0)) and
         summary.covered_count + summary.missing_count == summary.expected_count and
         valid_id?(summary.evidence_generation_id) and
         valid_checksum?(summary.evaluation_checksum),
       do: :ok,
       else: {:error, :invalid_coverage_summary_counts}
  end

  defp valid_id?(value), do: is_binary(value) and byte_size(value) in 1..255

  defp valid_checksum?(value),
    do: is_binary(value) and byte_size(value) == 64 and value =~ ~r/\A[0-9a-f]+\z/
end
