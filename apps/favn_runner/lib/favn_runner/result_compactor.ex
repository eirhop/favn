defmodule FavnRunner.ResultCompactor do
  @moduledoc """
  Trims optional result detail without discarding completion identity or write safety.

  The size target is best effort: essential fields remain even below their size
  floor, and the task codec still enforces its absolute transport limit.
  """

  alias Favn.Contracts.RunnerAssetEvidence
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerAssetResult
  alias Favn.Contracts.RunnerResult

  @spec compact(RunnerResult.t(), non_neg_integer()) ::
          {RunnerResult.t(), non_neg_integer(), boolean()}
  def compact(%RunnerResult{} = result, max_bytes)
      when is_integer(max_bytes) and max_bytes >= 0 do
    bytes = :erlang.external_size(result)

    if bytes <= max_bytes do
      {result, bytes, false}
    else
      compacted = %{
        result
        | asset_results: Enum.map(result.asset_results, &compact_asset_result/1),
          error: bounded_error(result.error),
          metadata: retention_metadata(result.metadata, bytes)
      }

      {compacted, :erlang.external_size(compacted), true}
    end
  end

  defp compact_asset_result(%RunnerAssetResult{} = result) do
    %{
      result
      | meta: %{retention_truncated: true},
        evidence: compact_evidence(result.evidence),
        error: bounded_error(result.error),
        attempts: []
    }
  end

  defp compact_evidence(nil), do: nil

  defp compact_evidence(%RunnerAssetEvidence{} = evidence) do
    %{
      evidence
      | check_results: [],
        metrics: %{},
        runtime_inputs: nil,
        contract_validation: nil,
        group_replacement: nil,
        command: nil,
        message: nil,
        reason: nil
    }
  end

  defp retention_metadata(_metadata, original_bytes) do
    %{
      retention: %{
        truncated: true,
        original_bytes: original_bytes,
        omitted: [:asset_meta, :asset_attempts, :runner_metadata, "asset_evidence_details"]
      }
    }
  end

  defp bounded_error(nil), do: nil

  defp bounded_error(%RunnerError{} = error), do: %{error | details: %{}}

  defp bounded_error(error) do
    if :erlang.external_size(error) <= 16 * 1_024 do
      error
    else
      %{
        type: :runner_result_error_truncated,
        kind: :boundary,
        message: "runner result error exceeded the retention boundary",
        retryable?: false
      }
    end
  end
end
