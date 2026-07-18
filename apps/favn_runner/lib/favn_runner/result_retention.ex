defmodule FavnRunner.ResultRetention do
  @moduledoc false

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

      compacted =
        if :erlang.external_size(compacted) <= max_bytes do
          compacted
        else
          minimal_result(compacted, bytes)
        end

      {compacted, :erlang.external_size(compacted), true}
    end
  end

  defp compact_asset_result(%RunnerAssetResult{} = result) do
    %{
      result
      | meta: %{retention_truncated: true},
        error: bounded_error(result.error),
        attempts: []
    }
  end

  defp compact_asset_result(result) when is_map(result) do
    result
    |> Map.take([
      :ref,
      "ref",
      :status,
      "status",
      :started_at,
      "started_at",
      :finished_at,
      "finished_at",
      :duration_ms,
      "duration_ms",
      :error,
      "error",
      :attempt_count,
      "attempt_count",
      :max_attempts,
      "max_attempts",
      :asset_step_id,
      "asset_step_id"
    ])
    |> Map.put(:retention_truncated, true)
  end

  defp compact_asset_result(_result), do: %{retention_truncated: true}

  defp retention_metadata(_metadata, original_bytes) do
    %{
      retention: %{
        truncated: true,
        original_bytes: original_bytes,
        omitted: [:asset_meta, :asset_attempts, :runner_metadata]
      }
    }
  end

  defp minimal_result(%RunnerResult{} = result, original_bytes) do
    %{
      result
      | asset_results: [],
        error: bounded_error(result.error),
        metadata: retention_metadata(%{}, original_bytes)
    }
  end

  defp bounded_error(nil), do: nil

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
