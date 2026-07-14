defmodule FavnOrchestrator.RunServer.Execution.ResultSanitizerTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerAssetResult
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias FavnOrchestrator.RunServer.Execution.ResultSanitizer

  test "redacts manually constructed runner errors throughout a result" do
    secret = "runner-secret-value"

    unsafe_error = %RunnerError{
      message: "request failed password=#{secret}",
      reason: "authorization=Bearer #{secret}",
      details: %{token: secret},
      redacted?: false
    }

    result = %RunnerResult{
      error: unsafe_error,
      asset_results: [
        %RunnerAssetResult{
          error: unsafe_error,
          attempts: [%{error: unsafe_error}]
        }
      ]
    }

    sanitized = ResultSanitizer.sanitize(result)

    assert %RunnerError{redacted?: true} = sanitized.error

    assert [%RunnerAssetResult{error: %RunnerError{}, attempts: [%{error: %RunnerError{}}]}] =
             sanitized.asset_results

    refute inspect(sanitized) =~ secret
    assert inspect(sanitized) =~ "[REDACTED]"
  end

  test "bounds arbitrary runner errors and strips stacktraces" do
    oversized = String.duplicate("x", 10_000)

    result = %RunnerResult{
      error: %{
        type: :unexpected_runner_error,
        message: oversized,
        stacktrace: [{__MODULE__, :test, 1}],
        nested: %{reason: "password=hidden"}
      }
    }

    sanitized = ResultSanitizer.sanitize(result)

    refute Map.has_key?(sanitized.error, :stacktrace)
    assert byte_size(sanitized.error.message) <= 8_195
    assert sanitized.error.nested.reason == "password=[REDACTED]"
  end

  test "treats malformed asset results as empty" do
    assert %RunnerResult{asset_results: []} =
             ResultSanitizer.sanitize(%RunnerResult{asset_results: :invalid})
  end

  test "nests only non-empty runner metadata" do
    metadata = %{request_id: "req_1"}

    assert ResultSanitizer.merge_metadata(metadata, %{}) == metadata
    assert ResultSanitizer.merge_metadata(metadata, :invalid) == metadata

    assert ResultSanitizer.merge_metadata(metadata, %{worker: "runner@node"}) ==
             %{request_id: "req_1", runner_metadata: %{worker: "runner@node"}}
  end
end
