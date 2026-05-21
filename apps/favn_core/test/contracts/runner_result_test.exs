defmodule Favn.Contracts.RunnerResultTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerAssetResult
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult

  test "keeps manifest identity in execution results" do
    result =
      %RunnerResult{
        run_id: "run_1",
        manifest_version_id: "mv_1",
        manifest_content_hash: "abc",
        status: :ok,
        asset_results: [%RunnerAssetResult{ref: {MyApp.Asset, :asset}, status: :ok}],
        metadata: %{duration_ms: 10}
      }

    assert result.manifest_version_id == "mv_1"
    assert result.manifest_content_hash == "abc"
    assert result.status == :ok
    assert [%RunnerAssetResult{ref: {MyApp.Asset, :asset}, status: :ok}] = result.asset_results
  end

  test "runner errors carry explicit retryability" do
    error = RunnerError.normalize(:bad_config, type: :missing_runtime_config, retryable?: false)

    assert error.type == :missing_runtime_config
    assert error.retryable? == false
    assert is_binary(error.reason)
  end

  test "runner errors redact nested operational detail text" do
    error =
      RunnerError.normalize(%{
        type: :backend_execution_failed,
        details: %{
          cause: %{
            message:
              "failed postgres://user:password@example/db token=abc123 Authorization: Bearer raw-token",
            reason: "retry failed with api_key=raw-key"
          }
        }
      })

    rendered = inspect(error)
    refute rendered =~ "user:password"
    refute rendered =~ "abc123"
    refute rendered =~ "raw-token"
    refute rendered =~ "raw-key"
    assert error.details.cause.message =~ "[REDACTED_URL]"
    assert error.details.cause.message =~ "token=[REDACTED]"
    assert error.details.cause.message =~ "Authorization=[REDACTED]"
  end

  test "runner errors redact structured sensitive detail keys" do
    error =
      RunnerError.normalize(%{
        type: :backend_execution_failed,
        details: %{
          authorization: "Bearer raw-token",
          api_key: "raw-key",
          url: "postgres://user:password@example/db"
        }
      })

    rendered = inspect(error)

    refute rendered =~ "raw-token"
    refute rendered =~ "raw-key"
    refute rendered =~ "user:password"
    assert error.details.authorization == :redacted
    assert error.details.api_key == :redacted
    assert error.details.url == :redacted
  end
end
