defmodule FavnOrchestrator.Storage.ExecutionOwnershipCodecTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.ExecutionOwnershipCodec

  test "encodes execution ownership as JSON-safe payload" do
    run =
      RunState.new(
        id: "run_codec",
        manifest_version_id: "mv_codec",
        manifest_content_hash: "hash_codec",
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        asset_ref: {MyApp.Assets.Raw, :asset}
      )

    ownership =
      run
      |> RunExecutionOwnership.new(
        asset_step_id: "run_codec:raw",
        node_key: {{MyApp.Assets.Raw, :asset}, nil},
        asset_ref: {MyApp.Assets.Raw, :asset},
        stage: 0,
        attempt: 1,
        execution_pool: :default
      )
      |> RunExecutionOwnership.submitted("exec_codec")
      |> RunExecutionOwnership.finish_persist_pending()

    assert {:ok, payload} = ExecutionOwnershipCodec.encode(ownership)
    assert {:ok, %{"format" => "json-v1"}} = Jason.decode(payload)
    assert {:ok, ^ownership} = ExecutionOwnershipCodec.decode(payload)
  end

  test "rejects unknown persisted statuses and malformed timestamps" do
    now = DateTime.utc_now()

    base = %{
      ownership_id: "ownership_invalid",
      run_id: "run_invalid",
      asset_step_id: "run_invalid:asset",
      dispatch_id: "ownership_invalid",
      inserted_at: now,
      updated_at: now
    }

    assert {:error, {:invalid_execution_ownership_field, :status}} =
             ExecutionOwnershipCodec.normalize(Map.put(base, :status, "invented_active_status"))

    assert {:error, {:invalid_execution_ownership_field, :inserted_at}} =
             ExecutionOwnershipCodec.normalize(Map.put(base, :inserted_at, "not-a-datetime"))
  end

  test "redacts and bounds persisted cancellation diagnostics" do
    now = DateTime.utc_now()

    assert {:ok, normalized} =
             ExecutionOwnershipCodec.normalize(%{
               ownership_id: "ownership_safe",
               run_id: "run_safe",
               asset_step_id: "run_safe:asset",
               dispatch_id: "ownership_safe",
               status: :best_effort_failed,
               cancel_status: :best_effort_failed,
               cancel_reason: %{token: "secret", message: String.duplicate("x", 10_000)},
               last_error: "password=secret",
               inserted_at: now,
               updated_at: now
             })

    assert normalized.cancel_reason.token == "[REDACTED]"
    assert byte_size(normalized.cancel_reason.message) == 8_192
    assert normalized.last_error == "password=[REDACTED]"
  end
end
