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
end
