defmodule FavnOrchestrator.RunServer.Execution.PipelineRetryCheckpointTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunServer.Execution.PipelineRetryCheckpoint

  test "round-trips a sparse retry selection in stage order" do
    stage = Enum.map(1..20, &{:node, &1})
    retry = [Enum.at(stage, 19), Enum.at(stage, 2), Enum.at(stage, 10)]

    assert {:ok, encoded} = PipelineRetryCheckpoint.encode(stage, retry)
    assert {:ok, selected} = PipelineRetryCheckpoint.decode(encoded, stage)
    assert selected == [Enum.at(stage, 2), Enum.at(stage, 10), Enum.at(stage, 19)]
  end

  test "keeps a hundred-thousand-node retry checkpoint compact and linear" do
    stage = Enum.map(1..100_000, &{:node, &1})
    retry = Enum.take_every(stage, 2)

    assert {:ok, encoded} = PipelineRetryCheckpoint.encode(stage, retry)
    assert byte_size(encoded.bits) < 20_000
    assert {:ok, ^retry} = PipelineRetryCheckpoint.decode(encoded, stage)
  end

  test "rejects selections outside the planned stage and malformed payloads" do
    assert {:error, :invalid_retry_selection} =
             PipelineRetryCheckpoint.encode([:one], [:other])

    assert {:error, :invalid_retry_checkpoint} =
             PipelineRetryCheckpoint.decode(
               %{encoding: "stage_bitset_v1", stage_size: 1, retry_count: 1, bits: ""},
               [:one]
             )
  end
end
