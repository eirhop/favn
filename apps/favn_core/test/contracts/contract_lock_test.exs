defmodule Favn.Contracts.ContractLockTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerEvent
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork

  test "runner work contract keys are locked" do
    assert_runner_keys(
      %RunnerWork{},
      [
        :asset_ref,
        :asset_refs,
        :manifest_content_hash,
        :manifest_version_id,
        :metadata,
        :params,
        :run_id,
        :trigger
      ]
    )
  end

  test "runner result contract keys are locked" do
    assert_runner_keys(
      %RunnerResult{},
      [
        :asset_results,
        :error,
        :manifest_content_hash,
        :manifest_version_id,
        :metadata,
        :run_id,
        :status
      ]
    )
  end

  test "runner event contract keys are locked" do
    assert_runner_keys(
      %RunnerEvent{},
      [
        :event_type,
        :manifest_content_hash,
        :manifest_version_id,
        :occurred_at,
        :payload,
        :run_id
      ]
    )
  end

  defp assert_runner_keys(struct, expected_keys) when is_list(expected_keys) do
    keys =
      struct
      |> Map.from_struct()
      |> Map.keys()
      |> Enum.sort()

    assert keys == Enum.sort(expected_keys)
  end
end
