defmodule Favn.Deployment.RunnerPoolSpecTest do
  use ExUnit.Case, async: true

  alias Favn.Deployment.RunnerPoolSpec

  @release "rr_" <> String.duplicate("a", 64)

  test "keeps arbitrary pool identity and calculates a safe hard timeout" do
    assert {:ok, spec} =
             RunnerPoolSpec.new(
               runner_pool: "duckdb_memory_heavy",
               runner_release_id: @release,
               lifecycle_mode: :elastic,
               max_runners: 12,
               polling_seconds: 5,
               runner_max_uptime_seconds: 3_600,
               max_task_seconds: 7_200,
               shutdown_grace_seconds: 120
             )

    assert RunnerPoolSpec.minimum_hard_timeout_seconds(spec) == 10_920

    assert RunnerPoolSpec.demand_path(spec) ==
             "/internal/runner-demand/duckdb_memory_heavy/#{@release}"
  end
end
