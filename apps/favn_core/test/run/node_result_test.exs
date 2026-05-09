defmodule Favn.Run.NodeResultTest do
  use ExUnit.Case, async: true

  alias Favn.Run.NodeResult

  test "node result preserves planned node identity separately from ref" do
    result =
      NodeResult.new(
        node_key: {{MyApp.Asset, :asset}, "2026-04-15"},
        ref: {MyApp.Asset, :asset},
        stage: 1,
        status: :ok,
        freshness_key: "my_app.asset:2026-04-15",
        input_versions: %{{MyApp.Upstream, :asset} => "v1"},
        runner_execution_id: "exec_1",
        meta: %{rows_affected: 10}
      )

    assert result.node_key == {{MyApp.Asset, :asset}, "2026-04-15"}
    assert result.ref == {MyApp.Asset, :asset}
    assert result.status == :ok
    assert result.freshness_key == "my_app.asset:2026-04-15"
    assert result.input_versions == %{{MyApp.Upstream, :asset} => "v1"}
    assert result.runner_execution_id == "exec_1"
  end

  test "constructor accepts skipped_fresh and blocked statuses" do
    skipped =
      NodeResult.new(
        node_key: {{MyApp.Asset, :fresh}, nil},
        ref: {MyApp.Asset, :fresh},
        status: :skipped_fresh,
        reason: :fresh_enough
      )

    blocked =
      NodeResult.new(
        node_key: {{MyApp.Asset, :blocked}, nil},
        ref: {MyApp.Asset, :blocked},
        status: :blocked,
        reason: {:upstream_failed, {MyApp.Asset, :upstream}}
      )

    assert skipped.status == :skipped_fresh
    assert skipped.reason == :fresh_enough
    assert blocked.status == :blocked
    assert blocked.reason == {:upstream_failed, {MyApp.Asset, :upstream}}
  end

  test "constructor rejects invalid statuses" do
    assert_raise ArgumentError, ~r/invalid node result status :unknown/, fn ->
      NodeResult.new(
        node_key: {{MyApp.Asset, :asset}, nil},
        ref: {MyApp.Asset, :asset},
        status: :unknown
      )
    end
  end
end
