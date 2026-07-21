defmodule FavnOrchestrator.RunServer.Execution.ResultBuilderTest do
  use ExUnit.Case, async: true

  alias Favn.Plan
  alias Favn.Run.NodeResult
  alias FavnOrchestrator.RunServer.Execution.ResultBuilder
  alias FavnOrchestrator.RunState

  @first_ref {__MODULE__.First, :asset}
  @second_ref {__MODULE__.Second, :asset}

  test "records string-keyed runner fields against the planned node" do
    run = run_state()
    node_key = {@second_ref, nil}
    started_at = ~U[2026-07-13 10:00:00Z]
    finished_at = DateTime.add(started_at, 25, :millisecond)

    entry = %{
      asset_ref: @second_ref,
      node_key: node_key,
      execution_id: "exec_1",
      execution_pool: :fallback,
      freshness_key: "latest"
    }

    asset_result = %{
      "ref" => @second_ref,
      "started_at" => started_at,
      "finished_at" => finished_at,
      "duration_ms" => 25,
      "attempt_count" => 2,
      "max_attempts" => 3,
      "meta" => %{worker: "runner@node"},
      "attempts" => [%{attempt: 1}],
      "asset_step_id" => "step_1"
    }

    recorded = ResultBuilder.record_execution(run, entry, 1, 2, :ok, [asset_result])

    assert recorded.event_seq == run.event_seq

    assert [
             %NodeResult{
               node_key: ^node_key,
               ref: @second_ref,
               execution_pool: :analytics,
               status: :ok,
               started_at: ^started_at,
               finished_at: ^finished_at,
               duration_ms: 25,
               attempt_count: 2,
               max_attempts: 3,
               runner_execution_id: "exec_1",
               meta: %{worker: "runner@node"},
               attempts: [%{attempt: 1}],
               asset_step_id: "step_1"
             }
           ] = ResultBuilder.node_results(recorded)
  end

  test "sorts known results by plan order and preserves unknown order" do
    results = [
      %{ref: @second_ref, id: :second},
      %{"ref" => {__MODULE__.Unknown, :one}, "id" => :unknown_one},
      %{ref: @first_ref, id: :first},
      %{ref: {__MODULE__.Unknown, :two}, id: :unknown_two}
    ]

    assert [
             %{id: :first},
             %{id: :second},
             %{"id" => :unknown_one},
             %{id: :unknown_two}
           ] = ResultBuilder.sort_asset_results(run_state(), results)
  end

  test "aggregate result retains node results and run-owned metadata" do
    first = NodeResult.new(%{node_key: {@first_ref, nil}, ref: @first_ref, status: :ok})
    second = NodeResult.new(%{node_key: {@second_ref, nil}, ref: @second_ref, status: :ok})

    run =
      run_state()
      |> ResultBuilder.append_node_result(first)
      |> ResultBuilder.append_node_result(second)

    assert ResultBuilder.node_results(run) == [second, first]

    assert %{
             status: :ok,
             asset_results: [%{ref: @first_ref}],
             node_results: [^first, ^second],
             metadata: %{request_id: "req_1"}
           } = ResultBuilder.pipeline_result(run, :ok, [%{ref: @first_ref}])
  end

  test "aggregate result exposes only the latest outcome for a retried node" do
    node_key = {@first_ref, nil}
    failed = NodeResult.new(%{node_key: node_key, ref: @first_ref, status: :error})
    succeeded = NodeResult.new(%{node_key: node_key, ref: @first_ref, status: :ok})

    run =
      run_state()
      |> ResultBuilder.append_node_result(failed)
      |> ResultBuilder.append_node_result(succeeded)

    assert %{node_results: [^succeeded]} = ResultBuilder.pipeline_result(run, :ok, [])
    assert ResultBuilder.node_result_count(run) == 2
  end

  test "large runs retain bounded detail and exact node counts" do
    run =
      Enum.reduce(1..2_000, run_state(), fn index, run ->
        ref = {__MODULE__.Generated, :asset}
        result = NodeResult.new(%{node_key: {ref, index}, ref: ref, status: :ok})
        ResultBuilder.append_node_result(run, result)
      end)

    assert length(ResultBuilder.node_results(run)) == 128
    assert ResultBuilder.node_result_count(run) == 2_000
    assert ResultBuilder.results_truncated?(run)
    assert :erlang.external_size(run.result) < 250_000

    aggregate = ResultBuilder.pipeline_result(run, :ok, Enum.to_list(1..2_000))
    assert length(aggregate.asset_results) == 128
    assert aggregate.metadata.result_retention.truncated
    assert aggregate.metadata.result_retention.node_result_count == 2_000
  end

  defp run_state do
    first_key = {@first_ref, nil}
    second_key = {@second_ref, nil}

    plan = %Plan{
      target_refs: [@second_ref],
      target_node_keys: [second_key],
      topo_order: [@first_ref, @second_ref],
      stages: [[@first_ref], [@second_ref]],
      node_stages: [[first_key], [second_key]],
      nodes: %{
        first_key => plan_node(@first_ref, first_key, 0, nil),
        second_key => plan_node(@second_ref, second_key, 1, :analytics)
      }
    }

    RunState.new(
      id: "run_result_builder",
      manifest_version_id: "mv_1",
      manifest_content_hash: "hash_1",
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      asset_ref: @second_ref,
      target_refs: [@second_ref],
      plan: plan,
      metadata: %{request_id: "req_1"},
      max_attempts: 3,
      submit_kind: :pipeline
    )
  end

  defp plan_node(ref, node_key, stage, execution_pool) do
    %{
      ref: ref,
      node_key: node_key,
      window: nil,
      upstream: [],
      downstream: [],
      stage: stage,
      execution_pool: execution_pool,
      action: :run
    }
  end
end
