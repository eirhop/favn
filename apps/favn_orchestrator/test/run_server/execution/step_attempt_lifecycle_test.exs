defmodule FavnOrchestrator.RunServer.Execution.StepAttemptLifecycleTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias Favn.Plan
  alias Favn.RelationRef
  alias Favn.Retry.Policy
  alias Favn.TargetIdentity
  alias FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle
  alias FavnOrchestrator.RunState

  test "maps runner statuses to run statuses and event types" do
    assert StepAttemptLifecycle.map_runner_status(:ok) == :ok
    assert StepAttemptLifecycle.map_runner_status(:cancelled) == :cancelled
    assert StepAttemptLifecycle.map_runner_status(:timed_out) == :timed_out
    assert StepAttemptLifecycle.map_runner_status(:anything_else) == :error

    assert StepAttemptLifecycle.step_outcome(:ok) == {:step_finished, false}
    assert StepAttemptLifecycle.step_outcome(:timed_out) == {:step_timed_out, true}
    assert StepAttemptLifecycle.step_outcome(:error) == {:step_failed, true}
  end

  test "runner retryability respects structured non-retryable errors" do
    retryable = %RunnerResult{
      status: :error,
      error:
        RunnerError.normalize(:temporary,
          retryable?: true,
          outcome: :safe_failure
        ),
      asset_results: []
    }

    non_retryable = %RunnerResult{
      status: :error,
      error: RunnerError.normalize(:bad_config, type: :missing_runtime_config, retryable?: false),
      asset_results: []
    }

    assert StepAttemptLifecycle.runner_result_retryable?(retryable)
    refute StepAttemptLifecycle.runner_result_retryable?(non_retryable)

    refute StepAttemptLifecycle.runner_result_retryable?(%RunnerResult{
             status: :error,
             error: %{details: %{asset_retryable?: true}},
             asset_results: []
           })

    refute StepAttemptLifecycle.runner_result_retryable?(%RunnerResult{
             status: :error,
             error: RunnerError.normalize(%{details: %{asset_retryable?: true}}),
             asset_results: []
           })

    assert StepAttemptLifecycle.runner_result_retryable?(%RunnerResult{
             status: :error,
             error: %{details: %{asset_retryable?: true}, outcome: :safe_failure},
             asset_results: []
           })

    refute StepAttemptLifecycle.runner_result_retryable?(:malformed_result)

    refute StepAttemptLifecycle.runner_result_retryable?(%RunnerResult{
             status: :error,
             asset_results: [:malformed_asset_result]
           })
  end

  test "schedule_retry returns explicit retry data until max attempts" do
    lifecycle = %StepAttemptLifecycle{
      run: run_state(max_attempts: 2, retry_backoff_ms: 25),
      node_key: {{MyApp.Assets.Lifecycle, :asset}, nil},
      asset_ref: {MyApp.Assets.Lifecycle, :asset},
      asset_step_id: "step_lifecycle",
      stage: 1,
      attempt: 1,
      max_attempts: 2,
      execution_pool: "default"
    }

    assert {:ok, retry} = StepAttemptLifecycle.schedule_retry(lifecycle)
    assert retry.next_attempt == 2
    assert retry.retry_after_ms == 25

    assert StepAttemptLifecycle.retry_event_payload(retry).asset_step_id == "step_lifecycle"
  end

  test "retry-after raises the policy delay without adding attempts" do
    lifecycle = %StepAttemptLifecycle{
      run: run_state(max_attempts: 2, retry_backoff_ms: 25),
      node_key: {{MyApp.Assets.Lifecycle, :asset}, nil},
      asset_ref: {MyApp.Assets.Lifecycle, :asset},
      asset_step_id: "step_lifecycle",
      stage: 1,
      attempt: 1,
      max_attempts: 2,
      execution_pool: "default"
    }

    failure = RunnerError.new(retryable?: true, outcome: :safe_failure, retry_after_ms: 250)

    assert {:ok, retry} = StepAttemptLifecycle.schedule_retry(lifecycle, failure)
    assert retry.next_attempt == 2
    assert retry.retry_after_ms == 250
  end

  test "schedule_retry is terminal at max attempts" do
    lifecycle = %StepAttemptLifecycle{
      run: run_state(max_attempts: 1),
      node_key: {{MyApp.Assets.Lifecycle, :asset}, nil},
      asset_ref: {MyApp.Assets.Lifecycle, :asset},
      asset_step_id: "step_lifecycle",
      attempt: 1,
      max_attempts: 1
    }

    assert StepAttemptLifecycle.schedule_retry(lifecycle) == :terminal
  end

  test "attaches one absolute deadline to work before runner preparation" do
    run = run_state(max_attempts: 1) |> Map.put(:timeout_ms, 100)
    work = %Favn.Contracts.RunnerWork{run_id: run.id, metadata: %{}}
    before_attach = DateTime.utc_now()

    prepared = StepAttemptLifecycle.attach_deadline(work, run)
    deadline_at = StepAttemptLifecycle.deadline_at(prepared)

    assert DateTime.compare(deadline_at, before_attach) == :gt
    assert DateTime.diff(deadline_at, before_attach, :millisecond) <= 100
    assert StepAttemptLifecycle.deadline_at(prepared) == deadline_at
  end

  test "runner work preserves the logical run start across attempts" do
    run = run_state(max_attempts: 2)
    node_key = {{MyApp.Assets.Lifecycle, :asset}, nil}

    assert {:ok, version} =
             Version.new(FavnTestSupport.with_manifest_contract(%Manifest{}),
               manifest_version_id: run.manifest_version_id
             )

    assert {:ok, index} = Index.build_from_version(version)
    lifecycle = StepAttemptLifecycle.new(run, version, node_key, 0, 2)

    assert {:ok, %{work: work}} = StepAttemptLifecycle.build_work(lifecycle, index)
    assert work.run_started_at == run.inserted_at
    assert work.required_runner_release_id == run.required_runner_release_id
  end

  test "runner work returns an explicit error when the compact index lacks the planned asset" do
    ref = {MyApp.Assets.MissingFromIndex, :asset}
    node_key = {ref, nil}

    plan = %Plan{
      target_refs: [ref],
      target_node_keys: [node_key],
      nodes: %{
        node_key => %{
          ref: ref,
          node_key: node_key,
          window: nil,
          upstream: [],
          downstream: [],
          stage: 0,
          execution_pool: nil,
          action: :run,
          retry_policy: Policy.default(),
          retry_policy_source: :default
        }
      }
    }

    run =
      RunState.new(
        id: "run_missing_compact_asset",
        manifest_version_id: "mv_missing_compact_asset",
        manifest_content_hash: "hash_missing_compact_asset",
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        asset_ref: ref,
        target_refs: [ref],
        plan: plan
      )

    version = %Version{
      manifest_version_id: run.manifest_version_id,
      content_hash: run.manifest_content_hash
    }

    lifecycle = StepAttemptLifecycle.new(run, version, node_key, 0, 1)

    assert {:error, :asset_not_found} =
             StepAttemptLifecycle.build_work(lifecycle, %Index{assets_by_ref: %{}})
  end

  test "runner work pins persisted output and upstream physical generations" do
    upstream_ref = {MyApp.Assets.GenerationUpstream, :asset}
    target_ref = {MyApp.Assets.GenerationTarget, :asset}
    upstream_generation_id = "018f47a0-7b0d-4b1a-8d8b-e18a9a987654"
    target_generation_id = "018f47a0-7b0d-4b1a-8d8b-e18a9a987655"
    upstream = persisted_asset(upstream_ref, "upstream")
    target = persisted_asset(target_ref, "target", [upstream_ref])
    {:ok, graph} = Graph.build([upstream, target])

    version = %Version{
      manifest_version_id: "mv_generation_work",
      content_hash: String.duplicate("c", 64),
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      manifest: %Manifest{assets: [upstream, target], graph: graph}
    }

    upstream_node = generation_node(upstream, upstream_generation_id, [], [node_key(target_ref)])

    target_node =
      generation_node(target, target_generation_id, [node_key(upstream_ref)], [])
      |> Map.put(:input_generations, [
        %{
          target_id: upstream.target_descriptor.target_id,
          target_generation_id: upstream_generation_id,
          evidence_generation_id: upstream_generation_id,
          physical_relation: upstream.target_descriptor.relation
        }
      ])

    plan = %Plan{
      target_refs: [target_ref],
      target_node_keys: [node_key(target_ref)],
      nodes: %{
        node_key(upstream_ref) => upstream_node,
        node_key(target_ref) => target_node
      }
    }

    run =
      RunState.new(
        id: "run_generation_work",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: version.required_runner_release_id,
        asset_ref: target_ref,
        target_refs: [target_ref],
        plan: plan
      )

    assert {:ok, index} = Index.build_from_version(version)

    lifecycle =
      StepAttemptLifecycle.new(run, %{version | manifest: nil}, node_key(target_ref), 1, 1)

    assert {:ok, %{work: work}} = StepAttemptLifecycle.build_work(lifecycle, index)
    assert work.target_operation == :normal_materialization
    assert work.logical_target_id == target.target_descriptor.target_id
    assert work.target_descriptor_hash == target.target_descriptor.descriptor_hash
    assert work.target_generation_id == target_generation_id
    assert work.active_relation == target.relation
    assert work.write_relation == target.relation

    assert [pin] = work.upstream_generation_pins
    assert pin.asset_ref == upstream_ref
    assert pin.target_generation_id == upstream_generation_id
    assert pin.relation == upstream.relation
    assert pin.descriptor_hash == upstream.target_descriptor.descriptor_hash
  end

  test "non-persisted output still pins persisted dependency reads" do
    upstream_ref = {MyApp.Assets.GenerationInput, :asset}
    target_ref = {MyApp.Assets.ElixirOutput, :asset}
    generation_id = "018f47a0-7b0d-4b1a-8d8b-e18a9a987654"
    upstream = persisted_asset(upstream_ref, "generation_input")

    target = %Asset{
      ref: target_ref,
      module: elem(target_ref, 0),
      name: :asset,
      type: :elixir,
      depends_on: [upstream_ref],
      semantic_generation_id: "ag_elixir_output"
    }

    {:ok, graph} = Graph.build([upstream, target])

    version = %Version{
      manifest_version_id: "mv_generation_read_work",
      content_hash: String.duplicate("d", 64),
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      manifest: %Manifest{assets: [upstream, target], graph: graph}
    }

    upstream_node = generation_node(upstream, generation_id, [], [node_key(target_ref)])

    target_node = %{
      ref: target_ref,
      node_key: node_key(target_ref),
      window: nil,
      upstream: [node_key(upstream_ref)],
      downstream: [],
      stage: 1,
      execution_pool: nil,
      action: :run,
      retry_policy: Policy.default(),
      retry_policy_source: :default,
      target_id: "asset:Elixir.MyApp.Assets.ElixirOutput:asset",
      target_generation_id: nil,
      evidence_generation_id: "ag_elixir_output",
      physical_relation: nil,
      input_generations: []
    }

    plan = %Plan{
      target_refs: [target_ref],
      target_node_keys: [node_key(target_ref)],
      nodes: %{
        node_key(upstream_ref) => upstream_node,
        node_key(target_ref) => target_node
      }
    }

    run =
      RunState.new(
        id: "run_generation_read_work",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: version.required_runner_release_id,
        asset_ref: target_ref,
        target_refs: [target_ref],
        plan: plan
      )

    assert {:ok, index} = Index.build_from_version(version)

    lifecycle =
      StepAttemptLifecycle.new(run, %{version | manifest: nil}, node_key(target_ref), 1, 1)

    assert {:ok, %{work: work}} = StepAttemptLifecycle.build_work(lifecycle, index)
    assert work.target_operation == nil
    assert [pin] = work.upstream_generation_pins
    assert pin.asset_ref == upstream_ref
    assert pin.target_generation_id == generation_id
  end

  test "persisted output omits generation pins for non-persisted dependency reads" do
    upstream_ref = {MyApp.Assets.SourceInput, :asset}
    target_ref = {MyApp.Assets.PersistedOutput, :asset}
    target_generation_id = "018f47a0-7b0d-4b1a-8d8b-e18a9a987655"

    upstream = %Asset{
      ref: upstream_ref,
      module: elem(upstream_ref, 0),
      name: :asset,
      type: :source,
      semantic_generation_id: "ag_source_input"
    }

    target = persisted_asset(target_ref, "persisted_output", [upstream_ref])
    {:ok, graph} = Graph.build([upstream, target])

    version = %Version{
      manifest_version_id: "mv_non_persisted_generation_read_work",
      content_hash: String.duplicate("e", 64),
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      manifest: %Manifest{assets: [upstream, target], graph: graph}
    }

    upstream_node = %{
      ref: upstream_ref,
      node_key: node_key(upstream_ref),
      window: nil,
      upstream: [],
      downstream: [node_key(target_ref)],
      stage: 0,
      execution_pool: nil,
      action: :run,
      retry_policy: Policy.default(),
      retry_policy_source: :default,
      target_id: TargetIdentity.for_asset(upstream_ref),
      target_generation_id: nil,
      evidence_generation_id: upstream.semantic_generation_id,
      physical_relation: nil,
      input_generations: []
    }

    target_node =
      generation_node(target, target_generation_id, [node_key(upstream_ref)], [])
      |> Map.put(:input_generations, [
        %{
          target_id: TargetIdentity.for_asset(upstream_ref),
          target_generation_id: nil,
          evidence_generation_id: upstream.semantic_generation_id,
          physical_relation: nil
        }
      ])

    plan = %Plan{
      target_refs: [target_ref],
      target_node_keys: [node_key(target_ref)],
      nodes: %{
        node_key(upstream_ref) => upstream_node,
        node_key(target_ref) => target_node
      }
    }

    run =
      RunState.new(
        id: "run_non_persisted_generation_read_work",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: version.required_runner_release_id,
        asset_ref: target_ref,
        target_refs: [target_ref],
        plan: plan
      )

    assert {:ok, index} = Index.build_from_version(version)

    lifecycle =
      StepAttemptLifecycle.new(run, %{version | manifest: nil}, node_key(target_ref), 1, 1)

    assert {:ok, %{work: work}} = StepAttemptLifecycle.build_work(lifecycle, index)
    assert work.target_generation_id == target_generation_id
    assert work.upstream_generation_pins == []
  end

  defp run_state(opts) do
    RunState.new(
      id: "run_lifecycle_test",
      manifest_version_id: "mv_lifecycle_test",
      manifest_content_hash: "hash_lifecycle_test",
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      asset_ref: {MyApp.Assets.Lifecycle, :asset},
      max_attempts: Keyword.get(opts, :max_attempts, 1),
      retry_backoff_ms: Keyword.get(opts, :retry_backoff_ms, 0)
    )
  end

  defp persisted_asset({module, name} = ref, relation_name, depends_on \\ []) do
    %Asset{
      ref: ref,
      module: module,
      name: name,
      type: :sql,
      relation:
        RelationRef.new!(connection: :warehouse, schema: "analytics", name: relation_name),
      materialization: :table,
      depends_on: depends_on,
      execution_package_hash: String.duplicate("a", 64)
    }
    |> FavnTestSupport.with_target_descriptor()
  end

  defp generation_node(asset, generation_id, upstream, downstream) do
    %{
      ref: asset.ref,
      node_key: node_key(asset.ref),
      window: nil,
      upstream: upstream,
      downstream: downstream,
      stage: if(upstream == [], do: 0, else: 1),
      execution_pool: nil,
      action: :run,
      retry_policy: Policy.default(),
      retry_policy_source: :default,
      target_id: asset.target_descriptor.target_id,
      target_generation_id: generation_id,
      evidence_generation_id: generation_id,
      physical_relation: asset.target_descriptor.relation,
      input_generations: []
    }
  end

  defp node_key(ref), do: {ref, nil}
end
