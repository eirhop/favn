defmodule FavnOrchestrator.Storage.RunSnapshotCodecTest do
  use ExUnit.Case, async: false

  alias Favn.Backfill.RangeResolver
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.Version
  alias Favn.Plan
  alias Favn.Run.AssetResult
  alias Favn.Run.NodeResult
  alias Favn.Retry.Policy
  alias Favn.Window.Selection
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.ManifestCodec
  alias FavnOrchestrator.Storage.RunSnapshotCodec

  defmodule UnexpectedRunnerError do
    defexception [:message, :token]
  end

  test "encodes run snapshots as explicit JSON-safe DTOs" do
    version = manifest_version("mv_run_snapshot_dto", __MODULE__.Asset)
    run = run_state("run_snapshot_dto", version, __MODULE__.Asset)

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    decoded = Jason.decode!(payload)

    assert decoded["format"] == "favn.run_snapshot.storage.v3"
    assert decoded["schema_version"] == 3
    assert decoded["required_runner_release_id"] == version.required_runner_release_id

    assert decoded["asset_ref"] == %{
             "module" => Atom.to_string(__MODULE__.Asset),
             "name" => "asset"
           }

    refute payload =~ "__type__"
    refute payload =~ "__struct__"
    refute payload =~ ~s("tuple")
    refute payload =~ ~s("atom")

    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert restored.id == run.id
    assert restored.asset_ref == {__MODULE__.Asset, :asset}
    assert restored.status == :pending
  end

  test "round-trips exact selections beyond the generic metadata entry limit" do
    version = manifest_version("mv_run_snapshot_selection_range", __MODULE__.Asset)

    assert {:ok, range} =
             RangeResolver.resolve(%{
               "kind" => "day",
               "from" => "2026-01-01",
               "to" => "2026-03-01",
               "timezone" => "Etc/UTC"
             })

    assert {:ok, selection} = Selection.backfill(range.anchors, range.timezone)

    run =
      "run_snapshot_selection_range"
      |> run_state(version, __MODULE__.Asset)
      |> Map.put(:metadata, %{window_selection: selection})
      |> RunState.with_snapshot_hash()

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert restored.metadata.window_selection == selection
    assert length(restored.metadata.window_selection.effective_anchors) == 60
  end

  test "maximum backfill selection fits the persisted run snapshot contract" do
    version = manifest_version("mv_run_snapshot_max_selection", __MODULE__.Asset)
    from = ~D[2026-01-01]
    through = Date.add(from, 9_999)
    timezone = "America/Argentina/Buenos_Aires"

    assert {:ok, range} =
             RangeResolver.resolve(%{
               "kind" => "day",
               "from" => Date.to_iso8601(from),
               "to" => Date.to_iso8601(through),
               "timezone" => timezone
             })

    assert range.requested_count == 10_000
    assert {:ok, selection} = Selection.backfill(range.anchors, timezone)

    run =
      "run_snapshot_max_selection"
      |> run_state(version, __MODULE__.Asset)
      |> Map.put(:metadata, %{window_selection: selection})
      |> RunState.with_snapshot_hash()

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert byte_size(payload) <= RunSnapshotCodec.max_persisted_bytes()
  end

  test "legacy snapshots are audit-readable only after terminalization" do
    version = manifest_version("mv_legacy_runner_release", __MODULE__.Asset)
    pending = run_state("run_legacy_runner_release", version, __MODULE__.Asset)
    assert {:ok, current_payload} = RunSnapshotCodec.encode_run(pending)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    legacy_pending = legacy_snapshot(current_payload)

    assert {:error, :legacy_runner_release_unbound} =
             RunSnapshotCodec.decode_run(
               %{run_blob: legacy_pending, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    terminal =
      pending
      |> RunState.transition(
        status: :ok,
        result: %{status: :ok, asset_results: []},
        metadata: %{terminal_event_type: :run_finished}
      )

    assert {:ok, terminal_payload} = RunSnapshotCodec.encode_run(terminal)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{
                 run_blob: legacy_snapshot(terminal_payload),
                 manifest_version_id: version.manifest_version_id
               },
               manifest_record
             )

    assert restored.required_runner_release_id == nil
    assert RunState.finalized?(restored)
  end

  test "rejects a current snapshot bound to a different runner release" do
    version = manifest_version("mv_mismatched_runner_release", __MODULE__.Asset)
    run = run_state("run_mismatched_runner_release", version, __MODULE__.Asset)
    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)
    alternate = FavnTestSupport.runner_release_id(:alternate)

    tampered =
      payload
      |> Jason.decode!()
      |> Map.put("required_runner_release_id", alternate)
      |> Jason.encode!()

    assert {:error, {:run_manifest_runner_release_mismatch, expected, ^alternate}} =
             RunSnapshotCodec.decode_run(
               %{run_blob: tampered, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert expected == version.required_runner_release_id
  end

  test "does not accept a current snapshot disguised with the legacy schema number" do
    version = manifest_version("mv_mixed_snapshot_format", __MODULE__.Asset)
    run = run_state("run_mixed_snapshot_format", version, __MODULE__.Asset)
    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    tampered =
      payload
      |> Jason.decode!()
      |> Map.put("schema_version", 2)
      |> Map.delete("required_runner_release_id")
      |> Jason.encode!()

    assert {:error, {:unsupported_run_snapshot_dto, _dto}} =
             RunSnapshotCodec.decode_run(
               %{run_blob: tampered, manifest_version_id: version.manifest_version_id},
               manifest_record
             )
  end

  test "free-form params and trigger ref-shaped maps remain JSON data" do
    version = manifest_version("mv_run_snapshot_free_form", __MODULE__.Asset)

    free_form_ref = %{"module" => Atom.to_string(__MODULE__.Asset), "name" => "asset"}

    run =
      RunState.new(
        id: "run_snapshot_free_form",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: version.required_runner_release_id,
        asset_ref: {__MODULE__.Asset, :asset},
        params: %{"payload" => free_form_ref},
        trigger: %{"payload" => free_form_ref}
      )

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert restored.params == %{"payload" => free_form_ref}
    assert restored.trigger == %{"payload" => free_form_ref}
  end

  test "pipeline replay metadata remains usable after DTO roundtrip" do
    deep_value = Enum.reduce(1..10, "leaf", &%{"level_#{&1}" => &2})

    settings =
      1..55
      |> Map.new(&{String.to_atom("snapshot_setting_#{&1}"), &1})
      |> Map.merge(%{
        source: "orders",
        api_url: "https://example.test/orders",
        token_ttl: 3_600,
        request: %{"path" => "/orders", "nested" => deep_value}
      })
      |> Favn.Settings.normalize!()

    anchor_window =
      Favn.Window.Anchor.new!(
        :day,
        DateTime.new!(
          ~D[2026-07-14],
          ~T[00:00:00],
          "Europe/Oslo",
          Favn.Timezone.database!()
        ),
        DateTime.new!(
          ~D[2026-07-15],
          ~T[00:00:00],
          "Europe/Oslo",
          Favn.Timezone.database!()
        ),
        timezone: "Europe/Oslo"
      )

    {:ok, window_selection} =
      Favn.Window.Selection.scheduled(anchor_window, 1, "Europe/Oslo")

    window = Favn.Window.Policy.new!(:day, timezone: "Europe/Oslo", lookback: 1)
    schedule = pipeline_schedule()

    version =
      pipeline_manifest_version(
        "mv_run_snapshot_pipeline_metadata",
        settings,
        window,
        schedule
      )

    run =
      RunState.new(
        id: "run_snapshot_pipeline_metadata",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: version.required_runner_release_id,
        asset_ref: {__MODULE__.Asset, :asset},
        metadata: %{
          replay_submit_kind: :pipeline,
          replay_mode: :exact_replay,
          pipeline_submit_ref: __MODULE__.Pipeline,
          pipeline_target_refs: [{__MODULE__.Asset, :asset}],
          window_selection: window_selection,
          pipeline_context: %{
            module: __MODULE__.Pipeline,
            name: :daily,
            resolved_refs: [{__MODULE__.Asset, :asset}],
            dependencies: :all,
            settings: settings,
            metadata: %{"owner" => "data-platform"},
            anchor_window: anchor_window,
            window_selection: window_selection,
            window: window,
            schedule: schedule
          }
        },
        submit_kind: :rerun
      )

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert restored.metadata.replay_submit_kind == :pipeline
    assert restored.metadata.replay_mode == :exact_replay
    assert restored.metadata.pipeline_submit_ref == __MODULE__.Pipeline
    assert restored.metadata.pipeline_target_refs == [{__MODULE__.Asset, :asset}]
    assert restored.metadata.window_selection == window_selection
    assert restored.metadata.pipeline_context.resolved_refs == [{__MODULE__.Asset, :asset}]

    pipeline_context = Favn.Run.PipelineContext.from_map(restored.metadata.pipeline_context)
    assert pipeline_context.ref == {__MODULE__.Pipeline, :daily}
    assert pipeline_context.dependencies == :all
    assert pipeline_context.settings == settings
    assert pipeline_context.metadata == %{"owner" => "data-platform"}
    assert pipeline_context.anchor_window == anchor_window
    assert pipeline_context.window_selection == window_selection
    assert pipeline_context.window == window
    assert pipeline_context.schedule == schedule

    work = %Favn.Contracts.RunnerWork{pipeline: pipeline_context}
    assert work.pipeline.anchor_window == anchor_window
    assert work.pipeline.window_selection == window_selection
    assert work.pipeline.window == window
    assert work.pipeline.schedule == schedule

    projected = Projector.project_run(restored)
    assert projected.replay_mode == :exact_replay
    assert projected.pipeline.resolved_refs == [{__MODULE__.Asset, :asset}]
    assert projected.submit_ref == __MODULE__.Pipeline
  end

  test "allows asset refs named tag when manifest contains tag selectors" do
    version = tag_asset_manifest_version("mv_run_snapshot_asset_named_tag")

    run =
      RunState.new(
        id: "run_snapshot_asset_named_tag",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: version.required_runner_release_id,
        asset_ref: {__MODULE__.TaggedAsset, :tag},
        target_refs: [{__MODULE__.TaggedAsset, :tag}],
        metadata: %{
          pipeline_context: %{
            module: __MODULE__.TaggedAsset,
            name: "source_raw_full_refresh",
            resolved_refs: [{__MODULE__.TaggedAsset, :tag}],
            dependencies: :none
          }
        }
      )

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert restored.asset_ref == {__MODULE__.TaggedAsset, :tag}
    assert restored.target_refs == [{__MODULE__.TaggedAsset, :tag}]
    assert restored.metadata.pipeline_context.resolved_refs == [{__MODULE__.TaggedAsset, :tag}]
  end

  test "operational in-flight execution ids remain atom-keyed after DTO roundtrip" do
    version = manifest_version("mv_run_snapshot_in_flight", __MODULE__.Asset)

    run =
      RunState.new(
        id: "run_snapshot_in_flight",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: version.required_runner_release_id,
        asset_ref: {__MODULE__.Asset, :asset},
        metadata: %{in_flight_execution_ids: ["exec_1", "exec_2"]}
      )

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert restored.metadata.in_flight_execution_ids == ["exec_1", "exec_2"]
    refute Map.has_key?(restored.metadata, "in_flight_execution_ids")
  end

  test "restores multi-ref plans from DTO snapshots" do
    version = multi_asset_manifest_version("mv_run_snapshot_multi_ref_plan")
    refs = [{__MODULE__.AssetA, :asset}, {__MODULE__.AssetB, :asset}]

    plan = %Plan{
      target_refs: refs,
      target_node_keys: Enum.map(refs, &{&1, nil}),
      dependencies: :all,
      nodes:
        Map.new(refs, fn ref ->
          {{ref, nil},
           %{
             ref: ref,
             node_key: {ref, nil},
             window: nil,
             upstream: [],
             downstream: [],
             stage: 0,
             execution_pool: :warehouse,
             action: :run,
             retry_policy: Policy.default(),
             retry_policy_source: :default
           }}
        end),
      topo_order: refs,
      stages: [refs],
      node_stages: [Enum.map(refs, &{&1, nil})]
    }

    run =
      RunState.new(
        id: "run_snapshot_multi_ref_plan",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: version.required_runner_release_id,
        asset_ref: {__MODULE__.AssetA, :asset},
        target_refs: refs,
        plan: plan
      )

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert restored.target_refs == refs
    assert restored.plan.target_refs == refs
    assert restored.plan.topo_order == refs
    assert restored.plan.stages == [refs]
    assert restored.plan.node_stages == [Enum.map(refs, &{&1, nil})]

    assert Enum.all?(restored.plan.nodes, fn {_key, node} -> node.execution_pool == :warehouse end)
  end

  test "round-trips named-zone windowed plans without timezone lookup and rejects corruption" do
    version = manifest_version("mv_run_snapshot_windowed_plan", __MODULE__.Asset)
    timezone = "Europe/Oslo"

    start_at =
      DateTime.new!(~D[2026-07-01], ~T[00:00:00], timezone, Favn.Timezone.database!())

    end_at =
      DateTime.new!(~D[2026-08-01], ~T[00:00:00], timezone, Favn.Timezone.database!())

    anchor_key = Favn.Window.Key.new!(:month, start_at, timezone)

    window =
      Favn.Window.Runtime.new!(:month, start_at, end_at, anchor_key, timezone: timezone)

    ref = {__MODULE__.Asset, :asset}
    node_key = {ref, window.key}

    plan = %Plan{
      target_refs: [ref],
      target_node_keys: [node_key],
      dependencies: :all,
      nodes: %{
        node_key => %{
          ref: ref,
          node_key: node_key,
          window: window,
          upstream: [],
          downstream: [],
          stage: 0,
          execution_pool: :warehouse,
          target_id: "asset:Elixir.FavnOrchestrator.Storage.RunSnapshotCodecTest.Asset:asset",
          target_generation_id: nil,
          evidence_generation_id: "ag_windowed_plan",
          physical_relation: nil,
          input_generations: [],
          action: :run,
          retry_policy: Policy.default(),
          retry_policy_source: :default
        }
      },
      topo_order: [ref],
      stages: [[ref]],
      node_stages: [[node_key]]
    }

    node_result =
      NodeResult.new(%{
        node_key: node_key,
        ref: ref,
        window: window,
        execution_pool: :warehouse,
        status: :error,
        started_at: start_at,
        finished_at: start_at,
        duration_ms: 0,
        attempt_count: 1,
        max_attempts: 1,
        error: %{type: :runtime_input_persistence_failed}
      })

    run =
      RunState.new(
        id: "run_snapshot_windowed_plan",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: version.required_runner_release_id,
        asset_ref: ref,
        target_refs: [ref],
        plan: plan
      )
      |> RunState.transition(
        status: :error,
        error: %{type: :runtime_input_persistence_failed},
        result: %{status: :error, node_results: [node_result]}
      )

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    without_timezone_database(fn ->
      assert {:ok, restored} =
               RunSnapshotCodec.decode_run(
                 %{run_blob: payload, manifest_version_id: version.manifest_version_id},
                 manifest_record
               )

      assert restored.plan == plan
      assert restored.plan_hash == run.plan_hash
      assert [%NodeResult{window: ^window, status: :error}] = restored.result.node_results
    end)

    assert {:error, :run_plan_hash_mismatch} =
             RunSnapshotCodec.decode_run(
               %{
                 run_blob: tamper_plan_stage(payload),
                 manifest_version_id: version.manifest_version_id
               },
               manifest_record
             )

    assert {:error, {:invalid_run_plan_hash, nil}} =
             RunSnapshotCodec.decode_run(
               %{
                 run_blob: delete_plan_hash(payload),
                 manifest_version_id: version.manifest_version_id
               },
               manifest_record
             )
  end

  test "round-trips persisted generation pins with an upstream input" do
    version = multi_asset_manifest_version("mv_run_snapshot_generation_pins")
    upstream_ref = {__MODULE__.AssetA, :asset}
    target_ref = {__MODULE__.AssetB, :asset}
    upstream_key = {upstream_ref, nil}
    target_key = {target_ref, nil}

    upstream_pin = %{
      target_id: "asset:Elixir.FavnOrchestrator.Storage.RunSnapshotCodecTest.AssetA:asset",
      target_generation_id: "018f47a0-7b0d-4b1a-8d8b-e18a9a987654",
      evidence_generation_id: "018f47a0-7b0d-4b1a-8d8b-e18a9a987654",
      physical_relation: %{
        "catalog" => nil,
        "connection" => "warehouse",
        "name" => "asset_a",
        "schema" => "analytics"
      }
    }

    target_pin = %{
      target_id: "asset:Elixir.FavnOrchestrator.Storage.RunSnapshotCodecTest.AssetB:asset",
      target_generation_id: "018f47a0-7b0d-4b1a-8d8b-e18a9a987655",
      evidence_generation_id: "018f47a0-7b0d-4b1a-8d8b-e18a9a987655",
      physical_relation: %{
        "catalog" => nil,
        "connection" => "warehouse",
        "name" => "asset_b",
        "schema" => "analytics"
      }
    }

    plan = %Plan{
      target_refs: [target_ref],
      target_node_keys: [target_key],
      nodes: %{
        upstream_key =>
          Map.merge(upstream_pin, %{
            ref: upstream_ref,
            node_key: upstream_key,
            window: nil,
            upstream: [],
            downstream: [target_key],
            stage: 0,
            execution_pool: :warehouse,
            input_generations: [],
            action: :run,
            retry_policy: Policy.default(),
            retry_policy_source: :default
          }),
        target_key =>
          Map.merge(target_pin, %{
            ref: target_ref,
            node_key: target_key,
            window: nil,
            upstream: [upstream_key],
            downstream: [],
            stage: 1,
            execution_pool: :warehouse,
            input_generations: [upstream_pin],
            action: :run,
            retry_policy: Policy.default(),
            retry_policy_source: :default
          })
      },
      topo_order: [upstream_ref, target_ref],
      stages: [[upstream_ref], [target_ref]],
      node_stages: [[upstream_key], [target_key]]
    }

    run =
      RunState.new(
        id: "run_snapshot_generation_pins",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: version.required_runner_release_id,
        asset_ref: target_ref,
        target_refs: [target_ref],
        plan: plan
      )

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert restored.plan == plan
    assert restored.plan_hash == run.plan_hash
  end

  test "normalizes unexpected exception structs before persistence" do
    version = manifest_version("mv_run_snapshot_exception", __MODULE__.Asset)

    error = %UnexpectedRunnerError{
      message: "database password=super-secret token=secret-token failed",
      token: "secret-token"
    }

    now = DateTime.utc_now()

    asset_result = %AssetResult{
      ref: {__MODULE__.Asset, :asset},
      stage: 0,
      status: :error,
      started_at: now,
      finished_at: now,
      duration_ms: 1,
      error: error,
      attempt_count: 1,
      max_attempts: 1,
      attempts: [%{attempt: 1, status: :error, error: error}]
    }

    run =
      "run_snapshot_exception"
      |> run_state(version, __MODULE__.Asset)
      |> RunState.transition(
        status: :error,
        error: error,
        result: %{status: :error, asset_results: [asset_result], metadata: %{password: "secret"}}
      )

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    refute payload =~ "super-secret"
    refute payload =~ "secret-token"
    refute payload =~ "__struct__"

    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert restored.status == :error
    assert restored.error["type"] == Atom.to_string(UnexpectedRunnerError)
    assert restored.error["message"] == "database password=[REDACTED] token=[REDACTED] failed"

    assert [%AssetResult{error: asset_error, attempts: [attempt]}] = restored.result.asset_results

    assert asset_error["message"] == "database password=[REDACTED] token=[REDACTED] failed"
    attempt_error = Map.get(attempt, "error") || Map.fetch!(attempt, :error)
    assert attempt_error["message"] == "database password=[REDACTED] token=[REDACTED] failed"
  end

  test "restores nil asset result datetimes as nil" do
    version = manifest_version("mv_run_snapshot_nil_asset_result_datetimes", __MODULE__.Asset)
    now = DateTime.utc_now()

    asset_result = %AssetResult{
      ref: {__MODULE__.Asset, :asset},
      stage: 0,
      status: :running,
      started_at: now,
      finished_at: nil,
      duration_ms: 0,
      error: nil,
      attempt_count: 1,
      max_attempts: 1,
      attempts: [],
      next_retry_at: nil
    }

    run =
      "run_snapshot_nil_asset_result_datetimes"
      |> run_state(version, __MODULE__.Asset)
      |> RunState.transition(status: :running, result: %{asset_results: [asset_result]})

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert [%AssetResult{} = restored_result] = restored.result.asset_results
    assert restored_result.finished_at == nil
    assert restored_result.next_retry_at == nil
  end

  test "preserves bounded SQL assurance evidence without generic JSON truncation" do
    version = manifest_version("mv_run_snapshot_assurance", __MODULE__.Asset)
    now = DateTime.utc_now()

    check_results =
      for index <- 1..53 do
        %{
          name: "check_#{index}",
          phase: :before_materialize,
          outcome: :passed,
          origin: if(index <= 3, do: :contract, else: :authored),
          claim_id: if(index <= 3, do: "claim.#{index}", else: nil),
          metrics: %{"evaluated_rows" => index}
        }
      end

    expected_columns =
      for index <- 1..60 do
        %{name: "column_#{index}", type: :integer, nullable?: false}
      end

    observed_columns =
      for index <- 1..60 do
        %{
          name: "column_#{index}",
          type: :integer,
          native_type: "INTEGER",
          nullable?: true,
          nullability_observed?: false
        }
      end

    asset_result = %AssetResult{
      ref: {__MODULE__.Asset, :asset},
      stage: 0,
      status: :ok,
      started_at: now,
      finished_at: now,
      duration_ms: 1,
      error: nil,
      attempt_count: 1,
      max_attempts: 1,
      attempts: [],
      meta: %{
        quality_status: :passed,
        write_outcome: :written,
        check_results: check_results,
        contract_validation: %Favn.SQL.ContractValidation{
          status: :passed,
          expected_columns: expected_columns,
          observed_columns: observed_columns,
          differences: [],
          observed_column_count: 60,
          observed_truncated?: false
        }
      }
    }

    run =
      "run_snapshot_assurance"
      |> run_state(version, __MODULE__.Asset)
      |> RunState.transition(status: :ok, result: %{asset_results: [asset_result]})

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    refute payload =~ "[TRUNCATED]"
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert [%AssetResult{meta: meta}] = restored.result.asset_results
    assert length(meta["check_results"]) == 53
    assert List.last(meta["check_results"])["metrics"]["evaluated_rows"] == 53
    assert length(meta["contract_validation"]["expected_columns"]) == 60
    assert length(meta["contract_validation"]["observed_columns"]) == 60
    assert hd(meta["contract_validation"]["expected_columns"])["type"] == "integer"
    assert meta["contract_validation"]["observed_column_count"] == 60
  end

  test "round-trips node results with freshness statuses" do
    version = manifest_version("mv_run_snapshot_node_results", __MODULE__.Asset)
    now = DateTime.utc_now()
    node_key = {{__MODULE__.Asset, :asset}, nil}

    skipped = %NodeResult{
      node_key: node_key,
      ref: {__MODULE__.Asset, :asset},
      window: %{date: ~D[2026-05-08]},
      stage: 0,
      execution_pool: :warehouse,
      status: :skipped_fresh,
      started_at: now,
      finished_at: now,
      duration_ms: 0,
      reason: %{fresh: true},
      freshness_key: "asset:2026-05-08",
      input_versions: %{upstream: "v1"},
      attempt_count: 0,
      max_attempts: 1,
      runner_execution_id: "runner-1",
      meta: %{cache: "hit"},
      attempts: []
    }

    blocked = %NodeResult{
      node_key: node_key,
      ref: {__MODULE__.Asset, :asset},
      stage: 1,
      status: :blocked,
      reason: :upstream_error,
      attempt_count: 0,
      max_attempts: 2,
      attempts: [%{attempt: 0, status: :blocked, reason: :upstream_error}]
    }

    run =
      "run_snapshot_node_results"
      |> run_state(version, __MODULE__.Asset)
      |> RunState.transition(status: :partial, result: %{node_results: [skipped, blocked]})

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert [restored_skipped, restored_blocked] = restored.result.node_results
    assert %NodeResult{status: :skipped_fresh} = restored_skipped
    assert restored_skipped.node_key == node_key
    assert restored_skipped.ref == {__MODULE__.Asset, :asset}
    assert restored_skipped.freshness_key == "asset:2026-05-08"
    assert restored_skipped.input_versions == %{upstream: "v1"}
    assert restored_skipped.reason == %{"fresh" => true}
    assert restored_skipped.started_at == now
    assert restored_skipped.execution_pool == :warehouse

    assert %NodeResult{status: :blocked} = restored_blocked
    assert restored_blocked.reason == "upstream_error"

    assert restored_blocked.attempts == [
             %{attempt: 0, status: "blocked", reason: "upstream_error"}
           ]
  end

  test "round-trips retrying asset and node results" do
    version = manifest_version("mv_run_snapshot_retrying_results", __MODULE__.Asset)
    ref = {__MODULE__.Asset, :asset}

    asset_result = %AssetResult{
      ref: ref,
      stage: 0,
      status: :retrying,
      duration_ms: 0,
      attempt_count: 1,
      max_attempts: 2
    }

    node_result =
      NodeResult.new(%{
        node_key: {ref, nil},
        ref: ref,
        status: :retrying,
        duration_ms: 0,
        attempt_count: 1,
        max_attempts: 2
      })

    run =
      "run_snapshot_retrying_results"
      |> run_state(version, __MODULE__.Asset)
      |> RunState.transition(
        status: :running,
        result: %{
          status: :running,
          asset_results: [asset_result],
          node_results: [node_result]
        }
      )

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert [%AssetResult{status: :retrying}] = restored.result.asset_results
    assert [%NodeResult{status: :retrying}] = restored.result.node_results
  end

  test "rejects refs that are not present in the associated manifest" do
    version = manifest_version("mv_run_snapshot_bad_ref", __MODULE__.Asset)
    run = run_state("run_snapshot_bad_ref", version, __MODULE__.Asset)

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    tampered =
      payload
      |> Jason.decode!()
      |> put_in(["asset_ref", "module"], "Elixir.Unknown.Asset")
      |> Jason.encode!()

    assert {:error, {:unknown_atom, "Elixir.Unknown.Asset"}} =
             RunSnapshotCodec.decode_run(
               %{run_blob: tampered, manifest_version_id: version.manifest_version_id},
               manifest_record
             )
  end

  test "rejects malformed persisted plan collections instead of dropping them" do
    version = multi_asset_manifest_version("mv_run_snapshot_bad_plan")
    run = run_state("run_snapshot_bad_plan", version, __MODULE__.AssetA)

    plan = %Plan{
      target_refs: [{__MODULE__.AssetA, :asset}],
      target_node_keys: [{{__MODULE__.AssetA, :asset}, nil}],
      dependencies: :all,
      nodes: %{
        {{__MODULE__.AssetA, :asset}, nil} => %{
          ref: {__MODULE__.AssetA, :asset},
          node_key: {{__MODULE__.AssetA, :asset}, nil},
          upstream: [],
          downstream: [],
          stage: 0
        }
      },
      topo_order: [{__MODULE__.AssetA, :asset}],
      stages: [[{__MODULE__.AssetA, :asset}]],
      node_stages: [[{{__MODULE__.AssetA, :asset}, nil}]]
    }

    assert {:ok, payload} = RunSnapshotCodec.encode_run(%{run | plan: plan})
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    tampered =
      payload |> Jason.decode!() |> put_in(["plan", "nodes"], "invalid") |> Jason.encode!()

    assert {:error, {:invalid_plan_nodes, "invalid"}} =
             RunSnapshotCodec.decode_run(
               %{run_blob: tampered, manifest_version_id: version.manifest_version_id},
               manifest_record
             )
  end

  test "rejects stale manifest content hash before trusting manifest refs" do
    version = manifest_version("mv_run_snapshot_stale_manifest", __MODULE__.Asset)
    run = run_state("run_snapshot_stale_manifest", version, __MODULE__.Asset)

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    stale_manifest_index_json =
      manifest_record.manifest_index_json
      |> Jason.decode!()
      |> put_in(["metadata", "changed"], true)
      |> Jason.encode!()

    stale_manifest_record = %{
      manifest_record
      | manifest_index_json: stale_manifest_index_json
    }

    assert {:error, {:manifest_content_hash_mismatch, _, _}} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               stale_manifest_record
             )
  end

  test "rejects run manifest content hash mismatch" do
    version = manifest_version("mv_run_snapshot_content_hash_mismatch", __MODULE__.Asset)
    run = run_state("run_snapshot_content_hash_mismatch", version, __MODULE__.Asset)

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    tampered =
      payload
      |> Jason.decode!()
      |> Map.put("manifest_content_hash", String.duplicate("f", 64))
      |> Jason.encode!()

    assert {:error, {:run_manifest_content_hash_mismatch, version_hash, other_hash}} =
             RunSnapshotCodec.decode_run(
               %{run_blob: tampered, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert version_hash == version.content_hash
    assert other_hash == String.duplicate("f", 64)
  end

  test "rejects structurally invalid run controls" do
    version = manifest_version("mv_run_snapshot_invalid_controls", __MODULE__.Asset)
    run = run_state("run_snapshot_invalid_controls", version, __MODULE__.Asset)

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)
    dto = Jason.decode!(payload)

    for {field, value, error} <- [
          {"lineage_depth", -1, {:invalid_run_field, :lineage_depth}},
          {"max_attempts", "many", {:invalid_run_field, :max_attempts}},
          {"retry_backoff_ms", -1, {:invalid_run_field, :retry_backoff_ms}},
          {"timeout_ms", 0, {:invalid_run_field, :timeout_ms}},
          {"params", [], {:invalid_run_field, :params}},
          {"trigger", "manual", {:invalid_run_field, :trigger}},
          {"metadata", [], {:invalid_run_field, :metadata}}
        ] do
      tampered = dto |> Map.put(field, value) |> Jason.encode!()

      assert {:error, ^error} =
               RunSnapshotCodec.decode_run(
                 %{run_blob: tampered, manifest_version_id: version.manifest_version_id},
                 manifest_record
               )
    end
  end

  test "rejects malformed persisted result collections and entries" do
    version = manifest_version("mv_run_snapshot_invalid_results", __MODULE__.Asset)

    run =
      "run_snapshot_invalid_results"
      |> run_state(version, __MODULE__.Asset)
      |> RunState.transition(status: :running, result: %{status: :running})

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)
    dto = Jason.decode!(payload)

    for {field, value, error} <- [
          {"asset_results", %{}, {:invalid_result_collection, :asset_results, %{}}},
          {"node_results", "invalid", {:invalid_result_collection, :node_results, "invalid"}},
          {"asset_results", ["invalid"], {:invalid_result_entry, :asset_results, "invalid"}},
          {"node_results", [42], {:invalid_result_entry, :node_results, 42}}
        ] do
      tampered = dto |> put_in(["result", field], value) |> Jason.encode!()

      assert {:error, ^error} =
               RunSnapshotCodec.decode_run(
                 %{run_blob: tampered, manifest_version_id: version.manifest_version_id},
                 manifest_record
               )
    end
  end

  test "rejects invalid typed fields in persisted result entries" do
    version = manifest_version("mv_run_snapshot_invalid_result_fields", __MODULE__.Asset)
    now = DateTime.utc_now()
    ref = {__MODULE__.Asset, :asset}

    asset_result = %AssetResult{
      ref: ref,
      stage: 0,
      status: :running,
      started_at: now,
      duration_ms: 0,
      attempt_count: 1,
      max_attempts: 1
    }

    node_result =
      NodeResult.new(%{
        node_key: {ref, nil},
        ref: ref,
        execution_pool: :warehouse,
        status: :running,
        started_at: now,
        duration_ms: 0,
        attempt_count: 1
      })

    run =
      "run_snapshot_invalid_result_fields"
      |> run_state(version, __MODULE__.Asset)
      |> RunState.transition(
        status: :running,
        result: %{status: :running, asset_results: [asset_result], node_results: [node_result]}
      )

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)
    dto = Jason.decode!(payload)

    for {collection, field, value} <- [
          {:asset_results, :status, "invented"},
          {:asset_results, :started_at, "yesterday"},
          {:asset_results, :duration_ms, -1},
          {:asset_results, :attempt_count, "many"},
          {:asset_results, :meta, []},
          {:node_results, :execution_pool, "unknown_pool"},
          {:node_results, :duration_ms, "slow"},
          {:node_results, :attempts, %{}},
          {:node_results, :max_attempts, 0}
        ] do
      collection_key = Atom.to_string(collection)
      field_key = Atom.to_string(field)

      tampered =
        dto
        |> update_in(
          ["result", collection_key, Access.at(0)],
          &Map.put(&1, field_key, value)
        )
        |> Jason.encode!()

      assert {:error,
              {:invalid_result_entry, ^collection,
               {:invalid_result_field, ^collection, ^field, ^value}}} =
               RunSnapshotCodec.decode_run(
                 %{run_blob: tampered, manifest_version_id: version.manifest_version_id},
                 manifest_record
               )
    end
  end

  defp manifest_version(manifest_version_id, module) do
    manifest = %Manifest{
      assets: [
        %Asset{
          ref: {module, :asset},
          module: module,
          name: :asset,
          execution_pool: :warehouse
        }
      ]
    }

    {:ok, version} =
      Version.new(
        manifest
        |> FavnTestSupport.with_manifest_graph()
        |> FavnTestSupport.with_manifest_contract(),
        manifest_version_id: manifest_version_id
      )

    version
  end

  defp pipeline_manifest_version(manifest_version_id, settings, window, schedule) do
    manifest = %Manifest{
      assets: [
        %Asset{
          ref: {__MODULE__.Asset, :asset},
          module: __MODULE__.Asset,
          name: :asset,
          execution_pool: :warehouse
        }
      ],
      pipelines: [
        %Pipeline{
          module: __MODULE__.Pipeline,
          name: :daily,
          selectors: [{:asset, {__MODULE__.Asset, :asset}}],
          deps: :all,
          schedule: {:inline, schedule},
          window: window,
          settings: settings,
          metadata: %{owner: "data-platform"}
        }
      ]
    }

    {:ok, version} =
      Version.new(
        manifest
        |> FavnTestSupport.with_manifest_graph()
        |> FavnTestSupport.with_manifest_contract(),
        manifest_version_id: manifest_version_id
      )

    version
  end

  defp pipeline_schedule do
    %Schedule{
      module: __MODULE__.Pipeline,
      name: :daily,
      ref: {__MODULE__.Pipeline, :daily},
      kind: :cron,
      cron: "0 2 * * *",
      timezone: "Etc/UTC",
      missed: :one,
      overlap: :queue_one,
      active: true,
      origin: :inline
    }
  end

  defp multi_asset_manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Asset{
          ref: {__MODULE__.AssetA, :asset},
          module: __MODULE__.AssetA,
          name: :asset,
          execution_pool: :warehouse
        },
        %Asset{
          ref: {__MODULE__.AssetB, :asset},
          module: __MODULE__.AssetB,
          name: :asset,
          execution_pool: :warehouse
        }
      ]
    }

    {:ok, version} =
      Version.new(
        manifest
        |> FavnTestSupport.with_manifest_graph()
        |> FavnTestSupport.with_manifest_contract(),
        manifest_version_id: manifest_version_id
      )

    version
  end

  defp tag_asset_manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Asset{
          ref: {__MODULE__.TaggedAsset, :tag},
          module: __MODULE__.TaggedAsset,
          name: :tag,
          metadata: %{tags: [:source_full_refresh]}
        }
      ],
      pipelines: [
        %Pipeline{
          module: __MODULE__.TaggedPipeline,
          name: :source_raw_full_refresh,
          selectors: [{:tag, :source_full_refresh}],
          deps: :none
        }
      ]
    }

    {:ok, version} =
      Version.new(
        manifest
        |> FavnTestSupport.with_manifest_graph()
        |> FavnTestSupport.with_manifest_contract(),
        manifest_version_id: manifest_version_id
      )

    version
  end

  defp run_state(run_id, version, module) do
    RunState.new(
      id: run_id,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: version.required_runner_release_id,
      asset_ref: {module, :asset},
      target_refs: [{module, :asset}]
    )
  end

  defp tamper_plan_stage(payload) do
    payload
    |> Jason.decode!()
    |> update_in(["plan", "nodes"], fn nodes ->
      Enum.map(nodes, &Map.update!(&1, "stage", fn stage -> stage + 1 end))
    end)
    |> Jason.encode!()
  end

  defp legacy_snapshot(payload) do
    payload
    |> Jason.decode!()
    |> Map.put("format", "favn.run_snapshot.storage.v2")
    |> Map.put("schema_version", 2)
    |> Map.delete("required_runner_release_id")
    |> Jason.encode!()
  end

  defp delete_plan_hash(payload) do
    payload
    |> Jason.decode!()
    |> Map.delete("plan_hash")
    |> Jason.encode!()
  end

  defp without_timezone_database(function) do
    previous = Application.fetch_env(:favn_core, :time_zone_database)
    Application.put_env(:favn_core, :time_zone_database, __MODULE__.UnavailableTimezoneDatabase)

    try do
      function.()
    after
      case previous do
        {:ok, database} -> Application.put_env(:favn_core, :time_zone_database, database)
        :error -> Application.delete_env(:favn_core, :time_zone_database)
      end
    end
  end
end
