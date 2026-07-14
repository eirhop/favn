defmodule FavnOrchestrator.ManifestStoreTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Version
  alias Favn.Run.AssetResult
  alias Favn.Run.NodeResult
  alias Favn.Window.Policy
  alias Favn.Window.Runtime, as: RuntimeWindow
  alias Favn.Window.Spec, as: WindowSpec
  alias FavnOrchestrator
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.AssetWindowProjector
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory
  alias FavnOrchestrator.TargetStatus.Projector, as: TargetStatusProjector
  alias FavnOrchestrator.TransitionWriter

  setup do
    Memory.reset()
    :ok
  end

  test "registers, lists, fetches, and activates manifests" do
    version_a = manifest_version("mv_a", {MyApp.AssetA, :asset})
    version_b = manifest_version("mv_b", {MyApp.AssetB, :asset})

    assert :ok = ManifestStore.register_manifest(version_a)
    assert :ok = ManifestStore.register_manifest(version_b)

    assert {:ok, versions} = ManifestStore.list_manifests()
    assert Enum.map(versions, & &1.manifest_version_id) == ["mv_a", "mv_b"]

    assert {:ok, fetched} = ManifestStore.get_manifest("mv_a")
    assert fetched.content_hash == version_a.content_hash

    assert :ok = ManifestStore.set_active_manifest("mv_b")
    assert {:ok, "mv_b"} = ManifestStore.get_active_manifest()
  end

  test "exposes operator manifest summaries and active-manifest targets" do
    window_spec = WindowSpec.new!(:day, lookback: 2, refresh_from: :hour, required: true)

    version_a =
      manifest_version(
        "mv_a",
        {MyApp.AssetA, :asset},
        [
          %Pipeline{
            module: MyApp.PipelineA,
            name: :pipeline_a,
            selectors: [{MyApp.AssetA, :asset}],
            deps: :all,
            window: Policy.new!(:daily),
            source: :dsl,
            outputs: [],
            config: %{},
            metadata: %{}
          }
        ],
        window_spec
      )

    version_b = manifest_version("mv_b", {MyApp.AssetB, :asset})

    assert :ok = ManifestStore.register_manifest(version_a)
    assert :ok = ManifestStore.register_manifest(version_b)
    assert :ok = ManifestStore.set_active_manifest("mv_a")

    assert {:ok, summaries} = FavnOrchestrator.list_manifest_summaries()
    assert Enum.map(summaries, & &1.manifest_version_id) == ["mv_a", "mv_b"]

    assert {:ok, summary} = FavnOrchestrator.get_manifest_summary("mv_a")
    assert summary.asset_count == 1
    assert summary.pipeline_count == 1

    assert {:ok, targets} = FavnOrchestrator.active_manifest_targets()
    assert targets.manifest_version_id == "mv_a"
    assert [asset] = targets.assets
    assert asset.label == "{MyApp.AssetA, :asset}"

    assert asset.window == %{
             kind: "day",
             lookback: 2,
             refresh_from: "hour",
             required: true,
             timezone: "Etc/UTC"
           }

    refute Map.has_key?(asset.window, "__struct__")

    assert [pipeline] = targets.pipelines
    assert pipeline.target_id == "pipeline:Elixir.MyApp.PipelineA"

    assert pipeline.window == %{
             kind: "day",
             anchor: "previous_complete_period",
             timezone: nil,
             allow_full_load: false
           }

    finished_at = ~U[2026-05-10 12:00:00Z]
    old_finished_at = ~U[2026-05-09 12:00:00Z]

    assert :ok =
             Storage.put_run(
               run_state(
                 "run_old_asset_a",
                 {MyApp.AssetA, :asset},
                 :error,
                 old_finished_at,
                 "mv_old"
               )
             )

    assert :ok =
             Storage.put_run(
               run_state("run_asset_a", {MyApp.AssetA, :asset}, :ok, finished_at, "mv_a")
             )

    assert {:ok, freshness} =
             AssetFreshnessState.new(%{
               asset_ref_module: MyApp.AssetA,
               asset_ref_name: :asset,
               freshness_key: "latest",
               status: :ok,
               latest_success_run_id: "run_asset_a",
               latest_success_at: finished_at,
               latest_attempt_status: :ok,
               latest_attempt_at: finished_at,
               manifest_version_id: "mv_a",
               updated_at: finished_at
             })

    assert :ok = Storage.put_asset_freshness_state(freshness)

    assert {:ok, stale_window_freshness} =
             AssetFreshnessState.new(%{
               asset_ref_module: MyApp.AssetA,
               asset_ref_name: :asset,
               freshness_key: "calendar:day:Etc/UTC:2026-05-09",
               status: :error,
               latest_attempt_run_id: "run_old_asset_a",
               latest_attempt_status: :error,
               latest_attempt_at: old_finished_at,
               manifest_version_id: "mv_a",
               updated_at: old_finished_at
             })

    assert :ok = Storage.put_asset_freshness_state(stale_window_freshness)
    assert {:ok, _count} = FavnOrchestrator.rebuild_target_statuses("mv_a")

    assert {:ok, [entry]} = FavnOrchestrator.active_asset_catalogue()
    assert entry.target_id == "asset:Elixir.MyApp.AssetA:asset"
    assert entry.status == :healthy
    assert entry.latest_run_id == "run_asset_a"
    assert entry.latest_run_status == :ok
    assert entry.latest_run_at == finished_at

    assert :ok =
             Storage.put_run(
               pipeline_run_state(
                 "run_pipeline_a",
                 MyApp.PipelineA,
                 [{MyApp.AssetA, :asset}],
                 :ok,
                 DateTime.add(finished_at, 5, :second),
                 "mv_a"
               )
             )

    assert {:ok, _count} = FavnOrchestrator.rebuild_target_statuses("mv_a")

    assert {:ok, [pipeline_entry]} = FavnOrchestrator.active_pipeline_catalogue()
    assert pipeline_entry.target_id == "pipeline:Elixir.MyApp.PipelineA"
    assert pipeline_entry.name == "pipeline_a"
    assert pipeline_entry.selected_assets == ["Elixir.MyApp.AssetA:asset"]
    assert pipeline_entry.dependencies == :all
    assert pipeline_entry.status == :healthy
    assert pipeline_entry.latest_run_id == "run_pipeline_a"
    assert pipeline_entry.latest_run_status == :ok
    assert pipeline_entry.latest_run_duration_ms == 1_000

    assert {:error, :not_found} =
             FavnOrchestrator.active_asset_detail("asset:Elixir.MyApp.AssetA:not_real")

    assert {:error, :invalid_asset_detail_options} =
             FavnOrchestrator.active_asset_detail(
               "asset:Elixir.MyApp.AssetA:asset",
               [:invalid]
             )

    assert {:error, {:unsupported_asset_detail_options, [:unknown]}} =
             FavnOrchestrator.active_asset_detail(
               "asset:Elixir.MyApp.AssetA:asset",
               unknown: true
             )

    assert {:error, {:invalid_asset_detail_option, :now, "invalid"}} =
             FavnOrchestrator.active_asset_detail(
               "asset:Elixir.MyApp.AssetA:asset",
               now: "invalid"
             )

    assert {:ok, detail} =
             FavnOrchestrator.active_asset_detail("asset:Elixir.MyApp.AssetA:asset")

    assert detail.target_id == "asset:Elixir.MyApp.AssetA:asset"
    assert detail.name == "asset"
    assert detail.asset_ref == "Elixir.MyApp.AssetA:asset"
    assert detail.type == asset.type
    assert detail.status == :healthy
    assert detail.latest_run_id == "run_asset_a"
    assert detail.latest_run_status == :ok
    assert detail.latest_run_at == finished_at
    assert detail.window == asset.window
    assert length(detail.timeline) == 30

    assert latest_window = Enum.find(detail.timeline, &(&1.date == ~D[2026-05-10]))
    assert latest_window.id == "window:day:2026-05-10"
    assert latest_window.label == "May 10"
    assert latest_window.range == "May 10, 2026"
    assert latest_window.status == :covered
    assert latest_window.latest_run_id == "run_asset_a"
    assert latest_window.latest_run_status == :ok
    assert latest_window.latest_run_at == finished_at
    assert latest_window.run_enabled?
    assert is_nil(latest_window.run_disabled_reason)
    assert latest_window.run_label == "Run this window"
    assert detail.canonical_asset_ref == {MyApp.AssetA, :asset}

    assert failed_window = Enum.find(detail.timeline, &(&1.date == ~D[2026-05-09]))
    assert failed_window.status == :failed
    assert failed_window.latest_run_id == "run_old_asset_a"
    assert failed_window.latest_run_status == :error
    assert failed_window.latest_run_at == old_finished_at

    assert unknown_window = Enum.find(detail.timeline, &(&1.date == ~D[2026-04-11]))
    assert unknown_window.status == :missing
    assert is_nil(unknown_window.latest_run_id)

    assert {:error, {:invalid_window_id, "not-a-window"}} =
             FavnOrchestrator.submit_asset_window_run(
               "mv_a",
               "asset:Elixir.MyApp.AssetA:asset",
               "not-a-window"
             )

    assert {:error, :invalid_asset_target} =
             FavnOrchestrator.submit_asset_window_run(
               "mv_a",
               "asset:Elixir.MyApp.AssetA:not_real",
               "window:day:2026-05-10"
             )

    assert {:error, :invalid_run_metadata} =
             FavnOrchestrator.submit_asset_window_run(
               "mv_a",
               "asset:Elixir.MyApp.AssetA:asset",
               "window:day:2026-05-10",
               metadata: :invalid
             )

    assert {:ok, run_id} =
             FavnOrchestrator.submit_asset_window_run(
               "mv_a",
               "asset:Elixir.MyApp.AssetA:asset",
               "window:day:2026-05-10",
               dependencies: :none
             )

    assert {:ok, run} = Storage.get_run(run_id)
    assert run.manifest_version_id == "mv_a"
    assert run.asset_ref == {MyApp.AssetA, :asset}
    assert run.metadata.selected_window.id == "window:day:2026-05-10"
    assert run.metadata.selected_window.kind == :day
    assert run.metadata.selected_window.start_at == ~U[2026-05-10 00:00:00Z]

    target_start_us = DateTime.to_unix(~U[2026-05-10 00:00:00Z], :microsecond)

    assert {{MyApp.AssetA, :asset}, target_window_key} =
             Enum.find(run.plan.target_node_keys, fn
               {{MyApp.AssetA, :asset}, %{start_at_us: ^target_start_us}} -> true
               _other -> false
             end)

    assert target_window_key.kind == :day
    assert target_window_key.start_at_us == target_start_us

    assert target_node = run.plan.nodes[{{MyApp.AssetA, :asset}, target_window_key}]
    assert target_node.window.kind == :day
    assert target_node.window.start_at == ~U[2026-05-10 00:00:00Z]

    assert :ok = ManifestStore.set_active_manifest("mv_b")

    assert {:ok, no_window_detail} =
             FavnOrchestrator.active_asset_detail("asset:Elixir.MyApp.AssetB:asset")

    assert no_window_detail.refresh_timeline != []
    assert is_nil(no_window_detail.data_coverage_timeline)
    assert no_window_detail.can_run_asset?

    assert {:ok, full_refresh_run_id} =
             FavnOrchestrator.submit_asset_run_for_manifest(
               "mv_b",
               "asset:Elixir.MyApp.AssetB:asset",
               %{selection: nil, config: %{dependencies: :none, refresh: :auto}}
             )

    assert {:ok, full_refresh_run} = Storage.get_run(full_refresh_run_id)
    assert full_refresh_run.asset_ref == {MyApp.AssetB, :asset}
  end

  test "asset refresh cadence follows resolved pipeline selectors" do
    asset_ref = {MyApp.ResolvedAsset, :asset}

    version =
      manifest_version(
        "mv_resolved_selector",
        asset_ref,
        [
          %Pipeline{
            module: MyApp.ResolvedPipeline,
            name: :resolved_pipeline,
            selectors: [{:asset, asset_ref}],
            deps: :all,
            window: Policy.new!(:monthly),
            source: :dsl,
            outputs: [],
            config: %{},
            metadata: %{}
          }
        ]
      )

    assert :ok = ManifestStore.register_manifest(version)
    assert :ok = ManifestStore.set_active_manifest(version.manifest_version_id)

    assert {:ok, detail} =
             FavnOrchestrator.active_asset_detail(
               "asset:Elixir.MyApp.ResolvedAsset:asset",
               now: ~U[2026-05-12 10:00:00Z]
             )

    assert detail.refresh_timeline_label == "Monthly refresh periods"
    assert detail.refresh_cadence_label == "Monthly refresh Etc/UTC"
  end

  test "asset detail timeline uses the asset window policy kind for runnable windows" do
    window_spec = WindowSpec.new!(:month, lookback: 1, required: true)

    version =
      manifest_version(
        "mv_monthly",
        {MyApp.MonthlyAsset, :asset},
        [
          %Pipeline{
            module: MyApp.MonthlyPipeline,
            name: :monthly_pipeline,
            selectors: [{MyApp.MonthlyAsset, :asset}],
            deps: :all,
            window: Policy.new!(:monthly),
            source: :dsl,
            outputs: [],
            config: %{},
            metadata: %{}
          }
        ],
        window_spec
      )

    assert :ok = ManifestStore.register_manifest(version)
    assert :ok = ManifestStore.set_active_manifest("mv_monthly")

    assert {:ok, detail} =
             FavnOrchestrator.active_asset_detail(
               "asset:Elixir.MyApp.MonthlyAsset:asset",
               today: ~D[2026-05-12]
             )

    assert detail.refresh_timeline != []
    assert detail.data_coverage_timeline != nil
    assert detail.has_data_windows?

    assert window = List.last(detail.data_coverage_timeline)
    assert window.id == "window:month:2026-05"
    assert window.label == "May 2026"
    assert window.range == "May 2026"
    assert window.run_enabled?
    assert is_nil(window.run_disabled_reason)

    assert {:ok, run_id} =
             FavnOrchestrator.submit_asset_run_for_manifest(
               "mv_monthly",
               "asset:Elixir.MyApp.MonthlyAsset:asset",
               %{
                 selection: %{
                   source: :data_coverage_timeline,
                   id: window.id,
                   kind: window.kind,
                   value: window.value,
                   timezone: window.timezone
                 },
                 config: %{dependencies: :none, refresh: :auto}
               }
             )

    assert {:ok, run} = Storage.get_run(run_id)
    assert run.metadata.selected_window.id == "window:month:2026-05"
    assert run.metadata.selected_window.kind == :month
    assert run.metadata.timeline_selection.source == :data_coverage_timeline

    assert run.plan.target_node_keys == [
             {{MyApp.MonthlyAsset, :asset}, run.metadata.selected_window.key}
           ]

    assert {:error, {:window_kind_mismatch, :month, :day}} =
             FavnOrchestrator.submit_asset_run_for_manifest(
               "mv_monthly",
               "asset:Elixir.MyApp.MonthlyAsset:asset",
               %{
                 selection: %{
                   source: :data_coverage_timeline,
                   id: "window:day:2026-05-12",
                   kind: :day,
                   value: "2026-05-12",
                   timezone: "Etc/UTC"
                 },
                 config: %{dependencies: :none, refresh: :auto}
               }
             )

    assert refresh_window = List.last(detail.refresh_timeline)

    assert {:ok, refresh_run_id} =
             FavnOrchestrator.submit_asset_run_for_manifest(
               "mv_monthly",
               "asset:Elixir.MyApp.MonthlyAsset:asset",
               %{
                 selection: %{
                   source: :refresh_timeline,
                   id: refresh_window.id,
                   kind: refresh_window.kind,
                   value: refresh_window.value,
                   timezone: refresh_window.timezone
                 },
                 config: %{dependencies: :none, refresh: :auto}
               }
             )

    assert {:ok, refresh_run} = Storage.get_run(refresh_run_id)
    assert refresh_run.metadata.timeline_selection.source == :refresh_timeline
    assert length(refresh_run.plan.target_node_keys) == 2
  end

  test "refresh timeline marks latest successful run without latest freshness state" do
    ref = {MyApp.DailySales, :asset}
    now = ~U[2026-05-19 12:00:00Z]
    {:ok, period} = Favn.TimePeriod.current(:month, now, "Etc/UTC")
    window_key = Favn.Window.Key.new!(:month, period.start_at, "Etc/UTC")

    manifest = %Manifest{
      assets: [
        freshness_asset(
          ref,
          Favn.Freshness.Policy.from_value!(window_success: true),
          [],
          WindowSpec.new!(:month)
        )
      ]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv_refresh_marker")
    assert :ok = ManifestStore.register_manifest(version)
    assert :ok = ManifestStore.set_active_manifest("mv_refresh_marker")

    assert :ok =
             Storage.put_run(run_state("run_daily_sales", ref, :ok, now, "mv_refresh_marker"))

    assert :ok =
             Storage.put_asset_freshness_state(
               freshness_state(ref, "sales:v1", now,
                 run_id: "run_daily_sales",
                 freshness_key: Favn.Freshness.Key.window!(window_key),
                 node_key: {ref, window_key},
                 manifest_version_id: "mv_refresh_marker"
               )
             )

    assert {:ok, detail} =
             FavnOrchestrator.active_asset_detail("asset:Elixir.MyApp.DailySales:asset",
               now: now
             )

    assert detail.freshness.state == :fresh

    assert data_window = Enum.find(detail.data_coverage_timeline, &(&1.value == "2026-05"))
    assert data_window.status == :covered
    assert data_window.latest_run_id == "run_daily_sales"

    assert refresh_window = Enum.find(detail.refresh_timeline, &(&1.value == "2026-05-19"))
    assert refresh_window.status == :fresh
    assert refresh_window.latest_run_id == "run_daily_sales"
  end

  test "asset detail data coverage timeline uses asset window state" do
    now = ~U[2026-05-20 12:00:00Z]
    ref = {MyApp.MonthlyCoverageAsset, :asset}
    window_spec = WindowSpec.new!(:month, required: true)

    version =
      manifest_version(
        "mv_asset_window_state",
        ref,
        [
          %Pipeline{
            module: MyApp.MonthlyCoveragePipeline,
            name: :monthly_coverage_pipeline,
            selectors: [ref],
            deps: :all,
            window: Policy.new!(:monthly),
            source: :dsl,
            outputs: [],
            config: %{},
            metadata: %{}
          }
        ],
        window_spec
      )

    assert :ok = ManifestStore.register_manifest(version)
    assert :ok = ManifestStore.set_active_manifest("mv_asset_window_state")

    {:ok, state} =
      AssetWindowState.new(%{
        asset_ref_module: elem(ref, 0),
        asset_ref_name: elem(ref, 1),
        manifest_version_id: "mv_asset_window_state",
        window_kind: :month,
        window_start_at: ~U[2026-01-01 00:00:00Z],
        window_end_at: ~U[2026-02-01 00:00:00Z],
        timezone: "Etc/UTC",
        window_key: "month:2026-01",
        status: :ok,
        latest_run_id: "run_monthly_jan",
        latest_success_run_id: "run_monthly_jan",
        updated_at: now
      })

    assert :ok = Storage.put_asset_window_state(state)

    assert {:ok, detail} =
             FavnOrchestrator.active_asset_detail(
               "asset:Elixir.MyApp.MonthlyCoverageAsset:asset",
               now: now
             )

    assert jan_window = Enum.find(detail.data_coverage_timeline, &(&1.value == "2026-01"))
    assert jan_window.status == :covered
    assert jan_window.latest_run_id == "run_monthly_jan"
  end

  test "terminal windowed asset runs project asset window state" do
    now = ~U[2026-01-02 00:00:00Z]
    ref = {MyApp.ProjectedWindowAsset, :asset}
    key = Favn.Window.Key.new!(:month, ~U[2026-01-01 00:00:00Z], "Etc/UTC")

    runtime_window =
      RuntimeWindow.new!(
        :month,
        ~U[2026-01-01 00:00:00Z],
        ~U[2026-02-01 00:00:00Z],
        key,
        timezone: "Etc/UTC"
      )

    run_state =
      RunState.new(
        id: "run_project_window",
        manifest_version_id: "mv_project_window",
        manifest_content_hash: "hash_project_window",
        asset_ref: ref,
        target_refs: [ref]
      )
      |> RunState.transition(
        status: :ok,
        result: %{
          node_results: [
            NodeResult.new(%{
              node_key: {ref, key},
              ref: ref,
              window: runtime_window,
              status: :ok,
              finished_at: now
            })
          ]
        }
      )
      |> Map.put(:updated_at, now)

    assert :ok = AssetWindowProjector.project_transition(run_state, :run_finished, %{})

    assert {:ok, state} =
             Storage.get_asset_window_state(
               elem(ref, 0),
               elem(ref, 1),
               Favn.Window.Key.encode(key)
             )

    assert state.status == :ok
    assert state.latest_success_run_id == "run_project_window"
  end

  test "asset window projection preserves prior success on failed attempts" do
    now = ~U[2026-01-03 00:00:00Z]
    ref = {MyApp.ProjectedFailedWindowAsset, :asset}
    key = Favn.Window.Key.new!(:month, ~U[2026-01-01 00:00:00Z], "Etc/UTC")
    window_key = Favn.Window.Key.encode(key)

    {:ok, existing} =
      AssetWindowState.new(%{
        asset_ref_module: elem(ref, 0),
        asset_ref_name: elem(ref, 1),
        manifest_version_id: "mv_project_window",
        window_kind: :month,
        window_start_at: ~U[2026-01-01 00:00:00Z],
        window_end_at: ~U[2026-02-01 00:00:00Z],
        timezone: "Etc/UTC",
        window_key: window_key,
        status: :ok,
        latest_run_id: "run_previous_success",
        latest_success_run_id: "run_previous_success",
        updated_at: ~U[2026-01-02 00:00:00Z]
      })

    assert :ok = Storage.put_asset_window_state(existing)

    run_state = projected_window_run_state("run_failed_window", ref, key, :error, now)

    assert :ok = AssetWindowProjector.project_transition(run_state, :run_failed, %{})
    assert {:ok, state} = Storage.get_asset_window_state(elem(ref, 0), elem(ref, 1), window_key)
    assert state.status == :error
    assert state.latest_run_id == "run_failed_window"
    assert state.latest_success_run_id == "run_previous_success"
  end

  test "asset window projection stops when existing state lookup fails" do
    now = ~U[2026-01-03 00:00:00Z]
    ref = {MyApp.ProjectedLookupFailureAsset, :asset}
    key = Favn.Window.Key.new!(:month, ~U[2026-01-01 00:00:00Z], "Etc/UTC")
    window_key = Favn.Window.Key.encode(key)

    {:ok, existing} =
      AssetWindowState.new(%{
        asset_ref_module: elem(ref, 0),
        asset_ref_name: elem(ref, 1),
        manifest_version_id: "mv_project_window",
        window_kind: :month,
        window_start_at: ~U[2026-01-01 00:00:00Z],
        window_end_at: ~U[2026-02-01 00:00:00Z],
        timezone: "Etc/UTC",
        window_key: window_key,
        status: :ok,
        latest_run_id: "run_previous_success",
        latest_success_run_id: "run_previous_success",
        updated_at: ~U[2026-01-02 00:00:00Z]
      })

    assert :ok = Storage.put_asset_window_state(existing)

    previous_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_opts = Application.get_env(:favn_orchestrator, :storage_adapter_opts)

    try do
      Application.put_env(:favn_orchestrator, :storage_adapter, String)
      Application.put_env(:favn_orchestrator, :storage_adapter_opts, [])

      assert {:error, _reason} =
               AssetWindowProjector.project_transition(
                 projected_window_run_state("run_lookup_failed", ref, key, :error, now),
                 :run_failed,
                 %{}
               )
    after
      restore_env(:favn_orchestrator, :storage_adapter, previous_adapter)
      restore_env(:favn_orchestrator, :storage_adapter_opts, previous_opts)
    end

    assert {:ok, state} = Storage.get_asset_window_state(elem(ref, 0), elem(ref, 1), window_key)
    assert state.status == :ok
    assert state.latest_run_id == "run_previous_success"
  end

  test "hourly asset detail timeline uses hours without collapsing same-day freshness" do
    window_spec = WindowSpec.new!(:hour, required: true)

    version =
      manifest_version(
        "mv_hourly",
        {MyApp.HourlyAsset, :asset},
        [
          %Pipeline{
            module: MyApp.HourlyPipeline,
            name: :hourly_pipeline,
            selectors: [{MyApp.HourlyAsset, :asset}],
            deps: :all,
            window: Policy.new!(:hourly),
            source: :dsl,
            outputs: [],
            config: %{},
            metadata: %{}
          }
        ],
        window_spec
      )

    assert :ok = ManifestStore.register_manifest(version)
    assert :ok = ManifestStore.set_active_manifest("mv_hourly")

    assert :ok =
             Storage.put_asset_freshness_state(
               freshness_state(
                 {MyApp.HourlyAsset, :asset},
                 "hourly:v8",
                 ~U[2026-05-12 08:30:00Z],
                 freshness_key: "calendar:hour:Etc/UTC:2026-05-12T08",
                 status: :error,
                 run_id: "run_hour_08",
                 manifest_version_id: "mv_hourly"
               )
             )

    assert :ok =
             Storage.put_asset_freshness_state(
               freshness_state(
                 {MyApp.HourlyAsset, :asset},
                 "hourly:v9",
                 ~U[2026-05-12 09:30:00Z],
                 freshness_key: "calendar:hour:Etc/UTC:2026-05-12T09",
                 status: :ok,
                 run_id: "run_hour_09",
                 manifest_version_id: "mv_hourly"
               )
             )

    assert {:ok, detail} =
             FavnOrchestrator.active_asset_detail(
               "asset:Elixir.MyApp.HourlyAsset:asset",
               now: ~U[2026-05-12 10:15:00Z]
             )

    assert Enum.take(detail.timeline, -3) |> Enum.map(& &1.id) == [
             "window:hour:2026-05-12T08",
             "window:hour:2026-05-12T09",
             "window:hour:2026-05-12T10"
           ]

    assert hour_08 = Enum.find(detail.timeline, &(&1.id == "window:hour:2026-05-12T08"))
    assert hour_08.status == :failed
    assert hour_08.latest_run_id == "run_hour_08"

    assert hour_09 = Enum.find(detail.timeline, &(&1.id == "window:hour:2026-05-12T09"))
    assert hour_09.status == :covered
    assert hour_09.latest_run_id == "run_hour_09"
  end

  test "yearly asset detail timeline uses yearly ids and labels" do
    window_spec = WindowSpec.new!(:year, required: true)

    version =
      manifest_version(
        "mv_yearly",
        {MyApp.YearlyAsset, :asset},
        [
          %Pipeline{
            module: MyApp.YearlyPipeline,
            name: :yearly_pipeline,
            selectors: [{MyApp.YearlyAsset, :asset}],
            deps: :all,
            window: Policy.new!(:yearly),
            source: :dsl,
            outputs: [],
            config: %{},
            metadata: %{}
          }
        ],
        window_spec
      )

    assert :ok = ManifestStore.register_manifest(version)
    assert :ok = ManifestStore.set_active_manifest("mv_yearly")

    assert {:ok, detail} =
             FavnOrchestrator.active_asset_detail(
               "asset:Elixir.MyApp.YearlyAsset:asset",
               today: ~D[2026-05-12]
             )

    assert window = List.last(detail.timeline)
    assert window.id == "window:year:2026"
    assert window.label == "2026"
    assert window.range == "2026"
    assert window.run_enabled?
  end

  test "publishes duplicate content under the existing canonical manifest version" do
    original = manifest_version("mv_original", {MyApp.AssetA, :asset})
    duplicate = manifest_version("mv_duplicate", {MyApp.AssetA, :asset})

    assert original.content_hash == duplicate.content_hash

    assert {:ok, :published, ^original} = ManifestStore.publish_manifest(original)
    assert {:ok, :already_published, canonical} = ManifestStore.publish_manifest(duplicate)
    assert canonical.manifest_version_id == "mv_original"

    assert {:error, :manifest_version_not_found} = ManifestStore.get_manifest("mv_duplicate")
  end

  test "active asset detail includes view-facing freshness explanations" do
    now = ~U[2026-05-10 12:00:00Z]

    manifest = %Manifest{
      assets: [
        freshness_asset(
          {MyApp.RawOrders, :asset},
          Favn.Freshness.Policy.from_value!({:daily, timezone: "Europe/Oslo"})
        ),
        freshness_asset(
          {MyApp.GoldOrders, :asset},
          Favn.Freshness.Policy.from_value!(max_age: {:hours, 24}),
          [
            {MyApp.RawOrders, :asset}
          ]
        ),
        freshness_asset(
          {MyApp.NeverRun, :asset},
          Favn.Freshness.Policy.from_value!(window_success: true)
        ),
        freshness_asset(
          {MyApp.WindowedOrders, :asset},
          Favn.Freshness.Policy.from_value!(window_success: true),
          [],
          WindowSpec.new!(:day)
        ),
        freshness_asset(
          {MyApp.FreshMaxAge, :asset},
          Favn.Freshness.Policy.from_value!(max_age: {:hours, 24})
        ),
        freshness_asset(
          {MyApp.ExpiredMaxAge, :asset},
          Favn.Freshness.Policy.from_value!(max_age: {:hours, 24})
        ),
        freshness_asset(
          {MyApp.WindowRaw, :asset},
          Favn.Freshness.Policy.from_value!(window_success: true),
          [],
          WindowSpec.new!(:day)
        ),
        freshness_asset(
          {MyApp.WindowGold, :asset},
          Favn.Freshness.Policy.from_value!(window_success: true),
          [{MyApp.WindowRaw, :asset}],
          WindowSpec.new!(:day)
        ),
        freshness_asset(
          {MyApp.NeverRunDownstream, :asset},
          Favn.Freshness.Policy.from_value!(max_age: {:hours, 24}),
          [{MyApp.RawOrders, :asset}]
        ),
        freshness_asset({MyApp.AlwaysRun, :asset}, Favn.Freshness.Policy.from_value!(:always)),
        freshness_asset({MyApp.NoPolicy, :asset}, nil)
      ]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv_freshness_detail")
    assert :ok = ManifestStore.register_manifest(version)
    assert :ok = ManifestStore.set_active_manifest("mv_freshness_detail")

    daily_key = Favn.Freshness.Key.calendar!(:day, "Europe/Oslo", ~D[2026-05-10])
    window_key = day_window_key(now)

    assert :ok =
             Storage.put_asset_freshness_state(
               freshness_state({MyApp.RawOrders, :asset}, "raw:v2", now,
                 run_id: "run_raw_v2",
                 freshness_key: daily_key
               )
             )

    assert :ok =
             Storage.put_asset_freshness_state(
               freshness_state({MyApp.GoldOrders, :asset}, "gold:v1", now,
                 run_id: "run_gold_v1",
                 input_versions: [
                   %{
                     upstream_ref: {MyApp.RawOrders, :asset},
                     upstream_node_key: {{MyApp.RawOrders, :asset}, nil},
                     freshness_version: "raw:v1",
                     success_run_id: "run_raw_v1"
                   }
                 ]
               )
             )

    assert :ok =
             Storage.put_asset_freshness_state(
               freshness_state({MyApp.WindowedOrders, :asset}, "windowed:v1", now,
                 run_id: "run_windowed_v1",
                 freshness_key: Favn.Freshness.Key.window!(window_key),
                 node_key: {{MyApp.WindowedOrders, :asset}, window_key}
               )
             )

    assert :ok =
             Storage.put_asset_freshness_state(
               freshness_state({MyApp.FreshMaxAge, :asset}, "fresh_age:v1", now,
                 run_id: "run_fresh_age"
               )
             )

    assert :ok =
             Storage.put_asset_freshness_state(
               freshness_state(
                 {MyApp.ExpiredMaxAge, :asset},
                 "expired_age:v1",
                 DateTime.add(now, -3, :day),
                 run_id: "run_expired_age"
               )
             )

    assert :ok =
             Storage.put_asset_freshness_state(
               freshness_state({MyApp.WindowRaw, :asset}, "window_raw:v2", now,
                 run_id: "run_window_raw_v2",
                 freshness_key: Favn.Freshness.Key.window!(window_key),
                 node_key: {{MyApp.WindowRaw, :asset}, window_key}
               )
             )

    assert :ok =
             Storage.put_asset_freshness_state(
               freshness_state({MyApp.WindowGold, :asset}, "window_gold:v1", now,
                 run_id: "run_window_gold_v1",
                 freshness_key: Favn.Freshness.Key.window!(window_key),
                 node_key: {{MyApp.WindowGold, :asset}, window_key},
                 input_versions: [
                   %{
                     upstream_ref: {MyApp.WindowRaw, :asset},
                     upstream_node_key: {{MyApp.WindowRaw, :asset}, window_key},
                     freshness_version: "window_raw:v1",
                     success_run_id: "run_window_raw_v1"
                   }
                 ]
               )
             )

    assert {:ok, raw_detail} =
             FavnOrchestrator.active_asset_detail("asset:Elixir.MyApp.RawOrders:asset",
               now: now
             )

    assert raw_detail.freshness.state == :fresh
    assert raw_detail.freshness.policy == %{kind: :daily, label: "daily Europe/Oslo"}
    assert raw_detail.freshness.latest_success.run_id == "run_raw_v2"
    assert raw_detail.freshness.latest_success.freshness_key == daily_key
    assert [%{kind: :policy_fresh}] = raw_detail.freshness.reasons

    assert {:ok, gold_detail} =
             FavnOrchestrator.active_asset_detail("asset:Elixir.MyApp.GoldOrders:asset",
               now: now
             )

    assert gold_detail.freshness.state == :stale
    assert gold_detail.freshness.policy == %{kind: :max_age, label: "max age 24 hours"}
    assert gold_detail.freshness.explanation =~ "GoldOrders.asset is stale"

    assert [reason] = gold_detail.freshness.reasons
    assert reason.kind == :upstream_version_changed
    assert reason.upstream_ref == "Elixir.MyApp.RawOrders:asset"
    assert reason.previous_version == "raw:v1"
    assert reason.current_version == "raw:v2"
    assert reason.run_id == "run_raw_v2"

    assert {:ok, windowed_detail} =
             FavnOrchestrator.active_asset_detail("asset:Elixir.MyApp.WindowedOrders:asset",
               now: now
             )

    assert windowed_detail.freshness.state == :fresh
    assert windowed_detail.freshness.policy == %{kind: :window_success, label: "window success"}

    assert windowed_detail.freshness.latest_success.freshness_key ==
             Favn.Freshness.Key.window!(window_key)

    assert {:ok, fresh_age_detail} =
             FavnOrchestrator.active_asset_detail("asset:Elixir.MyApp.FreshMaxAge:asset",
               now: now
             )

    assert fresh_age_detail.freshness.state == :fresh

    assert {:ok, expired_age_detail} =
             FavnOrchestrator.active_asset_detail("asset:Elixir.MyApp.ExpiredMaxAge:asset",
               now: now
             )

    assert expired_age_detail.freshness.state == :stale
    assert [%{kind: :freshness_expired}] = expired_age_detail.freshness.reasons

    assert {:ok, window_gold_detail} =
             FavnOrchestrator.active_asset_detail("asset:Elixir.MyApp.WindowGold:asset",
               now: now
             )

    assert window_gold_detail.freshness.state == :stale
    assert [window_reason] = window_gold_detail.freshness.reasons
    assert window_reason.kind == :upstream_version_changed
    assert window_reason.upstream_ref == "Elixir.MyApp.WindowRaw:asset"
    assert window_reason.previous_version == "window_raw:v1"
    assert window_reason.current_version == "window_raw:v2"

    assert {:ok, never_run_downstream_detail} =
             FavnOrchestrator.active_asset_detail("asset:Elixir.MyApp.NeverRunDownstream:asset",
               now: now
             )

    assert never_run_downstream_detail.freshness.state == :unknown
    assert [%{kind: :never_run}] = never_run_downstream_detail.freshness.reasons

    assert {:ok, never_run_detail} =
             FavnOrchestrator.active_asset_detail("asset:Elixir.MyApp.NeverRun:asset")

    assert never_run_detail.freshness.state == :unknown
    assert never_run_detail.freshness.policy == %{kind: :window_success, label: "window success"}
    assert [%{kind: :never_run}] = never_run_detail.freshness.reasons

    assert {:ok, always_detail} =
             FavnOrchestrator.active_asset_detail("asset:Elixir.MyApp.AlwaysRun:asset")

    assert always_detail.freshness.state == :always_run
    assert always_detail.freshness.policy == %{kind: :always, label: "always run"}
    assert [%{kind: :always_run}] = always_detail.freshness.reasons

    assert {:ok, no_policy_detail} =
             FavnOrchestrator.active_asset_detail("asset:Elixir.MyApp.NoPolicy:asset")

    assert no_policy_detail.freshness.state == :unknown
    assert no_policy_detail.freshness.policy == %{kind: :none, label: "no freshness policy"}
    assert [%{kind: :no_freshness_policy}] = no_policy_detail.freshness.reasons
  end

  test "catalogue returns unknown when target status projection row is missing" do
    version = manifest_version("mv_missing_status", {MyApp.AssetA, :asset})

    assert :ok = ManifestStore.register_manifest(version)
    assert :ok = ManifestStore.set_active_manifest("mv_missing_status")

    assert {:ok, [entry]} = FavnOrchestrator.active_asset_catalogue()
    assert entry.status == :unknown
    assert is_nil(entry.latest_run_id)
  end

  test "run transitions update target status projection" do
    run =
      RunState.new(
        id: "run_projection_asset",
        manifest_version_id: "mv_projection",
        manifest_content_hash: "hash_projection",
        asset_ref: {MyApp.AssetA, :asset},
        target_refs: [{MyApp.AssetA, :asset}]
      )

    assert :ok = TransitionWriter.persist_transition(run, :run_created, %{})

    assert {:ok, running_status} =
             Storage.get_target_status(
               "mv_projection",
               :asset,
               "asset:Elixir.MyApp.AssetA:asset"
             )

    assert running_status.status == :running
    assert running_status.in_flight_run_id == "run_projection_asset"

    finished = RunState.transition(run, status: :ok)
    assert :ok = TransitionWriter.persist_transition(finished, :run_finished, %{})

    assert {:ok, healthy_status} =
             Storage.get_target_status(
               "mv_projection",
               :asset,
               "asset:Elixir.MyApp.AssetA:asset"
             )

    assert healthy_status.status == :healthy
    assert healthy_status.latest_success_run_id == "run_projection_asset"
    assert is_nil(healthy_status.in_flight_run_id)
  end

  test "non-latest freshness states do not overwrite target status projection" do
    ref = {MyApp.AssetA, :asset}
    finished_at = ~U[2026-05-10 12:00:00Z]

    run =
      run_state("run_projection_latest", ref, :ok, finished_at, "mv_projection_freshness")

    assert :ok = TransitionWriter.persist_transition(run, :run_finished, %{})

    window_state =
      freshness_state(ref, "window:v1", DateTime.add(finished_at, 60, :second),
        freshness_key: "calendar:day:Etc/UTC:2026-05-10",
        status: :error,
        manifest_version_id: "mv_projection_freshness",
        run_id: "run_projection_window"
      )

    assert :ok = Storage.put_asset_freshness_state(window_state)
    assert :ok = TargetStatusProjector.project_freshness_state(window_state)

    assert {:ok, status} =
             Storage.get_target_status(
               "mv_projection_freshness",
               :asset,
               "asset:Elixir.MyApp.AssetA:asset"
             )

    assert status.status == :healthy
    assert status.latest_run_id == "run_projection_latest"
    assert status.latest_run_status == :ok
  end

  test "pipeline detail history is scoped before applying the run limit" do
    ref = {MyApp.AssetA, :asset}

    version =
      manifest_version(
        "mv_target_history",
        ref,
        [
          %Pipeline{module: MyApp.PipelineA, name: :pipeline_a, selectors: [ref], deps: :all},
          %Pipeline{module: MyApp.PipelineB, name: :pipeline_b, selectors: [ref], deps: :all}
        ]
      )

    assert :ok = ManifestStore.register_manifest(version)
    assert :ok = ManifestStore.set_active_manifest("mv_target_history")

    assert :ok =
             Storage.put_run(
               pipeline_run_state(
                 "run_quiet_pipeline_a",
                 MyApp.PipelineA,
                 [ref],
                 :ok,
                 ~U[2026-05-10 12:00:00Z],
                 "mv_target_history"
               )
             )

    for index <- 1..60 do
      assert :ok =
               Storage.put_run(
                 pipeline_run_state(
                   "run_noisy_pipeline_b_#{index}",
                   MyApp.PipelineB,
                   [ref],
                   :ok,
                   DateTime.add(~U[2026-05-10 12:00:00Z], index, :second),
                   "mv_target_history"
                 )
               )
    end

    assert {:ok, _count} = FavnOrchestrator.rebuild_target_statuses("mv_target_history")

    assert {:ok, detail} =
             FavnOrchestrator.active_pipeline_detail("pipeline:Elixir.MyApp.PipelineA")

    assert Enum.map(detail.runs, & &1.id) == ["run_quiet_pipeline_a"]
  end

  test "explicit target-ref pipeline runs are not attributed to named pipeline targets" do
    ref = {MyApp.AssetA, :asset}

    version =
      manifest_version(
        "mv_explicit_targets",
        ref,
        [%Pipeline{module: MyApp.PipelineA, name: :pipeline_a, selectors: [ref], deps: :all}]
      )

    assert :ok = ManifestStore.register_manifest(version)
    assert :ok = ManifestStore.set_active_manifest("mv_explicit_targets")

    assert :ok =
             Storage.put_run(
               explicit_target_pipeline_run_state(
                 "run_explicit_targets",
                 [ref],
                 :ok,
                 ~U[2026-05-10 12:00:00Z],
                 "mv_explicit_targets"
               )
             )

    assert {:ok, _count} = FavnOrchestrator.rebuild_target_statuses("mv_explicit_targets")

    assert {:ok, [entry]} = FavnOrchestrator.active_pipeline_catalogue()
    assert entry.status == :unknown
    assert is_nil(entry.latest_run_id)

    assert {:ok, detail} =
             FavnOrchestrator.active_pipeline_detail("pipeline:Elixir.MyApp.PipelineA")

    assert detail.runs == []
  end

  test "pipeline catalogue matches runs by pipeline identity before target refs" do
    ref = {MyApp.AssetA, :asset}

    version =
      manifest_version(
        "mv_same_targets",
        ref,
        [
          %Pipeline{module: MyApp.PipelineA, name: :pipeline_a, selectors: [ref], deps: :all},
          %Pipeline{module: MyApp.PipelineB, name: :pipeline_b, selectors: [ref], deps: :none}
        ]
      )

    assert :ok = ManifestStore.register_manifest(version)
    assert :ok = ManifestStore.set_active_manifest("mv_same_targets")

    assert :ok =
             Storage.put_run(
               pipeline_run_state(
                 "run_pipeline_b",
                 MyApp.PipelineB,
                 [ref],
                 :ok,
                 ~U[2026-05-10 12:00:00Z],
                 "mv_same_targets"
               )
             )

    assert :ok =
             Storage.put_run(
               run_state(
                 "run_manual_asset_a",
                 ref,
                 :error,
                 ~U[2026-05-10 12:05:00Z],
                 "mv_same_targets"
               )
             )

    assert {:ok, _count} = FavnOrchestrator.rebuild_target_statuses("mv_same_targets")

    assert {:ok, entries} = FavnOrchestrator.active_pipeline_catalogue()
    pipeline_a = Enum.find(entries, &(&1.name == "pipeline_a"))
    pipeline_b = Enum.find(entries, &(&1.name == "pipeline_b"))

    assert pipeline_a.status == :unknown
    assert is_nil(pipeline_a.latest_run_id)
    assert pipeline_b.status == :healthy
    assert pipeline_b.latest_run_id == "run_pipeline_b"

    assert :ok =
             Storage.put_run(
               pipeline_run_state(
                 "run_pipeline_b_rerun",
                 MyApp.PipelineB,
                 [ref],
                 :error,
                 ~U[2026-05-10 12:10:00Z],
                 "mv_same_targets",
                 submit_kind: :rerun
               )
             )

    assert {:ok, _count} = FavnOrchestrator.rebuild_target_statuses("mv_same_targets")

    assert {:ok, entries} = FavnOrchestrator.active_pipeline_catalogue()
    pipeline_a = Enum.find(entries, &(&1.name == "pipeline_a"))
    pipeline_b = Enum.find(entries, &(&1.name == "pipeline_b"))

    assert pipeline_a.status == :unknown
    assert is_nil(pipeline_a.latest_run_id)
    assert pipeline_b.status == :failed
    assert pipeline_b.latest_run_id == "run_pipeline_b_rerun"
  end

  defp manifest_version(manifest_version_id, ref, pipelines \\ [], window \\ nil) do
    manifest = %Manifest{
      assets: [
        %Favn.Manifest.Asset{ref: ref, module: elem(ref, 0), name: elem(ref, 1), window: window}
      ],
      pipelines: pipelines
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp freshness_asset(ref, freshness, depends_on \\ [], window \\ nil) do
    %Favn.Manifest.Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      freshness: freshness,
      depends_on: depends_on,
      window: window
    }
  end

  defp freshness_state(ref, version, at, opts) do
    {module, name} = ref
    run_id = Keyword.get(opts, :run_id, "run_#{name}")

    {:ok, state} =
      AssetFreshnessState.new(%{
        asset_ref_module: module,
        asset_ref_name: name,
        freshness_key: Keyword.get(opts, :freshness_key, Favn.Freshness.Key.latest()),
        status: Keyword.get(opts, :status, :ok),
        freshness_version: version,
        latest_success_run_id: run_id,
        latest_success_node_key: Keyword.get(opts, :node_key, {ref, nil}),
        latest_success_at: at,
        latest_attempt_run_id: run_id,
        latest_attempt_status: Keyword.get(opts, :status, :ok),
        latest_attempt_at: at,
        manifest_version_id: Keyword.get(opts, :manifest_version_id, "mv_freshness_detail"),
        input_versions: Keyword.get(opts, :input_versions, []),
        updated_at: at
      })

    state
  end

  defp day_window_key(now) do
    {:ok, period} = Favn.TimePeriod.current(:day, now, "Etc/UTC")
    Favn.Window.Key.new!(:day, period.start_at, "Etc/UTC")
  end

  defp run_state(id, ref, status, finished_at, manifest_version_id) do
    RunState.new(
      id: id,
      manifest_version_id: manifest_version_id,
      manifest_content_hash: "hash_a",
      asset_ref: ref,
      target_refs: [ref]
    )
    |> RunState.transition(
      status: status,
      result: %{
        asset_results: [
          %AssetResult{
            ref: ref,
            stage: 0,
            status: status,
            started_at: DateTime.add(finished_at, -1, :second),
            finished_at: finished_at,
            duration_ms: 1
          }
        ]
      }
    )
    |> Map.put(:updated_at, finished_at)
    |> RunState.with_snapshot_hash()
  end

  defp projected_window_run_state(id, ref, key, status, finished_at) do
    runtime_window =
      RuntimeWindow.new!(
        key.kind,
        DateTime.from_unix!(key.start_at_us, :microsecond),
        DateTime.from_unix!(key.start_at_us, :microsecond) |> Favn.TimePeriod.shift!(key.kind, 1),
        key,
        timezone: key.timezone
      )

    RunState.new(
      id: id,
      manifest_version_id: "mv_project_window",
      manifest_content_hash: "hash_project_window",
      asset_ref: ref,
      target_refs: [ref]
    )
    |> RunState.transition(
      status: if(status == :ok, do: :ok, else: :error),
      result: %{
        node_results: [
          NodeResult.new(%{
            node_key: {ref, key},
            ref: ref,
            window: runtime_window,
            status: status,
            finished_at: finished_at,
            error: if(status == :ok, do: nil, else: :failed)
          })
        ]
      }
    )
    |> Map.put(:updated_at, finished_at)
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)

  defp pipeline_run_state(
         id,
         pipeline_module,
         refs,
         status,
         finished_at,
         manifest_version_id,
         opts \\ []
       ) do
    started_at = DateTime.add(finished_at, -1, :second)

    RunState.new(
      id: id,
      manifest_version_id: manifest_version_id,
      manifest_content_hash: "hash_a",
      asset_ref: List.first(refs),
      target_refs: refs,
      submit_kind: Keyword.get(opts, :submit_kind, :pipeline),
      metadata: %{
        pipeline_submit_ref: pipeline_module,
        pipeline_target_refs: refs,
        pipeline_dependencies: :all
      }
    )
    |> RunState.transition(status: status, result: %{asset_results: []})
    |> Map.put(:inserted_at, started_at)
    |> Map.put(:updated_at, finished_at)
    |> RunState.with_snapshot_hash()
  end

  defp explicit_target_pipeline_run_state(id, refs, status, finished_at, manifest_version_id) do
    started_at = DateTime.add(finished_at, -1, :second)

    RunState.new(
      id: id,
      manifest_version_id: manifest_version_id,
      manifest_content_hash: "hash_a",
      asset_ref: List.first(refs),
      target_refs: refs,
      submit_kind: :pipeline,
      metadata: %{
        pipeline_target_refs: refs,
        pipeline_dependencies: :all
      }
    )
    |> RunState.transition(status: status, result: %{asset_results: []})
    |> Map.put(:inserted_at, started_at)
    |> Map.put(:updated_at, finished_at)
    |> RunState.with_snapshot_hash()
  end
end
