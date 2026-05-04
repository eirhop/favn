defmodule Favn.StorageFacadeTest do
  use ExUnit.Case, async: false

  alias Favn.Run
  alias Favn.Storage
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Storage, as: OrchestratorStorage
  alias FavnOrchestrator.Storage.Adapter.Memory

  defmodule ExactShapeAdapterStub do
    @behaviour Favn.Storage.Adapter

    @impl true
    def child_spec(_opts), do: :none

    @impl true
    def put_manifest_version(_version, _opts), do: :ok

    @impl true
    def get_manifest_version(_manifest_version_id, _opts), do: {:error, :not_found}

    @impl true
    def get_manifest_version_by_content_hash(_content_hash, _opts), do: {:error, :not_found}

    @impl true
    def list_manifest_versions(_opts), do: {:ok, []}

    @impl true
    def set_active_manifest_version(_manifest_version_id, _opts), do: :ok

    @impl true
    def get_active_manifest_version(_opts), do: {:error, :not_found}

    @impl true
    def put_run(_run_state, _opts), do: :ok

    @impl true
    def get_run(_run_id, _opts), do: {:error, :not_found}

    @impl true
    def list_runs(_opts, _adapter_opts), do: {:ok, []}

    @impl true
    def persist_run_transition(_run_state, _event, _opts), do: :ok

    @impl true
    def append_run_event(_run_id, _event, _opts), do: :ok

    @impl true
    def list_run_events(_run_id, _opts), do: {:ok, []}

    @impl true
    def list_global_run_events(_filters, _opts), do: {:ok, []}

    @impl true
    def put_scheduler_state(_key, _state, _opts), do: :ok

    @impl true
    def get_scheduler_state(_key, _opts), do: {:ok, nil}

    @impl true
    def put_coverage_baseline(_baseline, _opts), do: :ok

    @impl true
    def get_coverage_baseline(_baseline_id, _opts), do: {:error, :not_found}

    @impl true
    def list_coverage_baselines(_filters, _opts), do: {:ok, []}

    @impl true
    def put_backfill_window(_window, _opts), do: :ok

    @impl true
    def get_backfill_window(_backfill_run_id, _pipeline_module, _window_key, _opts),
      do: {:error, :not_found}

    @impl true
    def list_backfill_windows(_filters, _opts), do: {:ok, []}

    @impl true
    def put_asset_window_state(_state, _opts), do: :ok

    @impl true
    def get_asset_window_state(_asset_ref_module, _asset_ref_name, _window_key, _opts),
      do: {:error, :not_found}

    @impl true
    def list_asset_window_states(_filters, _opts), do: {:ok, []}

    @impl true
    def replace_backfill_read_models(
          _scope,
          _coverage_baselines,
          _backfill_windows,
          _states,
          _opts
        ),
        do: :ok
  end

  defmodule LegacyShapeAdapterStub do
    def child_spec(_opts), do: :none
    def scheduler_child_spec(_opts), do: :none
    def put_run(_run, _opts), do: :ok
    def get_run(_run_id, _opts), do: {:error, :not_found}
    def list_runs(_opts, _adapter_opts), do: {:ok, []}
    def put_scheduler_state(_state, _opts), do: :ok
    def get_scheduler_state(_module, _schedule_id, _opts), do: {:ok, nil}
  end

  defmodule ChildSpecProbeAdapterStub do
    @behaviour Favn.Storage.Adapter

    @impl true
    def child_spec(opts) do
      test_pid = Keyword.fetch!(opts, :test_pid)
      send(test_pid, :child_spec_called)
      Keyword.get(opts, :child_spec_result, :none)
    end

    @impl true
    def readiness(opts) do
      test_pid = Keyword.fetch!(opts, :test_pid)
      send(test_pid, {:readiness_called, opts})
      Keyword.get(opts, :readiness_result, {:ok, %{status: :probe_ready, ready?: true}})
    end

    @impl true
    def put_manifest_version(_version, _opts), do: :ok

    @impl true
    def get_manifest_version(_manifest_version_id, _opts), do: {:error, :not_found}

    @impl true
    def get_manifest_version_by_content_hash(_content_hash, _opts), do: {:error, :not_found}

    @impl true
    def list_manifest_versions(_opts), do: {:ok, []}

    @impl true
    def set_active_manifest_version(_manifest_version_id, _opts), do: :ok

    @impl true
    def get_active_manifest_version(_opts), do: {:error, :not_found}

    @impl true
    def put_run(_run_state, _opts), do: :ok

    @impl true
    def get_run(_run_id, _opts), do: {:error, :not_found}

    @impl true
    def list_runs(_opts, _adapter_opts), do: {:ok, []}

    @impl true
    def persist_run_transition(_run_state, _event, _opts), do: :ok

    @impl true
    def append_run_event(_run_id, _event, _opts), do: :ok

    @impl true
    def list_run_events(_run_id, _opts), do: {:ok, []}

    @impl true
    def list_global_run_events(_filters, _opts), do: {:ok, []}

    @impl true
    def put_scheduler_state(_key, _state, _opts), do: :ok

    @impl true
    def get_scheduler_state(_key, _opts), do: {:ok, nil}

    @impl true
    def put_coverage_baseline(_baseline, _opts), do: :ok

    @impl true
    def get_coverage_baseline(_baseline_id, _opts), do: {:error, :not_found}

    @impl true
    def list_coverage_baselines(_filters, _opts), do: {:ok, []}

    @impl true
    def put_backfill_window(_window, _opts), do: :ok

    @impl true
    def get_backfill_window(_backfill_run_id, _pipeline_module, _window_key, _opts),
      do: {:error, :not_found}

    @impl true
    def list_backfill_windows(_filters, _opts), do: {:ok, []}

    @impl true
    def put_asset_window_state(_state, _opts), do: :ok

    @impl true
    def get_asset_window_state(_asset_ref_module, _asset_ref_name, _window_key, _opts),
      do: {:error, :not_found}

    @impl true
    def list_asset_window_states(_filters, _opts), do: {:ok, []}

    @impl true
    def replace_backfill_read_models(
          _scope,
          _coverage_baselines,
          _backfill_windows,
          _states,
          _opts
        ),
        do: :ok
  end

  setup do
    previous_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_opts = Application.get_env(:favn_orchestrator, :storage_adapter_opts)

    Application.put_env(:favn_orchestrator, :storage_adapter, Memory)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, [])

    Memory.reset()

    on_exit(fn ->
      restore_env(:favn_orchestrator, :storage_adapter, previous_adapter)
      restore_env(:favn_orchestrator, :storage_adapter_opts, previous_opts)
      Memory.reset()
    end)

    :ok
  end

  test "validates adapters that implement the public orchestrator-backed behaviour" do
    assert :ok = Storage.validate_adapter(ExactShapeAdapterStub)
    assert :ok = OrchestratorStorage.validate_adapter(ExactShapeAdapterStub)

    assert {:error, {:store_error, {:invalid_storage_adapter, LegacyShapeAdapterStub}}} =
             Storage.validate_adapter(LegacyShapeAdapterStub)

    assert {:error, {:invalid_storage_adapter, LegacyShapeAdapterStub}} =
             OrchestratorStorage.validate_adapter(LegacyShapeAdapterStub)
  end

  test "reads and writes runs through orchestrator storage facade" do
    now = DateTime.utc_now()

    run =
      %Run{
        id: "run_storage_facade",
        manifest_version_id: "mv_storage_facade",
        manifest_content_hash: "hash_storage_facade",
        asset_ref: {MyApp.Assets.Gold, :asset},
        target_refs: [{MyApp.Assets.Gold, :asset}],
        submit_kind: :asset,
        status: :running,
        started_at: now,
        event_seq: 1
      }

    assert :ok = Storage.put_run(run)

    assert {:ok, projected} = Storage.get_run("run_storage_facade")
    assert projected.id == "run_storage_facade"
    assert projected.asset_ref == {MyApp.Assets.Gold, :asset}

    assert {:ok, run_state} = OrchestratorStorage.get_run("run_storage_facade")
    assert run_state.id == "run_storage_facade"
    assert run_state.manifest_version_id == "mv_storage_facade"
  end

  test "always delegates child spec setup to orchestrator storage" do
    Application.put_env(:favn_orchestrator, :storage_adapter, ChildSpecProbeAdapterStub)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, test_pid: self())

    assert {:ok, []} = Storage.child_specs()
    assert_receive :child_spec_called
  end

  test "propagates adapter child spec configuration errors" do
    Application.put_env(:favn_orchestrator, :storage_adapter, ChildSpecProbeAdapterStub)

    Application.put_env(:favn_orchestrator, :storage_adapter_opts,
      test_pid: self(),
      child_spec_result: {:error, :bad_storage_config}
    )

    assert {:error, :bad_storage_config} = OrchestratorStorage.child_specs()
    assert {:error, {:store_error, :bad_storage_config}} = Storage.child_specs()
    assert_receive :child_spec_called
    assert_receive :child_spec_called
  end

  test "delegates readiness diagnostics to the configured storage adapter" do
    Application.put_env(:favn_orchestrator, :storage_adapter, ChildSpecProbeAdapterStub)

    Application.put_env(:favn_orchestrator, :storage_adapter_opts,
      test_pid: self(),
      readiness_result: {:error, %{status: :probe_failed, ready?: false}}
    )

    assert {:error, %{status: :probe_failed, ready?: false}} = OrchestratorStorage.readiness()
    assert_receive {:readiness_called, opts}
    assert opts[:test_pid] == self()
  end

  test "readiness uses a local ok diagnostic when an adapter omits the optional callback" do
    Application.put_env(:favn_orchestrator, :storage_adapter, ExactShapeAdapterStub)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, [])

    assert {:ok, %{status: :ready, ready?: true, adapter: ExactShapeAdapterStub}} =
             OrchestratorStorage.readiness()
  end

  test "builds normalized backfill storage structs with hashed source identity" do
    now = DateTime.utc_now()
    start_at = ~U[2026-03-01 00:00:00Z]
    end_at = ~U[2026-04-01 00:00:00Z]

    assert {:ok, %CoverageBaseline{} = baseline} =
             CoverageBaseline.new(%{
               baseline_id: "baseline_1",
               pipeline_module: MyApp.Pipelines.Monthly,
               source_key: "orders_api",
               segment_key_hash: "sha256:abc123",
               segment_key_redacted: "abc***",
               window_kind: :month,
               timezone: "Etc/UTC",
               coverage_until: end_at,
               created_by_run_id: "run_full",
               manifest_version_id: "mv_1",
               status: :ok,
               created_at: now,
               updated_at: now
             })

    assert baseline.segment_key_hash == "sha256:abc123"
    refute Map.has_key?(Map.from_struct(baseline), :segment_id)

    assert {:error, {:raw_source_identity_not_allowed, :segment_id}} =
             CoverageBaseline.new(%{
               baseline_id: "baseline_raw",
               pipeline_module: MyApp.Pipelines.Monthly,
               source_key: "orders_api",
               segment_key_hash: "sha256:abc123",
               segment_id: "raw-source-id",
               window_kind: :month,
               timezone: "Etc/UTC",
               coverage_until: end_at,
               created_by_run_id: "run_full",
               manifest_version_id: "mv_1",
               status: :ok,
               created_at: now,
               updated_at: now
             })

    assert {:ok, %BackfillWindow{attempt_count: 0}} =
             BackfillWindow.new(%{
               backfill_run_id: "backfill_1",
               pipeline_module: MyApp.Pipelines.Monthly,
               manifest_version_id: "mv_1",
               window_kind: :month,
               window_start_at: start_at,
               window_end_at: end_at,
               timezone: "Etc/UTC",
               window_key: "month:2026-03",
               status: :pending,
               updated_at: now
             })

    assert {:ok, %AssetWindowState{metadata: %{rows_written: 10}}} =
             AssetWindowState.new(%{
               asset_ref_module: MyApp.Assets.Orders,
               asset_ref_name: :asset,
               pipeline_module: MyApp.Pipelines.Monthly,
               manifest_version_id: "mv_1",
               window_kind: :month,
               window_start_at: start_at,
               window_end_at: end_at,
               timezone: "Etc/UTC",
               window_key: "month:2026-03",
               status: :ok,
               latest_run_id: "run_child",
               latest_success_run_id: "run_child",
               metadata: %{rows_written: 10},
               updated_at: now
             })
  end

  test "stores normalized backfill state through orchestrator memory facade" do
    now = DateTime.utc_now()
    start_at = ~U[2026-03-01 00:00:00Z]
    end_at = ~U[2026-04-01 00:00:00Z]

    {:ok, baseline} =
      CoverageBaseline.new(%{
        baseline_id: "baseline_facade",
        pipeline_module: MyApp.Pipelines.Monthly,
        source_key: "orders_api",
        segment_key_hash: "sha256:abc123",
        window_kind: :month,
        timezone: "Etc/UTC",
        coverage_until: end_at,
        created_by_run_id: "run_full",
        manifest_version_id: "mv_1",
        status: :ok,
        created_at: now,
        updated_at: now
      })

    {:ok, window} =
      BackfillWindow.new(%{
        backfill_run_id: "backfill_facade",
        pipeline_module: MyApp.Pipelines.Monthly,
        manifest_version_id: "mv_1",
        coverage_baseline_id: baseline.baseline_id,
        window_kind: :month,
        window_start_at: start_at,
        window_end_at: end_at,
        timezone: "Etc/UTC",
        window_key: "month:2026-03",
        status: :running,
        latest_attempt_run_id: "run_child",
        updated_at: now
      })

    {:ok, asset_state} =
      AssetWindowState.new(%{
        asset_ref_module: MyApp.Assets.Orders,
        asset_ref_name: :asset,
        pipeline_module: MyApp.Pipelines.Monthly,
        manifest_version_id: "mv_1",
        window_kind: :monthly,
        window_start_at: start_at,
        window_end_at: end_at,
        timezone: "Etc/UTC",
        window_key: "month:2026-03",
        status: :ok,
        latest_run_id: "run_child",
        latest_parent_run_id: "backfill_facade",
        latest_success_run_id: "run_child",
        rows_written: 10,
        metadata: %{relation: "gold.orders"},
        updated_at: now
      })

    assert :ok = OrchestratorStorage.put_coverage_baseline(baseline)
    assert :ok = OrchestratorStorage.put_backfill_window(window)
    assert :ok = OrchestratorStorage.put_asset_window_state(asset_state)

    assert {:ok, ^baseline} = OrchestratorStorage.get_coverage_baseline("baseline_facade")

    assert {:ok, ^window} =
             OrchestratorStorage.get_backfill_window(
               "backfill_facade",
               MyApp.Pipelines.Monthly,
               "month:2026-03"
             )

    assert {:ok, ^asset_state} =
             OrchestratorStorage.get_asset_window_state(
               MyApp.Assets.Orders,
               :asset,
               "month:2026-03"
             )

    assert {:ok, page} = OrchestratorStorage.list_backfill_windows(status: :running)
    assert [^window] = page.items
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
