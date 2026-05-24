defmodule Favn.StorageFacadeTest do
  use ExUnit.Case, async: false

  alias Favn.Run
  alias Favn.Storage
  alias FavnOrchestrator.AssetFreshnessState
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
    def try_acquire_execution_lease(lease, _opts), do: {:ok, lease}

    @impl true
    def release_execution_lease(_lease_id, _opts), do: :ok

    @impl true
    def release_execution_leases_for_run(run_id, _opts) do
      {:ok, FavnOrchestrator.ExecutionAdmission.LeaseRelease.new(run_id, 0, [])}
    end

    @impl true
    def expire_execution_leases(_now, _opts), do: {:ok, 0}

    @impl true
    def list_execution_leases(_opts), do: {:ok, []}

    @impl true
    def upsert_execution_admission_waiter(waiter, _opts), do: {:ok, waiter}

    @impl true
    def delete_execution_admission_waiter(_waiter_id, _opts), do: :ok

    @impl true
    def delete_execution_admission_waiters_for_run(_run_id, _opts), do: {:ok, 0}

    @impl true
    def list_execution_admission_waiters_for_scope(_scope, _waiter_opts, _opts), do: {:ok, []}

    @impl true
    def expire_execution_admission_waiters(_now, _opts), do: {:ok, 0}

    @impl true
    def persist_log_entries(entries, _opts), do: {:ok, entries}

    @impl true
    def list_logs(_filter, _opts, _adapter_opts),
      do:
        {:ok,
         %FavnOrchestrator.Page{
           items: [],
           limit: 100,
           offset: 0,
           has_more?: false,
           next_offset: nil
         }}

    @impl true
    def replay_logs_after(_cursor, _filter, _opts, _adapter_opts), do: {:ok, []}

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
    def scan_backfill_windows(_filters, scan_opts, _opts),
      do: {:ok, FavnOrchestrator.CursorPage.from_fetched([], scan_opts, fn _ -> nil end)}

    @impl true
    def apply_backfill_child_projection(_window, _states, _opts), do: {:error, :not_found}

    @impl true
    def get_backfill_progress(_backfill_run_id, _opts), do: {:error, :not_found}

    @impl true
    def rebuild_backfill_progress(_backfill_run_id, _opts), do: {:error, :not_found}

    @impl true
    def put_asset_window_state(_state, _opts), do: :ok

    @impl true
    def get_asset_window_state(_asset_ref_module, _asset_ref_name, _window_key, _opts),
      do: {:error, :not_found}

    @impl true
    def list_asset_window_states(_filters, _opts), do: {:ok, []}

    @impl true
    def put_asset_freshness_state(_state, _opts), do: :ok

    @impl true
    def get_asset_freshness_state(_asset_ref_module, _asset_ref_name, _freshness_key, _opts),
      do: {:error, :not_found}

    @impl true
    def list_asset_freshness_states(_filters, _opts), do: {:ok, []}

    @impl true
    def scan_asset_freshness_states(_filters, scan_opts, _opts),
      do: {:ok, FavnOrchestrator.CursorPage.from_fetched([], scan_opts, fn _ -> nil end)}

    @impl true
    def get_asset_freshness_states_by_keys(_keys, _opts), do: {:ok, %{}}

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
    def try_acquire_execution_lease(lease, _opts), do: {:ok, lease}

    @impl true
    def release_execution_lease(_lease_id, _opts), do: :ok

    @impl true
    def release_execution_leases_for_run(run_id, _opts) do
      {:ok, FavnOrchestrator.ExecutionAdmission.LeaseRelease.new(run_id, 0, [])}
    end

    @impl true
    def expire_execution_leases(_now, _opts), do: {:ok, 0}

    @impl true
    def list_execution_leases(_opts), do: {:ok, []}

    @impl true
    def upsert_execution_admission_waiter(waiter, _opts), do: {:ok, waiter}

    @impl true
    def delete_execution_admission_waiter(_waiter_id, _opts), do: :ok

    @impl true
    def delete_execution_admission_waiters_for_run(_run_id, _opts), do: {:ok, 0}

    @impl true
    def list_execution_admission_waiters_for_scope(_scope, _waiter_opts, _opts), do: {:ok, []}

    @impl true
    def expire_execution_admission_waiters(_now, _opts), do: {:ok, 0}

    @impl true
    def persist_log_entries(entries, _opts), do: {:ok, entries}

    @impl true
    def list_logs(_filter, _opts, _adapter_opts),
      do:
        {:ok,
         %FavnOrchestrator.Page{
           items: [],
           limit: 100,
           offset: 0,
           has_more?: false,
           next_offset: nil
         }}

    @impl true
    def replay_logs_after(_cursor, _filter, _opts, _adapter_opts), do: {:ok, []}

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
    def scan_backfill_windows(_filters, scan_opts, _opts),
      do: {:ok, FavnOrchestrator.CursorPage.from_fetched([], scan_opts, fn _ -> nil end)}

    @impl true
    def apply_backfill_child_projection(_window, _states, _opts), do: {:error, :not_found}

    @impl true
    def get_backfill_progress(_backfill_run_id, _opts), do: {:error, :not_found}

    @impl true
    def rebuild_backfill_progress(_backfill_run_id, _opts), do: {:error, :not_found}

    @impl true
    def put_asset_window_state(_state, _opts), do: :ok

    @impl true
    def get_asset_window_state(_asset_ref_module, _asset_ref_name, _window_key, _opts),
      do: {:error, :not_found}

    @impl true
    def list_asset_window_states(_filters, _opts), do: {:ok, []}

    @impl true
    def put_asset_freshness_state(_state, _opts), do: :ok

    @impl true
    def get_asset_freshness_state(_asset_ref_module, _asset_ref_name, _freshness_key, _opts),
      do: {:error, :not_found}

    @impl true
    def list_asset_freshness_states(_filters, _opts), do: {:ok, []}

    @impl true
    def scan_asset_freshness_states(_filters, scan_opts, _opts),
      do: {:ok, FavnOrchestrator.CursorPage.from_fetched([], scan_opts, fn _ -> nil end)}

    @impl true
    def get_asset_freshness_states_by_keys(_keys, _opts), do: {:ok, %{}}

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

  test "memory storage persists paginates filters and replays log entries" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    first =
      Favn.Log.Entry.normalize(%{
        run_id: "run_logs",
        asset_step_id: "step_a",
        asset_ref: {MyApp.Assets.Gold, :asset},
        producer_id: "runner-1",
        producer_sequence: 1,
        occurred_at: now,
        level: :info,
        source: :runner,
        stream: :stdout,
        message: "hello\nworld",
        metadata: %{password: "secret", visible: "yes"}
      })

    second =
      Favn.Log.Entry.normalize(%{
        run_id: "run_logs",
        asset_step_id: "step_b",
        producer_id: "runner-1",
        producer_sequence: 2,
        occurred_at: DateTime.add(now, 1, :second),
        level: :error,
        source: :runner,
        stream: :stderr,
        message: "boom"
      })

    assert {:ok, [persisted_first, persisted_second]} =
             OrchestratorStorage.persist_log_entries([first, second])

    assert persisted_first.global_sequence == 1
    assert persisted_second.global_sequence == 2
    assert persisted_first.message == "hello\nworld"
    assert persisted_first.metadata["password"] == "[REDACTED]"

    assert {:ok, [^persisted_first]} = OrchestratorStorage.persist_log_entries([first])

    assert {:ok, page} =
             OrchestratorStorage.list_logs(%Favn.Log.Filter{run_id: "run_logs"}, limit: 1)

    assert Enum.map(page.items, & &1.global_sequence) == [1]
    assert page.has_more? == true

    assert {:ok, filtered} = OrchestratorStorage.list_logs(%Favn.Log.Filter{levels: [:error]})
    assert Enum.map(filtered.items, & &1.message) == ["boom"]

    cursor = %Favn.Log.Cursor{scope: :run, run_id: "run_logs", global_sequence: 1}

    assert {:ok, [^persisted_second]} =
             OrchestratorStorage.replay_logs_after(cursor, [run_id: "run_logs"], limit: 10)
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

  test "builds normalized asset freshness state" do
    now = ~U[2026-03-01 12:00:00Z]

    assert {:ok, %AssetFreshnessState{} = state} =
             AssetFreshnessState.new(%{
               asset_ref_module: MyApp.Assets.Orders,
               asset_ref_name: :asset,
               freshness_key: "calendar:day:Etc/UTC:2026-03-01",
               status: "ok",
               freshness_version: "freshness:v1",
               latest_success_run_id: "run_success",
               latest_success_node_key: {:node, :orders},
               latest_success_at: DateTime.to_iso8601(now),
               latest_attempt_run_id: "run_success",
               latest_attempt_status: "ok",
               latest_attempt_at: now,
               manifest_version_id: "mv_1",
               manifest_content_hash: "hash_1",
               input_versions: %{upstream: "v1"},
               metadata: %{relation: "gold.orders"},
               updated_at: DateTime.to_iso8601(now)
             })

    assert state.status == :ok
    assert state.latest_attempt_status == :ok
    assert state.latest_success_at == now

    assert {:error, {:missing_required_keys, [:freshness_key]}} =
             AssetFreshnessState.new(%{
               asset_ref_module: MyApp.Assets.Orders,
               asset_ref_name: :asset,
               status: :ok,
               updated_at: now
             })

    assert {:error, {:invalid_status, "stale"}} =
             AssetFreshnessState.new(%{
               asset_ref_module: MyApp.Assets.Orders,
               asset_ref_name: :asset,
               freshness_key: "calendar:day:Etc/UTC:2026-03-01",
               status: "stale",
               updated_at: now
             })

    assert {:error, {:invalid_freshness_key, :daily}} =
             AssetFreshnessState.new(%{
               asset_ref_module: MyApp.Assets.Orders,
               asset_ref_name: :asset,
               freshness_key: :daily,
               status: :ok,
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

  test "stores and filters asset freshness state through orchestrator memory facade" do
    now = ~U[2026-03-01 12:00:00Z]
    later = ~U[2026-03-01 13:00:00Z]

    {:ok, orders_state} =
      AssetFreshnessState.new(%{
        asset_ref_module: MyApp.Assets.Orders,
        asset_ref_name: :asset,
        freshness_key: "calendar:day:Etc/UTC:2026-03-01",
        status: :ok,
        freshness_version: "orders:daily:v1",
        latest_success_run_id: "run_orders",
        latest_success_node_key: {:node, :orders},
        latest_success_at: now,
        latest_attempt_run_id: "run_orders",
        latest_attempt_status: :ok,
        latest_attempt_at: now,
        manifest_version_id: "mv_1",
        manifest_content_hash: "hash_1",
        input_versions: %{raw_orders: "v1"},
        metadata: %{relation: "gold.orders"},
        updated_at: now
      })

    {:ok, customers_state} =
      AssetFreshnessState.new(%{
        asset_ref_module: MyApp.Assets.Customers,
        asset_ref_name: :asset,
        freshness_key: "calendar:day:Etc/UTC:2026-03-01",
        status: :running,
        latest_attempt_run_id: "run_customers",
        latest_attempt_status: :running,
        latest_attempt_at: later,
        updated_at: later
      })

    assert :ok = OrchestratorStorage.put_asset_freshness_state(orders_state)
    assert :ok = OrchestratorStorage.put_asset_freshness_state(customers_state)

    assert {:ok, ^orders_state} =
             OrchestratorStorage.get_asset_freshness_state(
               MyApp.Assets.Orders,
               :asset,
               "calendar:day:Etc/UTC:2026-03-01"
             )

    assert {:ok, page} = OrchestratorStorage.list_asset_freshness_states(status: :running)
    assert [^customers_state] = page.items

    assert {:ok, page} =
             OrchestratorStorage.list_asset_freshness_states(
               asset_ref_module: MyApp.Assets.Orders
             )

    assert [^orders_state] = page.items

    assert {:error, {:unsupported_filter, :unknown}} =
             OrchestratorStorage.list_asset_freshness_states(unknown: :value)
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
