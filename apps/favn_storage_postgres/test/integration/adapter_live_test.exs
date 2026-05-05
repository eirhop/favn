defmodule FavnStoragePostgres.Integration.AdapterLiveTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Favn.Manifest
  alias Favn.Manifest.Identity
  alias Favn.Manifest.Version
  alias Favn.Storage.Adapter.Postgres, as: Adapter
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.RunState
  alias FavnStoragePostgres.Migrations
  alias FavnStoragePostgres.Repo

  setup_all do
    case System.get_env("FAVN_POSTGRES_TEST_URL") do
      url when is_binary(url) and url != "" ->
        repo_config = repo_config_from_url(url)

        if valid_repo_config?(repo_config) do
          unique = System.unique_integer([:positive])
          supervisor_name = Module.concat([__MODULE__, "Supervisor#{unique}"])

          opts = [
            repo_mode: :managed,
            repo_config: Keyword.merge(repo_config, pool_size: 4),
            migration_mode: :auto,
            supervisor_name: supervisor_name
          ]

          assert {:ok, child_spec} = Adapter.child_spec(opts)
          start_supervised!(child_spec)

          {:ok, opts: opts}
        else
          {:ok, opts: nil}
        end

      _missing ->
        {:ok, opts: nil}
    end
  end

  test "round-trips manifests, runs, events, and scheduler state", context do
    case context[:opts] do
      nil ->
        :ok

      opts ->
        version = manifest_version("mv_pg_live_#{System.unique_integer([:positive])}")

        assert :ok = Adapter.put_manifest_version(version, opts)
        assert :ok = Adapter.set_active_manifest_version(version.manifest_version_id, opts)
        assert {:ok, active_manifest_version_id} = Adapter.get_active_manifest_version(opts)
        assert active_manifest_version_id == version.manifest_version_id

        run =
          RunState.new(
            id: "run_pg_live_#{System.unique_integer([:positive])}",
            manifest_version_id: version.manifest_version_id,
            manifest_content_hash: version.content_hash,
            asset_ref: {MyApp.Asset, :asset}
          )

        assert :ok = Adapter.put_run(run, opts)
        assert {:ok, stored_run} = Adapter.get_run(run.id, opts)
        assert stored_run.id == run.id

        event = %{
          sequence: 1,
          event_type: :run_started,
          occurred_at: DateTime.utc_now()
        }

        assert :ok = Adapter.append_run_event(run.id, event, opts)
        assert :ok = Adapter.append_run_event(run.id, event, opts)

        assert {:ok, [stored_event]} = Adapter.list_run_events(run.id, opts)
        assert stored_event.sequence == 1

        key = {MyApp.Pipeline, :daily}
        last_due_at = DateTime.utc_now() |> DateTime.truncate(:second)

        assert :ok =
                 Adapter.put_scheduler_state(key, %{version: 1, last_due_at: last_due_at}, opts)

        assert {:ok, %{rows: [[state_payload]]}} =
                 SQL.query(
                   Repo,
                   """
                   SELECT state_blob
                   FROM favn_scheduler_cursors
                   WHERE pipeline_module = $1 AND schedule_id = $2
                   LIMIT 1
                   """,
                   [Atom.to_string(MyApp.Pipeline), "daily"]
                 )

        state_dto = Jason.decode!(state_payload)
        assert state_dto["format"] == "favn.scheduler_state.storage"
        assert state_dto["schema_version"] == 1
        assert state_dto["state"]["last_due_at"] == DateTime.to_iso8601(last_due_at)
        refute Map.has_key?(state_dto["state"], "pipeline_module")
        refute Map.has_key?(state_dto["state"], "schedule_id")
        refute Map.has_key?(state_dto["state"], "version")
        refute state_payload =~ "__type__"
        refute state_payload =~ "__struct__"

        assert {:ok, %Favn.Scheduler.State{schedule_id: :daily} = stored_scheduler} =
                 Adapter.get_scheduler_state(key, opts)

        assert stored_scheduler.pipeline_module == MyApp.Pipeline
        assert stored_scheduler.version == 1
        assert stored_scheduler.last_due_at == last_due_at
    end
  end

  test "run recovery accepts only consumer module atoms from the run manifest", context do
    case context[:opts] do
      nil ->
        :ok

      opts ->
        version = manifest_version("mv_pg_run_snapshot_#{System.unique_integer([:positive])}")
        existing_module = Atom.to_string(MyApp.Asset)

        unknown_module =
          "Elixir.Favn.PostgresLiveTest.RestartAsset#{System.unique_integer([:positive])}"

        run =
          RunState.new(
            id: "run_pg_snapshot_#{System.unique_integer([:positive])}",
            manifest_version_id: version.manifest_version_id,
            manifest_content_hash: version.content_hash,
            asset_ref: {MyApp.Asset, :asset}
          )

        assert :ok = Adapter.put_manifest_version(version, opts)
        assert :ok = Adapter.put_run(run, opts)
        replace_run_atom(run.id, existing_module, unknown_module)

        content_hash =
          replace_manifest_value(run.manifest_version_id, existing_module, unknown_module)

        replace_run_manifest_content_hash(run.id, run.manifest_content_hash, content_hash)

        assert {:ok, fetched} = Adapter.get_run(run.id, opts)
        assert {module, :asset} = fetched.asset_ref
        assert Atom.to_string(module) == unknown_module
    end
  end

  test "run recovery rejects consumer module atoms absent from the run manifest", context do
    case context[:opts] do
      nil ->
        :ok

      opts ->
        version =
          manifest_version("mv_pg_run_snapshot_reject_#{System.unique_integer([:positive])}")

        existing_module = Atom.to_string(MyApp.Asset)

        unknown_module =
          "Elixir.Favn.PostgresLiveTest.UnknownAsset#{System.unique_integer([:positive])}"

        run =
          RunState.new(
            id: "run_pg_snapshot_reject_#{System.unique_integer([:positive])}",
            manifest_version_id: version.manifest_version_id,
            manifest_content_hash: version.content_hash,
            asset_ref: {MyApp.Asset, :asset}
          )

        assert :ok = Adapter.put_manifest_version(version, opts)
        assert :ok = Adapter.put_run(run, opts)
        replace_run_atom(run.id, existing_module, unknown_module)

        assert {:error, {:payload_decode_failed, {:unknown_atom, ^unknown_module}}} =
                 Adapter.get_run(run.id, opts)
    end
  end

  test "same-seq idempotent and stale/conflict semantics", context do
    case context[:opts] do
      nil ->
        :ok

      opts ->
        version = manifest_version("mv_pg_seq_#{System.unique_integer([:positive])}")
        assert :ok = Adapter.put_manifest_version(version, opts)

        base =
          RunState.new(
            id: "run_pg_seq_#{System.unique_integer([:positive])}",
            manifest_version_id: version.manifest_version_id,
            manifest_content_hash: version.content_hash,
            asset_ref: {MyApp.Asset, :asset}
          )

        assert :ok = Adapter.put_run(base, opts)
        assert :ok = Adapter.put_run(base, opts)

        conflict = %{base | status: :error} |> RunState.with_snapshot_hash()
        assert {:error, :conflicting_snapshot} = Adapter.put_run(conflict, opts)

        newer = %{base | event_seq: 2, status: :running} |> RunState.with_snapshot_hash()
        assert :ok = Adapter.put_run(newer, opts)

        stale = %{base | event_seq: 1, status: :running} |> RunState.with_snapshot_hash()
        assert {:error, :stale_write} = Adapter.put_run(stale, opts)
    end
  end

  test "enforces run write conflicts under concurrent updates", context do
    case context[:opts] do
      nil ->
        :ok

      opts ->
        version = manifest_version("mv_pg_concurrency_#{System.unique_integer([:positive])}")
        assert :ok = Adapter.put_manifest_version(version, opts)

        base =
          RunState.new(
            id: "run_pg_concurrent_#{System.unique_integer([:positive])}",
            manifest_version_id: version.manifest_version_id,
            manifest_content_hash: version.content_hash,
            asset_ref: {MyApp.Asset, :asset}
          )

        assert :ok = Adapter.put_run(base, opts)

        running = %{base | event_seq: 2, status: :running} |> RunState.with_snapshot_hash()
        failed = %{base | event_seq: 2, status: :error} |> RunState.with_snapshot_hash()

        results =
          concurrent_results(fn -> Adapter.put_run(running, opts) end, fn ->
            Adapter.put_run(failed, opts)
          end)

        assert Enum.sort(results) == [:ok, {:error, :conflicting_snapshot}]
    end
  end

  test "enforces scheduler version checks under concurrent updates", context do
    case context[:opts] do
      nil ->
        :ok

      opts ->
        key = {MyApp.Pipeline, :daily}
        assert :ok = Adapter.put_scheduler_state(key, %{version: 1}, opts)

        results =
          concurrent_results(
            fn ->
              Adapter.put_scheduler_state(
                key,
                %{version: 2, last_due_at: DateTime.utc_now()},
                opts
              )
            end,
            fn ->
              Adapter.put_scheduler_state(
                key,
                %{version: 2, last_due_at: DateTime.utc_now()},
                opts
              )
            end
          )

        assert Enum.sort(results) == [:ok, {:error, :stale_scheduler_state}]
    end
  end

  test "scheduler supports multiple schedule ids and exact nil keys", context do
    case context[:opts] do
      nil ->
        :ok

      opts ->
        key_daily = {MyApp.Pipeline, :daily}
        key_hourly = {MyApp.Pipeline, :hourly}

        assert :ok = Adapter.put_scheduler_state(key_daily, %{version: 1}, opts)

        Process.sleep(5)
        assert :ok = Adapter.put_scheduler_state(key_hourly, %{version: 1}, opts)

        assert {:ok, %Favn.Scheduler.State{schedule_id: :daily}} =
                 Adapter.get_scheduler_state(key_daily, opts)

        assert {:ok, %Favn.Scheduler.State{schedule_id: :hourly}} =
                 Adapter.get_scheduler_state(key_hourly, opts)

        assert {:ok, nil} = Adapter.get_scheduler_state({MyApp.Pipeline, nil}, opts)

        assert :ok = Adapter.put_scheduler_state({MyApp.Pipeline, nil}, %{version: 1}, opts)

        assert {:ok, %{rows: [[nil_state_payload]]}} =
                 SQL.query(
                   Repo,
                   """
                   SELECT state_blob
                   FROM favn_scheduler_cursors
                   WHERE pipeline_module = $1 AND schedule_id = $2
                   LIMIT 1
                   """,
                   [Atom.to_string(MyApp.Pipeline), "__nil__"]
                 )

        nil_state_dto = Jason.decode!(nil_state_payload)
        assert nil_state_dto["format"] == "favn.scheduler_state.storage"
        assert nil_state_dto["schema_version"] == 1
        refute Map.has_key?(nil_state_dto["state"], "schedule_id")
        refute nil_state_payload =~ "__type__"
        refute nil_state_payload =~ "__struct__"

        assert {:ok, %Favn.Scheduler.State{schedule_id: nil}} =
                 Adapter.get_scheduler_state({MyApp.Pipeline, nil}, opts)
    end
  end

  test "round-trips normalized backfill state", context do
    case context[:opts] do
      nil ->
        :ok

      opts ->
        unique = System.unique_integer([:positive])
        now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
        start_at = DateTime.add(now, -86_400, :second)

        assert {:ok, baseline} =
                 CoverageBaseline.new(%{
                   baseline_id: "baseline_pg_#{unique}",
                   pipeline_module: MyApp.Pipeline,
                   source_key: "orders",
                   segment_key_hash: "sha256:#{unique}",
                   segment_key_redacted: "tenant-***",
                   window_kind: :daily,
                   timezone: "Etc/UTC",
                   coverage_start_at: start_at,
                   coverage_until: now,
                   created_by_run_id: "run_baseline_#{unique}",
                   manifest_version_id: "mv_backfill_#{unique}",
                   status: :ok,
                   errors: [],
                   metadata: %{row_count: 10},
                   created_at: now,
                   updated_at: now
                 })

        assert :ok = Adapter.put_coverage_baseline(baseline, opts)
        assert {:ok, ^baseline} = Adapter.get_coverage_baseline(baseline.baseline_id, opts)

        assert {:ok, baseline_page} =
                 Adapter.list_coverage_baselines(
                   [pipeline_module: MyApp.Pipeline, status: :ok],
                   opts
                 )

        assert [^baseline] = baseline_page.items

        assert {:ok, window} =
                 BackfillWindow.new(%{
                   backfill_run_id: "backfill_pg_#{unique}",
                   child_run_id: "child_pg_#{unique}",
                   pipeline_module: MyApp.Pipeline,
                   manifest_version_id: baseline.manifest_version_id,
                   coverage_baseline_id: baseline.baseline_id,
                   window_kind: :daily,
                   window_start_at: start_at,
                   window_end_at: now,
                   timezone: "Etc/UTC",
                   window_key: "day:2026-04-27",
                   status: :running,
                   attempt_count: 1,
                   latest_attempt_run_id: "child_pg_#{unique}",
                   last_error: %{reason: :retryable},
                   errors: [%{message: "retry"}],
                   metadata: %{partition: "2026-04-27"},
                   started_at: start_at,
                   created_at: start_at,
                   updated_at: now
                 })

        assert :ok = Adapter.put_backfill_window(window, opts)

        assert {:ok, ^window} =
                 Adapter.get_backfill_window(
                   window.backfill_run_id,
                   window.pipeline_module,
                   window.window_key,
                   opts
                 )

        assert {:ok, window_page} =
                 Adapter.list_backfill_windows(
                   [pipeline_module: MyApp.Pipeline, status: :running],
                   opts
                 )

        assert [^window] = window_page.items

        assert {:ok, asset_state} =
                 AssetWindowState.new(%{
                   asset_ref_module: MyApp.Asset,
                   asset_ref_name: :asset,
                   pipeline_module: MyApp.Pipeline,
                   manifest_version_id: baseline.manifest_version_id,
                   window_kind: :daily,
                   window_start_at: start_at,
                   window_end_at: now,
                   timezone: "Etc/UTC",
                   window_key: window.window_key,
                   status: :ok,
                   latest_run_id: "asset_pg_#{unique}",
                   latest_parent_run_id: window.backfill_run_id,
                   latest_success_run_id: "asset_pg_#{unique}",
                   rows_written: 10,
                   errors: [],
                   metadata: %{relation: "gold.sales"},
                   updated_at: now
                 })

        assert :ok = Adapter.put_asset_window_state(asset_state, opts)

        assert {:ok, ^asset_state} =
                 Adapter.get_asset_window_state(
                   asset_state.asset_ref_module,
                   asset_state.asset_ref_name,
                   asset_state.window_key,
                   opts
                 )

        assert {:ok, state_page} =
                 Adapter.list_asset_window_states(
                   [pipeline_module: MyApp.Pipeline, window_key: window.window_key],
                   opts
                 )

        assert [^asset_state] = state_page.items
    end
  end

  test "replaces scoped backfill read models", context do
    case context[:opts] do
      nil ->
        :ok

      opts ->
        unique = System.unique_integer([:positive])
        now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
        start_at = DateTime.add(now, -86_400, :second)

        stale_baseline =
          sample_coverage_baseline("baseline_pg_stale_#{unique}", :ok, now, start_at)

        kept_baseline = %{
          sample_coverage_baseline("baseline_pg_kept_#{unique}", :ok, now, start_at)
          | pipeline_module: MyApp.OtherPipeline
        }

        replacement_baseline =
          sample_coverage_baseline("baseline_pg_new_#{unique}", :ok, now, start_at)

        stale_window =
          sample_backfill_window("window_pg_stale_#{unique}", :ok, now, start_at, stale_baseline)

        kept_window = %{
          sample_backfill_window("window_pg_kept_#{unique}", :ok, now, start_at, kept_baseline)
          | pipeline_module: MyApp.OtherPipeline
        }

        replacement_window =
          sample_backfill_window(
            "window_pg_new_#{unique}",
            :ok,
            now,
            start_at,
            replacement_baseline
          )

        stale_state =
          sample_asset_window_state(:stale_asset, stale_window.window_key, :ok, now, start_at)

        kept_state = %{
          sample_asset_window_state(:kept_asset, kept_window.window_key, :ok, now, start_at)
          | pipeline_module: MyApp.OtherPipeline
        }

        replacement_state =
          sample_asset_window_state(:new_asset, replacement_window.window_key, :ok, now, start_at)

        for item <- [stale_baseline, kept_baseline],
            do: assert(:ok = Adapter.put_coverage_baseline(item, opts))

        for item <- [stale_window, kept_window],
            do: assert(:ok = Adapter.put_backfill_window(item, opts))

        for item <- [stale_state, kept_state],
            do: assert(:ok = Adapter.put_asset_window_state(item, opts))

        assert :ok =
                 Adapter.replace_backfill_read_models(
                   [pipeline_module: MyApp.Pipeline],
                   [replacement_baseline],
                   [replacement_window],
                   [replacement_state],
                   opts
                 )

        assert {:error, :not_found} =
                 Adapter.get_coverage_baseline(stale_baseline.baseline_id, opts)

        assert {:ok, ^kept_baseline} =
                 Adapter.get_coverage_baseline(kept_baseline.baseline_id, opts)

        assert {:ok, ^replacement_baseline} =
                 Adapter.get_coverage_baseline(replacement_baseline.baseline_id, opts)

        assert {:error, :not_found} =
                 Adapter.get_backfill_window(
                   stale_window.backfill_run_id,
                   stale_window.pipeline_module,
                   stale_window.window_key,
                   opts
                 )

        assert {:ok, ^kept_window} =
                 Adapter.get_backfill_window(
                   kept_window.backfill_run_id,
                   kept_window.pipeline_module,
                   kept_window.window_key,
                   opts
                 )

        assert {:ok, ^replacement_window} =
                 Adapter.get_backfill_window(
                   replacement_window.backfill_run_id,
                   replacement_window.pipeline_module,
                   replacement_window.window_key,
                   opts
                 )

        assert {:error, :not_found} =
                 Adapter.get_asset_window_state(
                   stale_state.asset_ref_module,
                   stale_state.asset_ref_name,
                   stale_state.window_key,
                   opts
                 )

        assert {:ok, ^kept_state} =
                 Adapter.get_asset_window_state(
                   kept_state.asset_ref_module,
                   kept_state.asset_ref_name,
                   kept_state.window_key,
                   opts
                 )

        assert {:ok, ^replacement_state} =
                 Adapter.get_asset_window_state(
                   replacement_state.asset_ref_module,
                   replacement_state.asset_ref_name,
                   replacement_state.window_key,
                   opts
                 )
    end
  end

  test "decodes legacy backfill window kind aliases through constructors", context do
    case context[:opts] do
      nil ->
        :ok

      opts ->
        unique = System.unique_integer([:positive])
        now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
        start_at = DateTime.add(now, -86_400, :second)
        baseline = sample_coverage_baseline("baseline_pg_legacy_#{unique}", :ok, now, start_at)

        window =
          sample_backfill_window("window_pg_legacy_#{unique}", :running, now, start_at, baseline)

        state =
          sample_asset_window_state(:orders, "asset_pg_legacy_#{unique}", :running, now, start_at)

        assert :ok = Adapter.put_coverage_baseline(baseline, opts)
        assert :ok = Adapter.put_backfill_window(window, opts)
        assert :ok = Adapter.put_asset_window_state(state, opts)

        assert {:ok, _} =
                 SQL.query(
                   Repo,
                   "UPDATE favn_pipeline_coverage_baselines SET window_kind = $1 WHERE baseline_id = $2",
                   [
                     "daily",
                     baseline.baseline_id
                   ]
                 )

        assert {:ok, _} =
                 SQL.query(
                   Repo,
                   "UPDATE favn_backfill_windows SET window_kind = $1 WHERE window_key = $2",
                   [
                     "daily",
                     window.window_key
                   ]
                 )

        assert {:ok, _} =
                 SQL.query(
                   Repo,
                   "UPDATE favn_asset_window_states SET window_kind = $1 WHERE window_key = $2",
                   [
                     "daily",
                     state.window_key
                   ]
                 )

        assert {:ok, ^baseline} = Adapter.get_coverage_baseline(baseline.baseline_id, opts)

        assert {:ok, ^window} =
                 Adapter.get_backfill_window(
                   window.backfill_run_id,
                   window.pipeline_module,
                   window.window_key,
                   opts
                 )

        assert {:ok, ^state} =
                 Adapter.get_asset_window_state(
                   state.asset_ref_module,
                   state.asset_ref_name,
                   state.window_key,
                   opts
                 )
    end
  end

  test "rejects invalid persisted backfill read-model statuses", context do
    case context[:opts] do
      nil ->
        :ok

      opts ->
        unique = System.unique_integer([:positive])
        now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
        start_at = DateTime.add(now, -86_400, :second)

        baseline =
          sample_coverage_baseline("baseline_pg_bad_status_#{unique}", :ok, now, start_at)

        window =
          sample_backfill_window(
            "window_pg_bad_status_#{unique}",
            :running,
            now,
            start_at,
            baseline
          )

        state =
          sample_asset_window_state(
            :orders,
            "asset_pg_bad_status_#{unique}",
            :running,
            now,
            start_at
          )

        assert :ok = Adapter.put_coverage_baseline(baseline, opts)
        assert :ok = Adapter.put_backfill_window(window, opts)
        assert :ok = Adapter.put_asset_window_state(state, opts)

        assert {:ok, _} =
                 SQL.query(
                   Repo,
                   "UPDATE favn_pipeline_coverage_baselines SET status = $1 WHERE baseline_id = $2",
                   [
                     "bogus",
                     baseline.baseline_id
                   ]
                 )

        assert {:ok, _} =
                 SQL.query(
                   Repo,
                   "UPDATE favn_backfill_windows SET status = $1 WHERE window_key = $2",
                   [
                     "bogus",
                     window.window_key
                   ]
                 )

        assert {:ok, _} =
                 SQL.query(
                   Repo,
                   "UPDATE favn_asset_window_states SET status = $1 WHERE window_key = $2",
                   [
                     "bogus",
                     state.window_key
                   ]
                 )

        assert {:error, {:invalid_status, "bogus"}} =
                 Adapter.get_coverage_baseline(baseline.baseline_id, opts)

        assert {:error, {:invalid_status, "bogus"}} =
                 Adapter.get_backfill_window(
                   window.backfill_run_id,
                   window.pipeline_module,
                   window.window_key,
                   opts
                 )

        assert {:error, {:invalid_status, "bogus"}} =
                 Adapter.get_asset_window_state(
                   state.asset_ref_module,
                   state.asset_ref_name,
                   state.window_key,
                   opts
                 )
    end
  end

  test "rejects invalid persisted backfill read-model window kinds", context do
    case context[:opts] do
      nil ->
        :ok

      opts ->
        unique = System.unique_integer([:positive])
        now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
        start_at = DateTime.add(now, -86_400, :second)
        baseline = sample_coverage_baseline("baseline_pg_bad_kind_#{unique}", :ok, now, start_at)

        window =
          sample_backfill_window(
            "window_pg_bad_kind_#{unique}",
            :running,
            now,
            start_at,
            baseline
          )

        state =
          sample_asset_window_state(
            :orders,
            "asset_pg_bad_kind_#{unique}",
            :running,
            now,
            start_at
          )

        assert :ok = Adapter.put_coverage_baseline(baseline, opts)
        assert :ok = Adapter.put_backfill_window(window, opts)
        assert :ok = Adapter.put_asset_window_state(state, opts)

        assert {:ok, _} =
                 SQL.query(
                   Repo,
                   "UPDATE favn_pipeline_coverage_baselines SET window_kind = $1 WHERE baseline_id = $2",
                   [
                     "fortnight",
                     baseline.baseline_id
                   ]
                 )

        assert {:ok, _} =
                 SQL.query(
                   Repo,
                   "UPDATE favn_backfill_windows SET window_kind = $1 WHERE window_key = $2",
                   [
                     "fortnight",
                     window.window_key
                   ]
                 )

        assert {:ok, _} =
                 SQL.query(
                   Repo,
                   "UPDATE favn_asset_window_states SET window_kind = $1 WHERE window_key = $2",
                   [
                     "fortnight",
                     state.window_key
                   ]
                 )

        assert {:error, {:invalid_window_kind, "fortnight"}} =
                 Adapter.get_coverage_baseline(baseline.baseline_id, opts)

        assert {:error, {:invalid_window_kind, "fortnight"}} =
                 Adapter.get_backfill_window(
                   window.backfill_run_id,
                   window.pipeline_module,
                   window.window_key,
                   opts
                 )

        assert {:error, {:invalid_window_kind, "fortnight"}} =
                 Adapter.get_asset_window_state(
                   state.asset_ref_module,
                   state.asset_ref_name,
                   state.window_key,
                   opts
                 )
    end
  end

  test "rejects unknown persisted backfill identity atoms", context do
    case context[:opts] do
      nil ->
        :ok

      opts ->
        unique = System.unique_integer([:positive])
        now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
        start_at = DateTime.add(now, -86_400, :second)

        baseline =
          sample_coverage_baseline("baseline_pg_unknown_atom_#{unique}", :ok, now, start_at)

        state =
          sample_asset_window_state(
            :orders,
            "asset_pg_unknown_atom_#{unique}",
            :running,
            now,
            start_at
          )

        unknown_pipeline = "Elixir.FavnStoragePostgres.UnknownPipeline#{unique}"
        unknown_asset_name = "unknown_asset_name_#{unique}"

        assert :ok = Adapter.put_coverage_baseline(baseline, opts)
        assert :ok = Adapter.put_asset_window_state(state, opts)

        assert {:ok, _} =
                 SQL.query(
                   Repo,
                   "UPDATE favn_pipeline_coverage_baselines SET pipeline_module = $1 WHERE baseline_id = $2",
                   [unknown_pipeline, baseline.baseline_id]
                 )

        assert {:error, {:unknown_atom, ^unknown_pipeline}} =
                 Adapter.get_coverage_baseline(baseline.baseline_id, opts)

        assert {:ok, _} =
                 SQL.query(
                   Repo,
                   "UPDATE favn_asset_window_states SET asset_ref_name = $1 WHERE window_key = $2",
                   [unknown_asset_name, state.window_key]
                 )

        assert {:error, {:unknown_atom, ^unknown_asset_name}} =
                 Adapter.list_asset_window_states([], opts)
    end
  end

  test "manual schema readiness can recover after missing migration row", context do
    case context[:opts] do
      nil ->
        :ok

      _opts ->
        assert true == Migrations.schema_ready?(Repo)

        assert {:ok, _} = SQL.query(Repo, "DELETE FROM schema_migrations", [])
        assert false == Migrations.schema_ready?(Repo)

        :ok = Migrations.migrate!(Repo)
        assert true == Migrations.schema_ready?(Repo)
    end
  end

  defp sample_coverage_baseline(baseline_id, status, now, start_at) do
    {:ok, baseline} =
      CoverageBaseline.new(%{
        baseline_id: baseline_id,
        pipeline_module: MyApp.Pipeline,
        source_key: "orders",
        segment_key_hash: "sha256:#{baseline_id}",
        segment_key_redacted: "tenant-***",
        window_kind: :daily,
        timezone: "Etc/UTC",
        coverage_start_at: start_at,
        coverage_until: now,
        created_by_run_id: "run_#{baseline_id}",
        manifest_version_id: "mv_#{baseline_id}",
        status: status,
        errors: [],
        metadata: %{row_count: 10},
        created_at: now,
        updated_at: now
      })

    baseline
  end

  defp sample_backfill_window(window_key, status, now, start_at, baseline) do
    {:ok, window} =
      BackfillWindow.new(%{
        backfill_run_id: "backfill_#{window_key}",
        child_run_id: "child_#{window_key}",
        pipeline_module: MyApp.Pipeline,
        manifest_version_id: baseline.manifest_version_id,
        coverage_baseline_id: baseline.baseline_id,
        window_kind: :daily,
        window_start_at: start_at,
        window_end_at: now,
        timezone: "Etc/UTC",
        window_key: window_key,
        status: status,
        attempt_count: 1,
        latest_attempt_run_id: "child_#{window_key}",
        last_error: %{reason: :retryable},
        errors: [%{message: "retry"}],
        metadata: %{partition: "2026-04-27"},
        started_at: start_at,
        created_at: start_at,
        updated_at: now
      })

    window
  end

  defp sample_asset_window_state(asset_name, window_key, status, now, start_at) do
    {:ok, state} =
      AssetWindowState.new(%{
        asset_ref_module: MyApp.Asset,
        asset_ref_name: asset_name,
        pipeline_module: MyApp.Pipeline,
        manifest_version_id: "mv_#{window_key}",
        window_kind: :daily,
        window_start_at: start_at,
        window_end_at: now,
        timezone: "Etc/UTC",
        window_key: window_key,
        status: status,
        latest_run_id: "asset_#{window_key}",
        latest_parent_run_id: "backfill_#{window_key}",
        latest_success_run_id: if(status == :ok, do: "asset_#{window_key}", else: nil),
        rows_written: 10,
        errors: [],
        metadata: %{relation: "gold.sales"},
        updated_at: now
      })

    state
  end

  defp repo_config_from_url(url) do
    uri = URI.parse(url)

    [database | _rest] =
      uri.path
      |> to_string()
      |> String.trim_leading("/")
      |> String.split("/", trim: true)

    [
      hostname: uri.host,
      port: uri.port || 5432,
      database: database,
      username: uri.userinfo |> user_from_userinfo(),
      password: uri.userinfo |> password_from_userinfo(),
      ssl: false,
      show_sensitive_data_on_connection_error: true
    ]
  end

  defp user_from_userinfo(nil), do: nil

  defp user_from_userinfo(userinfo) do
    userinfo
    |> String.split(":", parts: 2)
    |> List.first()
  end

  defp password_from_userinfo(nil), do: nil

  defp password_from_userinfo(userinfo) do
    case String.split(userinfo, ":", parts: 2) do
      [_user] -> nil
      [_user, password] -> password
    end
  end

  defp valid_repo_config?(repo_config) do
    Enum.all?([:hostname, :database, :username, :password], fn key ->
      value = Keyword.get(repo_config, key)
      is_binary(value) and value != ""
    end)
  end

  defp manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Favn.Manifest.Asset{ref: {MyApp.Asset, :asset}, module: MyApp.Asset, name: :asset}
      ]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp replace_run_atom(run_id, from, to) do
    assert {:ok, %{rows: [[payload]]}} =
             SQL.query(Repo, "SELECT run_blob FROM favn_runs WHERE run_id = $1", [run_id])

    payload = replace_atom_value(payload, from, to)

    assert {:ok, _} =
             SQL.query(Repo, "UPDATE favn_runs SET run_blob = $1 WHERE run_id = $2", [
               payload,
               run_id
             ])
  end

  defp replace_manifest_value(manifest_version_id, from, to) do
    assert {:ok, %{rows: [[manifest_json]]}} =
             SQL.query(
               Repo,
               "SELECT manifest_json FROM favn_manifest_versions WHERE manifest_version_id = $1",
               [manifest_version_id]
             )

    manifest_json = replace_string_value(manifest_json, from, to)
    content_hash = manifest_content_hash!(manifest_json)

    assert {:ok, _} =
             SQL.query(
               Repo,
               "UPDATE favn_manifest_versions SET manifest_json = $1, content_hash = $2 WHERE manifest_version_id = $3",
               [manifest_json, content_hash, manifest_version_id]
             )

    content_hash
  end

  defp replace_run_manifest_content_hash(run_id, from, to) do
    assert {:ok, %{rows: [[payload]]}} =
             SQL.query(Repo, "SELECT run_blob FROM favn_runs WHERE run_id = $1", [run_id])

    payload = replace_string_value(payload, from, to)

    assert {:ok, _} =
             SQL.query(Repo, "UPDATE favn_runs SET run_blob = $1 WHERE run_id = $2", [
               payload,
               run_id
             ])
  end

  defp replace_atom_value(encoded, from, to) do
    encoded
    |> JSON.decode!()
    |> replace_atom_value_in_term(from, to)
    |> JSON.encode!()
  end

  defp replace_atom_value_in_term(%{"__type__" => "atom", "value" => value} = term, value, to) do
    %{term | "value" => to}
  end

  defp replace_atom_value_in_term(%{} = term, from, to) do
    Map.new(term, fn {key, value} -> {key, replace_atom_value_in_term(value, from, to)} end)
  end

  defp replace_atom_value_in_term(values, from, to) when is_list(values) do
    Enum.map(values, &replace_atom_value_in_term(&1, from, to))
  end

  defp replace_atom_value_in_term(value, _from, _to), do: value

  defp replace_string_value(encoded, from, to) do
    encoded
    |> JSON.decode!()
    |> replace_string_value_in_term(from, to)
    |> JSON.encode!()
  end

  defp replace_string_value_in_term(value, value, to) when is_binary(value), do: to

  defp replace_string_value_in_term(%{} = term, from, to) do
    Map.new(term, fn {key, value} -> {key, replace_string_value_in_term(value, from, to)} end)
  end

  defp replace_string_value_in_term(values, from, to) when is_list(values) do
    Enum.map(values, &replace_string_value_in_term(&1, from, to))
  end

  defp replace_string_value_in_term(value, _from, _to), do: value

  defp manifest_content_hash!(manifest_json) do
    manifest_json
    |> JSON.decode!()
    |> Identity.hash_manifest()
    |> case do
      {:ok, hash} -> hash
    end
  end

  defp concurrent_results(fun_a, fun_b) do
    parent = self()

    task_a = Task.async(fn -> await_release(parent, :task_a, fun_a) end)
    task_b = Task.async(fn -> await_release(parent, :task_b, fun_b) end)

    assert_receive {:ready, :task_a}
    assert_receive {:ready, :task_b}

    send(task_a.pid, :go)
    send(task_b.pid, :go)

    [Task.await(task_a, 5_000), Task.await(task_b, 5_000)]
  end

  defp await_release(parent, label, fun) do
    send(parent, {:ready, label})

    receive do
      :go -> fun.()
    end
  end
end
