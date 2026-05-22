defmodule FavnOrchestrator.Integration.StorageAdapterContractTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  @memory_server Module.concat(__MODULE__, MemoryServer)
  @sqlite_supervisor Module.concat(__MODULE__, SQLiteSupervisor)
  @postgres_supervisor Module.concat(__MODULE__, PostgresSupervisor)

  setup do
    previous_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_opts = Application.get_env(:favn_orchestrator, :storage_adapter_opts)

    on_exit(fn ->
      restore_env(:favn_orchestrator, :storage_adapter, previous_adapter)
      restore_env(:favn_orchestrator, :storage_adapter_opts, previous_opts)
    end)

    :ok
  end

  test "shared contract holds for memory adapter" do
    opts = [server: @memory_server]

    with_storage_adapter(Memory, opts, fn ->
      assert_shared_contract("memory")
    end)
  end

  test "shared contract holds for sqlite adapter when available" do
    case Code.ensure_loaded(Favn.Storage.Adapter.SQLite) do
      {:module, Favn.Storage.Adapter.SQLite} ->
        db_path =
          Path.join(
            System.tmp_dir!(),
            "favn_contract_sqlite_#{System.unique_integer([:positive])}.db"
          )

        opts = [
          database: db_path,
          supervisor_name: @sqlite_supervisor,
          migration_mode: :auto
        ]

        with_storage_adapter(Favn.Storage.Adapter.SQLite, opts, fn ->
          assert_shared_contract("sqlite")
        end)

        File.rm(db_path)

      error ->
        assert match?({:error, _reason}, error)
    end
  end

  test "shared contract holds for postgres adapter (opt-in)" do
    case postgres_opts() do
      nil ->
        :ok

      opts ->
        with_storage_adapter(Favn.Storage.Adapter.Postgres, opts, fn ->
          assert_shared_contract("postgres")
        end)
    end
  end

  defp with_storage_adapter(adapter, opts, fun) when is_function(fun, 0) do
    Application.put_env(:favn_orchestrator, :storage_adapter, adapter)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, opts)

    assert {:ok, child_specs} = Storage.child_specs()
    Enum.each(child_specs, &start_supervised!/1)

    fun.()
  end

  defp assert_shared_contract(label) do
    manifest_version_id = "mv_contract_#{label}_#{System.unique_integer([:positive])}"
    version = manifest_version(manifest_version_id)

    assert :ok = Storage.put_manifest_version(version)
    assert :ok = Storage.put_manifest_version(version)
    assert :ok = Storage.set_active_manifest_version(manifest_version_id)
    assert {:ok, ^manifest_version_id} = Storage.get_active_manifest_version()

    assert {:ok, stored_version} = Storage.get_manifest_version(manifest_version_id)
    assert %Manifest{} = stored_version.manifest
    assert [%Favn.Manifest.Asset{ref: {MyApp.Asset, :asset}}] = stored_version.manifest.assets

    assert {:ok, listed_versions} = Storage.list_manifest_versions()

    assert Enum.any?(listed_versions, fn %Version{manifest: %Manifest{} = manifest} ->
             Enum.any?(
               manifest.assets,
               &match?(%Favn.Manifest.Asset{ref: {MyApp.Asset, :asset}}, &1)
             )
           end)

    run =
      RunState.new(
        id: "run_contract_#{label}_#{System.unique_integer([:positive])}",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {MyApp.Asset, :asset}
      )

    assert :ok = Storage.put_run(run)
    assert :ok = Storage.put_run(run)

    stale = %{run | event_seq: run.event_seq - 1} |> RunState.with_snapshot_hash()
    assert {:error, :stale_write} = Storage.put_run(stale)

    conflict = %{run | status: :error} |> RunState.with_snapshot_hash()
    assert {:error, :conflicting_snapshot} = Storage.put_run(conflict)

    assert {:ok, listed} = Storage.list_runs(status: :pending, limit: 10)
    assert Enum.any?(listed, &(&1.id == run.id))

    child =
      RunState.new(
        id: "run_contract_#{label}_child_#{System.unique_integer([:positive])}",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {MyApp.Asset, :asset},
        parent_run_id: run.id,
        root_run_id: run.id,
        lineage_depth: 1
      )

    assert :ok = Storage.put_run(child)
    assert {:ok, group_runs} = Storage.list_execution_group_runs(run.id)
    assert Enum.map(group_runs, & &1.id) == [run.id, child.id]
    assert {:ok, [run.id, child.id]} == Storage.list_execution_group_run_ids(run.id)
    assert {:ok, group_page} = Storage.list_execution_groups(search: run.id, limit: 10, offset: 0)
    assert run.id in group_page.items

    schedule =
      RunState.new(
        id: "run_contract_#{label}_schedule_#{System.unique_integer([:positive])}",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {MyApp.Asset, :asset},
        trigger: %{kind: :schedule}
      )

    backfill =
      RunState.new(
        id: "run_contract_#{label}_backfill_#{System.unique_integer([:positive])}",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {MyApp.Asset, :asset},
        submit_kind: :backfill_asset
      )

    retry =
      RunState.new(
        id: "run_contract_#{label}_retry_#{System.unique_integer([:positive])}",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {MyApp.Asset, :asset},
        submit_kind: :rerun
      )

    target_prefix =
      RunState.new(
        id: "run_contract_#{label}_target_prefix_#{System.unique_integer([:positive])}",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {MyApp.Asset, :asset_extra}
      )

    Enum.each([schedule, backfill, retry, target_prefix], &assert(:ok = Storage.put_run(&1)))

    assert {:ok, manual_page} = Storage.list_execution_groups(trigger_type: :manual, limit: 20)
    assert run.id in manual_page.items
    refute schedule.id in manual_page.items

    assert {:ok, schedule_page} =
             Storage.list_execution_groups(trigger_type: :schedule, limit: 20)

    assert schedule.id in schedule_page.items
    refute run.id in schedule_page.items

    assert {:ok, backfill_page} =
             Storage.list_execution_groups(trigger_type: :backfill, limit: 20)

    assert backfill.id in backfill_page.items

    assert {:ok, retry_page} = Storage.list_execution_groups(trigger_type: :retry, limit: 20)
    assert retry.id in retry_page.items

    assert {:ok, target_page} =
             Storage.list_execution_groups(target_asset: "MyApp.Asset.asset", limit: 20)

    assert run.id in target_page.items
    refute target_prefix.id in target_page.items

    event = %{
      sequence: 1,
      event_type: :run_started,
      occurred_at: DateTime.utc_now(),
      data: %{kind: label}
    }

    assert :ok = Storage.append_run_event(run.id, event)
    assert :ok = Storage.append_run_event(run.id, event)

    assert {:error, :conflicting_event_sequence} =
             Storage.append_run_event(run.id, %{sequence: 1, event_type: :run_updated})

    assert {:ok, [stored_event]} = Storage.list_run_events(run.id)
    assert stored_event.sequence == 1

    assert :ok =
             Storage.append_run_event(child.id, %{
               sequence: 1,
               event_type: :run_started,
               occurred_at: DateTime.utc_now(),
               data: %{kind: label}
             })

    next_event = %{
      sequence: 2,
      event_type: :run_updated,
      occurred_at: DateTime.utc_now(),
      data: %{kind: label}
    }

    assert :ok = Storage.append_run_event(child.id, next_event)
    assert {:ok, [cursor_event]} = Storage.list_run_events(child.id, after_sequence: 1, limit: 1)
    assert cursor_event.sequence == 2

    assert {:ok, group_events} = Storage.list_execution_group_events(run.id)
    assert Enum.map(group_events, & &1.run_id) == [run.id, child.id, child.id]
    [first_group_event | _] = group_events

    assert {:ok, [group_cursor_event]} =
             Storage.list_execution_group_events(run.id,
               after_global_sequence: first_group_event.global_sequence,
               limit: 1
             )

    assert group_cursor_event.run_id == child.id

    now = DateTime.utc_now()
    lease = execution_lease(run.id, "step-1", now, [%{kind: :run, key: run.id, limit: 1}])

    assert {:ok, ^lease} = Storage.try_acquire_execution_lease(lease)

    blocked = execution_lease(run.id, "step-2", now, [%{kind: :run, key: run.id, limit: 1}])

    assert {:error, {:execution_capacity_exceeded, %{kind: :run, key: _, limit: 1}}} =
             Storage.try_acquire_execution_lease(blocked)

    assert :ok = Storage.release_execution_lease(lease.lease_id)
    assert {:ok, ^blocked} = Storage.try_acquire_execution_lease(blocked)
    assert {:ok, [^blocked]} = Storage.list_execution_leases()
    assert {:ok, 1} = Storage.expire_execution_leases(DateTime.add(now, 6, :second))
    assert {:ok, []} = Storage.list_execution_leases()

    assert_materialization_claim_contract(label, run)

    concurrent_now = DateTime.utc_now()
    shared_scope = [%{kind: :pool, key: "shared_api", limit: 1}]
    first_concurrent = execution_lease(run.id, "step-3", concurrent_now, shared_scope)
    second_concurrent = execution_lease(run.id, "step-4", concurrent_now, shared_scope)

    concurrent_results =
      [first_concurrent, second_concurrent]
      |> Enum.map(&Task.async(fn -> Storage.try_acquire_execution_lease(&1) end))
      |> Enum.map(&Task.await(&1, 5_000))

    assert Enum.count(concurrent_results, &match?({:ok, _lease}, &1)) == 1

    assert Enum.count(
             concurrent_results,
             &match?({:error, {:execution_capacity_exceeded, %{kind: :pool}}}, &1)
           ) == 1

    concurrent_results
    |> Enum.each(fn
      {:ok, lease} -> assert :ok = Storage.release_execution_lease(lease.lease_id)
      _other -> :ok
    end)

    running = RunState.transition(run, status: :running)

    transition_event = %{
      schema_version: 1,
      sequence: running.event_seq,
      event_type: :run_started,
      entity: :run,
      occurred_at: DateTime.utc_now(),
      stage: 0,
      status: running.status,
      data: %{source: :contract}
    }

    assert :ok = Storage.persist_run_transition(running, transition_event)
    assert :idempotent = Storage.persist_run_transition(running, transition_event)

    assert {:ok, run_events} = Storage.list_run_events(run.id)
    persisted_transition = Enum.find(run_events, &(&1.sequence == running.event_seq))
    assert persisted_transition.schema_version == 1
    assert persisted_transition.entity == :run
    assert persisted_transition.stage == 0

    assert {:error, :conflicting_event_sequence} =
             Storage.persist_run_transition(running, %{transition_event | data: %{source: :other}})

    stale = %{run | event_seq: 1} |> RunState.with_snapshot_hash()

    assert {:error, :stale_write} =
             Storage.persist_run_transition(stale, %{
               sequence: 1,
               event_type: :run_started,
               occurred_at: DateTime.utc_now()
             })

    daily_key = {MyApp.Pipeline, :daily}
    hourly_key = {MyApp.Pipeline, :hourly}
    nil_key = {MyApp.Pipeline, nil}

    assert :ok =
             Storage.put_scheduler_state(daily_key, %{version: 1, last_due_at: DateTime.utc_now()})

    assert {:error, :stale_scheduler_state} =
             Storage.put_scheduler_state(daily_key, %{version: 1})

    assert :ok =
             Storage.put_scheduler_state(daily_key, %{version: 2, last_due_at: DateTime.utc_now()})

    assert :ok = Storage.put_scheduler_state(hourly_key, %{version: 1})

    assert {:ok, %Favn.Scheduler.State{schedule_id: :daily}} =
             Storage.get_scheduler_state(daily_key)

    assert {:ok, %Favn.Scheduler.State{schedule_id: :hourly}} =
             Storage.get_scheduler_state(hourly_key)

    assert {:ok, nil} = Storage.get_scheduler_state(nil_key)

    assert :ok = Storage.put_scheduler_state(nil_key, %{version: 1})
    assert {:ok, %Favn.Scheduler.State{schedule_id: nil}} = Storage.get_scheduler_state(nil_key)

    assert_freshness_lookup_contract(label, run)
    assert_backfill_progress_contract(label, run)
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

  defp execution_lease(run_id, asset_step_id, now, scopes) do
    %{
      lease_id: "lease_#{asset_step_id}",
      run_id: run_id,
      asset_step_id: asset_step_id,
      scopes: scopes,
      acquired_at: now,
      expires_at: DateTime.add(now, 5, :second)
    }
  end

  defp assert_freshness_lookup_contract(label, run) do
    now = DateTime.utc_now()
    key = {MyApp.Asset, :asset, "freshness-#{label}"}
    missing_key = {MyApp.Asset, :asset, "missing-#{label}"}
    unrelated_key = {MyApp.Asset, :other, "freshness-#{label}"}

    assert {:ok, wanted} =
             AssetFreshnessState.new(%{
               asset_ref_module: MyApp.Asset,
               asset_ref_name: :asset,
               freshness_key: "freshness-#{label}",
               status: :ok,
               freshness_version: "version-#{label}",
               latest_success_run_id: run.id,
               latest_success_at: now,
               updated_at: now
             })

    assert {:ok, unrelated} =
             AssetFreshnessState.new(%{
               asset_ref_module: MyApp.Asset,
               asset_ref_name: :other,
               freshness_key: "freshness-#{label}",
               status: :ok,
               freshness_version: "other-version-#{label}",
               latest_success_run_id: run.id,
               latest_success_at: now,
               updated_at: now
             })

    assert :ok = Storage.put_asset_freshness_state(wanted)
    assert :ok = Storage.put_asset_freshness_state(unrelated)

    assert {:ok, %{^key => fetched}} =
             Storage.get_asset_freshness_states_by_keys([key, key, missing_key])

    assert fetched.asset_ref_module == wanted.asset_ref_module
    assert fetched.asset_ref_name == wanted.asset_ref_name
    assert fetched.freshness_key == wanted.freshness_key
    assert fetched.freshness_version == wanted.freshness_version
    assert fetched.latest_success_run_id == wanted.latest_success_run_id

    assert {:ok, states} = Storage.get_asset_freshness_states_by_keys([key, missing_key])
    assert Map.keys(states) == [key]
    refute Map.has_key?(states, unrelated_key)
    assert {:ok, %{}} = Storage.get_asset_freshness_states_by_keys([])
  end

  defp assert_backfill_progress_contract(label, run) do
    now = DateTime.utc_now()
    backfill_run_id = "backfill_progress_#{label}_#{System.unique_integer([:positive])}"

    assert {:error, :not_found} = Storage.get_backfill_progress("#{backfill_run_id}_missing")

    assert {:ok, first} = backfill_window(backfill_run_id, run, "window-1", :pending, now)
    assert {:ok, second} = backfill_window(backfill_run_id, run, "window-2", :pending, now)

    assert :ok = Storage.put_backfill_window(first)
    assert :ok = Storage.put_backfill_window(second)

    assert {:ok, progress} = Storage.get_backfill_progress(backfill_run_id)
    assert progress.total_count == 2
    assert progress.pending_count == 2
    assert progress.status == :running

    assert {:ok, progress} = Storage.rebuild_backfill_progress(backfill_run_id)
    assert progress.total_count == 2
    assert progress.pending_count == 2
    assert progress.status == :running

    assert_concurrent_same_window_projection(label, run)

    ok_window = %{
      first
      | status: :ok,
        child_run_id: "#{backfill_run_id}_child_1",
        latest_attempt_run_id: "#{backfill_run_id}_child_1",
        last_success_run_id: "#{backfill_run_id}_child_1",
        finished_at: DateTime.add(now, 1, :second),
        updated_at: DateTime.add(now, 1, :second)
    }

    assert {:ok, asset_state} = asset_window_state(ok_window, run, :ok)
    assert {:ok, progress} = Storage.apply_backfill_child_projection(ok_window, [asset_state])
    assert progress.total_count == 2
    assert progress.pending_count == 1
    assert progress.ok_count == 1
    assert progress.status == :running

    assert {:ok, ^asset_state} =
             Storage.get_asset_window_state(MyApp.Asset, :asset, ok_window.window_key)

    assert {:ok, repeated} = Storage.apply_backfill_child_projection(ok_window, [asset_state])
    assert repeated.total_count == 2
    assert repeated.pending_count == 1
    assert repeated.ok_count == 1

    error_window = %{
      second
      | status: :error,
        child_run_id: "#{backfill_run_id}_child_2",
        latest_attempt_run_id: "#{backfill_run_id}_child_2",
        last_error: %{message: "failed"},
        errors: [%{message: "failed"}],
        finished_at: DateTime.add(now, 2, :second),
        updated_at: DateTime.add(now, 2, :second)
    }

    assert {:ok, progress} = Storage.apply_backfill_child_projection(error_window, [])
    assert progress.total_count == 2
    assert progress.pending_count == 0
    assert progress.ok_count == 1
    assert progress.error_count == 1
    assert progress.status == :partial

    assert {:ok, stored_progress} = Storage.get_backfill_progress(backfill_run_id)
    assert stored_progress.status == :partial
    assert stored_progress.ok_count == 1
    assert stored_progress.error_count == 1
  end

  defp assert_concurrent_same_window_projection(label, run) do
    now = DateTime.utc_now()
    backfill_run_id = "backfill_race_#{label}_#{System.unique_integer([:positive])}"

    assert {:ok, first} = backfill_window(backfill_run_id, run, "race-window", :pending, now)
    assert {:ok, second} = backfill_window(backfill_run_id, run, "other-window", :pending, now)
    assert :ok = Storage.put_backfill_window(first)
    assert :ok = Storage.put_backfill_window(second)
    assert {:ok, _progress} = Storage.rebuild_backfill_progress(backfill_run_id)

    ok_window = %{
      first
      | status: :ok,
        child_run_id: "#{backfill_run_id}_ok",
        latest_attempt_run_id: "#{backfill_run_id}_ok",
        last_success_run_id: "#{backfill_run_id}_ok",
        finished_at: DateTime.add(now, 1, :second),
        updated_at: DateTime.add(now, 1, :second)
    }

    error_window = %{
      first
      | status: :error,
        child_run_id: "#{backfill_run_id}_error",
        latest_attempt_run_id: "#{backfill_run_id}_error",
        last_error: %{message: "failed"},
        errors: [%{message: "failed"}],
        finished_at: DateTime.add(now, 2, :second),
        updated_at: DateTime.add(now, 2, :second)
    }

    [ok_window, error_window]
    |> Enum.map(&Task.async(fn -> Storage.apply_backfill_child_projection(&1, []) end))
    |> Enum.each(fn task -> assert {:ok, _progress} = Task.await(task, 5_000) end)

    assert {:ok, progress} = Storage.get_backfill_progress(backfill_run_id)
    assert progress.total_count == 2
    assert progress.pending_count == 1
    assert progress.ok_count + progress.error_count == 1
    assert progress.status == :running
  end

  defp backfill_window(backfill_run_id, run, window_key, status, now) do
    BackfillWindow.new(%{
      backfill_run_id: backfill_run_id,
      pipeline_module: MyApp.Pipeline,
      manifest_version_id: run.manifest_version_id,
      window_kind: :day,
      window_start_at: now,
      window_end_at: DateTime.add(now, 86_400, :second),
      timezone: "Etc/UTC",
      window_key: window_key,
      status: status,
      attempt_count: 0,
      created_at: now,
      updated_at: now
    })
  end

  defp asset_window_state(window, run, status) do
    AssetWindowState.new(%{
      asset_ref_module: MyApp.Asset,
      asset_ref_name: :asset,
      pipeline_module: window.pipeline_module,
      manifest_version_id: run.manifest_version_id,
      window_kind: window.window_kind,
      window_start_at: window.window_start_at,
      window_end_at: window.window_end_at,
      timezone: window.timezone,
      window_key: window.window_key,
      status: status,
      latest_run_id: window.latest_attempt_run_id,
      latest_parent_run_id: window.backfill_run_id,
      latest_success_run_id: if(status == :ok, do: window.latest_attempt_run_id),
      updated_at: window.updated_at
    })
  end

  defp assert_materialization_claim_contract(label, run) do
    now = DateTime.utc_now()
    claim = materialization_claim(label, run, "claim-1", now)

    assert {:ok, ^claim} = Storage.try_acquire_materialization_claim(claim)
    assert {:already_claimed, ^claim} = Storage.try_acquire_materialization_claim(claim)
    assert {:ok, ^claim} = Storage.get_materialization_claim(claim.claim_key)

    assert {:ok, [^claim]} =
             Storage.list_materialization_claims(
               asset_ref_module: MyApp.Asset,
               asset_ref_name: :asset,
               freshness_key: "freshness-#{label}",
               status: :claimed
             )

    assert {:ok, completed} =
             Storage.complete_materialization_claim(claim.claim_key, %{
               freshness_version: "freshness-version-1",
               finished_at: DateTime.add(now, 1, :second),
               metadata: %{"rows_written" => 10}
             })

    assert completed.status == :succeeded
    assert completed.freshness_version == "freshness-version-1"
    assert completed.metadata == %{"rows_written" => 10}

    assert {:error, :not_found} =
             Storage.fail_materialization_claim(claim.claim_key, %{status: :failed})

    assert {:already_succeeded, ^completed} = Storage.try_acquire_materialization_claim(claim)

    expired_claim = materialization_claim(label, run, "claim-2", DateTime.add(now, -10, :second))
    reclaim = materialization_claim(label, run, "claim-2", DateTime.add(now, 10, :second))

    assert {:ok, ^expired_claim} = Storage.try_acquire_materialization_claim(expired_claim)
    assert {:ok, 1} = Storage.expire_materialization_claims(now)
    assert {:ok, expired} = Storage.get_materialization_claim(expired_claim.claim_key)
    assert expired.status == :expired
    assert {:error, :not_found} = Storage.complete_materialization_claim(expired.claim_key, %{})
    assert {:ok, ^reclaim} = Storage.try_acquire_materialization_claim(reclaim)

    failed_claim = materialization_claim(label, run, "claim-3", now)
    assert {:ok, ^failed_claim} = Storage.try_acquire_materialization_claim(failed_claim)

    assert {:ok, failed} =
             Storage.fail_materialization_claim(failed_claim.claim_key, %{
               status: :timed_out,
               error: %{message: "timeout"},
               finished_at: DateTime.add(now, 2, :second)
             })

    assert failed.status == :timed_out
    assert failed.error == %{message: "timeout"}
    assert {:ok, ^failed_claim} = Storage.try_acquire_materialization_claim(failed_claim)
  end

  defp materialization_claim(label, run, suffix, now) do
    %FavnOrchestrator.MaterializationClaim{
      claim_key: "#{run.id}:#{suffix}",
      asset_ref_module: MyApp.Asset,
      asset_ref_name: :asset,
      freshness_key: "freshness-#{label}",
      input_fingerprint: "input-#{suffix}",
      run_id: run.id,
      asset_step_id: "asset-step-#{suffix}",
      node_key: "node-#{suffix}",
      runner_execution_id: "runner-#{suffix}",
      manifest_version_id: run.manifest_version_id,
      manifest_content_hash: run.manifest_content_hash,
      status: :claimed,
      claimed_at: now,
      heartbeat_at: now,
      expires_at: DateTime.add(now, 5, :second),
      metadata: %{"label" => label}
    }
  end

  defp postgres_opts do
    case System.get_env("FAVN_POSTGRES_TEST_URL") do
      url when is_binary(url) and url != "" ->
        repo_config = repo_config_from_url(url)

        if valid_repo_config?(repo_config) do
          [
            repo_mode: :managed,
            repo_config: Keyword.merge(repo_config, pool_size: 1),
            migration_mode: :auto,
            supervisor_name: @postgres_supervisor
          ]
        else
          nil
        end

      _ ->
        nil
    end
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
      username: user_from_userinfo(uri.userinfo),
      password: password_from_userinfo(uri.userinfo),
      ssl: false,
      show_sensitive_data_on_connection_error: true
    ]
  end

  defp valid_repo_config?(repo_config) do
    Enum.all?([:hostname, :database, :username, :password], fn key ->
      value = Keyword.get(repo_config, key)
      is_binary(value) and value != ""
    end)
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

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
