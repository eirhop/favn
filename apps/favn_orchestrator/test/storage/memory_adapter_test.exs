defmodule FavnOrchestrator.Storage.MemoryAdapterTest do
  use ExUnit.Case, async: false

  alias Favn.Log.Entry
  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  setup do
    Memory.reset()
    :ok
  end

  test "stores manifest versions idempotently and supports activation" do
    version = manifest_version("mv_a")

    assert :ok = Storage.put_manifest_version(version)
    assert :ok = Storage.put_manifest_version(version)

    assert {:ok, stored} = Storage.get_manifest_version("mv_a")
    assert stored.content_hash == version.content_hash

    assert :ok = Storage.set_active_manifest_version("mv_a")
    assert {:ok, "mv_a"} = Storage.get_active_manifest_version()
  end

  test "rejects run snapshot stale and conflicting writes" do
    base =
      RunState.new(
        id: "run_1",
        manifest_version_id: "mv_a",
        manifest_content_hash: "hash",
        asset_ref: {MyApp.Asset, :asset}
      )

    assert :ok = Storage.put_run(base)
    assert :ok = Storage.put_run(base)

    stale = %{base | event_seq: 0} |> RunState.with_snapshot_hash()
    assert {:error, :stale_write} = Storage.put_run(stale)

    conflict = %{base | status: :error} |> RunState.with_snapshot_hash()
    assert {:error, :conflicting_snapshot} = Storage.put_run(conflict)
  end

  test "accepts higher-seq writes and returns latest stored run" do
    base =
      RunState.new(
        id: "run_2",
        manifest_version_id: "mv_a",
        manifest_content_hash: "hash",
        asset_ref: {MyApp.Asset, :asset}
      )

    assert :ok = Storage.put_run(base)

    newer =
      %{base | event_seq: base.event_seq + 1, status: :running} |> RunState.with_snapshot_hash()

    assert :ok = Storage.put_run(newer)

    assert {:ok, stored} = Storage.get_run(base.id)
    assert stored.event_seq == 2
  end

  test "normalizes and validates run events" do
    event = %{sequence: 1, event_type: :run_started, occurred_at: DateTime.utc_now()}

    assert :ok = Storage.append_run_event("run_1", event)
    assert :ok = Storage.append_run_event("run_1", event)

    assert {:ok, [stored]} = Storage.list_run_events("run_1")
    assert stored.run_id == "run_1"
    assert stored.sequence == 1

    assert {:error, {:invalid_run_event_field, :sequence, 0}} =
             Storage.append_run_event("run_1", %{sequence: 0, event_type: :run_started})

    assert {:error, :conflicting_event_sequence} =
             Storage.append_run_event("run_1", %{sequence: 1, event_type: :run_updated})

    assert {:error, :invalid_opts} = Storage.list_run_events("run_1", limit: 0)
    assert {:error, :invalid_opts} = Storage.list_run_events("run_1", after_sequence: "1")
    assert {:error, :invalid_opts} = Storage.list_run_events("run_1", order: :sideways)
  end

  test "validates scheduler state payload" do
    key = {MyApp.Pipeline, :daily}
    now = DateTime.utc_now()

    assert :ok = Storage.put_scheduler_state(key, %{last_due_at: now, version: 1})
    assert {:ok, stored} = Storage.get_scheduler_state(key)
    assert stored.last_due_at == now

    assert {:error, {:invalid_scheduler_field, :last_due_at, "bad"}} =
             Storage.put_scheduler_state(key, %{last_due_at: "bad", version: 2})
  end

  test "stores execution group summaries as a paged read model" do
    parent =
      RunState.new(
        id: "group_parent",
        manifest_version_id: "mv_a",
        manifest_content_hash: "hash",
        asset_ref: {MyApp.Asset, :asset},
        target_refs: [{MyApp.Asset, :asset}]
      )

    child =
      RunState.new(
        id: "group_child",
        manifest_version_id: "mv_a",
        manifest_content_hash: "hash",
        asset_ref: {MyApp.Asset, :asset},
        target_refs: [{MyApp.Asset, :asset}],
        parent_run_id: parent.id,
        root_run_id: parent.id,
        lineage_depth: 1
      )

    assert :ok = Storage.put_run(parent)
    assert :ok = Storage.put_run(child)

    assert {:ok, page} = Storage.list_execution_group_summaries(limit: 10)
    assert [summary] = page.items
    assert summary.id == parent.id
    assert summary.child_run_ids == [child.id]
    assert summary.target_assets == ["MyApp.Asset.asset"]
  end

  test "scans logs with cursor pagination" do
    now = DateTime.utc_now()

    entries = [
      Entry.normalize(%{
        run_id: "scan_logs_run",
        runner_execution_id: "runner_a",
        producer_id: "memory-scan",
        producer_sequence: 1,
        occurred_at: now,
        stream: :stdout,
        message: "first"
      }),
      Entry.normalize(%{
        run_id: "scan_logs_run",
        runner_execution_id: "runner_a",
        producer_id: "memory-scan",
        producer_sequence: 2,
        occurred_at: DateTime.add(now, 1, :second),
        stream: :stderr,
        message: "second"
      })
    ]

    assert {:ok, [_first, _second]} = Storage.persist_log_entries(entries)

    assert {:ok, page} =
             Storage.scan_logs([runner_execution_id: "runner_a", stream: :stdout], limit: 1)

    assert Enum.map(page.items, & &1.message) == ["first"]
    refute page.has_more?

    assert {:ok, next_page} =
             Storage.scan_logs([runner_execution_id: "runner_a"],
               after: %{global_sequence: 1},
               limit: 10
             )

    assert Enum.map(next_page.items, & &1.message) == ["second"]

    assert {:ok, level_page} = Storage.list_logs(%{"level" => "info"}, limit: 10)
    assert Enum.map(level_page.items, & &1.message) == ["first", "second"]

    assert {:ok, descending_page} = Storage.list_logs(%{}, order: :desc, limit: 10)
    assert Enum.map(descending_page.items, & &1.message) == ["second", "first"]

    assert {:error, {:unsupported_filter, "unknown"}} =
             Storage.list_logs(%{"unknown" => true}, limit: 10)

    assert {:error, :invalid_log_filter} = Storage.list_logs(:invalid, limit: 10)
    assert {:error, :invalid_log_filter} = Storage.list_logs([:invalid], limit: 10)
  end

  test "uses nil scheduler schedule ids as exact keys" do
    daily_key = {MyApp.Pipeline, :daily}
    hourly_key = {MyApp.Pipeline, :hourly}
    nil_key = {MyApp.Pipeline, nil}

    assert :ok = Storage.put_scheduler_state(daily_key, %{version: 1})
    assert :ok = Storage.put_scheduler_state(hourly_key, %{version: 1})

    assert {:ok, %Favn.Scheduler.State{schedule_id: :daily}} =
             Storage.get_scheduler_state(daily_key)

    assert {:ok, %Favn.Scheduler.State{schedule_id: :hourly}} =
             Storage.get_scheduler_state(hourly_key)

    assert {:ok, nil} = Storage.get_scheduler_state(nil_key)

    assert :ok = Storage.put_scheduler_state(nil_key, %{version: 1})
    assert {:ok, %Favn.Scheduler.State{schedule_id: nil}} = Storage.get_scheduler_state(nil_key)
  end

  test "keeps authentication secondary indexes consistent on updates" do
    now = DateTime.utc_now()
    actor = auth_actor("actor-1", "first", now)

    assert :ok = Storage.put_auth_actor(actor)
    assert :ok = Storage.put_auth_actor(%{actor | username: "renamed"})
    assert {:error, :not_found} = Storage.get_auth_actor_by_username("first")
    assert {:ok, %{id: "actor-1"}} = Storage.get_auth_actor_by_username("renamed")

    assert {:error, :username_taken} =
             Storage.put_auth_actor(auth_actor("actor-2", "renamed", now))

    session = auth_session("session-1", "hash-1", actor.id, now)
    assert :ok = Storage.put_auth_session(session)
    assert :ok = Storage.put_auth_session(%{session | token_hash: "hash-2"})
    assert {:error, :not_found} = Storage.get_auth_session_by_token_hash("hash-1")
    assert {:ok, %{id: "session-1"}} = Storage.get_auth_session_by_token_hash("hash-2")

    assert {:error, :session_token_taken} =
             Storage.put_auth_session(auth_session("session-2", "hash-2", actor.id, now))
  end

  test "lists authentication audits newest first like database adapters" do
    assert :ok = Storage.put_auth_audit(%{id: "audit-1"})
    assert :ok = Storage.put_auth_audit(%{id: "audit-2"})
    assert {:ok, [%{id: "audit-2"}, %{id: "audit-1"}]} = Storage.list_auth_audit(limit: 10)
  end

  test "rejects malformed idempotency maps without crashing storage" do
    record = %{
      id: "idem-1",
      request_fingerprint: "fingerprint",
      status: :in_progress,
      expires_at: DateTime.add(DateTime.utc_now(), 60, :second)
    }

    assert {:error, {:invalid_idempotency_record_key, "unexpected"}} =
             Storage.reserve_idempotency_record(Map.put(record, "unexpected", true))

    assert {:error, {:invalid_idempotency_status, "unknown"}} =
             Storage.reserve_idempotency_record(%{record | status: "unknown"})

    assert {:ok, {:reserved, %{id: "idem-1"}}} = Storage.reserve_idempotency_record(record)
  end

  defp manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Favn.Manifest.Asset{ref: {MyApp.Asset, :asset}, module: MyApp.Asset, name: :asset}
      ]
    }

    {:ok, version} =
      Version.new(FavnTestSupport.with_manifest_graph(manifest),
        manifest_version_id: manifest_version_id
      )

    version
  end

  defp auth_actor(id, username, now) do
    %{
      id: id,
      username: username,
      display_name: username,
      roles: [:viewer],
      status: :active,
      inserted_at: now,
      updated_at: now
    }
  end

  defp auth_session(id, token_hash, actor_id, now) do
    %{
      id: id,
      token_hash: token_hash,
      actor_id: actor_id,
      provider: "password",
      issued_at: now,
      expires_at: DateTime.add(now, 60, :second),
      revoked_at: nil
    }
  end
end
