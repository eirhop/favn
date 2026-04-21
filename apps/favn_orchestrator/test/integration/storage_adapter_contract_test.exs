defmodule FavnOrchestrator.Integration.StorageAdapterContractTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Version
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

  test "shared contract holds for sqlite adapter" do
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

    event = %{
      sequence: 1,
      event_type: :run_started,
      occurred_at: DateTime.utc_now(),
      data: %{kind: label}
    }

    assert :ok = Storage.append_run_event(run.id, event)

    assert {:error, :conflicting_event_sequence} =
             Storage.append_run_event(run.id, %{sequence: 1, event_type: :run_updated})

    assert {:ok, [stored_event]} = Storage.list_run_events(run.id)
    assert stored_event.sequence == 1

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

    key = {MyApp.Pipeline, :daily}
    assert :ok = Storage.put_scheduler_state(key, %{version: 1, last_due_at: DateTime.utc_now()})
    assert {:error, :stale_scheduler_state} = Storage.put_scheduler_state(key, %{version: 1})
    assert :ok = Storage.put_scheduler_state(key, %{version: 2, last_due_at: DateTime.utc_now()})
    assert {:ok, %Favn.Scheduler.State{schedule_id: :daily}} = Storage.get_scheduler_state(key)
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
