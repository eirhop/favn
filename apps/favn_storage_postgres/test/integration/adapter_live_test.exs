defmodule FavnStoragePostgres.Integration.AdapterLiveTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator.RunState
  alias FavnStoragePostgres.Adapter

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

        assert :ok =
                 Adapter.append_run_event(run.id, %{sequence: 1, event_type: :run_started}, opts)

        assert {:ok, [event]} = Adapter.list_run_events(run.id, opts)
        assert event.sequence == 1

        key = {MyApp.Pipeline, :daily}
        assert :ok = Adapter.put_scheduler_state(key, %{version: 1}, opts)

        assert {:ok, %Favn.Scheduler.State{schedule_id: :daily}} =
                 Adapter.get_scheduler_state(key, opts)
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
