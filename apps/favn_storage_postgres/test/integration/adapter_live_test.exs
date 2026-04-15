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
            repo_config: Keyword.merge(repo_config, pool_size: 1),
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
end
