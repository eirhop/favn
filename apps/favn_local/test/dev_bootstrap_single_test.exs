defmodule Favn.Dev.Bootstrap.SingleTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.Bootstrap.Single

  defmodule FakeClient do
    def verify_service_token(url, token) do
      send(test_pid(), {:verify_service_token, url, token})
      :ok
    end

    def publish_manifest(url, token, payload) do
      send(test_pid(), {:publish_manifest, url, token, payload})
      {:ok, %{"data" => %{"manifest" => %{}, "registration" => %{"status" => "accepted"}}}}
    end

    def activate_manifest(url, token, manifest_version_id) do
      send(test_pid(), {:activate_manifest, url, token, manifest_version_id})
      {:ok, %{"data" => %{"activated" => true}}}
    end

    def register_runner(url, token, payload) do
      send(test_pid(), {:register_runner, url, token, payload})
      {:ok, %{"data" => %{"registration" => %{"status" => "accepted"}}}}
    end

    def bootstrap_active_manifest(url, token) do
      send(test_pid(), {:bootstrap_active_manifest, url, token})
      {:ok, %{"manifest" => %{"manifest_version_id" => active_manifest_id()}}}
    end

    defp test_pid do
      Application.fetch_env!(:favn_local, :bootstrap_single_test_pid)
    end

    defp active_manifest_id do
      Application.fetch_env!(:favn_local, :bootstrap_single_active_manifest_id)
    end
  end

  defmodule RunnerConflictClient do
    def verify_service_token(_url, _token), do: :ok
    def publish_manifest(_url, _token, _payload), do: {:ok, %{}}
    def activate_manifest(_url, _token, _manifest_version_id), do: {:ok, %{}}
    def bootstrap_active_manifest(_url, _token), do: {:ok, %{}}

    def register_runner(_url, _token, _payload) do
      {:error, %{operation: :register_runner, reason: {:http_error, 409, %{}}}}
    end
  end

  setup do
    Application.put_env(:favn_local, :bootstrap_single_test_pid, self())

    root_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_bootstrap_single_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root_dir)

    on_exit(fn ->
      Application.delete_env(:favn_local, :bootstrap_single_test_pid)
      Application.delete_env(:favn_local, :bootstrap_single_active_manifest_id)
      File.rm_rf(root_dir)
    end)

    %{manifest_path: write_manifest(root_dir)}
  end

  test "run/1 verifies token, registers manifest, activates, registers runner, and summarizes", %{
    manifest_path: manifest_path
  } do
    {:ok, version} = Single.read_manifest_version(manifest_path)

    Application.put_env(
      :favn_local,
      :bootstrap_single_active_manifest_id,
      version.manifest_version_id
    )

    opts = [
      manifest_path: manifest_path,
      orchestrator_url: "http://127.0.0.1:4000",
      service_token: "token-1",
      client: FakeClient
    ]

    assert {:ok, summary} = Single.run(opts)

    assert summary.activated? == true
    assert summary.manifest_registration == "accepted"
    assert summary.runner_registration == "accepted"
    assert summary.active_manifest_verification == :matched

    assert_receive {:verify_service_token, "http://127.0.0.1:4000", "token-1"}
    assert_receive {:publish_manifest, _url, _token, manifest_payload}
    assert_receive {:activate_manifest, _url, _token, manifest_version_id}
    assert_receive {:register_runner, _url, _token, runner_payload}
    assert_receive {:bootstrap_active_manifest, _url, _token}

    assert manifest_payload.manifest_version_id == summary.manifest_version_id
    assert manifest_payload.content_hash == summary.content_hash
    assert manifest_version_id == summary.manifest_version_id
    assert runner_payload.manifest_version_id == summary.manifest_version_id
  end

  test "raw manifest input produces a stable manifest version for repeatable re-runs", %{
    manifest_path: manifest_path
  } do
    assert {:ok, first} = Single.read_manifest_version(manifest_path)
    assert {:ok, second} = Single.read_manifest_version(manifest_path)

    assert first.manifest_version_id == second.manifest_version_id
    assert first.content_hash == second.content_hash
  end

  test "runner registration conflict fails clearly", %{manifest_path: manifest_path} do
    assert {:error, %{operation: :register_runner, reason: {:http_error, 409, %{}}}} =
             Single.run(
               manifest_path: manifest_path,
               orchestrator_url: "http://127.0.0.1:4000",
               service_token: "token-1",
               client: RunnerConflictClient
              )
  end

  test "missing manifest file fails with structured read error", %{manifest_path: manifest_path} do
    missing_path = manifest_path <> ".missing"

    assert {:error, {:manifest_read_failed, ^missing_path, :enoent}} =
             Single.read_manifest_version(missing_path)
  end

  defp write_manifest(root_dir) do
    path = Path.join(root_dir, "manifest.json")

    File.write!(
      path,
      JSON.encode_to_iodata!(%{
        schema_version: 1,
        runner_contract_version: 1,
        assets: [],
        pipelines: [],
        schedules: [],
        graph: %{},
        metadata: %{}
      })
    )

    path
  end
end
