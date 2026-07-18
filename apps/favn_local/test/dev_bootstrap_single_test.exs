defmodule Favn.Dev.Bootstrap.SingleTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.Bootstrap.Single
  alias Favn.Manifest.Publication

  defmodule FakeClient do
    def verify_service_token(url, token) do
      send(test_pid(), {:verify_service_token, url, token})
      :ok
    end

    def publish_manifest(url, token, payload, session_context) do
      send(test_pid(), {:publish_manifest, url, token, payload, session_context})
      {:ok, %{"data" => %{"manifest" => %{}, "registration" => %{"status" => "accepted"}}}}
    end

    def password_login(url, token, workspace_id, username, password) do
      send(test_pid(), {:password_login, url, token, workspace_id, username, password})
      {:ok, %{"actor_id" => "act_1", "session_id" => "ses_1", "session_token" => "raw_1"}}
    end

    def activate_manifest(url, token, manifest_version_id, session_context) do
      send(test_pid(), {:activate_manifest, url, token, manifest_version_id, session_context})
      {:ok, %{"data" => %{"activated" => true}}}
    end

    def register_runner(url, token, session_context, payload) do
      send(test_pid(), {:register_runner, url, token, session_context, payload})
      {:ok, %{"data" => %{"registration" => %{"status" => "accepted"}}}}
    end

    def bootstrap_active_manifest(url, token, session_context) do
      send(test_pid(), {:bootstrap_active_manifest, url, token, session_context})
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
    def publish_manifest(_url, _token, _payload, _session_context), do: {:ok, %{}}
    def password_login(_url, _token, _workspace_id, _username, _password), do: {:ok, %{}}
    def activate_manifest(_url, _token, _manifest_version_id, _session_context), do: {:ok, %{}}
    def bootstrap_active_manifest(_url, _token, _session_context), do: {:ok, %{}}

    def register_runner(_url, _token, _session_context, _payload) do
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
      workspace_id: "workspace-1",
      operator_username: "admin",
      operator_password: "admin-password-long",
      client: FakeClient
    ]

    assert {:ok, summary} = Single.run(opts)

    assert summary.activated? == true
    assert summary.manifest_registration == "accepted"
    assert summary.runner_registration == "accepted"
    assert summary.active_manifest_verification == :matched

    assert_receive {:verify_service_token, "http://127.0.0.1:4000", "token-1"}
    assert_receive {:password_login, _url, _token, "workspace-1", "admin", "admin-password-long"}
    assert_receive {:publish_manifest, _url, _token, %Publication{} = publication, session_context}
    assert_receive {:activate_manifest, _url, _token, manifest_version_id, session_context}
    assert_receive {:register_runner, _url, _token, ^session_context, runner_payload}
    assert_receive {:bootstrap_active_manifest, _url, _token, ^session_context}

    assert session_context["actor_id"] == "act_1"
    assert session_context["session_token"] == "raw_1"
    assert publication.version.manifest_version_id == summary.manifest_version_id
    assert publication.version.content_hash == summary.content_hash
    assert publication.execution_packages == []
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
               workspace_id: "workspace-1",
               operator_username: "admin",
               operator_password: "admin-password-long",
               client: RunnerConflictClient
             )
  end

  test "missing manifest file fails with structured read error", %{manifest_path: manifest_path} do
    missing_path = manifest_path <> ".missing"

    assert {:error, {:manifest_read_failed, ^missing_path, :enoent}} =
             Single.read_manifest_version(missing_path)
  end

  defp write_manifest(root_dir) do
    path = Path.join(root_dir, "manifest-index.json")

    File.write!(
      path,
      JSON.encode_to_iodata!(%{
        schema_version: 8,
        runner_contract_version: 8,
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
