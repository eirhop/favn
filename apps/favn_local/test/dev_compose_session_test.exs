defmodule Favn.Dev.ComposeSessionTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.{ComposeProject, ComposeSession, State}
  alias Favn.Local.ComposeSessionFixture

  setup do
    root_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_compose_session_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root_dir)
    on_exit(fn -> File.rm_rf(root_dir) end)

    %{root_dir: root_dir}
  end

  test "resolves the authenticated local API session from canonical Compose state", context do
    url = "http://127.0.0.1:4101"
    assert :ok = ComposeSessionFixture.put!(context.root_dir, url)

    assert {:ok, ^url, %{service_token: "local-compose-session-test-token"}, session_context} =
             ComposeSession.resolve(root_dir: context.root_dir)

    assert session_context == %{
             "actor_id" => "local-dev-cli",
             "session_id" => "local-dev-cli",
             "local_dev_context" => "trusted",
             "workspace_id" => "local-dev"
           }
  end

  test "rejects runtime state from the removed host-process topology", context do
    assert :ok =
             State.write_runtime(
               %{"schema_version" => 4, "kind" => "host_process"},
               root_dir: context.root_dir
             )

    assert {:error, :stack_not_running} =
             ComposeSession.resolve(root_dir: context.root_dir)
  end

  test "rejects install state for a different Compose project", context do
    project_name = ComposeProject.project_name(context.root_dir)

    assert :ok =
             State.write_runtime(
               %{
                 "schema_version" => 5,
                 "kind" => "docker_compose",
                 "compose_project" => project_name
               },
               root_dir: context.root_dir
             )

    assert :ok =
             State.write_install(
               %{
                 "compose" => %{
                   "project_name" => "another-project",
                   "orchestrator_url" => "http://127.0.0.1:4101",
                   "workspace_id" => "local-dev"
                 }
               },
               root_dir: context.root_dir
             )

    assert {:error, :install_stale} = ComposeSession.resolve(root_dir: context.root_dir)
  end
end
