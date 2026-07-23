defmodule Favn.Local.ComposeSessionFixture do
  @moduledoc false

  alias Favn.Dev.{ComposeProject, State}

  @spec put!(Path.t(), String.t()) :: :ok
  def put!(root_dir, orchestrator_url) do
    project_name = ComposeProject.project_name(root_dir)

    :ok =
      State.write_secrets(
        %{"service_token" => "local-compose-session-test-token"},
        root_dir: root_dir
      )

    State.write_runtime(
      %{
        "schema_version" => 6,
        "kind" => "docker_compose",
        "compose_contract_version" => 1,
        "compose_profile" => "local",
        "compose_file" => Path.join(root_dir, "deploy/local/compose.yml"),
        "compose_project" => project_name,
        "compose_services" => %{
          "postgres" => "postgres",
          "control-plane-ops" => "control-plane-ops",
          "control-plane-verify" => "control-plane-verify",
          "runner" => "runner",
          "control-plane" => "control-plane"
        },
        "workspace_id" => "local-dev",
        "view_url" => "http://127.0.0.1:4173",
        "orchestrator_url" => orchestrator_url,
        "control_plane_image_reference" =>
          "ghcr.io/eirhop/favn-control-plane@sha256:#{String.duplicate("a", 64)}",
        "runner_image_reference" => "favn-local-runner-#{project_name}:rr_fixture"
      },
      root_dir: root_dir
    )
  end
end
