defmodule Favn.Local.ComposeSessionFixture do
  @moduledoc false

  alias Favn.Dev.{ComposeProject, State}

  @spec put!(Path.t(), String.t()) :: :ok
  def put!(root_dir, orchestrator_url) do
    project_name = ComposeProject.project_name(root_dir)

    :ok =
      State.write_install(
        %{
          "compose" => %{
            "project_name" => project_name,
            "orchestrator_url" => orchestrator_url,
            "workspace_id" => "local-dev"
          }
        },
        root_dir: root_dir
      )

    :ok =
      State.write_secrets(
        %{"service_token" => "local-compose-session-test-token"},
        root_dir: root_dir
      )

    State.write_runtime(
      %{
        "schema_version" => 5,
        "kind" => "docker_compose",
        "compose_project" => project_name
      },
      root_dir: root_dir
    )
  end
end
