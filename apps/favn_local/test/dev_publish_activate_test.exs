defmodule Favn.Dev.PublishActivateTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.{Activate, Publish}
  alias Favn.Dev.Build.Manifest, as: ManifestBuild
  alias Favn.Manifest.{Publication, Version}

  defmodule Client do
    def publish_manifest(url, token, publication, nil) do
      send(self(), {:published, url, token, publication.version.manifest_version_id})

      {:ok,
       %{
         "data" => %{
           "manifest" => %{
             "required_runner_release_id" => publication.version.required_runner_release_id
           },
           "registration" => %{
             "status" => "published",
             "manifest_version_id" => publication.version.manifest_version_id,
             "canonical_manifest_version_id" => publication.version.manifest_version_id
           }
         }
       }}
    end

    def activate_manifest_service(url, token, manifest_version_id, workspace_id) do
      send(self(), {:activated, url, token, manifest_version_id, workspace_id})

      {:ok,
       %{
         "data" => %{
           "activated" => true,
           "manifest_version_id" => manifest_version_id,
           "deployment_id" => "deployment:test",
           "required_runner_release_id" => FavnTestSupport.runner_release_id()
         }
       }}
    end
  end

  defmodule InvalidClient do
    def publish_manifest(_url, _token, _publication, nil), do: {:ok, %{"data" => %{}}}

    def activate_manifest_service(_url, _token, _manifest_version_id, _workspace_id),
      do: {:ok, %{"data" => %{"activated" => false}}}
  end

  defmodule AlreadyPublishedClient do
    def publish_manifest(_url, _token, publication, nil) do
      {:ok,
       %{
         "data" => %{
           "manifest" => %{
             "required_runner_release_id" => publication.version.required_runner_release_id
           },
           "registration" => %{
             "status" => "already_published",
             "manifest_version_id" => publication.version.manifest_version_id,
             "canonical_manifest_version_id" => "mv_canonical"
           }
         }
       }}
    end
  end

  defmodule MismatchClient do
    def publish_manifest(_url, _token, publication, nil) do
      {:ok,
       %{
         "data" => %{
           "manifest" => %{
             "required_runner_release_id" => publication.version.required_runner_release_id
           },
           "registration" => %{
             "status" => "published",
             "manifest_version_id" => "mv_wrong",
             "canonical_manifest_version_id" => "mv_wrong"
           }
         }
       }}
    end

    def activate_manifest_service(_url, _token, _manifest_version_id, _workspace_id) do
      {:ok,
       %{
         "data" => %{
           "activated" => true,
           "manifest_version_id" => "mv_wrong",
           "deployment_id" => "deployment:wrong",
           "required_runner_release_id" => FavnTestSupport.runner_release_id()
         }
       }}
    end
  end

  setup do
    root = Path.join("/tmp", "favn_publish_test_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(root) end)

    manifest =
      FavnTestSupport.with_manifest_contract(%{
        assets: [],
        pipelines: [],
        schedules: [],
        graph: %{},
        metadata: %{}
      })

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv_publish_activate_test")
    {:ok, publication} = Publication.from_parts(version, [])
    :ok = ManifestBuild.write_bundle(root, publication)

    %{manifest_path: Path.join(root, "manifest-index.json")}
  end

  test "publish reads its service token only from the dedicated environment", context do
    assert {:ok, summary} =
             Publish.run(
               manifest_path: context.manifest_path,
               orchestrator_url: "http://orchestrator.internal",
               client: Client,
               env: %{"FAVN_ORCHESTRATOR_SERVICE_TOKEN" => "environment-token"}
             )

    assert summary.status == "published"

    assert_received {:published, "http://orchestrator.internal", "environment-token",
                     "mv_publish_activate_test"}

    assert {:error, {:missing_required_env, "FAVN_ORCHESTRATOR_SERVICE_TOKEN"}} =
             Publish.run(
               manifest_path: context.manifest_path,
               orchestrator_url: "http://orchestrator.internal",
               client: Client,
               env: %{}
             )
  end

  test "publish returns the canonical content-addressed version on replay", context do
    assert {:ok, summary} =
             Publish.run(
               manifest_path: context.manifest_path,
               orchestrator_url: "http://orchestrator.internal",
               client: AlreadyPublishedClient,
               env: %{"FAVN_ORCHESTRATOR_SERVICE_TOKEN" => "environment-token"}
             )

    assert summary.status == "already_published"
    assert summary.manifest_version_id == "mv_canonical"
  end

  test "activate sends one exact manifest and workspace with the environment token" do
    assert {:ok, summary} =
             Activate.run(
               manifest_version_id: "mv_exact",
               workspace_id: "workspace-a",
               orchestrator_url: "http://orchestrator.internal",
               client: Client,
               env: %{"FAVN_ORCHESTRATOR_SERVICE_TOKEN" => "environment-token"}
             )

    assert summary.activated?
    assert summary.manifest_version_id == "mv_exact"

    assert_received {:activated, "http://orchestrator.internal", "environment-token", "mv_exact",
                     "workspace-a"}
  end

  test "successful HTTP responses must contain successful operation DTOs", context do
    env = %{"FAVN_ORCHESTRATOR_SERVICE_TOKEN" => "environment-token"}

    assert {:error, {:invalid_publication_response, _details}} =
             Publish.run(
               manifest_path: context.manifest_path,
               orchestrator_url: "https://orchestrator.internal",
               client: InvalidClient,
               env: env
             )

    assert {:error, :invalid_activation_response} =
             Activate.run(
               manifest_version_id: "mv_exact",
               workspace_id: "workspace-a",
               orchestrator_url: "https://orchestrator.internal",
               client: InvalidClient,
               env: env
             )
  end

  test "successful DTOs must echo the exact immutable manifest identity", context do
    env = %{"FAVN_ORCHESTRATOR_SERVICE_TOKEN" => "environment-token"}

    assert {:error, {:invalid_publication_response, _details}} =
             Publish.run(
               manifest_path: context.manifest_path,
               orchestrator_url: "https://orchestrator.internal",
               client: MismatchClient,
               env: env
             )

    assert {:error, :invalid_activation_response} =
             Activate.run(
               manifest_version_id: "mv_exact",
               workspace_id: "workspace-a",
               orchestrator_url: "https://orchestrator.internal",
               client: MismatchClient,
               env: env
             )
  end
end
