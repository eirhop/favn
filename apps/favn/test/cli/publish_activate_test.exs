defmodule Favn.CLI.PublishActivateTest do
  use ExUnit.Case, async: true

  alias Favn.CLI.{Activate, Publish}
  alias FavnAuthoring.Deployment.ManifestBuilder, as: ManifestBuild
  alias Favn.Manifest.{Publication, Version}

  defmodule Client do
    def publish_manifest(url, token, publication, nil) do
      send(self(), {:published, url, token, publication.version.manifest_version_id})

      {:ok,
       %{
         "data" => %{
           "manifest" => %{
             "runner_releases" => publication.version.runner_releases
           },
           "registration" => %{
             "status" => "published",
             "manifest_version_id" => publication.version.manifest_version_id,
             "canonical_manifest_version_id" => publication.version.manifest_version_id
           }
         }
       }}
    end

    def activate_manifest_service(url, token, manifest_version_id, workspace_id, opts) do
      send(self(), {:activated, url, token, manifest_version_id, workspace_id, opts})

      {:ok,
       %{
         "data" => %{
           "activated" => true,
           "manifest_version_id" => manifest_version_id,
           "deployment_id" => "deployment:test",
           "runner_releases" => %{"default" => FavnTestSupport.runner_release_id()}
         }
       }}
    end
  end

  defmodule InvalidClient do
    def publish_manifest(_url, _token, _publication, nil), do: {:ok, %{"data" => %{}}}

    def activate_manifest_service(_url, _token, _manifest_version_id, _workspace_id, _opts),
      do: {:ok, %{"data" => %{"activated" => false}}}
  end

  defmodule AlreadyPublishedClient do
    def publish_manifest(_url, _token, publication, nil) do
      {:ok,
       %{
         "data" => %{
           "manifest" => %{
             "runner_releases" => publication.version.runner_releases
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
             "runner_releases" => publication.version.runner_releases
           },
           "registration" => %{
             "status" => "published",
             "manifest_version_id" => "mv_wrong",
             "canonical_manifest_version_id" => "mv_wrong"
           }
         }
       }}
    end

    def activate_manifest_service(_url, _token, _manifest_version_id, _workspace_id, _opts) do
      {:ok,
       %{
         "data" => %{
           "activated" => true,
           "manifest_version_id" => "mv_wrong",
           "deployment_id" => "deployment:wrong",
           "runner_releases" => %{"default" => FavnTestSupport.runner_release_id()}
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
    refute summary.reconciled?
    assert summary.manifest_version_id == "mv_exact"

    assert_received {:activated, "http://orchestrator.internal", "environment-token", "mv_exact",
                     "workspace-a", activation_opts}

    assert activation_opts[:timeout_ms] == 360_000
    assert activation_opts[:reconcile_timeout_ms] == 10_000
    assert activation_opts[:operation_id] == summary.operation_id
  end

  test "activate validates bounded timeouts and operation identity before calling the client" do
    base_opts = [
      manifest_version_id: "mv_exact",
      workspace_id: "workspace-a",
      orchestrator_url: "http://orchestrator.internal",
      client: Client,
      env: %{"FAVN_ORCHESTRATOR_SERVICE_TOKEN" => "environment-token"}
    ]

    assert {:error, {:invalid_option, :timeout_ms}} =
             Activate.run(Keyword.put(base_opts, :timeout_ms, 900_001))

    assert {:error, {:invalid_option, :reconcile_timeout_ms}} =
             Activate.run(Keyword.put(base_opts, :reconcile_timeout_ms, 0))

    assert {:error, {:invalid_option, :operation_id}} =
             Activate.run(Keyword.put(base_opts, :operation_id, ""))

    assert {:error, {:invalid_option, :manifest_version_id}} =
             Activate.run(
               Keyword.put(base_opts, :manifest_version_id, String.duplicate("m", 256))
             )

    assert {:error, {:invalid_option, :workspace_id}} =
             Activate.run(Keyword.put(base_opts, :workspace_id, String.duplicate("w", 256)))

    refute_received {:activated, _, _, _, _, _}
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
