defmodule FavnOrchestrator.API.ManifestsRouterTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version
  alias FavnOrchestrator.API.ManifestsRouter
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.Persistence.{Runtime, Stores}

  @token "manifest-router-test-token-with-32-bytes"

  defmodule MissingManifestStore do
    alias FavnOrchestrator.Persistence.Error

    def get_manifest(_query), do: {:error, Error.new(:not_found, "manifest not found")}
    def get_runtime_state(_query), do: {:error, :active_manifest_not_set}
    def record_audit(_command), do: :ok
  end

  defmodule Protocol13ManifestStore do
    def get_manifest(_query) do
      manifest =
        FavnTestSupport.with_manifest_contract(%{
          assets: [],
          pipelines: [],
          schedules: [],
          graph: %{},
          metadata: %{}
        })

      Favn.Manifest.Version.new(manifest, manifest_version_id: "mv_protocol_13")
    end

    def get_runtime_state(_query), do: {:error, :active_manifest_not_set}
    def record_audit(_command), do: :ok
  end

  setup do
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [
        service_identity: "manifest_router_test",
        token_hash: ServiceTokens.hash_token(@token),
        enabled: true,
        platform_roles: [:platform_operator]
      ]
    ])

    on_exit(fn -> restore_env(:api_service_tokens, previous_tokens) end)

    :ok
  end

  test "accepts a publication envelope whose runner release map matches its manifest" do
    params = valid_envelope()

    assert {:ok, version} = ManifestsRouter.build_version(params)
    assert version.runner_releases == %{}
  end

  test "returns stable validation errors for missing, malformed, and mismatched release maps" do
    valid = valid_envelope()

    cases = [
      {Map.delete(valid, "runner_releases"), "Invalid runner release map"},
      {Map.put(valid, "runner_releases", %{"default" => "rr_INVALID"}),
       "Invalid runner release map"},
      {Map.put(valid, "runner_releases", %{
         "default" => FavnTestSupport.runner_release_id(:alternate)
       }), "Manifest runner release map does not match payload"}
    ]

    for {params, expected_message} <- cases do
      response = request(params)

      assert response.status == 422

      assert %{
               "error" => %{
                 "code" => "validation_failed",
                 "message" => ^expected_message,
                 "status" => 422
               }
             } = Jason.decode!(response.resp_body)
    end
  end

  test "service-token activation reaches the persisted manifest boundary without actor headers" do
    start_missing_manifest_runtime()

    response =
      :post
      |> conn("/mv_service_missing/activate", "")
      |> put_req_header("authorization", "Bearer #{@token}")
      |> put_req_header("x-favn-workspace-id", "workspace-a")
      |> put_req_header("idempotency-key", "service-activation-missing")
      |> Map.put(:body_params, %{
        "selection" => %{
          "common_assets" => "all",
          "common_pipelines" => "all",
          "workspace_assets" => [],
          "workspace_pipelines" => []
        },
        "configuration" => %{}
      })
      |> ManifestsRouter.call(ManifestsRouter.init([]))

    assert response.status == 404
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "not_found"
  end

  test "protocol 13 activation is rejected before the singleton runner path" do
    start_manifest_runtime(Protocol13ManifestStore)

    response =
      :post
      |> conn("/mv_protocol_13/activate", "")
      |> put_req_header("authorization", "Bearer #{@token}")
      |> put_req_header("x-favn-workspace-id", "workspace-a")
      |> put_req_header("idempotency-key", "protocol-13-not-activatable")
      |> Map.put(:body_params, %{
        "selection" => %{
          "common_assets" => "all",
          "common_pipelines" => "all",
          "workspace_assets" => [],
          "workspace_pipelines" => []
        },
        "configuration" => %{}
      })
      |> ManifestsRouter.call(ManifestsRouter.init([]))

    assert response.status == 503

    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) ==
             "runner_protocol_not_activatable"
  end

  test "platform operator service token can read the active manifest without actor headers" do
    start_missing_manifest_runtime()

    response =
      :get
      |> conn("/active")
      |> put_req_header("authorization", "Bearer #{@token}")
      |> put_req_header("x-favn-workspace-id", "workspace-a")
      |> ManifestsRouter.call(ManifestsRouter.init([]))

    assert response.status == 404
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "not_found"
  end

  test "platform admin service token does not imply platform operator access" do
    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [
        service_identity: "manifest_router_admin",
        token_hash: ServiceTokens.hash_token(@token),
        enabled: true,
        platform_roles: [:platform_admin]
      ]
    ])

    response =
      :get
      |> conn("/active")
      |> put_req_header("authorization", "Bearer #{@token}")
      |> put_req_header("x-favn-workspace-id", "workspace-a")
      |> ManifestsRouter.call(ManifestsRouter.init([]))

    assert response.status == 403
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "forbidden"
  end

  defp valid_envelope do
    manifest =
      FavnTestSupport.with_manifest_contract(%{
        assets: [],
        pipelines: [],
        schedules: [],
        graph: %{},
        metadata: %{}
      })

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv_router_release_binding")
    {:ok, encoded_manifest} = Serializer.encode_manifest(version.manifest)
    {:ok, decoded_manifest} = Jason.decode(encoded_manifest)

    %{
      "manifest_version_id" => version.manifest_version_id,
      "content_hash" => version.content_hash,
      "schema_version" => version.schema_version,
      "runner_contract_version" => version.runner_contract_version,
      "runner_releases" => version.runner_releases,
      "serialization_format" => version.serialization_format,
      "manifest" => decoded_manifest
    }
  end

  defp request(params) do
    :post
    |> conn("/", "")
    |> put_req_header("authorization", "Bearer #{@token}")
    |> Map.put(:body_params, params)
    |> ManifestsRouter.call(ManifestsRouter.init([]))
  end

  defp start_missing_manifest_runtime do
    start_manifest_runtime(MissingManifestStore)
  end

  defp start_manifest_runtime(registry_store) do
    stores = %Stores{
      registry: registry_store,
      runs: MissingManifestStore,
      run_submissions: MissingManifestStore,
      runner_tasks: FavnOrchestrator.TestRunnerTaskStore,
      run_ownership: MissingManifestStore,
      scheduler: MissingManifestStore,
      admission: MissingManifestStore,
      resource_circuits: MissingManifestStore,
      target_generations: MissingManifestStore,
      target_recovery: MissingManifestStore,
      rebuilds: MissingManifestStore,
      target_operation_locks: MissingManifestStore,
      materialization: MissingManifestStore,
      backfills: MissingManifestStore,
      operator_reads: MissingManifestStore,
      logs: MissingManifestStore,
      identity: MissingManifestStore,
      maintenance: MissingManifestStore
    }

    assert {:ok, runtime} =
             Runtime.start_link(%Runtime{backend: __MODULE__, options: [], stores: stores})

    on_exit(fn -> if Process.alive?(runtime), do: GenServer.stop(runtime) end)
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
