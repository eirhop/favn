defmodule FavnOrchestrator.API.ManifestsRouterTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Environment
  alias Favn.Manifest.Version
  alias FavnOrchestrator.API.ManifestsRouter
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.Persistence.{Runtime, Stores}

  @token "manifest-router-test-token-with-32-bytes"

  defmodule MissingManifestStore do
    alias FavnOrchestrator.Persistence.Error

    def get_manifest(_query), do: {:error, Error.new(:not_found, "manifest not found")}
    def begin_manifest_deployment(query), do: {:ok, {:new, query.idempotency}}
    def acquire_manifest_activation_lease(_command), do: {:ok, 1}
    def renew_manifest_activation_lease(_command), do: :ok
    def release_manifest_activation_lease(_command), do: :ok
    def heartbeat_manifest_deployment(_command), do: :ok
    def abandon_manifest_deployment(_command), do: :ok
    def get_runtime_state(_query), do: {:error, :active_manifest_not_set}
    def record_audit(_command), do: :ok

    def reserve_operator_command(command) do
      {:ok,
       %{
         key_hash: command.key_hash,
         request_fingerprint: command.request_fingerprint,
         expires_at: command.expires_at,
         replayed?: false
       }}
    end

    def complete_operator_command(_command), do: :ok
  end

  defmodule SuccessfulManifestStore do
    alias FavnOrchestrator.Persistence.Error
    alias FavnOrchestrator.Persistence.Results.RuntimeState

    def get_manifest(_query),
      do: {:ok, Application.fetch_env!(:favn_orchestrator, :manifest_router_test_version)}

    def begin_manifest_deployment(query), do: {:ok, {:new, query.idempotency}}
    def acquire_manifest_activation_lease(_command), do: {:ok, 1}
    def renew_manifest_activation_lease(_command), do: :ok
    def release_manifest_activation_lease(_command), do: :ok
    def heartbeat_manifest_deployment(_command), do: :ok
    def abandon_manifest_deployment(_command), do: :ok
    def get_runtime_state(_query), do: {:error, Error.new(:not_found, "no active deployment")}

    def get_active_deployment_configuration(_query),
      do: {:error, Error.new(:not_found, "no active deployment")}

    def get_deployment_configuration(_query), do: {:ok, %{}}

    def deploy_manifest(command) do
      if test_pid = Application.get_env(:favn_orchestrator, :manifest_router_test_pid) do
        send(test_pid, {:manifest_deployed, command})
      end

      {:ok,
       %RuntimeState{
         workspace_id: command.workspace_context.workspace_id,
         deployment_id: command.deployment_id,
         manifest_version_id: command.manifest_version_id,
         revision: 1,
         runner_releases: %{}
       }}
    end

    def record_audit(_command), do: :ok

    def reserve_operator_command(command) do
      {:ok,
       %{
         key_hash: command.key_hash,
         request_fingerprint: command.request_fingerprint,
         expires_at: command.expires_at,
         replayed?: false
       }}
    end

    def complete_operator_command(_command), do: :ok
  end

  defmodule ReplayedManifestStore do
    alias FavnOrchestrator.ManifestActivationDiagnostics
    alias FavnOrchestrator.Persistence.Results.RuntimeState

    def begin_manifest_deployment(query) do
      diagnostics = %ManifestActivationDiagnostics{
        unresolved_inspection_count: 1,
        unresolved_inspections: [
          %{
            target_id: "asset:historical",
            reason_code: "physical_inspection_unavailable"
          }
        ],
        truncated?: false,
        recovery: %{
          action: :repeat_manifest_activation,
          requires_new_idempotency_key: true,
          message: "Use a new key after recovery."
        }
      }

      {:ok,
       {:replay,
        %RuntimeState{
          workspace_id: query.workspace_context.workspace_id,
          deployment_id: "deployment:historical",
          manifest_version_id: "mv_historical",
          revision: 7,
          activation_diagnostics: diagnostics
        }}}
    end

    def get_manifest(_query), do: raise("replayed activation must not load or plan a manifest")
    def get_deployment_configuration(_query), do: {:ok, %{}}
    def record_audit(_command), do: :ok

    def reserve_operator_command(command) do
      {:ok,
       %{
         key_hash: command.key_hash,
         request_fingerprint: command.request_fingerprint,
         expires_at: command.expires_at,
         replayed?: true
       }}
    end

    def complete_operator_command(_command), do: :ok
  end

  defmodule ProtocolUnavailableManifestStore do
    def begin_manifest_deployment(_query),
      do: {:error, {:runner_protocol_not_activatable, 13}}

    def get_manifest(_query), do: raise("protocol rejection must not load the manifest")
    def record_audit(_command), do: :ok

    def reserve_operator_command(command) do
      {:ok,
       %{
         key_hash: command.key_hash,
         request_fingerprint: command.request_fingerprint,
         expires_at: command.expires_at,
         replayed?: false
       }}
    end

    def complete_operator_command(_command), do: :ok
  end

  defmodule ActivationBusyManifestStore do
    alias FavnOrchestrator.Persistence.Error

    def begin_manifest_deployment(query), do: {:ok, {:new, query.idempotency}}

    def acquire_manifest_activation_lease(_command) do
      {:error,
       Error.new(:conflict, "manifest activation is already in progress",
         details: %{reason: :manifest_activation_in_progress}
       )}
    end

    def heartbeat_manifest_deployment(_command), do: :ok
    def abandon_manifest_deployment(_command), do: :ok

    def record_audit(_command), do: :ok

    def reserve_operator_command(command) do
      {:ok,
       %{
         key_hash: command.key_hash,
         request_fingerprint: command.request_fingerprint,
         expires_at: command.expires_at,
         replayed?: false
       }}
    end

    def complete_operator_command(_command), do: :ok
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

  test "rejects an unauthenticated malformed publication before parsing it" do
    response =
      :post
      |> conn("/", "")
      |> Map.put(:body_params, %{"manifest" => "invalid"})
      |> ManifestsRouter.call(ManifestsRouter.init([]))

    assert response.status == 401
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "service_unauthorized"
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

  test "legacy activation reports the workspace activation fence with a stable conflict" do
    start_manifest_runtime(ActivationBusyManifestStore)

    response = activation_request("mv_busy", "service-activation-busy", %{})

    assert response.status == 409

    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) ==
             "manifest_activation_in_progress"
  end

  test "successful activation returns bounded inspection diagnostics" do
    {:ok, base} = ManifestsRouter.build_version(valid_envelope())

    manifest = %{
      base.manifest
      | environment: Environment.new!(default_timezone: "Europe/Oslo"),
        connection_circuits: %{
          "warehouse" =>
            Favn.CircuitBreaker.Policy.new!(
              failure_threshold: 5,
              probe_after_ms: 10_000
            )
        }
    }

    assert {:ok, version} =
             Version.new(manifest, manifest_version_id: "mv_router_workspace_environment")

    Application.put_env(:favn_orchestrator, :manifest_router_test_version, version)
    Application.put_env(:favn_orchestrator, :manifest_router_test_pid, self())

    on_exit(fn ->
      Application.delete_env(:favn_orchestrator, :manifest_router_test_version)
      Application.delete_env(:favn_orchestrator, :manifest_router_test_pid)
    end)

    start_manifest_runtime(SuccessfulManifestStore)

    response =
      :post
      |> conn("/#{version.manifest_version_id}/activate", "")
      |> put_req_header("authorization", "Bearer #{@token}")
      |> put_req_header("x-favn-workspace-id", "workspace-a")
      |> put_req_header("idempotency-key", "service-activation-success")
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

    assert response.status == 200

    assert_receive {:manifest_deployed, command}
    assert command.workspace_context.workspace_id == "workspace-a"

    assert {:ok, persisted_environment} =
             FavnOrchestrator.WorkspaceConfiguration.from_configuration(command.configuration)

    assert persisted_environment.default_timezone == "Europe/Oslo"
    assert persisted_environment.default_timezone_source == :application_default

    assert {:ok, connection_circuits} =
             FavnOrchestrator.ConnectionCircuitPolicy.effective(command.configuration)

    assert connection_circuits["warehouse"].failure_threshold == 5

    assert %{
             "data" => %{
               "activated" => true,
               "diagnostics" => %{
                 "unresolved_inspection_count" => 0,
                 "unresolved_inspections" => [],
                 "truncated" => false,
                 "recovery" => nil
               }
             }
           } = Jason.decode!(response.resp_body)
  end

  test "activation requires explicit approval for non-empty manifest pool defaults" do
    {:ok, base} = ManifestsRouter.build_version(valid_envelope())

    manifest = %{
      base.manifest
      | execution_pools: %{
          "partner_api" => %Favn.ExecutionPool.Policy{max_concurrency: 3}
        }
    }

    assert {:ok, version} =
             Version.new(manifest, manifest_version_id: "mv_router_execution_pool_policy")

    Application.put_env(:favn_orchestrator, :manifest_router_test_version, version)
    on_exit(fn -> Application.delete_env(:favn_orchestrator, :manifest_router_test_version) end)
    start_manifest_runtime(SuccessfulManifestStore)

    response = activation_request(version.manifest_version_id, "pool-policy-unapproved", %{})
    assert response.status == 422

    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) ==
             "execution_pool_policy_approval_required"

    response =
      activation_request(version.manifest_version_id, "pool-policy-approved", %{
        "execution_pool_policy" => %{"approve_manifest_defaults" => true}
      })

    assert response.status == 200

    response =
      activation_request(version.manifest_version_id, "pool-policy-invalid-override", %{
        "execution_pool_policy" => %{
          "approve_manifest_defaults" => true,
          "overrides" => %{"invalid pool name" => %{"max_concurrency" => 4}}
        }
      })

    assert response.status == 422

    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) ==
             "invalid_execution_pool_policy"
  end

  test "idempotent activation replay returns durable diagnostics without replanning" do
    start_manifest_runtime(ReplayedManifestStore)

    response =
      :post
      |> conn("/mv_historical/activate", "")
      |> put_req_header("authorization", "Bearer #{@token}")
      |> put_req_header("x-favn-workspace-id", "workspace-a")
      |> put_req_header("idempotency-key", "service-activation-replay")
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

    assert response.status == 200

    assert %{
             "data" => %{
               "revision" => 7,
               "diagnostics" => %{
                 "unresolved_inspection_count" => 1,
                 "unresolved_inspections" => [
                   %{
                     "target_id" => "asset:historical",
                     "reason_code" => "physical_inspection_unavailable"
                   }
                 ]
               }
             }
           } = Jason.decode!(response.resp_body)
  end

  test "runner protocol compatibility rejection remains a definitive conflict" do
    start_manifest_runtime(ProtocolUnavailableManifestStore)

    response = activation_request("mv_future_protocol", "future-protocol", %{})

    assert response.status == 409

    assert %{
             "error" => %{
               "code" => "runner_protocol_not_activatable",
               "details" => %{"runner_protocol_version" => 13}
             }
           } = Jason.decode!(response.resp_body)
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

  defp activation_request(manifest_version_id, idempotency_key, extra_body) do
    body =
      Map.merge(
        %{
          "selection" => %{
            "common_assets" => "all",
            "common_pipelines" => "all",
            "workspace_assets" => [],
            "workspace_pipelines" => []
          },
          "configuration" => %{}
        },
        extra_body
      )

    :post
    |> conn("/#{manifest_version_id}/activate", "")
    |> put_req_header("authorization", "Bearer #{@token}")
    |> put_req_header("x-favn-workspace-id", "workspace-a")
    |> put_req_header("idempotency-key", idempotency_key)
    |> Map.put(:body_params, body)
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
