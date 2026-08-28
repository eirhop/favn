defmodule FavnOrchestrator.WorkspaceConfigurationTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Environment
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Diagnostics
  alias FavnOrchestrator.OperatorContext
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.Results.RuntimeState
  alias FavnOrchestrator.WorkspaceConfiguration

  defmodule Store do
    alias FavnOrchestrator.Persistence.Results.Actor
    alias FavnOrchestrator.Persistence.Results.Session

    def get_session(%{workspace_context: %{workspace_id: "workspace-oslo"}}) do
      {:ok,
       %Session{
         session_id: "session-oslo",
         actor_id: "actor-oslo",
         workspace_id: "workspace-oslo",
         provider: "password_local",
         issued_at: ~U[2026-08-16 10:00:00Z],
         status: :active,
         expires_at: ~U[2026-08-16 12:00:00Z]
       }}
    end

    def get_actor(%{workspace_context: %{workspace_id: "workspace-oslo"}}) do
      {:ok,
       %Actor{
         actor_id: "actor-oslo",
         username: "operator",
         display_name: "Oslo Operator",
         status: :active,
         workspace_id: "workspace-oslo",
         membership_status: :active,
         roles: [:customer_reader],
         access_version: 1,
         version: 1
       }}
    end

    def get_active_deployment_configuration(%{
          workspace_context: %{workspace_id: "workspace-oslo"}
        }) do
      {:ok,
       {"deployment-oslo",
        Application.fetch_env!(:favn_orchestrator, :workspace_configuration_test_value)}}
    end

    def get_runtime_state(%{workspace_context: %{workspace_id: "workspace-oslo"}}) do
      {:ok,
       %RuntimeState{
         workspace_id: "workspace-oslo",
         deployment_id: "deployment-oslo",
         manifest_version_id: "manifest-oslo",
         revision: 1
       }}
    end

    def get_deployment_manifest(%{workspace_context: %{workspace_id: "workspace-oslo"}}) do
      {:ok, Application.fetch_env!(:favn_orchestrator, :workspace_configuration_test_version)}
    end

    def get_manifest_size(_selector), do: {:ok, 1_024}

    def get_deployment_configuration(%{
          workspace_context: %{workspace_id: "workspace-oslo"},
          deployment_id: "deployment-oslo"
        }) do
      {:ok, Application.fetch_env!(:favn_orchestrator, :workspace_configuration_test_value)}
    end
  end

  test "authorized retrieval returns the active deployment-scoped timezone DTO" do
    assert {:ok, configuration} =
             WorkspaceConfiguration.put(%{}, %Manifest{
               environment: Environment.new!(default_timezone: "Europe/Oslo")
             })

    assert {:ok, version} =
             Version.new(
               %Manifest{environment: Environment.new!(default_timezone: "Europe/Oslo")},
               manifest_version_id: "manifest-oslo"
             )

    Application.put_env(:favn_orchestrator, :workspace_configuration_test_value, configuration)
    Application.put_env(:favn_orchestrator, :workspace_configuration_test_version, version)
    Application.put_env(:favn_orchestrator, :workspace_ids, ["workspace-oslo"])

    on_exit(fn ->
      Application.delete_env(:favn_orchestrator, :workspace_configuration_test_value)
      Application.delete_env(:favn_orchestrator, :workspace_configuration_test_version)
      Application.delete_env(:favn_orchestrator, :workspace_ids)
    end)

    stores = %Stores{
      registry: Store,
      runs: Store,
      run_submissions: Store,
      runner_tasks: Store,
      run_ownership: Store,
      scheduler: Store,
      admission: Store,
      resource_circuits: Store,
      target_generations: Store,
      target_recovery: Store,
      rebuilds: Store,
      target_operation_locks: Store,
      materialization: Store,
      backfills: Store,
      operator_reads: Store,
      logs: Store,
      identity: Store,
      maintenance: Store
    }

    assert {:ok, runtime} =
             Runtime.start_link(%Runtime{backend: __MODULE__, options: [], stores: stores})

    on_exit(fn -> if Process.alive?(runtime), do: GenServer.stop(runtime) end)

    context = %OperatorContext{
      workspace_id: "workspace-oslo",
      actor_id: "actor-oslo",
      session_id: "session-oslo"
    }

    assert {:ok,
            %WorkspaceConfiguration{
              workspace_id: "workspace-oslo",
              deployment_id: "deployment-oslo",
              default_timezone: "Europe/Oslo",
              default_timezone_source: :application_default
            }} = FavnOrchestrator.active_workspace_configuration(context)

    active_manifest =
      Diagnostics.report().checks
      |> Enum.find(&(&1.check == :active_manifest))

    assert active_manifest.status == :ok

    assert [diagnostics] = active_manifest.details.manifests
    assert diagnostics.workspace_id == "workspace-oslo"
    assert diagnostics.deployment_id == "deployment-oslo"
    assert diagnostics.default_timezone == "Europe/Oslo"
    assert diagnostics.default_timezone_source == :application_default

    invalid_configuration =
      put_in(
        configuration,
        ["workspace_environment", "environment", "default_timezone"],
        "Invalid/Timezone"
      )

    Application.put_env(
      :favn_orchestrator,
      :workspace_configuration_test_value,
      invalid_configuration
    )

    invalid_active_manifest =
      Diagnostics.report().checks
      |> Enum.find(&(&1.check == :active_manifest))

    assert invalid_active_manifest.status == :error

    assert invalid_active_manifest.reason ==
             {"workspace-oslo", {:invalid_timezone, "Invalid/Timezone"}}
  end
end
