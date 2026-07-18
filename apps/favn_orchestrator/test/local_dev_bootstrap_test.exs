defmodule FavnOrchestrator.LocalDevBootstrapTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.LocalDevBootstrap
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace

  setup do
    previous_workspace_ids = Application.get_env(:favn_orchestrator, :workspace_ids)

    on_exit(fn ->
      restore_env(:workspace_ids, previous_workspace_ids)
    end)

    :ok
  end

  test "provisions each configured workspace idempotently before local services start" do
    Application.put_env(:favn_orchestrator, :workspace_ids, [
      "local-dev",
      "customer-dev",
      "local-dev"
    ])

    occurred_at = ~U[2026-07-18 08:00:00Z]
    test_pid = self()

    provision = fn %ProvisionWorkspace{} = command ->
      send(test_pid, {:provisioned, command})
      :ok
    end

    assert :ok =
             LocalDevBootstrap.provision_configured_workspaces(
               provision_workspace: provision,
               clock: fn -> occurred_at end
             )

    assert_receive {:provisioned, %ProvisionWorkspace{} = local}
    assert local.workspace_id == "local-dev"
    assert local.slug == "local-dev"
    assert local.display_name == "local-dev"
    assert local.occurred_at == occurred_at
    assert local.platform_context.roles == [:platform_admin]

    assert_receive {:provisioned, %ProvisionWorkspace{workspace_id: "customer-dev"}}
    refute_receive {:provisioned, _command}
  end

  test "returns the workspace id with a provisioning failure" do
    Application.put_env(:favn_orchestrator, :workspace_ids, ["local-dev"])

    assert {:error, {:workspace_provision_failed, "local-dev", :database_unavailable}} =
             LocalDevBootstrap.provision_configured_workspaces(
               provision_workspace: fn _command -> {:error, :database_unavailable} end
             )
  end

  test "rejects missing and invalid local workspace configuration" do
    Application.put_env(:favn_orchestrator, :workspace_ids, [])

    assert {:error, :local_dev_workspace_ids_required} =
             LocalDevBootstrap.provision_configured_workspaces()

    Application.put_env(:favn_orchestrator, :workspace_ids, [""])

    assert {:error, :invalid_local_dev_workspace_ids} =
             LocalDevBootstrap.provision_configured_workspaces()
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
