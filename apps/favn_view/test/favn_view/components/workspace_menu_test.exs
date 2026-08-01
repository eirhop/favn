defmodule FavnView.Components.WorkspaceMenuTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest, only: [render_component: 2]

  alias FavnView.Auth.Scope
  alias FavnView.Components.WorkspaceMenu

  test "shows the current workspace without a switch control when it is the only workspace" do
    html = render_component(&WorkspaceMenu.workspace_menu/1, assigns(:workspace_one))

    assert html =~ ~s(data-testid="workspace-menu")
    assert html =~ "Workspace One"
    refute html =~ ~s(data-testid="workspace-menu-trigger")
    refute html =~ "/workspaces/switch"
  end

  test "opens a workspace dropdown when multiple active workspaces are available" do
    html = render_component(&WorkspaceMenu.workspace_menu/1, assigns(:workspace_two))

    assert html =~ ~s(data-testid="workspace-menu-trigger")
    assert html =~ "Switch workspace"
    assert html =~ "Workspace One"
    assert html =~ "Workspace Two"
    assert html =~ ~s(name="workspace_id" value="workspace-two")
    assert html =~ ~s(action="/workspaces/switch")
  end

  defp assigns(:workspace_one) do
    [
      current_scope: %Scope{workspace_id: "workspace-one"},
      workspaces: [%{id: "workspace-one", name: "Workspace One", status: :active}]
    ]
  end

  defp assigns(:workspace_two) do
    [
      current_scope: %Scope{workspace_id: "workspace-one"},
      workspaces: [
        %{id: "workspace-one", name: "Workspace One", status: :active},
        %{id: "workspace-two", name: "Workspace Two", status: :active}
      ]
    ]
  end
end
