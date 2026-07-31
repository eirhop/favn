defmodule FavnView.Layouts do
  @moduledoc """
  Root layout for the Favn operator UI.

  The root layout owns only the HTML document: `<head>`, theme attribute, and
  asset tags. Every visible frame — navigation, header, mode rail, flash — is
  owned by `FavnView.Components.AppShell`, so a LiveView renders one page
  component and nothing else.
  """
  use FavnView, :html

  embed_templates "layouts/*"

  attr :current_scope, :any, default: nil
  attr :workspaces, :list, default: []

  def workspace_switcher(assigns) do
    workspaces = Enum.filter(assigns.workspaces, &(Map.get(&1, :status) == :active))
    assigns = assign(assigns, :workspaces, workspaces)

    ~H"""
    <div
      :if={match?(%FavnView.Auth.Scope{}, @current_scope) && length(@workspaces) > 1}
      class="fixed bottom-3 left-3 z-50 flex items-end gap-2 rounded-box border border-base-content/10 bg-base-200/95 p-2 shadow-lg"
      data-testid="workspace-switcher"
    >
      <form action={~p"/workspaces/switch"} method="post" class="flex items-end gap-2">
        <input type="hidden" name="_csrf_token" value={get_csrf_token()} />
        <label class="form-control">
          <span class="label py-0 text-xs">Workspace</span>
          <select
            name="workspace_id"
            class="select select-sm select-bordered"
            aria-label="Workspace"
          >
            <option
              :for={workspace <- @workspaces}
              value={workspace.id}
              selected={workspace.id == @current_scope.workspace_id}
            >
              {Map.get(workspace, :name) || Map.get(workspace, :slug) || workspace.id}
            </option>
          </select>
        </label>
        <button type="submit" class="btn btn-sm btn-primary">Switch</button>
      </form>
    </div>
    """
  end

  attr :current_scope, :any, default: nil

  def operator_controls(assigns) do
    ~H"""
    <nav
      :if={match?(%FavnView.Auth.Scope{}, @current_scope)}
      aria-label="Account"
      class="fixed right-3 bottom-20 z-50 flex gap-2 lg:bottom-3"
      data-testid="operator-controls"
    >
      <.link navigate={~p"/account/security"} class="btn btn-sm">Account</.link>
      <.link
        :if={FavnView.Auth.Scope.has_role?(@current_scope, :admin)}
        navigate={~p"/admin"}
        class="btn btn-sm"
      >
        Admin
      </.link>
    </nav>
    """
  end

  defp operator_command_scope(%FavnView.Auth.Scope{
         workspace_id: workspace_id,
         actor: %{id: actor_id}
       }) do
    Enum.join([workspace_id, actor_id], ":")
  end

  defp operator_command_scope(_scope), do: nil
end
