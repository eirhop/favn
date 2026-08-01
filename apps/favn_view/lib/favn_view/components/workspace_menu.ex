defmodule FavnView.Components.WorkspaceMenu do
  @moduledoc """
  The current workspace control in the primary navigation.

  A workspace is always visible when an operator is authenticated. The control
  becomes a dropdown only when the operator has more than one active workspace,
  keeping the common one-workspace case quiet while making switching easy when
  it is needed.
  """

  use FavnView, :html

  alias FavnView.Auth.Scope

  attr :current_scope, :any, default: nil
  attr :workspaces, :list, default: []
  attr :class, :any, default: nil

  def workspace_menu(assigns) do
    current_workspace_id = Map.get(assigns.current_scope || %{}, :workspace_id)
    workspaces = Enum.filter(assigns.workspaces, &(Map.get(&1, :status) == :active))

    current_workspace =
      Enum.find(workspaces, &(Map.get(&1, :id) == current_workspace_id)) ||
        %{id: current_workspace_id}

    assigns =
      assigns
      |> assign(:workspaces, workspaces)
      |> assign(:current_workspace, current_workspace)

    ~H"""
    <div
      :if={match?(%Scope{}, @current_scope)}
      class={["min-w-0", @class]}
      data-testid="workspace-menu"
    >
      <details :if={length(@workspaces) > 1} class="dropdown w-full">
        <summary
          class="favn-workspace-control favn-surface-control flex w-full min-w-0 cursor-pointer list-none items-center gap-2 rounded-field px-2 py-2 text-left text-xs favn-text-muted transition hover:border-primary/40 hover:text-primary focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary [&::-webkit-details-marker]:hidden"
          aria-label="Switch workspace"
          title={workspace_name(@current_workspace)}
          data-testid="workspace-menu-trigger"
        >
          <.icon name="hero-building-office-2" size={:sm} class="shrink-0 text-primary" />
          <span class="favn-workspace-control__name min-w-0 flex-1">
            {workspace_name(@current_workspace)}
          </span>
          <.icon
            name="hero-chevron-down"
            size={:xs}
            class="favn-workspace-control__chevron shrink-0"
          />
        </summary>

        <div class="dropdown-content favn-surface-rail z-50 mt-2 w-64 rounded-box p-2 shadow-xl md:top-0 md:left-full md:mt-0 md:ml-2">
          <p class="px-2 pb-2 text-xs uppercase tracking-[0.14em] favn-text-subtle">
            Switch workspace
          </p>

          <div class="space-y-1">
            <form :for={workspace <- @workspaces} action={~p"/workspaces/switch"} method="post">
              <input type="hidden" name="_csrf_token" value={get_csrf_token()} />
              <input type="hidden" name="workspace_id" value={workspace.id} />
              <.button
                type="submit"
                variant={
                  if(workspace.id == @current_scope.workspace_id, do: :secondary, else: :ghost)
                }
                block
                trailing_icon={
                  if(workspace.id == @current_scope.workspace_id, do: "hero-check", else: nil)
                }
                class="justify-between text-left"
              >
                <span class="min-w-0 truncate">{workspace_name(workspace)}</span>
              </.button>
            </form>
          </div>
        </div>
      </details>

      <div
        :if={length(@workspaces) <= 1}
        class="favn-workspace-control favn-surface-control flex min-w-0 items-center gap-2 rounded-field px-2 py-2 text-xs favn-text-muted"
        aria-label={"Current workspace: #{workspace_name(@current_workspace)}"}
        title={workspace_name(@current_workspace)}
      >
        <.icon name="hero-building-office-2" size={:sm} class="shrink-0 text-primary" />
        <span class="favn-workspace-control__name min-w-0">
          {workspace_name(@current_workspace)}
        </span>
      </div>
    </div>
    """
  end

  defp workspace_name(workspace) do
    Map.get(workspace, :name) || Map.get(workspace, :slug) || Map.get(workspace, :id)
  end
end
