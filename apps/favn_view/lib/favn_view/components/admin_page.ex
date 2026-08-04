defmodule FavnView.Components.AdminPage do
  @moduledoc """
  Workspace administration page sections.

  The page keeps the high-risk operator controls, session controls, and audit
  history in separate URL-addressable tabs so each task has a focused surface.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell

  attr :current_scope, :any, required: true
  attr :operator_workspaces, :list, default: []
  attr :nav_items, :list, required: true
  attr :admin_tab, :atom, values: [:operators, :sessions, :audit], default: :operators
  attr :actors, :list, default: []
  attr :actors_has_more?, :boolean, default: false
  attr :sessions, :list, default: []
  attr :sessions_has_more?, :boolean, default: false
  attr :audit, :list, default: []
  attr :audit_has_more?, :boolean, default: false
  attr :flash, :map, default: %{}

  def admin_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Administration"
      subtitle="Manage operators and access for the current workspace"
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      admin_active?
      flash={@flash}
    >
      <.stack gap={:lg} class="mx-auto w-full max-w-7xl pb-24" data-testid="workspace-admin">
        <header class="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
          <div class="min-w-0">
            <.eyebrow>Workspace {@current_scope.workspace_id}</.eyebrow>

            <.meta>Changes here apply only to this workspace.</.meta>
          </div>

          <.badge variant={:outline} tone={:info} icon="hero-shield-check">
            Administrator access
          </.badge>
        </header>

        <.scope_rail
          label="Administration sections"
          choices={admin_tabs(@admin_tab)}
          data-testid="admin-tabs"
        />
        <div :if={@admin_tab == :operators} class="space-y-5" data-testid="admin-tab-operators">
          <.panel padding={:none}>
            <:header
              title="Add operators"
              subtitle="Create a new identity or attach an existing one to this workspace."
              icon="hero-user-plus"
            />
            <div class="p-5 sm:p-6">
              <.columns count={2} gap={:lg}>
                <form phx-submit="create_actor" class="space-y-3">
                  <div class="space-y-1">
                    <.section_title>New operator</.section_title>

                    <.meta>Set credentials and the first workspace role.</.meta>
                  </div>

                  <.input
                    name="username"
                    value=""
                    label="Username"
                    autocomplete="username"
                    placeholder="Username"
                    required
                    class="input input-sm favn-surface-control w-full"
                  />
                  <.input
                    name="display_name"
                    value=""
                    label="Display name"
                    placeholder="Display name"
                    required
                    class="input input-sm favn-surface-control w-full"
                  />
                  <.input
                    name="password"
                    value=""
                    type="password"
                    label="Password"
                    autocomplete="new-password"
                    placeholder="Password"
                    required
                    class="input input-sm favn-surface-control w-full"
                  />
                  <.input
                    name="password_confirmation"
                    value=""
                    type="password"
                    label="Confirm password"
                    autocomplete="new-password"
                    placeholder="Confirm password"
                    required
                    class="input input-sm favn-surface-control w-full"
                  /> <.role_select />
                  <.button type="submit" icon="hero-user-plus">Create operator</.button>
                </form>

                <form phx-submit="attach_actor" class="space-y-3">
                  <div class="space-y-1">
                    <.section_title>Existing operator</.section_title>

                    <.meta>Attach by exact username without exposing other workspaces.</.meta>
                  </div>

                  <.input
                    name="username"
                    value=""
                    label="Username"
                    autocomplete="username"
                    placeholder="Exact username"
                    required
                    class="input input-sm favn-surface-control w-full"
                  /> <.role_select />
                  <.button type="submit" variant={:secondary} icon="hero-link">
                    Attach operator
                  </.button>
                </form>
              </.columns>
            </div>
          </.panel>

          <.panel padding={:none}>
            <:header
              title="Workspace operators"
              subtitle="Manage roles and membership status for this workspace."
              icon="hero-user-group"
            />
            <div class="p-5 sm:p-6">
              <.empty_state
                :if={@actors == []}
                title="No operators yet"
                description="Create or attach an operator above to grant workspace access."
                icon="hero-user-group"
              />
              <.stack :if={@actors != []} gap={:sm}>
                <.list_card :for={actor <- @actors} class="p-4">
                  <div class="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
                    <div class="min-w-0">
                      <div class="flex flex-wrap items-center gap-2">
                        <span class="truncate font-medium">
                          {actor.display_name || actor.username || actor.id}
                        </span>

                        <.badge variant={:outline} tone={:neutral}>{actor.status}</.badge>
                      </div>

                      <.meta title={actor.username}>{actor.username}</.meta>
                    </div>

                    <form phx-submit="update_membership" class="min-w-0 xl:w-auto">
                      <input type="hidden" name="actor_id" value={actor.id} />
                      <.inline gap={:sm} align={:end} class="w-full">
                        <.role_select selected={primary_role(actor.roles)} />
                        <.membership_status_select actor={actor} />
                        <.button
                          type="submit"
                          variant={:secondary}
                          disabled={actor.id == @current_scope.actor.id}
                        >
                          Update
                        </.button>
                      </.inline>
                    </form>
                  </div>
                </.list_card>
              </.stack>

              <div :if={@actors_has_more?} class="mt-5 flex justify-center">
                <.button phx-click="load_more_actors" variant={:ghost} trailing_icon="hero-arrow-down">
                  Load more operators
                </.button>
              </div>
            </div>
          </.panel>
        </div>

        <div :if={@admin_tab == :sessions} data-testid="admin-tab-sessions">
          <.panel padding={:none}>
            <:header
              title="Sessions"
              subtitle="Review sign-ins and revoke sessions that should no longer be trusted."
              icon="hero-arrow-right-on-rectangle"
            />
            <div class="p-5 sm:p-6">
              <.empty_state
                :if={@sessions == []}
                title="No sessions found"
                description="There are no session records to display for this workspace."
                icon="hero-arrow-right-on-rectangle"
              />
              <.stack :if={@sessions != []} gap={:sm}>
                <.list_card :for={session <- @sessions} class="p-4">
                  <div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                    <div class="min-w-0 space-y-1">
                      <div class="flex flex-wrap items-center gap-2">
                        <.mono value={session.id} truncate />
                        <.status_badge
                          tone={session_tone(session)}
                          label={String.capitalize(session_state(session))}
                          size={:xs}
                        />
                      </div>

                      <.meta>
                        Actor {session.actor_id} · {session.provider || "Unknown provider"}
                      </.meta>

                      <.meta :if={session.expires_at}>
                        Expires {format_time(session.expires_at)}
                      </.meta>
                    </div>

                    <.button
                      :if={session_state(session) == "active"}
                      type="button"
                      phx-click="revoke_session"
                      phx-value-session_id={session.id}
                      variant={:danger}
                      disabled={session.id == @current_scope.session.id}
                    >
                      Revoke session
                    </.button>
                  </div>
                </.list_card>
              </.stack>

              <div :if={@sessions_has_more?} class="mt-5 flex justify-center">
                <.button
                  phx-click="load_more_sessions"
                  variant={:ghost}
                  trailing_icon="hero-arrow-down"
                >
                  Load more sessions
                </.button>
              </div>
            </div>
          </.panel>
        </div>

        <div :if={@admin_tab == :audit} data-testid="admin-tab-audit">
          <.panel padding={:none}>
            <:header
              title="Authorization audit"
              subtitle="A redacted record of access and membership changes in this workspace."
              icon="hero-clipboard-document-list"
            />
            <div class="p-5 sm:p-6">
              <.empty_state
                :if={@audit == []}
                title="No audit records"
                description="Security events will appear here as administrators manage access."
                icon="hero-clipboard-document-list"
              />
              <.stack :if={@audit != []} gap={:sm}>
                <.list_card :for={entry <- @audit} class="p-4">
                  <div class="grid gap-3 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] sm:items-center">
                    <div class="min-w-0">
                      <.badge variant={:outline} tone={:neutral}>{entry.action}</.badge>

                      <.meta class="mt-1">
                        {entry.subject_kind}: {entry.subject_id}
                      </.meta>
                    </div>

                    <.meta>Actor {entry.principal_id}</.meta>

                    <time
                      class="text-sm favn-text-muted"
                      datetime={datetime_attr(entry.occurred_at)}
                    >
                      {format_time(entry.occurred_at)}
                    </time>
                  </div>
                </.list_card>
              </.stack>

              <div :if={@audit_has_more?} class="mt-5 flex justify-center">
                <.button phx-click="load_more_audit" variant={:ghost} trailing_icon="hero-arrow-down">
                  Load more audit
                </.button>
              </div>
            </div>
          </.panel>
        </div>
      </.stack>
    </AppShell.app_shell>
    """
  end

  defp admin_tabs(active_tab) do
    [
      %{
        id: "operators",
        label: "Operators",
        icon: "hero-user-group",
        tone: :primary,
        count: nil,
        active?: active_tab == :operators,
        patch: ~p"/admin?tab=operators",
        hint: "Create operators and manage workspace membership"
      },
      %{
        id: "sessions",
        label: "Sessions",
        icon: "hero-arrow-right-on-rectangle",
        tone: :primary,
        count: nil,
        active?: active_tab == :sessions,
        patch: ~p"/admin?tab=sessions",
        hint: "Review and revoke operator sessions"
      },
      %{
        id: "audit",
        label: "Audit",
        icon: "hero-clipboard-document-list",
        tone: :primary,
        count: nil,
        active?: active_tab == :audit,
        patch: ~p"/admin?tab=audit",
        hint: "Review redacted authorization events"
      }
    ]
  end

  attr :selected, :atom, default: :viewer

  defp role_select(assigns) do
    ~H"""
    <.input
      type="select"
      name="role"
      value={@selected}
      label="Role"
      options={role_options()}
      class="select select-sm favn-surface-control min-w-36"
    />
    """
  end

  attr :actor, :map, required: true

  defp membership_status_select(assigns) do
    ~H"""
    <.input
      type="select"
      name="status"
      value={membership_status_value(@actor)}
      label="Membership status"
      options={membership_status_options()}
      class="select select-sm favn-surface-control min-w-44"
    />
    """
  end

  defp role_options do
    [{"Viewer", :viewer}, {"Operator", :operator}, {"Admin", :admin}]
  end

  defp membership_status_options do
    [{"Active", :active}, {"Suspended", :suspended}, {"Revoked", :revoked}]
  end

  defp membership_status_value(%{membership_status: status})
       when status in [:active, :suspended, :revoked],
       do: status

  defp membership_status_value(%{status: :active}), do: :active
  defp membership_status_value(_actor), do: :suspended

  defp primary_role(roles) do
    cond do
      :admin in roles -> :admin
      :operator in roles -> :operator
      true -> :viewer
    end
  end

  defp session_state(%{status: status}) when status in [:active, :revoked, :expired],
    do: Atom.to_string(status)

  defp session_state(%{revoked_at: revoked_at}) when not is_nil(revoked_at), do: "revoked"
  defp session_state(_session), do: "active"

  defp session_tone(session) do
    case session_state(session) do
      "active" -> :success
      "revoked" -> :warning
      _other -> :neutral
    end
  end

  defp format_time(%DateTime{} = value), do: Calendar.strftime(value, "%Y-%m-%d %H:%M:%S UTC")
  defp format_time(_value), do: "unknown"

  defp datetime_attr(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp datetime_attr(_value), do: nil
end
