defmodule FavnView.Components.AccountSecurityPage do
  @moduledoc """
  Account security page for rotating the current operator's password.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell

  attr :current_scope, :any, required: true
  attr :operator_workspaces, :list, default: []
  attr :nav_items, :list, required: true
  attr :flash, :map, default: %{}

  def account_security_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Account security"
      subtitle="Changing your password revokes every active session in every workspace"
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      flash={@flash}
    >
      <div class="mx-auto w-full max-w-xl pb-24" data-testid="account-security">
        <.panel padding={:none}>
          <:header
            title="Change password"
            subtitle="You will need to sign in again on every device."
            icon="hero-key"
          />

          <form phx-submit="change_password" class="space-y-4 p-5 sm:p-6">
            <.input
              name="current_password"
              value=""
              type="password"
              label="Current password"
              autocomplete="current-password"
              required
              class="input input-sm favn-surface-control w-full"
            />
            <.input
              name="new_password"
              value=""
              type="password"
              label="New password"
              autocomplete="new-password"
              required
              class="input input-sm favn-surface-control w-full"
            />
            <.input
              name="new_password_confirmation"
              value=""
              type="password"
              label="Confirm new password"
              autocomplete="new-password"
              required
              class="input input-sm favn-surface-control w-full"
            />
            <.button type="submit" block>Change password</.button>
          </form>
        </.panel>
      </div>
    </AppShell.app_shell>
    """
  end
end
