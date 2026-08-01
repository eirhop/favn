defmodule FavnView.AccountSecurityLive do
  @moduledoc """
  Self-service password rotation for an authenticated operator.

  The password values are consumed directly from the event and are never
  assigned to the socket, rendered back to the browser, or logged.
  """

  use FavnView, :live_view

  alias FavnView.Components.AccountSecurityPage
  alias FavnView.Components.Navigation

  @impl true
  def mount(_params, _session, socket), do: {:ok, socket}

  @impl true
  def handle_event("change_password", params, socket) do
    current_password = Map.get(params, "current_password", "")
    new_password = Map.get(params, "new_password", "")

    with true <- new_password != "",
         true <- new_password == Map.get(params, "new_password_confirmation"),
         :ok <-
           call(
             :change_operator_password_fun,
             &FavnOrchestrator.change_operator_password/3,
             [socket.assigns.current_scope.operator_context, current_password, new_password]
           ) do
      {:noreply,
       socket
       |> put_flash(:info, "Password changed. Sign in again.")
       |> Phoenix.LiveView.redirect(to: "/login")}
    else
      _error ->
        {:noreply, put_flash(socket, :error, "Password could not be changed")}
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <AccountSecurityPage.account_security_page
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      nav_items={Navigation.items()}
      flash={@flash}
    />
    """
  end

  defp call(key, default, args) do
    fun = Application.get_env(:favn_view, key, default)

    if is_function(fun, length(args)),
      do: apply(fun, args),
      else: apply(default, args)
  end
end
