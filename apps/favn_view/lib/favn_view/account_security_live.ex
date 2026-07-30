defmodule FavnView.AccountSecurityLive do
  @moduledoc """
  Self-service password rotation for an authenticated operator.

  The password values are consumed directly from the event and are never
  assigned to the socket, rendered back to the browser, or logged.
  """

  use FavnView, :live_view

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
    <main class="mx-auto max-w-xl space-y-6 p-6" data-testid="account-security">
      <header>
        <h1 class="text-2xl font-semibold">Account security</h1>
        <p class="mt-1 text-sm opacity-70">
          Changing your password revokes every active session in every workspace.
        </p>
      </header>

      <.flash_group flash={@flash} />

      <form phx-submit="change_password" class="space-y-4 rounded-box border p-4">
        <input
          name="current_password"
          type="password"
          autocomplete="current-password"
          required
          placeholder="Current password"
          class="input input-bordered w-full"
        />
        <input
          name="new_password"
          type="password"
          autocomplete="new-password"
          required
          placeholder="New password"
          class="input input-bordered w-full"
        />
        <input
          name="new_password_confirmation"
          type="password"
          autocomplete="new-password"
          required
          placeholder="Confirm new password"
          class="input input-bordered w-full"
        />
        <button type="submit" class="btn btn-primary">Change password</button>
      </form>
    </main>
    """
  end

  defp call(key, default, args) do
    fun = Application.get_env(:favn_view, key, default)

    if is_function(fun, length(args)),
      do: apply(fun, args),
      else: apply(default, args)
  end
end
