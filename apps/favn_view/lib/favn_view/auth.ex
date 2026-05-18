defmodule FavnView.Auth do
  @moduledoc """
  Browser and LiveView authentication boundary for operator sessions.

  This module talks to the orchestrator only through the public
  `FavnOrchestrator` facade. Because Phoenix cookie sessions are signed but
  client-readable, the raw orchestrator bearer token is stored only in the
  server-side browser session store.
  """

  import Phoenix.Controller
  import Plug.Conn

  alias FavnView.Auth.Scope
  alias FavnView.Auth.BrowserSessionStore
  alias FavnView.Endpoint

  @session_key :operator_browser_session_id
  @live_socket_key :live_socket_id

  @doc """
  Fetches the current sanitized operator scope into `conn.assigns` when present.
  """
  @spec fetch_current_scope(Plug.Conn.t(), keyword()) :: Plug.Conn.t()
  def fetch_current_scope(conn, _opts) do
    case scope_from_session(conn) do
      {:ok, scope} ->
        assign(conn, :current_scope, scope)

      {:error, :missing_session} ->
        conn
        |> assign(:current_scope, nil)

      {:error, :invalid_session} ->
        conn
        |> delete_browser_session_mapping()
        |> delete_session(@session_key)
        |> delete_session(@live_socket_key)
        |> assign(:current_scope, nil)
    end
  end

  @doc """
  Requires an authenticated operator for protected browser requests.
  """
  @spec require_operator_authenticated(Plug.Conn.t(), keyword()) :: Plug.Conn.t()
  def require_operator_authenticated(conn, _opts) do
    case conn.assigns[:current_scope] do
      %Scope{} = scope ->
        if Scope.has_role?(scope, :viewer) do
          conn
        else
          conn
          |> put_flash(:error, "Please sign in to continue")
          |> Phoenix.Controller.redirect(to: login_path(conn))
          |> halt()
        end

      _other ->
        conn
        |> put_flash(:error, "Please sign in to continue")
        |> Phoenix.Controller.redirect(to: login_path(conn))
        |> halt()
    end
  end

  @doc """
  Redirects authenticated operators away from the login page.
  """
  @spec redirect_if_operator_authenticated(Plug.Conn.t(), keyword()) :: Plug.Conn.t()
  def redirect_if_operator_authenticated(conn, _opts) do
    case conn.assigns[:current_scope] do
      %Scope{} -> conn |> Phoenix.Controller.redirect(to: "/assets") |> halt()
      _other -> conn
    end
  end

  @doc """
  Logs an operator in and stores only safe browser session keys.
  """
  @spec log_in_operator(Plug.Conn.t(), map(), String.t() | nil) :: Plug.Conn.t()
  def log_in_operator(conn, session, return_to) when is_map(session) do
    {:ok, browser_session} = BrowserSessionStore.put(session)

    conn
    |> renew_session()
    |> put_session(@session_key, browser_session.id)
    |> put_session(@live_socket_key, browser_session.live_socket_id)
    |> put_flash(:info, "Signed in")
    |> Phoenix.Controller.redirect(to: safe_return_to(return_to) || "/assets")
  end

  @doc """
  Revokes the current operator session, disconnects LiveViews, and clears session state.
  """
  @spec log_out_operator(Plug.Conn.t()) :: Plug.Conn.t()
  def log_out_operator(conn) do
    scope = conn.assigns[:current_scope]
    browser_session_id = get_session(conn, @session_key)
    live_socket_id = get_session(conn, @live_socket_key)

    case scope do
      %Scope{} -> _ = FavnOrchestrator.revoke_operator_session(scope.session.id)
      _other -> :ok
    end

    if is_binary(live_socket_id) do
      Endpoint.broadcast(live_socket_id, "disconnect", %{})
    end

    if is_binary(browser_session_id) do
      BrowserSessionStore.delete(browser_session_id)
    end

    conn
    |> renew_session()
    |> put_flash(:info, "Signed out")
    |> Phoenix.Controller.redirect(to: "/login")
  end

  @doc """
  LiveView `on_mount` hook that authenticates operator LiveViews on every mount.
  """
  @spec on_mount(atom(), map(), map(), Phoenix.LiveView.Socket.t()) ::
          {:cont, Phoenix.LiveView.Socket.t()} | {:halt, Phoenix.LiveView.Socket.t()}
  def on_mount(:require_authenticated_operator, _params, session, socket) do
    with {:ok, token} <- fetch_operator_session_token(session),
         {:ok, orchestrator_session, actor} <- FavnOrchestrator.introspect_operator_session(token) do
      scope = Scope.new(actor, orchestrator_session)

      if Scope.has_role?(scope, :viewer) do
        socket =
          socket
          |> Phoenix.Component.assign(:current_scope, scope)
          |> Phoenix.Component.assign(:current_actor, scope.actor)
          |> Phoenix.Component.assign(:can_submit_runs?, Scope.has_role?(scope, :operator))

        {:cont, socket}
      else
        {:halt, Phoenix.LiveView.redirect(socket, to: "/login")}
      end
    else
      _error ->
        delete_browser_session_mapping(session)
        {:halt, Phoenix.LiveView.redirect(socket, to: "/login")}
    end
  end

  @doc """
  Returns a local path when `return_to` is safe for redirects.
  """
  @spec safe_return_to(String.t() | nil) :: String.t() | nil
  def safe_return_to(return_to) when is_binary(return_to) do
    uri = URI.parse(return_to)

    cond do
      uri.scheme || uri.host -> nil
      not String.starts_with?(return_to, "/") -> nil
      String.starts_with?(return_to, "//") -> nil
      true -> return_to
    end
  end

  def safe_return_to(_return_to), do: nil

  @doc """
  Returns the LiveView socket topic for a browser session id.
  """
  @spec live_socket_id(String.t()) :: String.t()
  def live_socket_id(browser_session_id) when is_binary(browser_session_id),
    do: BrowserSessionStore.live_socket_id(browser_session_id)

  defp scope_from_session(conn) do
    with {:ok, token} <- fetch_operator_session_token(conn),
         {:ok, session, actor} <- FavnOrchestrator.introspect_operator_session(token) do
      {:ok, Scope.new(actor, session)}
    end
  end

  defp fetch_operator_session_token(%Plug.Conn{} = conn) do
    case get_session(conn, @session_key) do
      browser_session_id when is_binary(browser_session_id) and browser_session_id != "" ->
        fetch_operator_session_token(browser_session_id)

      _other ->
        {:error, :missing_session}
    end
  end

  defp fetch_operator_session_token(session) when is_map(session) do
    case Map.get(session, Atom.to_string(@session_key)) || Map.get(session, @session_key) do
      browser_session_id when is_binary(browser_session_id) and browser_session_id != "" ->
        fetch_operator_session_token(browser_session_id)

      _other ->
        {:error, :missing_session}
    end
  end

  defp fetch_operator_session_token(browser_session_id) when is_binary(browser_session_id) do
    case BrowserSessionStore.fetch(browser_session_id) do
      {:ok, browser_session} -> {:ok, browser_session.operator_session_token}
      {:error, _reason} -> {:error, :invalid_session}
    end
  end

  defp delete_browser_session_mapping(%Plug.Conn{} = conn) do
    case get_session(conn, @session_key) do
      browser_session_id when is_binary(browser_session_id) ->
        BrowserSessionStore.delete(browser_session_id)

      _other ->
        :ok
    end

    conn
  end

  defp delete_browser_session_mapping(session) when is_map(session) do
    case Map.get(session, Atom.to_string(@session_key)) || Map.get(session, @session_key) do
      browser_session_id when is_binary(browser_session_id) ->
        BrowserSessionStore.delete(browser_session_id)

      _other ->
        :ok
    end
  end

  defp login_path(conn) do
    case safe_return_to(current_path(conn)) do
      nil -> "/login"
      return_to -> "/login?return_to=" <> URI.encode_www_form(return_to)
    end
  end

  defp renew_session(conn) do
    conn
    |> configure_session(renew: true)
    |> clear_session()
  end
end
