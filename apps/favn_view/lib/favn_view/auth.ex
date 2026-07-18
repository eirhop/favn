defmodule FavnView.Auth do
  @moduledoc """
  Browser and LiveView authentication boundary for operator sessions.

  This module talks to the orchestrator only through the public
  `FavnOrchestrator` facade. The raw opaque orchestrator token and selected
  workspace are stored in Phoenix's authenticated encrypted session cookie, so
  any web node can reconstruct and revalidate the session against PostgreSQL.
  """

  import Phoenix.Controller
  import Plug.Conn

  alias FavnView.Auth.Scope
  alias FavnView.Endpoint

  @session_token_key :operator_session_token
  @workspace_key :operator_workspace_id
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
        |> delete_session(@session_token_key)
        |> delete_session(@workspace_key)
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
  Logs an operator in and rotates the encrypted browser session.
  """
  @spec log_in_operator(Plug.Conn.t(), String.t(), map(), String.t() | nil) :: Plug.Conn.t()
  def log_in_operator(conn, workspace_id, session, return_to)
      when is_binary(workspace_id) and workspace_id != "" and is_map(session) do
    token = Map.fetch!(session, :token)
    session_id = Map.fetch!(session, :id)

    conn
    |> renew_session()
    |> put_session(@session_token_key, token)
    |> put_session(@workspace_key, workspace_id)
    |> put_session(@live_socket_key, live_socket_id(session_id))
    |> put_flash(:info, "Signed in")
    |> Phoenix.Controller.redirect(to: safe_return_to(return_to) || "/assets")
  end

  @doc """
  Revokes the current operator session, disconnects LiveViews, and clears session state.
  """
  @spec log_out_operator(Plug.Conn.t()) :: Plug.Conn.t()
  def log_out_operator(conn) do
    scope = conn.assigns[:current_scope]
    live_socket_id = get_session(conn, @live_socket_key)

    case scope do
      %Scope{} ->
        _ = FavnOrchestrator.revoke_operator_session(scope.operator_context)

      _other ->
        :ok
    end

    if is_binary(live_socket_id) do
      Endpoint.broadcast(live_socket_id, "disconnect", %{})
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
    with {:ok, workspace_id, token} <- fetch_operator_session_credentials(session),
         {:ok, orchestrator_session, actor} <-
           FavnOrchestrator.introspect_operator_session(workspace_id, token) do
      scope = Scope.new(workspace_id, actor, orchestrator_session)

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
  Returns the LiveView socket topic for an orchestrator session id.
  """
  @spec live_socket_id(String.t()) :: String.t()
  def live_socket_id(session_id) when is_binary(session_id),
    do: "operator_sessions:#{session_id}"

  defp scope_from_session(conn) do
    with {:ok, workspace_id, token} <- fetch_operator_session_credentials(conn),
         {:ok, session, actor} <-
           FavnOrchestrator.introspect_operator_session(workspace_id, token) do
      {:ok, Scope.new(workspace_id, actor, session)}
    end
  end

  defp fetch_operator_session_credentials(%Plug.Conn{} = conn) do
    credentials(get_session(conn, @workspace_key), get_session(conn, @session_token_key))
  end

  defp fetch_operator_session_credentials(session) when is_map(session) do
    workspace_id =
      Map.get(session, Atom.to_string(@workspace_key)) || Map.get(session, @workspace_key)

    token =
      Map.get(session, Atom.to_string(@session_token_key)) || Map.get(session, @session_token_key)

    credentials(workspace_id, token)
  end

  defp credentials(workspace_id, token)
       when is_binary(workspace_id) and workspace_id != "" and is_binary(token) and token != "",
       do: {:ok, workspace_id, token}

  defp credentials(nil, nil), do: {:error, :missing_session}
  defp credentials(_workspace_id, _token), do: {:error, :invalid_session}

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
