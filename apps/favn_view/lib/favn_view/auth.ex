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

  alias FavnView.Orchestrator
  alias FavnView.Auth.Scope
  alias FavnView.Endpoint
  alias FavnView.ProductionRuntimeConfig

  @session_token_key :operator_session_token
  @workspace_key :operator_workspace_id
  @live_socket_key :live_socket_id
  @default_revalidation_interval_ms 30_000

  @doc """
  Fetches the current sanitized operator scope into `conn.assigns` when present.
  """
  @spec fetch_current_scope(Plug.Conn.t(), keyword()) :: Plug.Conn.t()
  def fetch_current_scope(conn, _opts) do
    case scope_from_session(conn) do
      {:ok, scope} ->
        conn
        |> assign(:current_scope, scope)
        |> assign(:operator_workspaces, active_workspaces(scope))

      {:error, :missing_session} ->
        conn
        |> assign(:current_scope, nil)
        |> assign(:operator_workspaces, [])

      {:error, :invalid_session} ->
        conn
        |> delete_session(@session_token_key)
        |> delete_session(@workspace_key)
        |> delete_session(@live_socket_key)
        |> assign(:current_scope, nil)
        |> assign(:operator_workspaces, [])

      {:error, {:workspace_configuration_unavailable, reason}} ->
        conn
        |> assign(:current_scope, nil)
        |> assign(:operator_workspaces, [])
        |> assign(:workspace_configuration_error, reason)
    end
  end

  @doc """
  Requires an authenticated operator for protected browser requests.
  """
  @spec require_operator_authenticated(Plug.Conn.t(), keyword()) :: Plug.Conn.t()
  def require_operator_authenticated(conn, _opts) do
    case {conn.assigns[:current_scope], conn.assigns[:workspace_configuration_error]} do
      {_scope, reason} when not is_nil(reason) ->
        conn
        |> put_status(:service_unavailable)
        |> send_resp(503, "Workspace configuration unavailable")
        |> halt()

      {%Scope{} = scope, _error} ->
        if Scope.has_role?(scope, :viewer) do
          conn
        else
          conn
          |> put_flash(:error, "Please sign in to continue")
          |> Phoenix.Controller.redirect(to: login_path(conn))
          |> halt()
        end

      {_scope, _error} ->
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
    # `renew_session/1` clears the stored session, but a flash put by the guard
    # that sent the operator here is already in this connection's assigns, so it
    # would be persisted alongside this one and the first signed-in page would
    # open showing "Please sign in to continue" in red.
    |> Phoenix.Controller.clear_flash()
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
        _ = Orchestrator.revoke_operator_session(scope.operator_context)

      _other ->
        :ok
    end

    if is_binary(live_socket_id) do
      Endpoint.broadcast(live_socket_id, "disconnect", %{})
    end

    conn
    |> renew_session()
    |> put_flash(:info, "Signed out")
    |> Phoenix.Controller.redirect(to: logout_redirect())
  end

  @doc """
  Atomically rotates the current operator session into another active workspace.
  """
  @spec switch_operator_workspace(Plug.Conn.t(), String.t()) ::
          {:ok, Plug.Conn.t()} | {:error, Plug.Conn.t()}
  def switch_operator_workspace(conn, target_workspace_id)
      when is_binary(target_workspace_id) do
    scope = conn.assigns[:current_scope]
    old_live_socket_id = get_session(conn, @live_socket_key)

    result =
      case scope do
        %Scope{} ->
          call(
            :switch_operator_workspace_fun,
            &Orchestrator.switch_operator_workspace/2,
            [scope.operator_context, String.trim(target_workspace_id)]
          )

        _scope ->
          {:error, :unauthenticated}
      end

    case result do
      {:ok, session} ->
        if is_binary(old_live_socket_id) do
          Endpoint.broadcast(old_live_socket_id, "disconnect", %{})
        end

        {:ok,
         conn
         |> renew_session()
         |> put_session(@session_token_key, Map.fetch!(session, :token))
         |> put_session(@workspace_key, Map.fetch!(session, :workspace_id))
         |> put_session(@live_socket_key, live_socket_id(Map.fetch!(session, :id)))
         |> put_flash(:info, "Workspace switched")
         |> Phoenix.Controller.redirect(to: "/")}

      {:error, _reason} ->
        {:error,
         conn
         |> put_flash(:error, "Could not switch workspace")
         |> Phoenix.Controller.redirect(to: "/")}
    end
  end

  @doc """
  LiveView `on_mount` hook that authenticates operator LiveViews on every mount.
  """
  @spec on_mount(atom() | {atom(), atom()}, map(), map(), Phoenix.LiveView.Socket.t()) ::
          {:cont, Phoenix.LiveView.Socket.t()} | {:halt, Phoenix.LiveView.Socket.t()}
  def on_mount(:require_authenticated_operator, _params, session, socket) do
    authenticate_live_operator(session, socket, :viewer)
  end

  def on_mount({:require_authenticated_operator, required_role}, _params, session, socket)
      when required_role in [:viewer, :operator, :admin] do
    authenticate_live_operator(session, socket, required_role)
  end

  defp authenticate_live_operator(session, socket, required_role) do
    with {:ok, workspace_id, token} <- fetch_operator_session_credentials(session),
         {:ok, orchestrator_session, actor} <-
           call(
             :introspect_operator_session_fun,
             &Orchestrator.introspect_operator_session/2,
             [workspace_id, token]
           ),
         {:ok, scope} <- configured_scope(workspace_id, actor, orchestrator_session) do
      if Scope.has_role?(scope, required_role) do
        subscribe_live_identity(socket, scope, workspace_id, token, required_role)
      else
        deny_live_scope(socket, scope)
      end
    else
      {:error, {:workspace_configuration_unavailable, _reason}} ->
        workspace_configuration_unavailable(socket)

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

  @doc false
  def handle_identity_invalidation({:favn_identity_invalidated, _reason}, socket) do
    {:halt, Phoenix.LiveView.redirect(socket, to: "/login")}
  end

  def handle_identity_invalidation(_message, socket), do: {:cont, socket}

  defp scope_from_session(conn) do
    with {:ok, workspace_id, token} <- fetch_operator_session_credentials(conn),
         {:ok, session, actor} <-
           call(
             :introspect_operator_session_fun,
             &Orchestrator.introspect_operator_session/2,
             [workspace_id, token]
           ),
         {:ok, scope} <- configured_scope(workspace_id, actor, session) do
      {:ok, scope}
    end
  end

  defp active_workspaces(%Scope{} = scope) do
    case call(
           :list_operator_workspaces_fun,
           &Orchestrator.list_operator_workspaces/1,
           [scope.operator_context]
         ) do
      {:ok, workspaces} when is_list(workspaces) ->
        Enum.filter(workspaces, &(Map.get(&1, :status) == :active))

      {:error, _reason} ->
        []
    end
  end

  defp subscribe_live_identity(socket, scope, workspace_id, token, required_role) do
    if Phoenix.LiveView.connected?(socket) do
      case call(
             :subscribe_operator_identity_fun,
             &Orchestrator.subscribe_operator_identity/1,
             [scope.operator_context]
           ) do
        :ok ->
          case call(
                 :introspect_operator_session_fun,
                 &Orchestrator.introspect_operator_session/2,
                 [workspace_id, token]
               ) do
            {:ok, session, actor} ->
              with {:ok, refreshed_scope} <- configured_scope(workspace_id, actor, session) do
                if Scope.has_role?(refreshed_scope, required_role) do
                  schedule_identity_revalidation()

                  {:cont,
                   socket
                   |> assign_live_scope(refreshed_scope)
                   |> Phoenix.LiveView.attach_hook(
                     :operator_identity_invalidation,
                     :handle_info,
                     &handle_identity_message(&1, &2, workspace_id, token, required_role)
                   )}
                else
                  deny_live_scope(socket, refreshed_scope)
                end
              else
                {:error, {:workspace_configuration_unavailable, _reason}} ->
                  workspace_configuration_unavailable(socket)
              end

            {:error, _reason} ->
              {:halt, Phoenix.LiveView.redirect(socket, to: "/login")}
          end

        {:error, _reason} ->
          {:halt, Phoenix.LiveView.redirect(socket, to: "/login")}
      end
    else
      {:cont, assign_live_scope(socket, scope)}
    end
  end

  defp assign_live_scope(socket, scope) do
    socket
    |> Phoenix.Component.assign(:current_scope, scope)
    |> Phoenix.Component.assign(:current_actor, scope.actor)
    |> Phoenix.Component.assign(:timezone, scope.workspace_configuration.default_timezone)
    |> Phoenix.Component.assign(:can_submit_runs?, Scope.has_role?(scope, :operator))
    |> Phoenix.Component.assign(:operator_workspaces, active_workspaces(scope))
  end

  defp configured_scope(workspace_id, actor, session) do
    scope = Scope.new(workspace_id, actor, session)

    case call(
           :active_workspace_configuration_fun,
           &Orchestrator.active_workspace_configuration/1,
           [scope.operator_context]
         ) do
      {:ok, configuration} ->
        case Scope.put_workspace_configuration(scope, configuration) do
          {:ok, configured_scope} -> {:ok, configured_scope}
          {:error, reason} -> {:error, {:workspace_configuration_unavailable, reason}}
        end

      {:error, reason} ->
        {:error, {:workspace_configuration_unavailable, reason}}
    end
  end

  # Both identity messages are the hook's own, so they halt. Continuing would
  # deliver them to the page's `handle_info/2`, which crashes every LiveView
  # whose clauses are not exhaustive.
  defp handle_identity_message(
         {:favn_identity_invalidated, _reason} = message,
         socket,
         _workspace_id,
         _token,
         _required_role
       ) do
    handle_identity_invalidation(message, socket)
  end

  defp handle_identity_message(
         :favn_revalidate_operator_identity,
         socket,
         workspace_id,
         token,
         required_role
       ) do
    case call(
           :introspect_operator_session_fun,
           &Orchestrator.introspect_operator_session/2,
           [workspace_id, token]
         ) do
      {:ok, session, actor} ->
        with {:ok, refreshed_scope} <- configured_scope(workspace_id, actor, session) do
          if Scope.has_role?(refreshed_scope, required_role) do
            schedule_identity_revalidation()
            {:halt, assign_live_scope(socket, refreshed_scope)}
          else
            deny_live_scope(socket, refreshed_scope)
          end
        else
          {:error, {:workspace_configuration_unavailable, _reason}} ->
            workspace_configuration_unavailable(socket)
        end

      {:error, _reason} ->
        {:halt, Phoenix.LiveView.redirect(socket, to: "/login")}
    end
  end

  defp handle_identity_message(_message, socket, _workspace_id, _token, _required_role),
    do: {:cont, socket}

  defp deny_live_scope(socket, scope) do
    destination = if Scope.has_role?(scope, :viewer), do: "/", else: "/login"
    {:halt, Phoenix.LiveView.redirect(socket, to: destination)}
  end

  defp workspace_configuration_unavailable(socket) do
    {:halt,
     socket
     |> Phoenix.LiveView.put_flash(:error, "Workspace configuration is temporarily unavailable")
     |> Phoenix.LiveView.redirect(to: "/")}
  end

  defp schedule_identity_revalidation do
    Process.send_after(
      self(),
      :favn_revalidate_operator_identity,
      @default_revalidation_interval_ms
    )

    :ok
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

  defp logout_redirect do
    case ProductionRuntimeConfig.auth() do
      %{mode: :azure_container_apps_entra} -> "/.auth/logout"
      %{mode: :password} -> "/login"
    end
  end

  defp call(key, default, args) do
    fun = Application.get_env(:favn_view, key, default)

    if is_function(fun, length(args)),
      do: apply(fun, args),
      else: apply(default, args)
  end
end
