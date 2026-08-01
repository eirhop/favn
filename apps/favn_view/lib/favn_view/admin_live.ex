defmodule FavnView.AdminLive do
  @moduledoc """
  Minimal workspace administration surface.

  Every read and mutation crosses a public orchestrator facade which
  reauthorizes the durable operator session. This LiveView never constructs
  workspace or platform authority from browser parameters.
  """

  use FavnView, :live_view

  alias FavnView.Auth.Scope
  alias FavnView.Components.AdminPage
  alias FavnView.Components.Navigation

  @page_size 50

  @impl true
  def mount(_params, _session, socket) do
    if Scope.has_role?(socket.assigns[:current_scope], :admin) do
      {:ok, socket |> assign(:admin_tab, :operators) |> load_initial()}
    else
      {:ok, Phoenix.LiveView.redirect(socket, to: "/")}
    end
  end

  @impl true
  def handle_params(params, _uri, socket) do
    {:noreply, assign(socket, :admin_tab, normalize_admin_tab(params["tab"]))}
  end

  @impl true
  def handle_event("create_actor", params, socket) do
    with :ok <- confirm_password(params),
         {:ok, roles} <- parse_roles(params),
         {:ok, _actor} <-
           call(
             :create_operator_actor_fun,
             &FavnOrchestrator.create_operator_actor/5,
             [
               operator_context(socket),
               Map.get(params, "username", ""),
               Map.get(params, "password", ""),
               Map.get(params, "display_name", ""),
               roles
             ]
           ) do
      {:noreply, socket |> put_flash(:info, "Actor created") |> load_initial()}
    else
      _error -> {:noreply, put_flash(socket, :error, "Actor could not be created")}
    end
  end

  def handle_event("attach_actor", params, socket) do
    with {:ok, roles} <- parse_roles(params),
         {:ok, _actor} <-
           call(
             :attach_operator_actor_fun,
             &FavnOrchestrator.attach_operator_actor/3,
             [
               operator_context(socket),
               Map.get(params, "username", ""),
               roles
             ]
           ) do
      {:noreply, socket |> put_flash(:info, "Actor attached") |> load_initial()}
    else
      _error -> {:noreply, put_flash(socket, :error, "Actor could not be attached")}
    end
  end

  def handle_event("update_membership", %{"actor_id" => actor_id} = params, socket) do
    with false <- actor_id == socket.assigns.current_scope.actor.id,
         {:ok, roles} <- parse_roles(params),
         {:ok, status} <- parse_membership_status(params),
         {:ok, _actor} <-
           call(
             :update_operator_actor_membership_fun,
             &FavnOrchestrator.update_operator_actor_membership/4,
             [operator_context(socket), actor_id, roles, status]
           ) do
      {:noreply, socket |> put_flash(:info, "Membership updated") |> load_initial()}
    else
      true ->
        {:noreply,
         put_flash(socket, :error, "Use another administrator to change your membership")}

      _error ->
        {:noreply, put_flash(socket, :error, "Membership could not be updated")}
    end
  end

  def handle_event("update_membership", _params, socket),
    do: {:noreply, put_flash(socket, :error, "Membership could not be updated")}

  def handle_event("revoke_session", %{"session_id" => session_id}, socket) do
    if session_id == socket.assigns.current_scope.session.id do
      {:noreply, put_flash(socket, :error, "Use logout to revoke your current session")}
    else
      case call(
             :revoke_operator_managed_session_fun,
             &FavnOrchestrator.revoke_operator_managed_session/2,
             [operator_context(socket), session_id]
           ) do
        :ok ->
          {:noreply, socket |> put_flash(:info, "Session revoked") |> load_initial()}

        {:error, _reason} ->
          {:noreply, put_flash(socket, :error, "Session could not be revoked")}
      end
    end
  end

  def handle_event("revoke_session", _params, socket),
    do: {:noreply, put_flash(socket, :error, "Session could not be revoked")}

  def handle_event("load_more_actors", _params, socket),
    do: {:noreply, append_page(socket, :actors)}

  def handle_event("load_more_sessions", _params, socket),
    do: {:noreply, append_page(socket, :sessions)}

  def handle_event("load_more_audit", _params, socket),
    do: {:noreply, append_page(socket, :audit)}

  @impl true
  def render(assigns) do
    ~H"""
    <AdminPage.admin_page
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      nav_items={Navigation.items(:admin)}
      admin_tab={@admin_tab}
      actors={@actors}
      actors_has_more?={@actors_has_more?}
      sessions={@sessions}
      sessions_has_more?={@sessions_has_more?}
      audit={@audit}
      audit_has_more?={@audit_has_more?}
      flash={@flash}
    />
    """
  end

  defp normalize_admin_tab("sessions"), do: :sessions
  defp normalize_admin_tab("audit"), do: :audit
  defp normalize_admin_tab(_tab), do: :operators

  defp load_initial(socket) do
    socket
    |> assign_page(:actors, page(:actors, operator_context(socket), nil))
    |> assign_page(:sessions, page(:sessions, operator_context(socket), nil))
    |> assign_page(:audit, page(:audit, operator_context(socket), nil))
  end

  defp append_page(socket, kind) do
    {items_key, cursor_key, has_more_key} = page_assigns(kind)
    cursor = socket.assigns[cursor_key]

    if cursor do
      case page(kind, operator_context(socket), cursor) do
        {:ok, result} ->
          socket
          |> assign(items_key, socket.assigns[items_key] ++ result.items)
          |> assign(cursor_key, result.next_cursor)
          |> assign(has_more_key, result.has_more?)

        {:error, _reason} ->
          put_flash(socket, :error, "More records could not be loaded")
      end
    else
      socket
    end
  end

  defp assign_page(socket, kind, {:ok, result}) do
    {items_key, cursor_key, has_more_key} = page_assigns(kind)

    socket
    |> assign(items_key, result.items)
    |> assign(cursor_key, result.next_cursor)
    |> assign(has_more_key, result.has_more?)
  end

  defp assign_page(socket, kind, {:error, _reason}) do
    {items_key, cursor_key, has_more_key} = page_assigns(kind)

    socket
    |> assign(items_key, [])
    |> assign(cursor_key, nil)
    |> assign(has_more_key, false)
    |> put_flash(:error, "Some administration data is unavailable")
  end

  defp page_assigns(:actors), do: {:actors, :actors_next_cursor, :actors_has_more?}
  defp page_assigns(:sessions), do: {:sessions, :sessions_next_cursor, :sessions_has_more?}
  defp page_assigns(:audit), do: {:audit, :audit_next_cursor, :audit_has_more?}

  defp page(:actors, context, cursor) do
    call(:page_operator_actors_fun, &FavnOrchestrator.page_operator_actors/2, [
      context,
      page_opts(cursor)
    ])
  end

  defp page(:sessions, context, cursor) do
    call(:page_operator_sessions_fun, &FavnOrchestrator.page_operator_sessions/2, [
      context,
      page_opts(cursor)
    ])
  end

  defp page(:audit, context, cursor) do
    call(:page_operator_audit_fun, &FavnOrchestrator.page_operator_audit/2, [
      context,
      page_opts(cursor)
    ])
  end

  defp page_opts(nil), do: [limit: @page_size]
  defp page_opts(cursor), do: [limit: @page_size, after: cursor]

  defp parse_roles(%{"role" => "admin"}), do: {:ok, [:admin]}
  defp parse_roles(%{"role" => "operator"}), do: {:ok, [:operator]}
  defp parse_roles(%{"role" => "viewer"}), do: {:ok, [:viewer]}
  defp parse_roles(_params), do: {:error, :invalid_role}

  defp parse_membership_status(%{"status" => "active"}), do: {:ok, :active}
  defp parse_membership_status(%{"status" => "suspended"}), do: {:ok, :suspended}
  defp parse_membership_status(%{"status" => "revoked"}), do: {:ok, :revoked}
  defp parse_membership_status(_params), do: {:error, :invalid_membership_status}

  defp confirm_password(%{
         "password" => password,
         "password_confirmation" => password
       })
       when password != "",
       do: :ok

  defp confirm_password(_params), do: {:error, :password_confirmation_mismatch}

  defp operator_context(socket), do: socket.assigns.current_scope.operator_context

  defp call(key, default, args) do
    fun = Application.get_env(:favn_view, key, default)

    if is_function(fun, length(args)),
      do: apply(fun, args),
      else: apply(default, args)
  end
end
