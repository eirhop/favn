defmodule FavnView.AdminLive do
  @moduledoc """
  Minimal workspace administration surface.

  Every read and mutation crosses a public orchestrator facade which
  reauthorizes the durable operator session. This LiveView never constructs
  workspace or platform authority from browser parameters.
  """

  use FavnView, :live_view

  alias FavnView.Auth.Scope

  @page_size 50

  @impl true
  def mount(_params, _session, socket) do
    if Scope.has_role?(socket.assigns[:current_scope], :admin) do
      {:ok, load_initial(socket)}
    else
      {:ok, Phoenix.LiveView.redirect(socket, to: "/")}
    end
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
    <main class="mx-auto max-w-7xl space-y-8 p-6" data-testid="workspace-admin">
      <header class="space-y-1">
        <p class="text-sm opacity-70">Workspace {@current_scope.workspace_id}</p>
        <h1 class="text-2xl font-semibold">Administration</h1>
        <p class="text-sm opacity-70">
          Changes on this page apply only to the current workspace.
        </p>
      </header>

      <.flash_group flash={@flash} />

      <section class="grid gap-6 lg:grid-cols-2">
        <form phx-submit="create_actor" class="space-y-3 rounded-box border p-4">
          <h2 class="font-semibold">Create actor</h2>
          <input
            name="username"
            type="text"
            autocomplete="off"
            required
            placeholder="Username"
            class="input input-bordered w-full"
          />
          <input
            name="display_name"
            type="text"
            required
            placeholder="Display name"
            class="input input-bordered w-full"
          />
          <input
            name="password"
            type="password"
            autocomplete="new-password"
            required
            placeholder="Password"
            class="input input-bordered w-full"
          />
          <input
            name="password_confirmation"
            type="password"
            autocomplete="new-password"
            required
            placeholder="Confirm password"
            class="input input-bordered w-full"
          />
          <.role_select />
          <button class="btn btn-primary" type="submit">Create</button>
        </form>

        <form phx-submit="attach_actor" class="space-y-3 rounded-box border p-4">
          <h2 class="font-semibold">Attach existing actor</h2>
          <p class="text-sm opacity-70">
            The exact username must already exist. Other workspace memberships are not disclosed.
          </p>
          <input
            name="username"
            type="text"
            autocomplete="off"
            required
            placeholder="Exact username"
            class="input input-bordered w-full"
          />
          <.role_select />
          <button class="btn btn-primary" type="submit">Attach</button>
        </form>
      </section>

      <section class="space-y-3">
        <h2 class="text-xl font-semibold">Actors and memberships</h2>
        <p :if={@actors == []} class="opacity-70">No actors found.</p>
        <div :for={actor <- @actors} class="rounded-box border p-4">
          <div class="mb-3">
            <strong>{actor.display_name || actor.username || actor.id}</strong>
            <span class="ml-2 text-sm opacity-70">{actor.username}</span>
            <span class="ml-2 text-sm">{actor.status}</span>
          </div>
          <form phx-submit="update_membership" class="flex flex-wrap items-end gap-3">
            <input type="hidden" name="actor_id" value={actor.id} />
            <.role_select selected={primary_role(actor.roles)} />
            <label>
              <span class="block text-xs">Membership status</span>
              <select name="status" class="select select-bordered">
                <option
                  :for={status <- [:active, :suspended, :revoked]}
                  value={status}
                  selected={membership_status_value(actor) == status}
                >
                  {status}
                </option>
              </select>
            </label>
            <button
              type="submit"
              class="btn"
              disabled={actor.id == @current_scope.actor.id}
            >
              Update
            </button>
          </form>
        </div>
        <button :if={@actors_has_more?} phx-click="load_more_actors" class="btn">
          Load more actors
        </button>
      </section>

      <section class="space-y-3">
        <h2 class="text-xl font-semibold">Sessions</h2>
        <p :if={@sessions == []} class="opacity-70">No sessions found.</p>
        <div
          :for={session <- @sessions}
          class="flex flex-wrap items-center gap-3 rounded-box border p-4"
        >
          <code>{session.id}</code>
          <span>Actor {session.actor_id}</span>
          <span>{session.provider}</span>
          <span>{session_state(session)}</span>
          <span :if={session.expires_at}>expires {format_time(session.expires_at)}</span>
          <button
            :if={session_state(session) == "active"}
            phx-click="revoke_session"
            phx-value-session_id={session.id}
            class="btn btn-error btn-sm"
            disabled={session.id == @current_scope.session.id}
          >
            Revoke
          </button>
        </div>
        <button :if={@sessions_has_more?} phx-click="load_more_sessions" class="btn">
          Load more sessions
        </button>
      </section>

      <section class="space-y-3">
        <h2 class="text-xl font-semibold">Authorization audit</h2>
        <p class="text-sm opacity-70">
          This view intentionally shows redacted metadata, not stored request detail.
        </p>
        <p :if={@audit == []} class="opacity-70">No audit records found.</p>
        <div :for={entry <- @audit} class="grid gap-1 rounded-box border p-4 md:grid-cols-4">
          <span>{entry.action}</span>
          <span>{entry.subject_kind}: {entry.subject_id}</span>
          <span>Actor {entry.principal_id}</span>
          <time>{format_time(entry.occurred_at)}</time>
        </div>
        <button :if={@audit_has_more?} phx-click="load_more_audit" class="btn">
          Load more audit
        </button>
      </section>
    </main>
    """
  end

  attr :selected, :atom, default: :viewer

  defp role_select(assigns) do
    ~H"""
    <label>
      <span class="block text-xs">Role</span>
      <select name="role" class="select select-bordered">
        <option
          :for={role <- [:viewer, :operator, :admin]}
          value={role}
          selected={@selected == role}
        >
          {role}
        </option>
      </select>
    </label>
    """
  end

  defp load_initial(socket) do
    socket
    |> assign_page(:actors, page(:actors, operator_context(socket), nil))
    |> assign_page(:sessions, page(:sessions, operator_context(socket), nil))
    |> assign_page(:audit, page(:audit, operator_context(socket), nil))
  end

  defp append_page(socket, kind) do
    cursor = socket.assigns[:"#{kind}_next_cursor"]

    if cursor do
      case page(kind, operator_context(socket), cursor) do
        {:ok, result} ->
          socket
          |> assign(kind, socket.assigns[kind] ++ result.items)
          |> assign(:"#{kind}_next_cursor", result.next_cursor)
          |> assign(:"#{kind}_has_more?", result.has_more?)

        {:error, _reason} ->
          put_flash(socket, :error, "More records could not be loaded")
      end
    else
      socket
    end
  end

  defp assign_page(socket, kind, {:ok, result}) do
    socket
    |> assign(kind, result.items)
    |> assign(:"#{kind}_next_cursor", result.next_cursor)
    |> assign(:"#{kind}_has_more?", result.has_more?)
  end

  defp assign_page(socket, kind, {:error, _reason}) do
    socket
    |> assign(kind, [])
    |> assign(:"#{kind}_next_cursor", nil)
    |> assign(:"#{kind}_has_more?", false)
    |> put_flash(:error, "Some administration data is unavailable")
  end

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

  defp confirm_password(%{
         "password" => password,
         "password_confirmation" => password
       })
       when password != "",
       do: :ok

  defp confirm_password(_params), do: {:error, :password_confirmation_mismatch}

  defp session_state(%{status: status}) when status in [:active, :revoked, :expired],
    do: Atom.to_string(status)

  defp session_state(%{revoked_at: revoked_at}) when not is_nil(revoked_at), do: "revoked"
  defp session_state(_session), do: "active"

  defp format_time(%DateTime{} = value), do: Calendar.strftime(value, "%Y-%m-%d %H:%M:%S UTC")
  defp format_time(_value), do: "unknown"

  defp operator_context(socket), do: socket.assigns.current_scope.operator_context

  defp call(key, default, args) do
    fun = Application.get_env(:favn_view, key, default)

    if is_function(fun, length(args)),
      do: apply(fun, args),
      else: apply(default, args)
  end
end
