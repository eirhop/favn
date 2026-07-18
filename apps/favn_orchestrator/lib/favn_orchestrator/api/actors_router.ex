defmodule FavnOrchestrator.API.ActorsRouter do
  @moduledoc false

  use Plug.Router

  alias FavnOrchestrator.API.Audit
  alias FavnOrchestrator.API.Authentication
  alias FavnOrchestrator.API.DTO
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.Auth

  @allowed_roles ["viewer", "operator", "admin"]

  plug(:match)
  plug(:dispatch)

  get "/" do
    case authorize_admin(conn) do
      {:ok, _session, _actor, context} ->
        case page_actors(context) do
          {:ok, page} ->
            Response.data(conn, 200, %{
              items: Enum.map(page.items, &DTO.actor/1),
              next_cursor: page.next_cursor
            })

          {:error, reason} ->
            authentication_error(conn, reason)
        end

      {:error, reason} ->
        authentication_error(conn, reason)
    end
  end

  post "/" do
    params = conn.body_params

    with {:ok, session, actor, context} <- authorize_admin(conn),
         {:ok, username} <- required_string(params, "username"),
         {:ok, password} <- required_string(params, "password"),
         {:ok, display_name} <- display_name(params, username),
         {:ok, roles} <- roles(Map.get(params, "roles", ["viewer"])),
         {:ok, created_actor} <-
           create_actor(context, username, password, display_name, roles) do
      audit_mutation(conn, context, "actor.create", created_actor.id, session, actor)
      Response.data(conn, 201, %{actor: DTO.actor(created_actor)})
    else
      {:error, {:missing_field, field}} ->
        missing_field(conn, field)

      {:error, :invalid_roles} ->
        validation_error(conn, "Invalid roles")

      {:error, :invalid_username} ->
        validation_error(conn, "Invalid username")

      {:error, :username_taken} ->
        Response.error(conn, 409, "conflict", "Username already exists")

      {:error, reason}
      when reason in [:password_too_short, :password_too_long, :password_blank] ->
        validation_error(conn, "Password does not meet policy")

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/:actor_id" do
    with {:ok, _session, _actor, context} <- authorize_admin(conn),
         {:ok, actor} <- get_actor(context, actor_id) do
      Response.data(conn, 200, %{actor: DTO.actor(actor)})
    else
      {:error, :actor_not_found} ->
        Response.error(conn, 404, "not_found", "Actor was not found")

      {:error, reason} ->
        authentication_error(conn, reason)
    end
  end

  put "/:actor_id/roles" do
    with {:ok, session, actor, context} <- authorize_admin(conn),
         {:ok, role_values} <- required_roles(conn.body_params),
         {:ok, updated_actor} <- update_actor_roles(context, actor_id, role_values) do
      audit_mutation(conn, context, "actor.roles.update", actor_id, session, actor)
      Response.data(conn, 200, %{actor: DTO.actor(updated_actor)})
    else
      {:error, :actor_not_found} ->
        Response.error(conn, 404, "not_found", "Actor was not found")

      {:error, {:missing_field, field}} ->
        missing_field(conn, field)

      {:error, :invalid_roles} ->
        validation_error(conn, "Invalid roles")

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  put "/:actor_id/password" do
    with {:ok, session, actor, context} <- authorize_viewer(conn),
         {:ok, password} <- required_string(conn.body_params, "password"),
         :ok <- set_actor_password(context, actor_id, password) do
      audit_mutation(conn, context, "actor.password.set", actor_id, session, actor)
      Response.data(conn, 200, %{updated: true, actor_id: actor_id})
    else
      {:error, :actor_not_found} ->
        Response.error(conn, 404, "not_found", "Actor was not found")

      {:error, reason}
      when reason in [:password_too_short, :password_too_long, :password_blank] ->
        validation_error(conn, "Password does not meet policy")

      {:error, {:missing_field, field}} ->
        missing_field(conn, field)

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  match _ do
    Response.error(conn, 404, "not_found", "Route was not found")
  end

  defp authorize_admin(conn) do
    with :ok <- Authentication.ensure_service(conn) do
      Authentication.workspace_context(conn, :admin)
    end
  end

  defp authorize_viewer(conn) do
    with :ok <- Authentication.ensure_service(conn) do
      Authentication.workspace_context(conn, :viewer)
    end
  end

  defp page_actors(context), do: Auth.page_actors(context, limit: 100)

  defp create_actor(context, username, password, display_name, roles),
    do: Auth.create_actor(context, username, password, display_name, roles)

  defp get_actor(context, actor_id), do: Auth.get_actor(context, actor_id)

  defp update_actor_roles(context, actor_id, roles),
    do: Auth.update_actor_roles(context, actor_id, roles)

  defp set_actor_password(context, actor_id, password),
    do: Auth.set_actor_password(context, actor_id, password)

  defp audit_mutation(conn, context, action, resource_id, session, actor) do
    Audit.put_best_effort(context, %{
      action: action,
      actor_id: actor.id,
      session_id: session.id,
      resource_type: "actor",
      resource_id: resource_id,
      outcome: "accepted",
      service_identity: Authentication.service_identity(conn)
    })
  end

  defp required_string(params, key) do
    case Map.get(params, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing_or_invalid -> {:error, {:missing_field, key}}
    end
  end

  defp display_name(params, username) do
    case Map.get(params, "display_name") do
      nil -> {:ok, username}
      value when is_binary(value) and value != "" -> {:ok, value}
      _invalid -> {:error, {:missing_field, "display_name"}}
    end
  end

  defp required_roles(params) do
    case Map.fetch(params, "roles") do
      {:ok, values} -> roles(values)
      :error -> {:error, {:missing_field, "roles"}}
    end
  end

  defp roles(values) when is_list(values) do
    roles = Enum.map(values, &normalize_role/1)

    if roles != [] and Enum.all?(roles, &(&1 in @allowed_roles)),
      do: {:ok, roles},
      else: {:error, :invalid_roles}
  end

  defp roles(_values), do: {:error, :invalid_roles}

  defp normalize_role(value) when is_binary(value), do: String.trim(value)
  defp normalize_role(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_role(_value), do: ""

  defp authentication_error(conn, :forbidden),
    do: Response.error(conn, 403, "forbidden", "Actor does not have access")

  defp authentication_error(conn, :service_unauthorized),
    do: Response.error(conn, 401, "service_unauthorized", "Invalid service credentials")

  defp authentication_error(conn, _reason),
    do: Response.error(conn, 401, "unauthenticated", "Missing or invalid actor context")

  defp missing_field(conn, field),
    do: Response.error(conn, 422, "validation_failed", "Missing required field", %{field: field})

  defp validation_error(conn, message),
    do: Response.error(conn, 422, "validation_failed", message)
end
