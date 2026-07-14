defmodule FavnOrchestrator.API.AuthRouter do
  @moduledoc false

  use Plug.Router

  alias FavnOrchestrator.API.Audit
  alias FavnOrchestrator.API.Authentication
  alias FavnOrchestrator.API.DTO
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.Auth

  plug(:match)
  plug(:dispatch)

  post "/password/sessions" do
    params = conn.body_params

    with :ok <- Authentication.ensure_service(conn),
         {:ok, username} <- required_string(params, "username"),
         {:ok, password} <- required_string(params, "password"),
         {:ok, session, actor} <-
           Auth.password_login(username, password,
             remote_identity: Authentication.remote_identity(conn)
           ) do
      audit(conn, "auth.password.login", session, actor)

      Response.data(conn, 201, %{
        session: DTO.session(session),
        session_token: session.token,
        actor: DTO.actor(actor)
      })
    else
      {:error, :invalid_credentials} ->
        Response.error(conn, 401, "unauthenticated", "Invalid username or password")

      {:error, {:missing_field, field}} ->
        missing_field(conn, field)

      {:error, :service_unauthorized} ->
        service_unauthorized(conn)

      {:error, _reason} ->
        Response.error(conn, 500, "internal_error", "Request failed")
    end
  end

  post "/sessions/introspect" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, session_token} <- required_string(conn.body_params, "session_token"),
         {:ok, session, actor} <- Auth.introspect_session(session_token) do
      Response.data(conn, 200, %{session: DTO.session(session), actor: DTO.actor(actor)})
    else
      {:error, :invalid_session} ->
        Response.error(conn, 401, "unauthenticated", "Session is invalid")

      {:error, :actor_not_found} ->
        Response.error(conn, 404, "not_found", "Actor not found")

      {:error, {:missing_field, field}} ->
        missing_field(conn, field)

      {:error, :service_unauthorized} ->
        service_unauthorized(conn)
    end
  end

  post "/sessions/revoke" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, token} <- session_token(conn),
         {:ok, session, actor} <- Auth.introspect_session(token),
         :ok <- Auth.revoke_session(session.id) do
      audit(conn, "auth.session.revoke_current", session, actor)

      Response.data(conn, 200, %{
        revoked: true,
        session: DTO.session(%{session | revoked_at: DateTime.utc_now()}),
        actor: DTO.actor(actor)
      })
    else
      {:error, :invalid_session} ->
        Response.error(conn, 401, "unauthenticated", "Session is invalid")

      {:error, :actor_not_found} ->
        Response.error(conn, 404, "not_found", "Actor not found")

      {:error, {:missing_field, field}} ->
        missing_field(conn, field)

      {:error, :service_unauthorized} ->
        service_unauthorized(conn)

      {:error, _reason} ->
        Response.error(conn, 500, "internal_error", "Request failed")
    end
  end

  post "/sessions/:session_id/revoke" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, session, actor} <- Authentication.actor_context(conn, :admin),
         :ok <- Auth.revoke_session(session_id) do
      %{
        action: "auth.session.revoke",
        actor_id: actor.id,
        session_id: session.id,
        target_session_id: session_id,
        outcome: "accepted",
        service_identity: Authentication.service_identity(conn)
      }
      |> Audit.put_best_effort()

      Response.data(conn, 200, %{revoked: true, session_id: session_id})
    else
      {:error, :forbidden} ->
        Response.error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        service_unauthorized(conn)

      {:error, _reason} ->
        Response.error(conn, 401, "unauthenticated", "Missing or invalid actor context")
    end
  end

  match _ do
    Response.error(conn, 404, "not_found", "Route was not found")
  end

  defp audit(conn, action, session, actor) do
    Audit.put_best_effort(%{
      action: action,
      actor_id: actor.id,
      session_id: session.id,
      outcome: "accepted",
      service_identity: Authentication.service_identity(conn)
    })
  end

  defp session_token(conn) do
    case Map.get(conn.body_params, "session_token") || header(conn, "x-favn-session-token") do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing_or_invalid -> {:error, {:missing_field, "session_token"}}
    end
  end

  defp required_string(params, key) do
    case Map.get(params, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing_or_invalid -> {:error, {:missing_field, key}}
    end
  end

  defp header(conn, key) do
    case Plug.Conn.get_req_header(conn, key) do
      [value | _rest] -> value
      _missing -> nil
    end
  end

  defp missing_field(conn, field),
    do: Response.error(conn, 422, "validation_failed", "Missing required field", %{field: field})

  defp service_unauthorized(conn),
    do: Response.error(conn, 401, "service_unauthorized", "Invalid service credentials")
end
