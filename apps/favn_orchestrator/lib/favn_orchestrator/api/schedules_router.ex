defmodule FavnOrchestrator.API.SchedulesRouter do
  @moduledoc false

  use Plug.Router

  alias FavnOrchestrator
  alias FavnOrchestrator.API.Authentication
  alias FavnOrchestrator.API.DTO
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.Operator.Schedules

  plug(:match)
  plug(:dispatch)

  get "/" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor, context} <-
           Authentication.workspace_or_service_context(conn, :viewer),
         {:ok, page} <- Schedules.page_entries(context, limit: 100) do
      Response.data(conn, 200, %{items: Enum.map(page.items, &DTO.schedule/1)})
    else
      {:error, :active_manifest_not_set} ->
        Response.error(conn, 404, "not_found", "Active manifest is not set")

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/:schedule_id/occurrences/preview" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor, context} <-
           Authentication.workspace_or_service_context(conn, :viewer),
         {:ok, limit} <- preview_limit(conn.params),
         {:ok, occurrences} <- Schedules.preview_occurrences(context, schedule_id, limit: limit) do
      Response.data(conn, 200, %{items: Enum.map(occurrences, &DTO.schedule_occurrence/1)})
    else
      {:error, :schedule_not_found} ->
        Response.error(conn, 404, "not_found", "Schedule was not found")

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Response.error(conn, 422, "validation_failed", "Schedule preview failed")
    end
  end

  post "/:schedule_id/activate" do
    change_activation(conn, schedule_id, true)
  end

  post "/:schedule_id/deactivate" do
    change_activation(conn, schedule_id, false)
  end

  get "/:schedule_id" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor, context} <-
           Authentication.workspace_or_service_context(conn, :viewer),
         {:ok, schedule} <- Schedules.get_entry(context, schedule_id) do
      Response.data(conn, 200, %{schedule: DTO.schedule(schedule)})
    else
      {:error, :active_manifest_not_set} ->
        Response.error(conn, 404, "not_found", "Active manifest is not set")

      {:error, :schedule_not_found} ->
        Response.error(conn, 404, "not_found", "Schedule was not found")

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  match _ do
    Response.error(conn, 404, "not_found", "Route was not found")
  end

  defp authentication_error(conn, :forbidden),
    do: Response.error(conn, 403, "forbidden", "Actor does not have access")

  defp authentication_error(conn, :service_unauthorized),
    do: Response.error(conn, 401, "service_unauthorized", "Invalid service credentials")

  defp authentication_error(conn, _reason),
    do: Response.error(conn, 401, "unauthenticated", "Missing or invalid actor context")

  defp change_activation(conn, schedule_id, enabled) do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, actor, context} <-
           Authentication.workspace_or_service_context(conn, :operator),
         {:ok, reason} <- activation_reason(conn.body_params),
         {:ok, command_id} <- command_id(conn),
         result <-
           activation_command(
             context,
             schedule_id,
             actor.id,
             reason,
             enabled,
             command_id: command_id
           ),
         {:ok, activation} <- result do
      Response.data(conn, 200, activation)
    else
      {:error, :schedule_not_found} ->
        Response.error(conn, 404, "not_found", "Schedule was not found")

      {:error, :idempotency_key_required} ->
        Response.error(conn, 422, "validation_failed", "Idempotency-Key is required")

      {:error, %{kind: :conflict}} ->
        Response.error(conn, 409, "idempotency_conflict", "Idempotency key has different content")

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Response.error(conn, 422, "validation_failed", "Schedule activation command failed")
    end
  end

  defp activation_command(context, schedule_id, actor_id, reason, true, opts),
    do: Schedules.activate(context, schedule_id, actor_id, reason, opts)

  defp activation_command(context, schedule_id, actor_id, reason, false, opts),
    do: Schedules.deactivate(context, schedule_id, actor_id, reason, opts)

  defp activation_reason(%{"reason" => reason}) when is_binary(reason) do
    if String.trim(reason) == "", do: {:error, :invalid_reason}, else: {:ok, reason}
  end

  defp activation_reason(_params), do: {:error, :invalid_reason}

  defp preview_limit(params) do
    case Map.get(params, "limit", "10") do
      value when is_integer(value) and value in 1..100 ->
        {:ok, value}

      value when is_binary(value) ->
        case Integer.parse(value) do
          {limit, ""} when limit in 1..100 -> {:ok, limit}
          _invalid -> {:error, :invalid_limit}
        end

      _invalid ->
        {:error, :invalid_limit}
    end
  end

  defp command_id(conn) do
    case get_req_header(conn, "idempotency-key") do
      [key] when key != "" and byte_size(key) <= 200 -> {:ok, "schedule:" <> key}
      _missing -> {:error, :idempotency_key_required}
    end
  end
end
