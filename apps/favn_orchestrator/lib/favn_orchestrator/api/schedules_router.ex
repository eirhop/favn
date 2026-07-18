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
         {:ok, _session, _actor, context} <- Authentication.workspace_context(conn, :viewer),
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

  get "/:schedule_id" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor, context} <- Authentication.workspace_context(conn, :viewer),
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
end
