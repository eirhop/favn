defmodule FavnOrchestrator.API.Router do
  @moduledoc """
  Private orchestrator HTTP API v1.
  """

  use Plug.Router

  require Logger

  alias FavnOrchestrator
  alias FavnOrchestrator.API.ActorsRouter
  alias FavnOrchestrator.API.Authentication
  alias FavnOrchestrator.API.AuthRouter
  alias FavnOrchestrator.API.BackfillsRouter
  alias FavnOrchestrator.API.DTO
  alias FavnOrchestrator.API.Filters
  alias FavnOrchestrator.API.ManifestPublication
  alias FavnOrchestrator.API.ManifestsRouter
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.API.RunsRouter
  alias FavnOrchestrator.API.SchedulesRouter
  alias FavnOrchestrator.API.SSE
  alias FavnOrchestrator.API.SSE.Cursor
  alias FavnOrchestrator.Auth

  plug(Plug.RequestId)

  if Mix.env() == :dev and Code.ensure_loaded?(Tidewave) do
    plug(Tidewave)
  end

  plug(ManifestPublication)

  plug(Plug.Parsers,
    parsers: [:json],
    pass: ["application/json"],
    length: 1_000_000,
    json_decoder: Jason
  )

  plug(:match)
  plug(:dispatch)

  get "/api/orchestrator/v1/health" do
    data(conn, 200, %{status: "ok"})
  end

  get "/api/orchestrator/v1/health/live" do
    data(conn, 200, DTO.normalize(FavnOrchestrator.liveness()))
  end

  get "/api/orchestrator/v1/health/ready" do
    readiness = FavnOrchestrator.readiness()
    status = if readiness.status == :ready, do: 200, else: 503

    data(conn, status, DTO.normalize(readiness))
  end

  get "/api/orchestrator/v1/diagnostics" do
    case ensure_service_auth(conn) do
      :ok ->
        data(conn, 200, DTO.normalize(FavnOrchestrator.diagnostics()))

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")
    end
  end

  get "/api/orchestrator/v1/bootstrap/service-token" do
    case ensure_service_auth(conn) do
      :ok ->
        data(conn, 200, service_token_diagnostics(conn))

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")
    end
  end

  get "/api/orchestrator/v1/bootstrap/active-manifest" do
    with :ok <- ensure_service_auth(conn),
         {:ok, manifest_version_id} <- FavnOrchestrator.active_manifest(),
         {:ok, summary} <- FavnOrchestrator.get_manifest_summary(manifest_version_id) do
      data(conn, 200, %{manifest: summary})
    else
      {:error, :active_manifest_not_set} ->
        error(conn, 404, "not_found", "Active manifest is not set")

      {:error, :manifest_version_not_found} ->
        error(conn, 404, "not_found", "Active manifest version was not found")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, reason} ->
        Logger.error("bootstrap.active_manifest failed: #{inspect(reason)}")
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  forward("/api/orchestrator/v1/auth", to: AuthRouter)

  get "/api/orchestrator/v1/me" do
    with :ok <- ensure_service_auth(conn),
         {:ok, session, actor} <- ensure_actor_context(conn, :viewer) do
      data(conn, 200, %{session: DTO.session(session), actor: DTO.actor(actor)})
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, _reason} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")
    end
  end

  forward("/api/orchestrator/v1/manifests", to: ManifestsRouter)

  forward("/api/orchestrator/v1/runs", to: RunsRouter)

  forward("/api/orchestrator/v1/schedules", to: SchedulesRouter)

  forward("/api/orchestrator/v1/backfills", to: BackfillsRouter)

  get "/api/orchestrator/v1/assets/window-states" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, filters} <- Filters.asset_window_states(conn.params),
         {:ok, page} <- FavnOrchestrator.list_asset_window_states(filters) do
      data(conn, 200, page_response(page, &DTO.asset_window_state/1))
    else
      {:error, :invalid_asset_ref} ->
        error(conn, 422, "validation_failed", "Invalid asset ref filter")

      {:error, :invalid_filter} ->
        error(conn, 422, "validation_failed", "Invalid asset window state filter")

      {:error, :invalid_pagination} ->
        error(conn, 422, "validation_failed", "Invalid pagination parameters")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, {:manifest_filter_lookup_failed, reason}} ->
        Logger.error("asset_window_state.filter_lookup failed: #{inspect(reason)}")
        error(conn, 400, "bad_request", "Request failed")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/api/orchestrator/v1/streams/runs" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, global_sequence} <- Cursor.global(header(conn, "last-event-id")) do
      SSE.stream(conn, {:global, global_sequence})
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :invalid_last_event_id} ->
        error(conn, 400, "validation_failed", "Invalid Last-Event-ID header")

      {:error, :cursor_invalid} ->
        error(conn, 410, "cursor_expired", "Cursor is invalid or no longer replayable")

      {:error, _reason} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")
    end
  end

  get "/api/orchestrator/v1/streams/runs/:run_id" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, _run} <- FavnOrchestrator.get_run(run_id),
         {:ok, sequence} <- Cursor.run(header(conn, "last-event-id"), run_id) do
      SSE.stream(conn, {:run, run_id, sequence})
    else
      {:error, :not_found} ->
        error(conn, 404, "not_found", "Run was not found")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :invalid_last_event_id} ->
        error(conn, 400, "validation_failed", "Invalid Last-Event-ID header")

      {:error, :cursor_invalid} ->
        error(conn, 410, "cursor_expired", "Cursor is invalid or no longer replayable")

      {:error, _reason} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")
    end
  end

  get "/api/orchestrator/v1/audit" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :admin) do
      data(conn, 200, %{items: Auth.list_audit(limit: 200) |> Enum.map(&DTO.audit/1)})
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, _reason} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")
    end
  end

  forward("/api/orchestrator/v1/actors", to: ActorsRouter)

  match _ do
    error(conn, 404, "not_found", "Route was not found")
  end

  defp ensure_service_auth(conn), do: Authentication.ensure_service(conn)

  defp service_token_diagnostics(conn), do: Authentication.service_token_diagnostics(conn)

  defp ensure_actor_context(conn, required_role),
    do: Authentication.actor_context(conn, required_role)

  defp header(conn, key) do
    case get_req_header(conn, key) do
      [value | _] -> value
      _ -> nil
    end
  end

  defp data(conn, status, payload), do: Response.data(conn, status, payload)

  defp page_response(page, mapper) when is_function(mapper, 1) do
    Response.page(page, mapper)
  end

  defp error(conn, status, code, message, details \\ %{}) do
    Response.error(conn, status, code, message, details)
  end
end
