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
  alias FavnOrchestrator.API.CoverageRouter
  alias FavnOrchestrator.API.DTO
  alias FavnOrchestrator.API.ExecutionPackagesRouter
  alias FavnOrchestrator.API.ManifestPublication
  alias FavnOrchestrator.API.MutationAdmission
  alias FavnOrchestrator.API.ManifestsRouter
  alias FavnOrchestrator.API.Parsers
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.API.RunsRouter
  alias FavnOrchestrator.API.SchedulesRouter
  alias FavnOrchestrator.API.SSE
  alias FavnOrchestrator.API.SSE.Cursor
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Manifests
  alias FavnOrchestrator.Runs

  plug(Plug.RequestId)

  if Mix.env() == :dev and Code.ensure_loaded?(Tidewave) do
    plug(Tidewave)
  end

  plug(MutationAdmission)

  plug(ManifestPublication)

  plug(Parsers)

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
         {:ok, summary} <- bootstrap_active_manifest(conn) do
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

  post "/api/orchestrator/v1/maintenance/runner-replacement" do
    with {:ok, _context} <- Authentication.platform_context(conn, :platform_operator),
         token when is_binary(token) and token != "" <- header(conn, "x-favn-maintenance-token"),
         {:ok, token} <- FavnOrchestrator.begin_runner_replacement(token) do
      data(conn, 200, %{status: "draining", maintenance_token: token})
    else
      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Platform operator access is required")

      {:error, :maintenance_active} ->
        error(conn, 409, "maintenance_active", "Runner replacement is already active")

      {:error, :invalid_maintenance_token} ->
        error(conn, 422, "invalid_maintenance_token", "A valid maintenance token is required")

      nil ->
        error(conn, 422, "invalid_maintenance_token", "A valid maintenance token is required")

      {:error, _reason} ->
        error(conn, 503, "runtime_not_accepting", "Control plane is not accepting maintenance")
    end
  end

  get "/api/orchestrator/v1/maintenance/runner-replacement" do
    with {:ok, _context} <- Authentication.platform_context(conn, :platform_operator) do
      data(conn, 200, FavnOrchestrator.runner_replacement_status())
    else
      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Platform operator access is required")
    end
  end

  post "/api/orchestrator/v1/maintenance/runner-replacement/verify-runner" do
    expected_release_id = conn.body_params["runner_release_id"]

    with {:ok, _context} <- Authentication.platform_context(conn, :platform_operator),
         true <- is_binary(expected_release_id) and expected_release_id != "",
         {:ok, verified} <- FavnOrchestrator.verify_replacement_runner(expected_release_id) do
      data(conn, 200, verified)
    else
      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Platform operator access is required")

      false ->
        error(conn, 422, "validation_failed", "runner_release_id is required")

      {:error, reason} ->
        error(conn, 409, "runner_not_aligned", "Runner verification failed", %{
          reason: runner_verification_reason(reason)
        })
    end
  end

  delete "/api/orchestrator/v1/maintenance/runner-replacement" do
    with {:ok, _context} <- Authentication.platform_context(conn, :platform_operator),
         token when is_binary(token) and token != "" <- header(conn, "x-favn-maintenance-token"),
         :ok <- FavnOrchestrator.finish_runner_replacement(token) do
      data(conn, 200, %{status: "accepting"})
    else
      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Platform operator access is required")

      _invalid ->
        error(conn, 403, "invalid_maintenance_token", "Maintenance ownership is required")
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

  forward("/api/orchestrator/v1/execution-packages", to: ExecutionPackagesRouter)

  forward("/api/orchestrator/v1/runs", to: RunsRouter)

  forward("/api/orchestrator/v1/schedules", to: SchedulesRouter)

  forward("/api/orchestrator/v1/backfills", to: BackfillsRouter)

  forward("/api/orchestrator/v1/coverage", to: CoverageRouter)

  get "/api/orchestrator/v1/streams/runs" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor, context} <- stream_actor_context(conn),
         {:ok, global_sequence} <- Cursor.global(header(conn, "last-event-id")) do
      stream(conn, context, {:global, global_sequence})
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
         {:ok, _session, _actor, context} <- stream_actor_context(conn),
         {:ok, _run} <- get_stream_run(context, run_id),
         {:ok, sequence} <- Cursor.run(header(conn, "last-event-id"), run_id) do
      stream(conn, context, {:run, run_id, sequence})
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
         {:ok, _session, _actor, context} <- Authentication.workspace_context(conn, :admin),
         {:ok, page} <- Auth.page_audit(context, limit: 200) do
      data(conn, 200, %{
        items: Enum.map(page.items, &DTO.audit/1),
        next_cursor: page.next_cursor
      })
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

  defp stream_actor_context(conn), do: Authentication.workspace_context(conn, :viewer)

  defp get_stream_run(context, run_id), do: Runs.get(context, run_id)

  defp stream(conn, context, stream), do: SSE.stream(conn, context, stream)

  defp bootstrap_active_manifest(conn) do
    with {:ok, _session, _actor, context} <- Authentication.service_workspace_context(conn),
         {:ok, %{manifest: summary}} <- Manifests.active(context) do
      {:ok, summary}
    end
  end

  defp header(conn, key) do
    case get_req_header(conn, key) do
      [value | _] -> value
      _ -> nil
    end
  end

  defp runner_verification_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp runner_verification_reason(_reason), do: "runner_verification_failed"

  defp data(conn, status, payload), do: Response.data(conn, status, payload)

  defp error(conn, status, code, message, details \\ %{}) do
    Response.error(conn, status, code, message, details)
  end
end
