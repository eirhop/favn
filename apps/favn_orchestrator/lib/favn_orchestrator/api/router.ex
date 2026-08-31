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
  alias FavnOrchestrator.API.ManifestDeployment
  alias FavnOrchestrator.API.MutationAdmission
  alias FavnOrchestrator.API.ManifestsRouter
  alias FavnOrchestrator.API.Parsers
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.API.RunsRouter
  alias FavnOrchestrator.API.RebuildsRouter
  alias FavnOrchestrator.API.SchedulesRouter
  alias FavnOrchestrator.API.TargetRecoveriesRouter
  alias FavnOrchestrator.API.SSE
  alias FavnOrchestrator.API.SSE.Cursor
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Manifests
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.RunnerCapacity
  alias FavnOrchestrator.RunnerDemandLimiter
  alias FavnOrchestrator.Runs

  plug(Plug.RequestId, assign_as: :request_id)

  if Mix.env() == :dev and Code.ensure_loaded?(Tidewave) do
    plug(Tidewave)
  end

  plug(MutationAdmission)

  plug(ManifestDeployment)

  plug(ManifestPublication)

  plug(Parsers)

  plug(:match)
  plug(:dispatch)

  get "/internal/runner-demand/:runner_pool/:runner_release_id" do
    serve_runner_demand(conn, runner_pool, runner_release_id, :json)
  end

  get "/internal/runner-demand/:runner_pool/:runner_release_id/metrics" do
    serve_runner_demand(conn, runner_pool, runner_release_id, :openmetrics)
  end

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

  get "/api/orchestrator/v1/runner-capacity" do
    with {:ok, context} <- Authentication.platform_context(conn, :platform_reader),
         {:ok, diagnostics} <- RunnerCapacity.diagnostics(context) do
      data(conn, 200, DTO.normalize(diagnostics))
    else
      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Platform reader access is required")

      {:error, reason} ->
        Logger.error("runner_capacity.diagnostics failed: #{inspect(reason)}")
        error(conn, 503, "capacity_unavailable", "Runner capacity is unavailable")
    end
  end

  get "/api/orchestrator/v1/runner-capacity/:pool/:release_id" do
    with {:ok, context} <- Authentication.platform_context(conn, :platform_reader),
         {:ok, diagnostics} <- RunnerCapacity.drain(context, pool, release_id) do
      data(conn, 200, DTO.normalize(diagnostics))
    else
      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Platform reader access is required")

      {:error, %Error{kind: :unavailable}} ->
        error(conn, 503, "capacity_unavailable", "Runner release drain is unavailable")

      {:error, _reason} ->
        error(conn, 422, "validation_failed", "Runner pool or release is invalid")
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

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Platform operator access is required")

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

  forward("/api/orchestrator/v1/execution-packages", to: ExecutionPackagesRouter)

  forward("/api/orchestrator/v1/runs", to: RunsRouter)

  forward("/api/orchestrator/v1/schedules", to: SchedulesRouter)

  forward("/api/orchestrator/v1/backfills", to: BackfillsRouter)

  forward("/api/orchestrator/v1/coverage", to: CoverageRouter)

  forward("/api/orchestrator/v1/rebuilds", to: RebuildsRouter)
  forward("/api/orchestrator/v1/target-recoveries", to: TargetRecoveriesRouter)

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
      {:error, %Error{kind: :not_found}} ->
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

  defp serve_runner_demand(conn, pool, release_id, format) do
    started_at = System.monotonic_time()

    result =
      with {:ok, context, identity} <- Authentication.capacity_context(conn),
           true <- RunnerDemandLimiter.allow?(identity),
           {:ok, demand} <- RunnerCapacity.demand(context, pool, release_id) do
        {:ok, demand}
      else
        false -> {:error, :rate_limited}
        {:error, reason} -> {:error, reason}
      end

    emit_runner_demand_read(started_at, pool, release_id, result)
    conn = Plug.Conn.put_resp_header(conn, "cache-control", "no-store")

    case result do
      {:ok, demand} when format == :json ->
        conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.send_resp(200, Jason.encode!(%{outstanding: demand.outstanding_count}))

      {:ok, demand} ->
        labels =
          ~s(runner_pool="#{pool}",runner_release_id="#{release_id}")

        body =
          "# TYPE favn_runner_outstanding gauge\n" <>
            "favn_runner_outstanding{#{labels}} #{demand.outstanding_count}\n" <>
            "# EOF\n"

        conn
        |> Plug.Conn.put_resp_content_type("application/openmetrics-text; version=1.0.0")
        |> Plug.Conn.send_resp(200, body)

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Capacity reader access is required")

      {:error, :rate_limited} ->
        error(conn, 429, "rate_limited", "Capacity read rate limit exceeded")

      {:error, {:runner_pool_not_configured, _pool}} ->
        error(conn, 404, "not_found", "Runner pool is not configured")

      {:error, %Error{kind: :unavailable}} ->
        error(conn, 503, "capacity_unavailable", "Runner capacity is unavailable")

      {:error, _reason} ->
        error(conn, 422, "validation_failed", "Runner pool or release is invalid")
    end
  end

  defp emit_runner_demand_read(started_at, pool, release_id, result) do
    duration = System.monotonic_time() - started_at

    FavnOrchestrator.Telemetry.emit(
      :runner_demand_read,
      %{duration: duration, outstanding: demand_outstanding(result)},
      Map.merge(
        %{
          partition_status: runner_demand_partition_status(result),
          outcome: if(match?({:ok, _demand}, result), do: :ok, else: :error)
        },
        known_runner_demand_partition(result, pool, release_id)
      )
    )
  end

  defp known_runner_demand_partition({:ok, _demand}, pool, release_id),
    do: %{runner_pool: pool, runner_release_id: release_id}

  defp known_runner_demand_partition(_result, _pool, _release_id), do: %{}

  defp runner_demand_partition_status({:ok, _demand}), do: :known

  defp runner_demand_partition_status({:error, {:runner_pool_not_configured, _pool}}),
    do: :unknown_pool

  defp runner_demand_partition_status({:error, %Error{kind: :invalid}}), do: :malformed
  defp runner_demand_partition_status({:error, _reason}), do: :unavailable

  defp demand_outstanding({:ok, demand}), do: demand.outstanding_count
  defp demand_outstanding(_result), do: 0

  defp data(conn, status, payload), do: Response.data(conn, status, payload)

  defp error(conn, status, code, message, details \\ %{}) do
    Response.error(conn, status, code, message, details)
  end
end
