defmodule FavnOrchestrator.API.Router do
  @moduledoc """
  Private orchestrator HTTP API v1.
  """

  use Plug.Router

  require Logger

  alias Favn.Manifest.Version
  alias Favn.Window.Policy
  alias Favn.Window.Request, as: WindowRequest
  alias FavnOrchestrator
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline

  plug(Plug.RequestId)

  plug(Plug.Parsers,
    parsers: [:urlencoded, :multipart, :json],
    pass: ["*/*"],
    json_decoder: Jason
  )

  plug(:match)
  plug(:dispatch)

  get "/api/orchestrator/v1/health" do
    data(conn, 200, %{status: "ok"})
  end

  post "/api/orchestrator/v1/auth/password/sessions" do
    with :ok <- ensure_service_auth(conn),
         {:ok, params} <- fetch_json_body(conn),
         {:ok, username} <- fetch_required_string(params, "username"),
         {:ok, password} <- fetch_required_string(params, "password"),
         {:ok, session, actor} <- Auth.password_login(username, password) do
      Auth.put_audit(%{
        action: "auth.password.login",
        actor_id: actor.id,
        session_id: session.id,
        outcome: "accepted",
        service_identity: service_identity(conn)
      })

      data(conn, 201, %{session: session_dto(session), actor: actor_dto(actor)})
    else
      {:error, :invalid_credentials} ->
        error(conn, 401, "unauthenticated", "Invalid username or password")

      {:error, {:missing_field, field}} ->
        error(conn, 422, "validation_failed", "Missing required field", %{field: field})

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")
    end
  end

  post "/api/orchestrator/v1/auth/sessions/introspect" do
    with :ok <- ensure_service_auth(conn),
         {:ok, params} <- fetch_json_body(conn),
         {:ok, session_id} <- fetch_required_string(params, "session_id"),
         {:ok, session, actor} <- Auth.introspect_session(session_id) do
      data(conn, 200, %{session: session_dto(session), actor: actor_dto(actor)})
    else
      {:error, :invalid_session} ->
        error(conn, 401, "unauthenticated", "Session is invalid")

      {:error, :actor_not_found} ->
        error(conn, 404, "not_found", "Actor not found")

      {:error, {:missing_field, field}} ->
        error(conn, 422, "validation_failed", "Missing required field", %{field: field})

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")
    end
  end

  post "/api/orchestrator/v1/auth/sessions/:session_id/revoke" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, actor} <- ensure_actor_context(conn, :admin) do
      :ok = Auth.revoke_session(session_id)

      Auth.put_audit(%{
        action: "auth.session.revoke",
        actor_id: actor.id,
        session_id: session_id,
        outcome: "accepted",
        service_identity: service_identity(conn)
      })

      data(conn, 200, %{revoked: true, session_id: session_id})
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, _reason} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")
    end
  end

  get "/api/orchestrator/v1/me" do
    with :ok <- ensure_service_auth(conn),
         {:ok, session, actor} <- ensure_actor_context(conn, :viewer) do
      data(conn, 200, %{session: session_dto(session), actor: actor_dto(actor)})
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, _reason} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")
    end
  end

  get "/api/orchestrator/v1/manifests" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, manifests} <- FavnOrchestrator.list_manifest_summaries() do
      data(conn, 200, %{items: manifests})
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, :active_manifest_not_set} ->
        error(conn, 404, "not_found", "Active manifest is not set")

      {:error, reason} ->
        Logger.error("manifest.list failed: #{inspect(reason)}")
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  post "/api/orchestrator/v1/manifests" do
    with :ok <- ensure_service_auth(conn),
         {:ok, params} <- fetch_json_body(conn),
         {:ok, version} <- build_manifest_version(params),
         :ok <- FavnOrchestrator.register_manifest(version),
         {:ok, summary} <- FavnOrchestrator.get_manifest_summary(version.manifest_version_id) do
      Auth.put_audit(%{
        action: "manifest.register",
        session_id: header(conn, "x-favn-session-id"),
        resource_type: "manifest",
        resource_id: version.manifest_version_id,
        outcome: "accepted",
        service_identity: service_identity(conn)
      })

      data(conn, 201, %{manifest: summary})
    else
      {:error, {:missing_field, field}} ->
        error(conn, 422, "validation_failed", "Missing required field", %{field: field})

      {:error, {:invalid_manifest_version_id, _value}} ->
        error(conn, 422, "validation_failed", "Invalid manifest version id")

      {:error, {:invalid_content_hash, _value}} ->
        error(conn, 422, "validation_failed", "Invalid manifest content hash")

      {:error, {:manifest_content_hash_mismatch, _expected, _computed}} ->
        error(conn, 422, "validation_failed", "Manifest content hash does not match payload")

      {:error, {:manifest_schema_version_mismatch, _expected, _actual}} ->
        error(conn, 422, "validation_failed", "Manifest schema version does not match payload")

      {:error, {:manifest_runner_contract_version_mismatch, _expected, _actual}} ->
        error(
          conn,
          422,
          "validation_failed",
          "Manifest runner contract version does not match payload"
        )

      {:error, :manifest_version_conflict} ->
        error(
          conn,
          409,
          "manifest_conflict",
          "Manifest version id already exists with different content"
        )

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, reason} ->
        Logger.error("manifest.register failed: #{inspect(reason)}")
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/api/orchestrator/v1/manifests/active" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, manifest_version_id} <- FavnOrchestrator.active_manifest(),
         {:ok, summary} <- FavnOrchestrator.get_manifest_summary(manifest_version_id),
         {:ok, targets} <- FavnOrchestrator.manifest_targets(manifest_version_id) do
      data(conn, 200, %{manifest: summary, targets: targets})
    else
      {:error, :active_manifest_not_set} ->
        error(conn, 404, "not_found", "Active manifest is not set")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/api/orchestrator/v1/manifests/:manifest_version_id" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, summary} <- FavnOrchestrator.get_manifest_summary(manifest_version_id),
         {:ok, targets} <- FavnOrchestrator.manifest_targets(manifest_version_id) do
      data(conn, 200, %{manifest: summary, targets: targets})
    else
      {:error, :manifest_version_not_found} ->
        error(conn, 404, "not_found", "Manifest version was not found")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  post "/api/orchestrator/v1/manifests/:manifest_version_id/activate" do
    with :ok <- ensure_service_auth(conn),
         {:ok, actor_id, session_id} <- ensure_activation_context(conn),
         :ok <- FavnOrchestrator.activate_manifest(manifest_version_id),
         :ok <- maybe_reload_scheduler() do
      Auth.put_audit(%{
        action: "manifest.activate",
        actor_id: actor_id,
        session_id: session_id,
        resource_type: "manifest",
        resource_id: manifest_version_id,
        outcome: "accepted",
        service_identity: service_identity(conn)
      })

      data(conn, 200, %{activated: true, manifest_version_id: manifest_version_id})
    else
      {:error, :manifest_version_not_found} ->
        error(conn, 404, "not_found", "Manifest version was not found")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/api/orchestrator/v1/runs/in-flight" do
    with :ok <- ensure_service_auth(conn),
         {:ok, runs} <- FavnOrchestrator.list_runs(limit: 500) do
      running_ids =
        runs
        |> Enum.filter(&(&1.status == :running))
        |> Enum.map(& &1.id)

      data(conn, 200, %{count: length(running_ids), run_ids: running_ids})
    else
      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/api/orchestrator/v1/runs" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, runs} <- FavnOrchestrator.list_runs(limit: 100) do
      data(conn, 200, %{items: Enum.map(runs, &run_summary_dto/1)})
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/api/orchestrator/v1/runs/:run_id" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, run} <- FavnOrchestrator.get_run(run_id) do
      data(conn, 200, %{run: run_detail_dto(run)})
    else
      {:error, :not_found} ->
        error(conn, 404, "not_found", "Run was not found")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/api/orchestrator/v1/runs/:run_id/events" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, events} <- FavnOrchestrator.list_run_events(run_id, run_event_opts(conn.params)) do
      data(conn, 200, %{items: Enum.map(events, &run_event_dto/1)})
    else
      {:error, :invalid_opts} ->
        error(conn, 422, "validation_failed", "Invalid event query options")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/api/orchestrator/v1/schedules" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, schedules} <- FavnOrchestrator.list_schedule_entries() do
      data(conn, 200, %{items: Enum.map(schedules, &schedule_dto/1)})
    else
      {:error, :active_manifest_not_set} ->
        error(conn, 404, "not_found", "Active manifest is not set")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/api/orchestrator/v1/schedules/:schedule_id" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, schedule} <- FavnOrchestrator.get_schedule_entry(schedule_id) do
      data(conn, 200, %{schedule: schedule_dto(schedule)})
    else
      {:error, :active_manifest_not_set} ->
        error(conn, 404, "not_found", "Active manifest is not set")

      {:error, :schedule_not_found} ->
        error(conn, 404, "not_found", "Schedule was not found")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  post "/api/orchestrator/v1/runs" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, actor} <- ensure_actor_context(conn, :operator),
         {:ok, params} <- fetch_json_body(conn),
         {:ok, run_id} <- submit_run_from_request(params),
         {:ok, run} <- FavnOrchestrator.get_run(run_id) do
      Auth.put_audit(%{
        action: "run.submit",
        actor_id: actor.id,
        session_id: header(conn, "x-favn-session-id"),
        resource_type: "run",
        resource_id: run_id,
        outcome: "accepted",
        service_identity: service_identity(conn)
      })

      data(conn, 201, %{run: run_summary_dto(run)})
    else
      {:error, :invalid_target} ->
        error(conn, 422, "validation_failed", "Invalid run target request")

      {:error, :invalid_manifest_selection} ->
        error(conn, 422, "validation_failed", "Invalid manifest selection")

      {:error, :invalid_dependencies} ->
        error(conn, 422, "validation_failed", "Invalid dependency mode")

      {:error, :invalid_asset_target} ->
        error(conn, 422, "validation_failed", "Invalid asset target id")

      {:error, :invalid_pipeline_target} ->
        error(conn, 422, "validation_failed", "Invalid pipeline target id")

      {:error, reason} when is_tuple(reason) ->
        maybe_window_policy_error(conn, reason)

      {:error, :active_manifest_not_set} ->
        error(conn, 404, "not_found", "Active manifest is not set")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  post "/api/orchestrator/v1/runs/:run_id/cancel" do
    with :ok <- ensure_service_auth(conn),
         {:ok, actor_id, session_id} <- ensure_operator_context(conn),
         :ok <- FavnOrchestrator.cancel_run(run_id, %{actor_id: actor_id}) do
      Auth.put_audit(%{
        action: "run.cancel",
        actor_id: actor_id,
        session_id: session_id,
        resource_type: "run",
        resource_id: run_id,
        outcome: "accepted",
        service_identity: service_identity(conn)
      })

      data(conn, 200, %{cancelled: true, run_id: run_id})
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :not_found} ->
        error(conn, 404, "not_found", "Run was not found")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  post "/api/orchestrator/v1/runs/:run_id/rerun" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, actor} <- ensure_actor_context(conn, :operator),
         {:ok, rerun_id} <- FavnOrchestrator.rerun(run_id),
         {:ok, rerun_run} <- FavnOrchestrator.get_run(rerun_id) do
      Auth.put_audit(%{
        action: "run.rerun",
        actor_id: actor.id,
        session_id: header(conn, "x-favn-session-id"),
        resource_type: "run",
        resource_id: rerun_id,
        outcome: "accepted",
        service_identity: service_identity(conn)
      })

      data(conn, 201, %{run: run_summary_dto(rerun_run)})
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :not_found} ->
        error(conn, 404, "not_found", "Run was not found")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  post "/api/orchestrator/v1/backfills" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, actor} <- ensure_actor_context(conn, :operator),
         {:ok, params} <- fetch_json_body(conn),
         {:ok, run_id} <- submit_backfill_from_request(params),
         {:ok, run} <- FavnOrchestrator.get_run(run_id) do
      Auth.put_audit(%{
        action: "backfill.submit",
        actor_id: actor.id,
        session_id: header(conn, "x-favn-session-id"),
        resource_type: "run",
        resource_id: run_id,
        outcome: "accepted",
        service_identity: service_identity(conn)
      })

      data(conn, 201, %{run: run_summary_dto(run)})
    else
      {:error, :invalid_target} ->
        error(conn, 422, "validation_failed", "Invalid backfill target request")

      {:error, :invalid_manifest_selection} ->
        error(conn, 422, "validation_failed", "Invalid manifest selection")

      {:error, :invalid_backfill_range_request} ->
        error(conn, 422, "validation_failed", "Invalid backfill range request")

      {:error, reason} when is_tuple(reason) ->
        maybe_backfill_range_error(conn, reason)

      {:error, :active_manifest_not_set} ->
        error(conn, 404, "not_found", "Active manifest is not set")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/api/orchestrator/v1/backfills/:backfill_run_id/windows" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, windows} <-
           FavnOrchestrator.list_backfill_windows(
             backfill_window_filters(conn.params, backfill_run_id)
           ) do
      data(conn, 200, %{items: Enum.map(windows, &backfill_window_dto/1)})
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  post "/api/orchestrator/v1/backfills/:backfill_run_id/windows/rerun" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, actor} <- ensure_actor_context(conn, :operator),
         {:ok, params} <- fetch_json_body(conn),
         {:ok, window_key} <- fetch_required_string(params, "window_key"),
         {:ok, window} <- find_backfill_window(backfill_run_id, window_key),
         {:ok, rerun_id} <-
           FavnOrchestrator.rerun_backfill_window(
             backfill_run_id,
             window.pipeline_module,
             window.window_key
           ),
         {:ok, run} <- FavnOrchestrator.get_run(rerun_id) do
      Auth.put_audit(%{
        action: "backfill.window.rerun",
        actor_id: actor.id,
        session_id: header(conn, "x-favn-session-id"),
        resource_type: "run",
        resource_id: rerun_id,
        outcome: "accepted",
        service_identity: service_identity(conn)
      })

      data(conn, 201, %{run: run_summary_dto(run)})
    else
      {:error, {:missing_field, field}} ->
        error(conn, 422, "validation_failed", "Missing required field", %{field: field})

      {:error, :not_found} ->
        error(conn, 404, "not_found", "Backfill window was not found")

      {:error, :backfill_window_not_rerunnable} ->
        error(conn, 409, "conflict", "Backfill window is not rerunnable")

      {:error, :backfill_window_has_no_attempt} ->
        error(conn, 409, "conflict", "Backfill window has no attempt to rerun")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/api/orchestrator/v1/backfills/coverage-baselines" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, baselines} <-
           FavnOrchestrator.list_coverage_baselines(coverage_baseline_filters(conn.params)) do
      data(conn, 200, %{items: Enum.map(baselines, &coverage_baseline_dto/1)})
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/api/orchestrator/v1/assets/window-states" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, filters} <- asset_window_state_filters(conn.params),
         {:ok, states} <- FavnOrchestrator.list_asset_window_states(filters) do
      data(conn, 200, %{items: Enum.map(states, &asset_window_state_dto/1)})
    else
      {:error, :invalid_asset_ref} ->
        error(conn, 422, "validation_failed", "Invalid asset ref filter")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/api/orchestrator/v1/streams/runs" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, last_event_id} <- validate_last_event_id(header(conn, "last-event-id")) do
      sse_ready(conn, "runs", last_event_id)
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :invalid_last_event_id} ->
        error(conn, 422, "validation_failed", "Invalid Last-Event-ID header")

      {:error, _reason} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")
    end
  end

  get "/api/orchestrator/v1/streams/runs/:run_id" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, _run} <- FavnOrchestrator.get_run(run_id),
         {:ok, last_event_id} <- validate_last_event_id(header(conn, "last-event-id")),
         {:ok, sequence} <- parse_run_cursor(last_event_id, run_id),
         {:ok, events} <-
           FavnOrchestrator.list_run_stream_events(run_id, after_sequence: sequence, limit: 200) do
      sse_run_stream(conn, run_id, events)
    else
      {:error, :not_found} ->
        error(conn, 404, "not_found", "Run was not found")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :invalid_last_event_id} ->
        error(conn, 422, "validation_failed", "Invalid Last-Event-ID header")

      {:error, :cursor_invalid} ->
        error(conn, 422, "cursor_invalid", "Cursor is invalid or no longer replayable")

      {:error, _reason} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")
    end
  end

  get "/api/orchestrator/v1/audit" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :admin) do
      data(conn, 200, %{items: Auth.list_audit(limit: 200) |> Enum.map(&audit_dto/1)})
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, _reason} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")
    end
  end

  get "/api/orchestrator/v1/actors" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :admin) do
      data(conn, 200, %{items: Auth.list_actors() |> Enum.map(&actor_dto/1)})
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, _reason} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")
    end
  end

  post "/api/orchestrator/v1/actors" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, actor} <- ensure_actor_context(conn, :admin),
         {:ok, params} <- fetch_json_body(conn),
         {:ok, username} <- fetch_required_string(params, "username"),
         {:ok, password} <- fetch_required_string(params, "password"),
         {:ok, display_name} <- fetch_actor_display_name(params, username),
         {:ok, roles} <- fetch_actor_roles(params),
         {:ok, created_actor} <- Auth.create_actor(username, password, display_name, roles) do
      Auth.put_audit(%{
        action: "actor.create",
        actor_id: actor.id,
        session_id: header(conn, "x-favn-session-id"),
        resource_type: "actor",
        resource_id: created_actor.id,
        outcome: "accepted",
        service_identity: service_identity(conn)
      })

      data(conn, 201, %{actor: actor_dto(created_actor)})
    else
      {:error, {:missing_field, field}} ->
        error(conn, 422, "validation_failed", "Missing required field", %{field: field})

      {:error, :invalid_roles} ->
        error(conn, 422, "validation_failed", "Invalid roles")

      {:error, :username_taken} ->
        error(conn, 409, "conflict", "Username already exists")

      {:error, :password_too_short} ->
        error(conn, 422, "validation_failed", "Password is too short")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/api/orchestrator/v1/actors/:actor_id" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :admin),
         {:ok, actor} <- Auth.get_actor(actor_id) do
      data(conn, 200, %{actor: actor_dto(actor)})
    else
      {:error, :actor_not_found} ->
        error(conn, 404, "not_found", "Actor was not found")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, _reason} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")
    end
  end

  put "/api/orchestrator/v1/actors/:actor_id/roles" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, actor} <- ensure_actor_context(conn, :admin),
         {:ok, params} <- fetch_json_body(conn),
         {:ok, roles} <- fetch_required_roles(params, "roles"),
         {:ok, updated_actor} <- Auth.update_actor_roles(actor_id, roles) do
      Auth.put_audit(%{
        action: "actor.roles.update",
        actor_id: actor.id,
        session_id: header(conn, "x-favn-session-id"),
        resource_type: "actor",
        resource_id: actor_id,
        outcome: "accepted",
        service_identity: service_identity(conn)
      })

      data(conn, 200, %{actor: actor_dto(updated_actor)})
    else
      {:error, :actor_not_found} ->
        error(conn, 404, "not_found", "Actor was not found")

      {:error, {:missing_field, field}} ->
        error(conn, 422, "validation_failed", "Missing required field", %{field: field})

      {:error, :invalid_roles} ->
        error(conn, 422, "validation_failed", "Invalid roles")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  put "/api/orchestrator/v1/actors/:actor_id/password" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, actor} <- ensure_actor_context(conn, :admin),
         {:ok, params} <- fetch_json_body(conn),
         {:ok, password} <- fetch_required_string(params, "password"),
         :ok <- Auth.set_actor_password(actor_id, password) do
      Auth.put_audit(%{
        action: "actor.password.set",
        actor_id: actor.id,
        session_id: header(conn, "x-favn-session-id"),
        resource_type: "actor",
        resource_id: actor_id,
        outcome: "accepted",
        service_identity: service_identity(conn)
      })

      data(conn, 200, %{updated: true, actor_id: actor_id})
    else
      {:error, :actor_not_found} ->
        error(conn, 404, "not_found", "Actor was not found")

      {:error, :password_too_short} ->
        error(conn, 422, "validation_failed", "Password is too short")

      {:error, {:missing_field, field}} ->
        error(conn, 422, "validation_failed", "Missing required field", %{field: field})

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  match _ do
    error(conn, 404, "not_found", "Route was not found")
  end

  defp ensure_service_auth(conn) do
    provided = bearer_token(conn)

    valid_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens, [])

    if valid_service_token?(provided, valid_tokens) do
      :ok
    else
      {:error, :service_unauthorized}
    end
  end

  defp ensure_actor_context(conn, required_role) do
    actor_id = header(conn, "x-favn-actor-id")
    session_id = header(conn, "x-favn-session-id")

    case {actor_id, session_id} do
      {id, sid} when is_binary(id) and id != "" and is_binary(sid) and sid != "" ->
        case Auth.actor_from_forwarded_context(id, sid) do
          {:ok, session, actor} ->
            case Auth.has_role?(actor, required_role) do
              true -> {:ok, session, actor}
              false -> {:error, :forbidden}
            end

          {:error, :actor_session_mismatch} ->
            {:error, :unauthenticated}

          {:error, _reason} ->
            {:error, :unauthenticated}
        end

      _other ->
        {:error, :unauthenticated}
    end
  end

  defp ensure_activation_context(conn) do
    case ensure_actor_context(conn, :operator) do
      {:ok, session, actor} -> {:ok, actor.id, session.id}
      {:error, :unauthenticated} -> {:ok, nil, nil}
      {:error, :forbidden} = error -> error
    end
  end

  defp ensure_operator_context(conn) do
    case ensure_actor_context(conn, :operator) do
      {:ok, session, actor} -> {:ok, actor.id, session.id}
      {:error, :unauthenticated} -> {:ok, nil, nil}
      {:error, :forbidden} = error -> error
    end
  end

  defp validate_last_event_id(nil), do: {:ok, nil}

  defp validate_last_event_id(value) when is_binary(value) do
    trimmed = String.trim(value)

    if trimmed == "" do
      {:ok, nil}
    else
      if String.match?(trimmed, ~r/\A[a-zA-Z0-9:_\-\.]{1,128}\z/) do
        {:ok, trimmed}
      else
        {:error, :invalid_last_event_id}
      end
    end
  end

  defp maybe_reload_scheduler do
    case FavnOrchestrator.reload_scheduler() do
      :ok -> :ok
      {:error, :scheduler_not_running} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp parse_run_cursor(nil, _run_id), do: {:ok, 0}

  defp parse_run_cursor(value, run_id) when is_binary(value) and is_binary(run_id) do
    case String.split(value, ":", parts: 3) do
      ["run", cursor_run_id, sequence] ->
        with true <- cursor_run_id == run_id,
             {int, ""} <- Integer.parse(sequence),
             true <- int > 0 do
          {:ok, int}
        else
          _ -> {:error, :cursor_invalid}
        end

      _other ->
        {:error, :cursor_invalid}
    end
  end

  defp valid_service_token?(provided, valid_tokens)
       when is_binary(provided) and is_list(valid_tokens) do
    Enum.any?(valid_tokens, fn token ->
      is_binary(token) and byte_size(token) == byte_size(provided) and
        Plug.Crypto.secure_compare(token, provided)
    end)
  end

  defp valid_service_token?(_provided, _valid_tokens), do: false

  defp bearer_token(conn) do
    case get_req_header(conn, "authorization") do
      ["Bearer " <> token] -> token
      _ -> nil
    end
  end

  defp service_identity(conn) do
    header(conn, "x-favn-service") || "favn_web"
  end

  defp header(conn, key) do
    case get_req_header(conn, key) do
      [value | _] -> value
      _ -> nil
    end
  end

  defp fetch_json_body(conn) do
    {:ok, conn.body_params}
  end

  defp fetch_required_string(params, key) do
    case Map.get(params, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:missing_field, key}}
    end
  end

  defp build_manifest_version(params) when is_map(params) do
    with %{} = manifest <- Map.get(params, "manifest"),
         opts <- manifest_version_opts(params),
         {:ok, version} <- Version.from_published(manifest, opts) do
      {:ok, version}
    else
      nil -> {:error, {:missing_field, "manifest"}}
      {:error, _reason} = error -> error
      _other -> {:error, {:missing_field, "manifest"}}
    end
  end

  defp manifest_version_opts(params) when is_map(params) do
    []
    |> put_manifest_version_opt(:manifest_version_id, Map.get(params, "manifest_version_id"))
    |> put_manifest_version_opt(:content_hash, Map.get(params, "content_hash"))
    |> put_manifest_version_opt(:schema_version, Map.get(params, "schema_version"))
    |> put_manifest_version_opt(
      :runner_contract_version,
      Map.get(params, "runner_contract_version")
    )
    |> put_manifest_version_opt(:serialization_format, Map.get(params, "serialization_format"))
  end

  defp put_manifest_version_opt(opts, key, value) when is_integer(value),
    do: Keyword.put(opts, key, value)

  defp put_manifest_version_opt(opts, key, value) when is_binary(value) and value != "",
    do: Keyword.put(opts, key, value)

  defp put_manifest_version_opt(opts, _key, _value), do: opts

  defp fetch_actor_display_name(params, username) when is_map(params) and is_binary(username) do
    case Map.get(params, "display_name") do
      nil -> {:ok, username}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:missing_field, "display_name"}}
    end
  end

  defp fetch_actor_roles(params) when is_map(params) do
    case Map.get(params, "roles", ["viewer"]) do
      roles when is_list(roles) ->
        normalized =
          Enum.map(roles, fn
            value when is_binary(value) -> String.trim(value)
            value when is_atom(value) -> Atom.to_string(value)
            _ -> ""
          end)

        if normalized != [] and Enum.all?(normalized, &(&1 in ["viewer", "operator", "admin"])) do
          {:ok, normalized}
        else
          {:error, :invalid_roles}
        end

      _ ->
        {:error, :invalid_roles}
    end
  end

  defp fetch_required_roles(params, key) when is_map(params) and is_binary(key) do
    case Map.get(params, key) do
      roles when is_list(roles) ->
        case fetch_actor_roles(%{"roles" => roles}) do
          {:ok, value} -> {:ok, value}
          {:error, :invalid_roles} -> {:error, :invalid_roles}
        end

      _ ->
        {:error, {:missing_field, key}}
    end
  end

  defp submit_run_from_request(params) do
    with {:ok, target} <- fetch_target(params),
         {:ok, manifest_version_id} <- select_manifest_version(params),
         {:ok, dependencies} <- fetch_dependencies(params, target),
         {:ok, window_request} <- fetch_window_request(params, target) do
      case target do
        %{type: "asset", id: target_id} ->
          FavnOrchestrator.submit_asset_run_for_manifest(manifest_version_id, target_id,
            dependencies: dependencies
          )

        %{type: "pipeline", id: target_id} ->
          FavnOrchestrator.submit_pipeline_run_for_manifest(manifest_version_id, target_id,
            window_request: window_request
          )

        _ ->
          {:error, :invalid_target}
      end
    end
  end

  defp submit_backfill_from_request(params) do
    with {:ok, %{type: "pipeline", id: target_id}} <- fetch_target(params),
         {:ok, manifest_version_id} <- select_manifest_version(params),
         {:ok, range_request} <- fetch_backfill_range_request(params) do
      FavnOrchestrator.submit_pipeline_backfill_for_manifest(
        manifest_version_id,
        target_id,
        backfill_submit_opts(params, range_request)
      )
    else
      {:ok, _target} -> {:error, :invalid_target}
      {:error, _reason} = error -> error
    end
  end

  defp fetch_backfill_range_request(params) when is_map(params) do
    case Map.get(params, "range") || Map.get(params, "range_request") do
      %{} = range -> {:ok, range}
      nil -> {:error, :invalid_backfill_range_request}
      _other -> {:error, :invalid_backfill_range_request}
    end
  end

  defp backfill_submit_opts(params, range_request) when is_map(params) do
    []
    |> Keyword.put(:range_request, range_request)
    |> maybe_put_string_opt(:coverage_baseline_id, Map.get(params, "coverage_baseline_id"))
    |> maybe_put_map_opt(:metadata, Map.get(params, "metadata"))
    |> maybe_put_positive_int_opt(:max_attempts, Map.get(params, "max_attempts"))
    |> maybe_put_non_neg_int_opt(:retry_backoff_ms, Map.get(params, "retry_backoff_ms"))
    |> maybe_put_positive_int_opt(:timeout_ms, Map.get(params, "timeout_ms"))
  end

  defp backfill_window_filters(params, backfill_run_id) when is_map(params) do
    []
    |> Keyword.put(:backfill_run_id, backfill_run_id)
    |> maybe_put_existing_atom_opt(:pipeline_module, Map.get(params, "pipeline_module"))
    |> maybe_put_string_opt(:window_key, Map.get(params, "window_key"))
    |> maybe_put_atom_opt(:status, Map.get(params, "status"))
  end

  defp coverage_baseline_filters(params) when is_map(params) do
    []
    |> maybe_put_existing_atom_opt(:pipeline_module, Map.get(params, "pipeline_module"))
    |> maybe_put_string_opt(:source_key, Map.get(params, "source_key"))
    |> maybe_put_string_opt(:segment_key_hash, Map.get(params, "segment_key_hash"))
    |> maybe_put_atom_opt(:status, Map.get(params, "status"))
  end

  defp asset_window_state_filters(params) when is_map(params) do
    with {:ok, opts} <- maybe_put_asset_ref_filters([], params) do
      {:ok,
       opts
       |> maybe_put_existing_atom_opt(:pipeline_module, Map.get(params, "pipeline_module"))
       |> maybe_put_string_opt(:window_key, Map.get(params, "window_key"))
       |> maybe_put_atom_opt(:status, Map.get(params, "status"))}
    end
  end

  defp maybe_put_asset_ref_filters(opts, params) do
    module = Map.get(params, "asset_ref_module")
    name = Map.get(params, "asset_ref_name")

    case {module, name} do
      {nil, nil} ->
        {:ok, opts}

      {module, name} when is_binary(module) and is_binary(name) ->
        with {:ok, module_atom} <- existing_atom(module),
             {:ok, name_atom} <- existing_atom(name) do
          {:ok,
           opts
           |> Keyword.put(:asset_ref_module, module_atom)
           |> Keyword.put(:asset_ref_name, name_atom)}
        else
          {:error, :invalid_existing_atom} -> {:error, :invalid_asset_ref}
        end

      _other ->
        {:error, :invalid_asset_ref}
    end
  end

  defp find_backfill_window(backfill_run_id, window_key) do
    with {:ok, windows} <-
           FavnOrchestrator.list_backfill_windows(
             backfill_run_id: backfill_run_id,
             window_key: window_key
           ) do
      case windows do
        [window] -> {:ok, window}
        [] -> {:error, :not_found}
        [window | _rest] -> {:ok, window}
      end
    end
  end

  defp fetch_window_request(params, %{type: "asset"}) when is_map(params) do
    if Map.has_key?(params, "window") do
      {:error, :invalid_window_request}
    else
      {:ok, nil}
    end
  end

  defp fetch_window_request(params, %{type: "pipeline"}) when is_map(params) do
    case Map.get(params, "window") do
      nil ->
        {:ok, nil}

      %{} = window ->
        WindowRequest.from_value(window)

      _other ->
        {:error, :invalid_window_request}
    end
  end

  defp fetch_dependencies(params, %{type: "pipeline"}) when is_map(params) do
    if Map.has_key?(params, "dependencies") do
      {:error, :invalid_dependencies}
    else
      {:ok, nil}
    end
  end

  defp fetch_dependencies(params, %{type: "asset"}) when is_map(params) do
    case Map.get(params, "dependencies", "all") do
      "all" -> {:ok, :all}
      "none" -> {:ok, :none}
      :all -> {:ok, :all}
      :none -> {:ok, :none}
      _other -> {:error, :invalid_dependencies}
    end
  end

  defp fetch_target(params) when is_map(params) do
    with %{} = target <- Map.get(params, "target"),
         {:ok, type} <- fetch_required_string(target, "type"),
         {:ok, id} <- fetch_required_string(target, "id") do
      case type in ["asset", "pipeline"] do
        true -> {:ok, %{type: type, id: id}}
        false -> {:error, :invalid_target}
      end
    else
      _ -> {:error, :invalid_target}
    end
  end

  defp select_manifest_version(params) do
    selection = Map.get(params, "manifest_selection", %{"mode" => "active"})

    case selection do
      %{"mode" => "active"} ->
        FavnOrchestrator.active_manifest()

      %{"mode" => "version", "manifest_version_id" => manifest_version_id}
      when is_binary(manifest_version_id) and manifest_version_id != "" ->
        {:ok, manifest_version_id}

      _ ->
        {:error, :invalid_manifest_selection}
    end
  end

  defp run_event_opts(params) do
    []
    |> maybe_put_int_opt(:after_sequence, Map.get(params, "after_sequence"))
    |> maybe_put_int_opt(:limit, Map.get(params, "limit"))
  end

  defp maybe_put_int_opt(opts, _key, nil), do: opts

  defp maybe_put_int_opt(opts, key, value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} -> Keyword.put(opts, key, int)
      _ -> opts
    end
  end

  defp maybe_put_int_opt(opts, key, value) when is_integer(value) do
    Keyword.put(opts, key, value)
  end

  defp maybe_put_int_opt(opts, _key, _value), do: opts

  defp maybe_put_string_opt(opts, key, value) when is_binary(value) and value != "",
    do: Keyword.put(opts, key, value)

  defp maybe_put_string_opt(opts, _key, _value), do: opts

  defp maybe_put_map_opt(opts, key, value) when is_map(value), do: Keyword.put(opts, key, value)
  defp maybe_put_map_opt(opts, _key, _value), do: opts

  defp maybe_put_positive_int_opt(opts, key, value) when is_integer(value) and value > 0,
    do: Keyword.put(opts, key, value)

  defp maybe_put_positive_int_opt(opts, _key, _value), do: opts

  defp maybe_put_non_neg_int_opt(opts, key, value) when is_integer(value) and value >= 0,
    do: Keyword.put(opts, key, value)

  defp maybe_put_non_neg_int_opt(opts, _key, _value), do: opts

  defp maybe_put_atom_opt(opts, key, value) when is_binary(value) and value != "" do
    Keyword.put(opts, key, String.to_atom(value))
  end

  defp maybe_put_atom_opt(opts, _key, _value), do: opts

  defp maybe_put_existing_atom_opt(opts, key, value) when is_binary(value) and value != "" do
    case existing_atom(value) do
      {:ok, atom} -> Keyword.put(opts, key, atom)
      {:error, :invalid_existing_atom} -> opts
    end
  end

  defp maybe_put_existing_atom_opt(opts, _key, _value), do: opts

  defp existing_atom(value) when is_binary(value) do
    {:ok, String.to_existing_atom(value)}
  rescue
    ArgumentError -> existing_elixir_module_atom(value)
  end

  defp existing_elixir_module_atom("Elixir." <> _module), do: {:error, :invalid_existing_atom}

  defp existing_elixir_module_atom(value) do
    {:ok, String.to_existing_atom("Elixir." <> value)}
  rescue
    ArgumentError -> {:error, :invalid_existing_atom}
  end

  defp data(conn, status, payload) do
    body = Jason.encode!(%{data: payload})

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, body)
  end

  defp error(conn, status, code, message, details \\ %{}) do
    body =
      Jason.encode!(%{
        error: %{
          code: code,
          message: message,
          status: status,
          request_id: request_id(conn),
          retryable: false,
          details: details
        }
      })

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, body)
  end

  defp maybe_window_policy_error(conn, reason) do
    case window_policy_error(reason) do
      {:ok, message, details} -> error(conn, 422, "validation_failed", message, details)
      :error -> error(conn, 400, "bad_request", "Request failed")
    end
  end

  defp window_policy_error({:missing_window_request, kind}) do
    {:ok, "Pipeline requires an explicit #{kind} window", %{kind: atom_name(kind)}}
  end

  defp window_policy_error({:full_load_not_allowed, kind}) do
    {:ok, "Pipeline does not allow full-load submissions for #{kind} windows",
     %{kind: atom_name(kind)}}
  end

  defp window_policy_error({:window_kind_mismatch, expected, actual}) do
    {:ok, "Window kind #{actual} does not match pipeline policy #{expected}",
     %{expected: atom_name(expected), actual: atom_name(actual)}}
  end

  defp window_policy_error({:window_request_without_policy, kind}) do
    {:ok, "Window request #{kind} was provided for a pipeline without a window policy",
     %{kind: atom_name(kind)}}
  end

  defp window_policy_error({:invalid_window_request, reason}) do
    {:ok, "Invalid window request", %{reason: inspect(reason)}}
  end

  defp window_policy_error({:invalid_window_value, kind, value}) do
    {:ok, "Invalid #{kind} window value", %{kind: atom_name(kind), value: value}}
  end

  defp window_policy_error({:invalid_timezone, timezone}) do
    {:ok, "Invalid window timezone", %{timezone: timezone}}
  end

  defp window_policy_error(_reason), do: :error

  defp maybe_backfill_range_error(conn, reason) do
    case backfill_range_error(reason) do
      {:ok, message, details} -> error(conn, 422, "validation_failed", message, details)
      :error -> error(conn, 400, "bad_request", "Request failed")
    end
  end

  defp backfill_range_error({:invalid_backfill_range_request, value}) do
    {:ok, "Invalid backfill range request", %{value: inspect(value)}}
  end

  defp backfill_range_error({:missing_backfill_reference, _opts}) do
    {:ok, "Backfill range request is missing a relative reference", %{}}
  end

  defp backfill_range_error({:invalid_last_request, value}) do
    {:ok, "Invalid relative backfill range", %{value: inspect(value)}}
  end

  defp backfill_range_error({:invalid_window_policy_kind, kind}) do
    {:ok, "Invalid backfill window kind", %{kind: inspect(kind)}}
  end

  defp backfill_range_error({:invalid_timezone, timezone}) do
    {:ok, "Invalid backfill timezone", %{timezone: timezone}}
  end

  defp backfill_range_error(_reason), do: :error

  defp request_id(conn) do
    case get_resp_header(conn, "x-request-id") do
      [value | _] -> value
      _ -> conn.assigns[:request_id]
    end
  end

  defp sse_ready(conn, stream, last_event_id) do
    cursor = last_event_id || "cursor:0"

    body = sse_ready_body(stream, cursor)

    conn
    |> put_resp_content_type("text/event-stream")
    |> put_resp_header("cache-control", "no-cache")
    |> put_resp_header("x-accel-buffering", "no")
    |> send_resp(200, body)
  end

  defp sse_run_stream(conn, run_id, events) when is_binary(run_id) and is_list(events) do
    latest_cursor =
      case List.last(events) do
        nil -> "cursor:0"
        event -> run_cursor(event)
      end

    body =
      events
      |> Enum.map_join("", &sse_run_event_body(&1, run_id))
      |> Kernel.<>(sse_ready_body("run:" <> run_id, latest_cursor))

    conn
    |> put_resp_content_type("text/event-stream")
    |> put_resp_header("cache-control", "no-cache")
    |> put_resp_header("x-accel-buffering", "no")
    |> send_resp(200, body)
  end

  defp sse_run_event_body(event, run_id) do
    cursor = run_cursor(event)
    event_name = event_name(event.event_type)

    payload =
      Jason.encode!(%{
        schema_version: 1,
        event_id: "evt:" <> event.run_id <> ":" <> Integer.to_string(event.sequence),
        stream: "run:" <> run_id,
        topic: %{type: "run", id: run_id},
        event_type: event_name,
        occurred_at: datetime(event.occurred_at),
        actor: %{type: "system", id: "orchestrator"},
        resource: %{type: "run", id: run_id},
        sequence: event.sequence,
        cursor: cursor,
        data: run_event_dto(event)
      })

    "id: #{cursor}\nevent: #{event_name}\ndata: #{payload}\n\n"
  end

  defp sse_ready_body(stream, cursor) when is_binary(stream) and is_binary(cursor) do
    payload =
      Jason.encode!(%{
        schema_version: 1,
        stream: stream,
        event_type: "stream.ready",
        cursor: cursor,
        occurred_at: DateTime.utc_now()
      })

    "id: #{cursor}\nevent: stream.ready\ndata: #{payload}\n\n"
  end

  defp run_cursor(event), do: "run:" <> event.run_id <> ":" <> Integer.to_string(event.sequence)

  defp actor_dto(actor) do
    %{
      id: actor.id,
      username: actor.username,
      display_name: actor.display_name,
      roles: Enum.map(actor.roles, &Atom.to_string/1),
      status: Atom.to_string(actor.status),
      inserted_at: datetime(Map.get(actor, :inserted_at)),
      updated_at: datetime(Map.get(actor, :updated_at))
    }
  end

  defp schedule_dto(entry) do
    %{
      id: FavnOrchestrator.schedule_entry_id(entry),
      pipeline_module: module_name(entry.pipeline_module),
      schedule_id: atom_name(entry.schedule_id),
      cron: entry.cron,
      timezone: entry.timezone,
      overlap: atom_name(entry.overlap),
      missed: atom_name(entry.missed),
      active: entry.active,
      window: window_policy_dto(entry.window),
      schedule_fingerprint: entry.schedule_fingerprint,
      manifest_version_id: entry.manifest_version_id,
      manifest_content_hash: entry.manifest_content_hash,
      last_evaluated_at: datetime(entry.last_evaluated_at),
      last_due_at: datetime(entry.last_due_at),
      last_submitted_due_at: datetime(entry.last_submitted_due_at),
      in_flight_run_id: entry.in_flight_run_id,
      queued_due_at: datetime(entry.queued_due_at),
      updated_at: datetime(entry.updated_at)
    }
  end

  defp session_dto(session) do
    %{
      id: session.id,
      actor_id: session.actor_id,
      provider: session.provider,
      issued_at: datetime(session.issued_at),
      expires_at: datetime(session.expires_at),
      revoked_at: datetime(session.revoked_at)
    }
  end

  defp run_summary_dto(run) do
    %{
      id: run.id,
      status: Atom.to_string(run.status),
      submit_kind: Atom.to_string(run.submit_kind),
      manifest_version_id: run.manifest_version_id,
      event_seq: run.event_seq,
      started_at: datetime(run.started_at),
      finished_at: datetime(run.finished_at),
      error: inspect_term(run.error)
    }
  end

  defp run_detail_dto(run) do
    %{
      id: run.id,
      status: Atom.to_string(run.status),
      submit_kind: Atom.to_string(run.submit_kind),
      manifest_version_id: run.manifest_version_id,
      manifest_content_hash: run.manifest_content_hash,
      event_seq: run.event_seq,
      started_at: datetime(run.started_at),
      finished_at: datetime(run.finished_at),
      timeout_ms: run.timeout_ms,
      retry_backoff_ms: run.retry_backoff_ms,
      rerun_of_run_id: run.rerun_of_run_id,
      parent_run_id: run.parent_run_id,
      root_run_id: run.root_run_id,
      target_refs: Enum.map(List.wrap(run.target_refs), &ref_to_string/1),
      error: inspect_term(run.error)
    }
  end

  defp run_event_dto(event) do
    %{
      schema_version: event.schema_version,
      run_id: event.run_id,
      sequence: event.sequence,
      event_type: event_name(event.event_type),
      entity: Atom.to_string(event.entity),
      occurred_at: datetime(event.occurred_at),
      status: event_status(event.status),
      manifest_version_id: event.manifest_version_id,
      manifest_content_hash: event.manifest_content_hash,
      asset_ref: ref_to_string(event.asset_ref),
      stage: event.stage,
      data: normalize_data(event.data)
    }
  end

  defp backfill_window_dto(%BackfillWindow{} = window) do
    %{
      backfill_run_id: window.backfill_run_id,
      child_run_id: window.child_run_id,
      pipeline_module: module_name(window.pipeline_module),
      manifest_version_id: window.manifest_version_id,
      coverage_baseline_id: window.coverage_baseline_id,
      window_kind: atom_name(window.window_kind),
      window_start_at: datetime(window.window_start_at),
      window_end_at: datetime(window.window_end_at),
      timezone: window.timezone,
      window_key: window.window_key,
      status: atom_name(window.status),
      attempt_count: window.attempt_count,
      latest_attempt_run_id: window.latest_attempt_run_id,
      last_success_run_id: window.last_success_run_id,
      last_error: inspect_term(window.last_error),
      errors: Enum.map(window.errors, &inspect_term/1),
      metadata: normalize_data(window.metadata),
      started_at: datetime(window.started_at),
      finished_at: datetime(window.finished_at),
      created_at: datetime(window.created_at),
      updated_at: datetime(window.updated_at)
    }
  end

  defp coverage_baseline_dto(%CoverageBaseline{} = baseline) do
    %{
      baseline_id: baseline.baseline_id,
      pipeline_module: module_name(baseline.pipeline_module),
      source_key: baseline.source_key,
      segment_key_hash: baseline.segment_key_hash,
      segment_key_redacted: baseline.segment_key_redacted,
      window_kind: atom_name(baseline.window_kind),
      timezone: baseline.timezone,
      coverage_start_at: datetime(baseline.coverage_start_at),
      coverage_until: datetime(baseline.coverage_until),
      created_by_run_id: baseline.created_by_run_id,
      manifest_version_id: baseline.manifest_version_id,
      status: atom_name(baseline.status),
      errors: Enum.map(baseline.errors, &inspect_term/1),
      metadata: normalize_data(baseline.metadata),
      created_at: datetime(baseline.created_at),
      updated_at: datetime(baseline.updated_at)
    }
  end

  defp asset_window_state_dto(%AssetWindowState{} = state) do
    %{
      asset_ref_module: module_name(state.asset_ref_module),
      asset_ref_name: atom_name(state.asset_ref_name),
      pipeline_module: module_name(state.pipeline_module),
      manifest_version_id: state.manifest_version_id,
      window_kind: atom_name(state.window_kind),
      window_start_at: datetime(state.window_start_at),
      window_end_at: datetime(state.window_end_at),
      timezone: state.timezone,
      window_key: state.window_key,
      status: atom_name(state.status),
      latest_run_id: state.latest_run_id,
      latest_parent_run_id: state.latest_parent_run_id,
      latest_success_run_id: state.latest_success_run_id,
      latest_error: inspect_term(state.latest_error),
      errors: Enum.map(state.errors, &inspect_term/1),
      rows_written: state.rows_written,
      metadata: normalize_data(state.metadata),
      updated_at: datetime(state.updated_at)
    }
  end

  defp audit_dto(entry) do
    entry
    |> normalize_data()
    |> Map.update(:occurred_at, nil, &datetime/1)
  end

  defp normalize_data(value) when is_map(value) do
    value
    |> Enum.map(fn {key, val} -> {to_string(key), normalize_data(val)} end)
    |> Map.new()
  end

  defp normalize_data(value) when is_list(value), do: Enum.map(value, &normalize_data/1)
  defp normalize_data({module, name}), do: ref_to_string({module, name})
  defp normalize_data(%DateTime{} = value), do: datetime(value)
  defp normalize_data(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_data(value), do: value

  defp event_name(value) when is_atom(value), do: Atom.to_string(value)
  defp event_name(value), do: to_string(value)

  defp event_status(nil), do: nil
  defp event_status(value) when is_atom(value), do: Atom.to_string(value)
  defp event_status(value), do: to_string(value)

  defp ref_to_string(nil), do: nil

  defp ref_to_string({module, name}) when is_atom(module) and is_atom(name) do
    Atom.to_string(module) <> ":" <> Atom.to_string(name)
  end

  defp ref_to_string(value), do: inspect(value)

  defp inspect_term(nil), do: nil
  defp inspect_term(value), do: inspect(value)

  defp datetime(nil), do: nil
  defp datetime(%DateTime{} = dt), do: DateTime.to_iso8601(dt)

  defp atom_name(nil), do: nil
  defp atom_name(value) when is_atom(value), do: Atom.to_string(value)

  defp window_policy_dto(nil), do: nil

  defp window_policy_dto(%Policy{} = policy) do
    %{
      kind: atom_name(policy.kind),
      anchor: atom_name(policy.anchor),
      timezone: policy.timezone,
      allow_full_load: policy.allow_full_load
    }
  end

  defp module_name(nil), do: nil
  defp module_name(value) when is_atom(value), do: Atom.to_string(value)
end
