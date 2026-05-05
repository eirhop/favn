defmodule FavnOrchestrator.API.Router do
  @moduledoc """
  Private orchestrator HTTP API v1.
  """

  use Plug.Router

  require Logger

  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Manifest.Version
  alias Favn.Run.AssetResult
  alias Favn.Window.Policy
  alias Favn.Window.Request, as: WindowRequest
  alias FavnOrchestrator
  alias FavnOrchestrator.API.Config
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Idempotency
  alias FavnOrchestrator.Readiness
  alias FavnOrchestrator.RunEvent

  @read_model_status_filters %{
    "pending" => :pending,
    "running" => :running,
    "ok" => :ok,
    "partial" => :partial,
    "error" => :error,
    "cancelled" => :cancelled,
    "timed_out" => :timed_out
  }

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

  get "/api/orchestrator/v1/health/live" do
    data(conn, 200, normalize_data(Readiness.liveness()))
  end

  get "/api/orchestrator/v1/health/ready" do
    readiness = Readiness.readiness()
    status = if readiness.status == :ready, do: 200, else: 503

    data(conn, status, normalize_data(readiness))
  end

  get "/api/orchestrator/v1/diagnostics" do
    case ensure_service_auth(conn) do
      :ok ->
        data(conn, 200, normalize_data(FavnOrchestrator.diagnostics()))

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

  post "/api/orchestrator/v1/auth/password/sessions" do
    with :ok <- ensure_service_auth(conn),
         {:ok, params} <- fetch_json_body(conn),
         {:ok, username} <- fetch_required_string(params, "username"),
         {:ok, password} <- fetch_required_string(params, "password"),
         {:ok, session, actor} <- Auth.password_login(username, password),
         :ok <-
           Auth.put_audit(%{
             action: "auth.password.login",
             actor_id: actor.id,
             session_id: session.id,
             outcome: "accepted",
             service_identity: service_identity(conn)
           }) do
      data(conn, 201, %{
        session: session_dto(session),
        session_token: session.token,
        actor: actor_dto(actor)
      })
    else
      {:error, :invalid_credentials} ->
        error(conn, 401, "unauthenticated", "Invalid username or password")

      {:error, {:missing_field, field}} ->
        error(conn, 422, "validation_failed", "Missing required field", %{field: field})

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, _reason} ->
        error(conn, 500, "internal_error", "Request failed")
    end
  end

  post "/api/orchestrator/v1/auth/sessions/introspect" do
    with :ok <- ensure_service_auth(conn),
         {:ok, params} <- fetch_json_body(conn),
         {:ok, session_token} <- fetch_required_string(params, "session_token"),
         {:ok, session, actor} <- Auth.introspect_session(session_token) do
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

  post "/api/orchestrator/v1/auth/sessions/revoke" do
    with :ok <- ensure_service_auth(conn),
         {:ok, params} <- fetch_json_body(conn),
         {:ok, session_token} <- fetch_session_token(conn, params),
         {:ok, session, actor} <- Auth.introspect_session(session_token),
         :ok <- Auth.revoke_session(session.id),
         :ok <-
           Auth.put_audit(%{
             action: "auth.session.revoke_current",
             actor_id: actor.id,
             session_id: session.id,
             outcome: "accepted",
             service_identity: service_identity(conn)
           }) do
      revoked_at = DateTime.utc_now()
      revoked_session = %{session | revoked_at: revoked_at}

      data(conn, 200, %{
        revoked: true,
        session: session_dto(revoked_session),
        actor: actor_dto(actor)
      })
    else
      {:error, :invalid_session} ->
        error(conn, 401, "unauthenticated", "Session is invalid")

      {:error, :actor_not_found} ->
        error(conn, 404, "not_found", "Actor not found")

      {:error, {:missing_field, field}} ->
        error(conn, 422, "validation_failed", "Missing required field", %{field: field})

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, _reason} ->
        error(conn, 500, "internal_error", "Request failed")
    end
  end

  post "/api/orchestrator/v1/auth/sessions/:session_id/revoke" do
    with :ok <- ensure_service_auth(conn),
         {:ok, session, actor} <- ensure_actor_context(conn, :admin),
         :ok <- Auth.revoke_session(session_id),
         :ok <-
           Auth.put_audit(%{
             action: "auth.session.revoke",
             actor_id: actor.id,
             session_id: session.id,
             target_session_id: session_id,
             outcome: "accepted",
             service_identity: service_identity(conn)
           }) do
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
         {:ok, registration_status, canonical_version} <- publish_manifest_version(version),
         {:ok, summary} <-
           FavnOrchestrator.get_manifest_summary(canonical_version.manifest_version_id),
         :ok <-
           Auth.put_audit(%{
             action: "manifest.register",
             session_id: nil,
             resource_type: "manifest",
             resource_id: canonical_version.manifest_version_id,
             outcome: "accepted",
             service_identity: service_identity(conn)
           }) do
      data(conn, manifest_publish_status_code(registration_status), %{
        manifest: summary,
        registration: %{
          status: Atom.to_string(registration_status),
          manifest_version_id: version.manifest_version_id,
          canonical_manifest_version_id: canonical_version.manifest_version_id
        }
      })
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

  get "/api/orchestrator/v1/manifests/:manifest_version_id/assets/:target_id/inspection" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, sample_limit} <- inspection_sample_limit(conn.params),
         {:ok, result} <-
           FavnOrchestrator.inspect_manifest_asset(manifest_version_id, target_id,
             sample_limit: sample_limit
           ) do
      data(conn, 200, %{inspection: inspection_result_dto(result)})
    else
      {:error, :manifest_version_not_found} ->
        error(conn, 404, "not_found", "Manifest version was not found")

      {:error, :invalid_asset_target} ->
        error(conn, 404, "not_found", "Asset target was not found")

      {:error, reason}
      when reason in [
             :asset_not_found,
             :asset_relation_not_found,
             :relation_connection_missing,
             :invalid_relation,
             :invalid_inspection_target
           ] ->
        error(conn, 422, "validation_failed", "Asset relation is not inspectable", %{
          reason: atom_name(reason)
        })

      {:error, :invalid_sample_limit} ->
        error(conn, 422, "validation_failed", "Invalid sample limit")

      {:error, :runner_client_not_available} ->
        error(conn, 503, "service_unavailable", "Runner inspection is not available")

      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, reason} ->
        Logger.error("inspection failed: #{inspect(reason)}")
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  post "/api/orchestrator/v1/manifests/:manifest_version_id/activate" do
    with :ok <- ensure_service_auth(conn),
         {:ok, actor_id, session_id} <- ensure_activation_context(conn) do
      run_idempotent_command(
        conn,
        "manifest.activate",
        actor_id,
        session_id,
        %{manifest_version_id: manifest_version_id},
        fn idempotency ->
          with :ok <- FavnOrchestrator.activate_manifest(manifest_version_id),
               :ok <- maybe_reload_scheduler(),
               :ok <-
                 Auth.put_audit(
                   %{
                     action: "manifest.activate",
                     actor_id: actor_id,
                     session_id: session_id,
                     resource_type: "manifest",
                     resource_id: manifest_version_id,
                     outcome: "accepted",
                     service_identity: service_identity(conn)
                   }
                   |> Map.merge(audit_idempotency(idempotency, "accepted"))
                 ) do
            {:ok, 200, %{activated: true, manifest_version_id: manifest_version_id}, "manifest",
             manifest_version_id}
          else
            {:error, :manifest_version_not_found} ->
              {:error, 404, "not_found", "Manifest version was not found", %{}}

            {:error, _reason} ->
              {:error, 400, "bad_request", "Request failed", %{}}
          end
        end
      )
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")
    end
  end

  post "/api/orchestrator/v1/manifests/:manifest_version_id/runner/register" do
    with :ok <- ensure_service_auth(conn),
         {:ok, registration} <-
           FavnOrchestrator.register_manifest_with_runner(manifest_version_id) do
      data(conn, 200, %{registration: registration})
    else
      {:error, :manifest_version_not_found} ->
        error(conn, 404, "not_found", "Manifest version was not found")

      {:error, :runner_manifest_conflict} ->
        error(
          conn,
          409,
          "runner_manifest_conflict",
          "Runner has a different manifest for this version id"
        )

      {:error, reason} when reason in [:runner_client_not_available, :runner_unavailable] ->
        error(conn, 503, "runner_unavailable", "Runner manifest registration is unavailable")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, reason} ->
        Logger.error("runner manifest registration failed: #{inspect(reason)}")
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
         {:ok, session, actor} <- ensure_actor_context(conn, :operator),
         {:ok, params} <- fetch_json_body(conn) do
      run_idempotent_command(conn, "run.submit", actor.id, session.id, params, fn idempotency ->
        with {:ok, run_id} <- submit_run_from_request(params),
             {:ok, run} <- FavnOrchestrator.get_run(run_id),
             :ok <-
               Auth.put_audit(
                 %{
                   action: "run.submit",
                   actor_id: actor.id,
                   session_id: session.id,
                   resource_type: "run",
                   resource_id: run_id,
                   outcome: "accepted",
                   service_identity: service_identity(conn)
                 }
                 |> Map.merge(audit_idempotency(idempotency, "accepted"))
               ) do
          {:ok, 201, %{run: run_summary_dto(run)}, "run", run_id}
        else
          {:error, :invalid_target} ->
            {:error, 422, "validation_failed", "Invalid run target request", %{}}

          {:error, :invalid_manifest_selection} ->
            {:error, 422, "validation_failed", "Invalid manifest selection", %{}}

          {:error, :invalid_dependencies} ->
            {:error, 422, "validation_failed", "Invalid dependency mode", %{}}

          {:error, :invalid_asset_target} ->
            {:error, 422, "validation_failed", "Invalid asset target id", %{}}

          {:error, :invalid_pipeline_target} ->
            {:error, 422, "validation_failed", "Invalid pipeline target id", %{}}

          {:error, reason} when is_tuple(reason) ->
            command_window_policy_error(reason)

          {:error, :active_manifest_not_set} ->
            {:error, 404, "not_found", "Active manifest is not set", %{}}

          {:error, _reason} ->
            {:error, 400, "bad_request", "Request failed", %{}}
        end
      end)
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")
    end
  end

  post "/api/orchestrator/v1/runs/:run_id/cancel" do
    with :ok <- ensure_service_auth(conn),
         {:ok, actor_id, session_id} <- ensure_operator_context(conn) do
      run_idempotent_command(conn, "run.cancel", actor_id, session_id, %{run_id: run_id}, fn
        idempotency ->
          with :ok <- FavnOrchestrator.cancel_run(run_id, %{actor_id: actor_id}),
               :ok <-
                 Auth.put_audit(
                   %{
                     action: "run.cancel",
                     actor_id: actor_id,
                     session_id: session_id,
                     resource_type: "run",
                     resource_id: run_id,
                     outcome: "accepted",
                     service_identity: service_identity(conn)
                   }
                   |> Map.merge(audit_idempotency(idempotency, "accepted"))
                 ) do
            {:ok, 200, %{cancelled: true, run_id: run_id}, "run", run_id}
          else
            {:error, :not_found} ->
              {:error, 404, "not_found", "Run was not found", %{}}

            {:error, :backfill_parent_cancel_not_supported} ->
              {:error, 409, "conflict",
               "Backfill parent runs cannot be cancelled through generic run cancellation", %{}}

            {:error, _reason} ->
              {:error, 400, "bad_request", "Request failed", %{}}
          end
      end)
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")
    end
  end

  post "/api/orchestrator/v1/runs/:run_id/rerun" do
    with :ok <- ensure_service_auth(conn),
         {:ok, session, actor} <- ensure_actor_context(conn, :operator) do
      run_idempotent_command(conn, "run.rerun", actor.id, session.id, %{run_id: run_id}, fn
        idempotency ->
          with {:ok, rerun_id} <- FavnOrchestrator.rerun(run_id),
               {:ok, rerun_run} <- FavnOrchestrator.get_run(rerun_id),
               :ok <-
                 Auth.put_audit(
                   %{
                     action: "run.rerun",
                     actor_id: actor.id,
                     session_id: session.id,
                     resource_type: "run",
                     resource_id: rerun_id,
                     outcome: "accepted",
                     service_identity: service_identity(conn)
                   }
                   |> Map.merge(audit_idempotency(idempotency, "accepted"))
                 ) do
            {:ok, 201, %{run: run_summary_dto(rerun_run)}, "run", rerun_id}
          else
            {:error, :not_found} ->
              {:error, 404, "not_found", "Run was not found", %{}}

            {:error, :backfill_parent_rerun_not_supported} ->
              {:error, 409, "conflict",
               "Backfill parent runs cannot be rerun through generic run rerun", %{}}

            {:error, _reason} ->
              {:error, 400, "bad_request", "Request failed", %{}}
          end
      end)
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  post "/api/orchestrator/v1/backfills" do
    with :ok <- ensure_service_auth(conn),
         {:ok, session, actor} <- ensure_actor_context(conn, :operator),
         {:ok, params} <- fetch_json_body(conn) do
      run_idempotent_command(conn, "backfill.submit", actor.id, session.id, params, fn
        idempotency ->
          with {:ok, run_id} <- submit_backfill_from_request(params),
               {:ok, run} <- FavnOrchestrator.get_run(run_id),
               :ok <-
                 Auth.put_audit(
                   %{
                     action: "backfill.submit",
                     actor_id: actor.id,
                     session_id: session.id,
                     resource_type: "run",
                     resource_id: run_id,
                     outcome: "accepted",
                     service_identity: service_identity(conn)
                   }
                   |> Map.merge(audit_idempotency(idempotency, "accepted"))
                 ) do
            {:ok, 201, %{run: run_summary_dto(run)}, "run", run_id}
          else
            {:error, :invalid_target} ->
              {:error, 422, "validation_failed", "Invalid backfill target request", %{}}

            {:error, :invalid_manifest_selection} ->
              {:error, 422, "validation_failed", "Invalid manifest selection", %{}}

            {:error, :invalid_backfill_range_request} ->
              {:error, 422, "validation_failed", "Invalid backfill range request", %{}}

            {:error, reason} when is_tuple(reason) ->
              command_backfill_range_error(reason)

            {:error, :active_manifest_not_set} ->
              {:error, 404, "not_found", "Active manifest is not set", %{}}

            {:error, _reason} ->
              {:error, 400, "bad_request", "Request failed", %{}}
          end
      end)
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")
    end
  end

  get "/api/orchestrator/v1/backfills/:backfill_run_id/windows" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, filters} <- backfill_window_filters(conn.params, backfill_run_id),
         {:ok, page} <- FavnOrchestrator.list_backfill_windows(filters) do
      data(conn, 200, page_response(page, &backfill_window_dto/1))
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, :invalid_filter} ->
        error(conn, 422, "validation_failed", "Invalid backfill window filter")

      {:error, :invalid_pagination} ->
        error(conn, 422, "validation_failed", "Invalid pagination parameters")

      {:error, {:manifest_filter_lookup_failed, reason}} ->
        Logger.error("backfill_window.filter_lookup failed: #{inspect(reason)}")
        error(conn, 400, "bad_request", "Request failed")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  post "/api/orchestrator/v1/backfills/:backfill_run_id/windows/rerun" do
    with :ok <- ensure_service_auth(conn),
         {:ok, session, actor} <- ensure_actor_context(conn, :operator),
         {:ok, params} <- fetch_json_body(conn),
         {:ok, window_key} <- fetch_required_string(params, "window_key"),
         {:ok, window} <- find_backfill_window(backfill_run_id, window_key) do
      run_idempotent_command(
        conn,
        "backfill.window.rerun",
        actor.id,
        session.id,
        %{backfill_run_id: backfill_run_id, window_key: window_key},
        fn idempotency ->
          with {:ok, rerun_id} <-
                 FavnOrchestrator.rerun_backfill_window(
                   backfill_run_id,
                   window.pipeline_module,
                   window.window_key
                 ),
               {:ok, run} <- FavnOrchestrator.get_run(rerun_id),
               :ok <-
                 Auth.put_audit(
                   %{
                     action: "backfill.window.rerun",
                     actor_id: actor.id,
                     session_id: session.id,
                     resource_type: "run",
                     resource_id: rerun_id,
                     outcome: "accepted",
                     service_identity: service_identity(conn)
                   }
                   |> Map.merge(audit_idempotency(idempotency, "accepted"))
                 ) do
            {:ok, 201, %{run: run_summary_dto(run)}, "run", rerun_id}
          else
            {:error, :backfill_window_not_rerunnable} ->
              {:error, 409, "conflict", "Backfill window is not rerunnable", %{}}

            {:error, :backfill_window_has_no_attempt} ->
              {:error, 409, "conflict", "Backfill window has no attempt to rerun", %{}}

            {:error, _reason} ->
              {:error, 400, "bad_request", "Request failed", %{}}
          end
        end
      )
    else
      {:error, {:missing_field, field}} ->
        error(conn, 422, "validation_failed", "Missing required field", %{field: field})

      {:error, :not_found} ->
        error(conn, 404, "not_found", "Backfill window was not found")

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

  post "/api/orchestrator/v1/backfills/projections/repair" do
    with :ok <- ensure_service_auth(conn),
         {:ok, session, actor} <- ensure_actor_context(conn, :operator),
         {:ok, params} <- fetch_json_body(conn),
         {:ok, opts} <- backfill_repair_opts(params),
         {:ok, report} <- FavnOrchestrator.repair_backfill_projections(opts),
         :ok <-
           maybe_put_audit(Keyword.get(opts, :apply, false), %{
             action: "backfill.projections.repair",
             actor_id: actor.id,
             session_id: session.id,
             resource_type: "backfill_projection",
             resource_id: repair_resource_id(opts),
             outcome: "accepted",
             service_identity: service_identity(conn)
           }) do
      data(conn, 200, %{repair: normalize_data(report)})
    else
      {:error, :invalid_repair_scope} ->
        error(conn, 422, "validation_failed", "Invalid backfill projection repair scope")

      {:error, :invalid_filter} ->
        error(conn, 422, "validation_failed", "Invalid backfill projection repair filter")

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
         {:ok, filters} <- coverage_baseline_filters(conn.params),
         {:ok, page} <- FavnOrchestrator.list_coverage_baselines(filters) do
      data(conn, 200, page_response(page, &coverage_baseline_dto/1))
    else
      {:error, :forbidden} ->
        error(conn, 403, "forbidden", "Actor does not have access")

      {:error, :service_unauthorized} ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :unauthenticated} ->
        error(conn, 401, "unauthenticated", "Missing or invalid actor context")

      {:error, :invalid_filter} ->
        error(conn, 422, "validation_failed", "Invalid coverage baseline filter")

      {:error, :invalid_pagination} ->
        error(conn, 422, "validation_failed", "Invalid pagination parameters")

      {:error, {:manifest_filter_lookup_failed, reason}} ->
        Logger.error("coverage_baseline.filter_lookup failed: #{inspect(reason)}")
        error(conn, 400, "bad_request", "Request failed")

      {:error, _reason} ->
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/api/orchestrator/v1/assets/window-states" do
    with :ok <- ensure_service_auth(conn),
         {:ok, _session, _actor} <- ensure_actor_context(conn, :viewer),
         {:ok, filters} <- asset_window_state_filters(conn.params),
         {:ok, page} <- FavnOrchestrator.list_asset_window_states(filters) do
      data(conn, 200, page_response(page, &asset_window_state_dto/1))
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
         {:ok, last_event_id} <- validate_last_event_id(header(conn, "last-event-id")),
         {:ok, global_sequence} <- parse_global_cursor(last_event_id) do
      sse_stream(conn, {:global, global_sequence})
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
         {:ok, last_event_id} <- validate_last_event_id(header(conn, "last-event-id")),
         {:ok, sequence} <- parse_run_cursor(last_event_id, run_id) do
      sse_stream(conn, {:run, run_id, sequence})
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
         {:ok, session, actor} <- ensure_actor_context(conn, :admin),
         {:ok, params} <- fetch_json_body(conn),
         {:ok, username} <- fetch_required_string(params, "username"),
         {:ok, password} <- fetch_required_string(params, "password"),
         {:ok, display_name} <- fetch_actor_display_name(params, username),
         {:ok, roles} <- fetch_actor_roles(params),
         {:ok, created_actor} <- Auth.create_actor(username, password, display_name, roles),
         :ok <-
           Auth.put_audit(%{
             action: "actor.create",
             actor_id: actor.id,
             session_id: session.id,
             resource_type: "actor",
             resource_id: created_actor.id,
             outcome: "accepted",
             service_identity: service_identity(conn)
           }) do
      data(conn, 201, %{actor: actor_dto(created_actor)})
    else
      {:error, {:missing_field, field}} ->
        error(conn, 422, "validation_failed", "Missing required field", %{field: field})

      {:error, :invalid_roles} ->
        error(conn, 422, "validation_failed", "Invalid roles")

      {:error, :username_taken} ->
        error(conn, 409, "conflict", "Username already exists")

      {:error, reason}
      when reason in [:password_too_short, :password_too_long, :password_blank] ->
        error(conn, 422, "validation_failed", "Password does not meet policy")

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
         {:ok, session, actor} <- ensure_actor_context(conn, :admin),
         {:ok, params} <- fetch_json_body(conn),
         {:ok, roles} <- fetch_required_roles(params, "roles"),
         {:ok, updated_actor} <- Auth.update_actor_roles(actor_id, roles),
         :ok <-
           Auth.put_audit(%{
             action: "actor.roles.update",
             actor_id: actor.id,
             session_id: session.id,
             resource_type: "actor",
             resource_id: actor_id,
             outcome: "accepted",
             service_identity: service_identity(conn)
           }) do
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
         {:ok, session, actor} <- ensure_actor_context(conn, :admin),
         {:ok, params} <- fetch_json_body(conn),
         {:ok, password} <- fetch_required_string(params, "password"),
         :ok <- Auth.set_actor_password(actor_id, password),
         :ok <-
           Auth.put_audit(%{
             action: "actor.password.set",
             actor_id: actor.id,
             session_id: session.id,
             resource_type: "actor",
             resource_id: actor_id,
             outcome: "accepted",
             service_identity: service_identity(conn)
           }) do
      data(conn, 200, %{updated: true, actor_id: actor_id})
    else
      {:error, :actor_not_found} ->
        error(conn, 404, "not_found", "Actor was not found")

      {:error, reason}
      when reason in [:password_too_short, :password_too_long, :password_blank] ->
        error(conn, 422, "validation_failed", "Password does not meet policy")

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
    if local_dev_context_allowed_for_request?(conn) do
      :ok
    else
      provided = bearer_token(conn)

      case ServiceTokens.authenticate(provided, configured_service_tokens()) do
        {:ok, _service_identity} -> :ok
        {:error, :service_unauthorized} -> {:error, :service_unauthorized}
      end
    end
  end

  defp service_token_diagnostics(conn) do
    %{
      authenticated: true,
      service_identity: service_identity(conn),
      service_tokens: %{
        configured_count: ServiceTokens.configured_count(configured_service_tokens()),
        redacted: true
      }
    }
  end

  defp ensure_actor_context(conn, required_role) do
    actor_id = header(conn, "x-favn-actor-id")
    session_token = header(conn, "x-favn-session-token")

    case session_token do
      token when is_binary(token) and token != "" ->
        case Auth.actor_from_forwarded_context(actor_id, token) do
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
        ensure_local_dev_context(conn, required_role)
    end
  end

  defp ensure_local_dev_context(conn, required_role) do
    cond do
      local_dev_context_allowed_for_request?(conn) ->
        local_dev_actor_context(required_role)

      local_dev_context_requested?(conn) ->
        {:error, :forbidden}

      true ->
        {:error, :unauthenticated}
    end
  end

  defp local_dev_context_requested?(conn),
    do: header(conn, "x-favn-local-dev-context") == "trusted"

  defp local_dev_context_allowed_for_request?(conn) do
    local_dev_context_requested?(conn) and Config.local_dev_trusted_context_allowed?() and
      loopback_peer?(conn.remote_ip)
  end

  defp loopback_peer?({127, _b, _c, _d}), do: true
  defp loopback_peer?({0, 0, 0, 0, 0, 0, 0, 1}), do: true
  defp loopback_peer?(_remote_ip), do: false

  defp local_dev_actor_context(required_role) do
    now = DateTime.utc_now()

    session = %{
      id: "local-dev-cli",
      actor_id: "local-dev-cli",
      provider: "local_dev_trusted",
      issued_at: now,
      expires_at: DateTime.add(now, 86_400, :second),
      revoked_at: nil
    }

    actor = %{
      id: "local-dev-cli",
      username: "local-dev-cli",
      display_name: "Local Dev CLI",
      roles: [:admin],
      status: :active,
      inserted_at: now,
      updated_at: now
    }

    if Auth.has_role?(actor, required_role), do: {:ok, session, actor}, else: {:error, :forbidden}
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

  defp parse_global_cursor(nil), do: {:ok, nil}

  defp parse_global_cursor(value) when is_binary(value) do
    case String.split(value, ":", parts: 2) do
      ["global", sequence] ->
        with {int, ""} <- Integer.parse(sequence),
             true <- int > 0 do
          {:ok, int}
        else
          _ -> {:error, :cursor_invalid}
        end

      _other ->
        {:error, :cursor_invalid}
    end
  end

  defp bearer_token(conn) do
    case get_req_header(conn, "authorization") do
      ["Bearer " <> token] -> token
      _ -> nil
    end
  end

  defp service_identity(conn) do
    if local_dev_context_allowed_for_request?(conn) do
      "local-dev-cli"
    else
      case ServiceTokens.authenticate(bearer_token(conn), configured_service_tokens()) do
        {:ok, identity} -> identity
        {:error, :service_unauthorized} -> nil
      end
    end
  end

  defp configured_service_tokens do
    ServiceTokens.configured_tokens()
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

  defp fetch_session_token(conn, params) when is_map(params) do
    case Map.get(params, "session_token") || header(conn, "x-favn-session-token") do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:missing_field, "session_token"}}
    end
  end

  defp run_idempotent_command(conn, operation, actor_id, session_id, request_input, execute_fun)
       when is_binary(operation) and is_function(execute_fun, 1) do
    case idempotency_key_hash(conn) do
      {:ok, key_hash} ->
        fingerprint =
          Idempotency.request_fingerprint(%{
            operation: operation,
            request: request_input
          })

        scope = %{
          operation: operation,
          actor_id: actor_id,
          session_id: session_id,
          service_identity: service_identity(conn),
          idempotency_key_hash: key_hash
        }

        record = Idempotency.new_record(scope, fingerprint)

        case Idempotency.reserve(record) do
          {:ok, {:reserved, reserved}} ->
            execute_idempotent_command(
              conn,
              reserved,
              %{operation: operation, key_hash: key_hash},
              execute_fun
            )

          {:ok, {:replay, stored}} ->
            replay_idempotent_response(conn, stored)

          {:error, :idempotency_conflict} ->
            error(
              conn,
              409,
              "idempotency_conflict",
              "Idempotency key was reused with different input"
            )

          {:error, :operation_in_progress} ->
            error(conn, 409, "operation_in_progress", "Original operation is still in progress")

          {:error, reason} ->
            Logger.error("idempotency.reserve failed: #{inspect(reason)}")
            error(conn, 500, "internal_error", "Idempotency reservation failed")
        end

      {:error, :missing_idempotency_key} ->
        error(conn, 422, "validation_failed", "Missing required Idempotency-Key header", %{
          header: "Idempotency-Key"
        })

      {:error, :invalid_idempotency_key} ->
        error(conn, 422, "validation_failed", "Invalid Idempotency-Key header", %{
          header: "Idempotency-Key"
        })
    end
  end

  defp execute_idempotent_command(conn, record, idempotency, execute_fun) do
    case execute_fun.(idempotency) do
      {:ok, status, payload, resource_type, resource_id} ->
        :ok =
          Idempotency.complete(record.id, %{
            operation: record.operation,
            status: :completed,
            response_status: status,
            response_body: payload,
            resource_type: resource_type,
            resource_id: resource_id
          })

        data(conn, status, payload)

      {:error, status, code, message, details} ->
        :ok =
          Idempotency.complete(record.id, %{
            operation: record.operation,
            status: :failed,
            response_status: status,
            response_body: %{code: code, message: message, details: details},
            resource_type: nil,
            resource_id: nil
          })

        error(conn, status, code, message, details)
    end
  end

  defp replay_idempotent_response(conn, %{status: :completed} = record) do
    data(conn, record.response_status, record.response_body || %{})
  end

  defp replay_idempotent_response(conn, %{status: :failed} = record) do
    body = record.response_body || %{}

    error(
      conn,
      record.response_status,
      body_field(body, "code") || "bad_request",
      body_field(body, "message") || "Request failed",
      body_field(body, "details") || %{}
    )
  end

  defp idempotency_key_hash(conn) do
    case header(conn, "idempotency-key") do
      nil ->
        {:error, :missing_idempotency_key}

      value ->
        value = String.trim(value)

        if value != "" and byte_size(value) <= 512 do
          {:ok, Idempotency.key_hash(value)}
        else
          {:error, :invalid_idempotency_key}
        end
    end
  end

  defp audit_idempotency(%{operation: operation, key_hash: key_hash}, outcome) do
    %{
      operation: operation,
      idempotency: %{outcome: outcome, key_hash: key_hash}
    }
  end

  defp body_field(body, "code") when is_map(body),
    do: Map.get(body, "code") || Map.get(body, :code)

  defp body_field(body, "message") when is_map(body),
    do: Map.get(body, "message") || Map.get(body, :message)

  defp body_field(body, "details") when is_map(body),
    do: Map.get(body, "details") || Map.get(body, :details)

  defp maybe_put_audit(true, entry), do: Auth.put_audit(entry)
  defp maybe_put_audit(false, _entry), do: :ok

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

  defp publish_manifest_version(%Version{} = version) do
    case FavnOrchestrator.publish_manifest(version) do
      {:ok, :published, %Version{} = canonical} -> {:ok, :published, canonical}
      {:ok, :already_published, %Version{} = canonical} -> {:ok, :already_published, canonical}
      {:error, reason} -> {:error, reason}
    end
  end

  defp manifest_publish_status_code(:published), do: 201
  defp manifest_publish_status_code(:already_published), do: 200

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
    with :ok <- reject_backfill_lookback_params(params),
         {:ok, %{type: "pipeline", id: target_id}} <- fetch_target(params),
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

  defp reject_backfill_lookback_params(params) when is_map(params) do
    cond do
      Map.has_key?(params, "lookback") ->
        {:error, {:unsupported_backfill_option, :lookback}}

      Map.has_key?(params, "lookback_policy") ->
        {:error, {:unsupported_backfill_option, :lookback_policy}}

      true ->
        :ok
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

  defp backfill_repair_opts(params) when is_map(params) do
    opts =
      []
      |> Keyword.put(:apply, Map.get(params, "apply") == true)
      |> maybe_put_string_opt(:backfill_run_id, Map.get(params, "backfill_run_id"))

    with {:ok, opts} <- maybe_put_pipeline_module_filter(opts, Map.get(params, "pipeline_module")) do
      if Keyword.has_key?(opts, :backfill_run_id) and Keyword.has_key?(opts, :pipeline_module) do
        {:error, :invalid_repair_scope}
      else
        {:ok, opts}
      end
    end
  end

  defp repair_resource_id(opts) do
    cond do
      Keyword.has_key?(opts, :backfill_run_id) ->
        Keyword.fetch!(opts, :backfill_run_id)

      Keyword.has_key?(opts, :pipeline_module) ->
        Atom.to_string(Keyword.fetch!(opts, :pipeline_module))

      true ->
        "all"
    end
  end

  defp backfill_window_filters(params, backfill_run_id) when is_map(params) do
    with {:ok, filters} <- pagination_filters(params),
         {:ok, filters} <-
           maybe_put_pipeline_module_filter(filters, Map.get(params, "pipeline_module")),
         {:ok, filters} <- maybe_put_status_filter(filters, Map.get(params, "status")) do
      {:ok,
       filters
       |> Keyword.put(:backfill_run_id, backfill_run_id)
       |> maybe_put_string_opt(:window_key, Map.get(params, "window_key"))}
    end
  end

  defp coverage_baseline_filters(params) when is_map(params) do
    with {:ok, filters} <- pagination_filters(params),
         {:ok, filters} <-
           maybe_put_pipeline_module_filter(filters, Map.get(params, "pipeline_module")),
         {:ok, filters} <- maybe_put_status_filter(filters, Map.get(params, "status")) do
      {:ok,
       filters
       |> maybe_put_string_opt(:source_key, Map.get(params, "source_key"))
       |> maybe_put_string_opt(:segment_key_hash, Map.get(params, "segment_key_hash"))}
    end
  end

  defp asset_window_state_filters(params) when is_map(params) do
    with {:ok, opts} <- pagination_filters(params),
         {:ok, opts} <- maybe_put_asset_ref_filters(opts, params),
         {:ok, opts} <- maybe_put_pipeline_module_filter(opts, Map.get(params, "pipeline_module")),
         {:ok, opts} <- maybe_put_status_filter(opts, Map.get(params, "status")) do
      {:ok,
       opts
       |> maybe_put_string_opt(:window_key, Map.get(params, "window_key"))}
    end
  end

  defp maybe_put_asset_ref_filters(opts, params) do
    module = Map.get(params, "asset_ref_module")
    name = Map.get(params, "asset_ref_name")

    case {module, name} do
      {nil, nil} ->
        {:ok, opts}

      {module, name} when is_binary(module) and is_binary(name) ->
        case allowed_manifest_asset_ref(module, name) do
          {:ok, {module_atom, name_atom}} ->
            {:ok,
             opts
             |> Keyword.put(:asset_ref_module, module_atom)
             |> Keyword.put(:asset_ref_name, name_atom)}

          {:error, :invalid_manifest_asset_ref} ->
            {:error, :invalid_asset_ref}

          {:error, {:manifest_filter_lookup_failed, _reason}} = error ->
            error
        end

      _other ->
        {:error, :invalid_asset_ref}
    end
  end

  defp find_backfill_window(backfill_run_id, window_key) do
    with {:ok, page} <-
           FavnOrchestrator.list_backfill_windows(
             backfill_run_id: backfill_run_id,
             window_key: window_key,
             limit: 1
           ) do
      case page.items do
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

  defp pagination_filters(params) when is_map(params) do
    with {:ok, limit} <- pagination_int(Map.get(params, "limit", "100"), 1, 500),
         {:ok, offset} <- pagination_int(Map.get(params, "offset", "0"), 0, nil) do
      {:ok, [limit: limit, offset: offset]}
    end
  end

  defp pagination_int(value, min, max) when is_integer(value),
    do: validate_page_int(value, min, max)

  defp pagination_int(value, min, max) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} -> validate_page_int(int, min, max)
      _ -> {:error, :invalid_pagination}
    end
  end

  defp pagination_int(_value, _min, _max), do: {:error, :invalid_pagination}

  defp validate_page_int(value, min, nil) when value >= min, do: {:ok, value}
  defp validate_page_int(value, min, max) when value >= min and value <= max, do: {:ok, value}
  defp validate_page_int(_value, _min, _max), do: {:error, :invalid_pagination}

  defp inspection_sample_limit(params) when is_map(params) do
    value = Map.get(params, "sample_limit") || Map.get(params, "limit") || "20"

    case value do
      int when is_integer(int) and int >= 0 -> {:ok, min(int, 20)}
      value when is_binary(value) -> parse_sample_limit(value)
      _ -> {:error, :invalid_sample_limit}
    end
  end

  defp parse_sample_limit(value) do
    case Integer.parse(value) do
      {int, ""} when int >= 0 -> {:ok, min(int, 20)}
      _ -> {:error, :invalid_sample_limit}
    end
  end

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

  defp maybe_put_status_filter(opts, nil), do: {:ok, opts}
  defp maybe_put_status_filter(opts, ""), do: {:ok, opts}

  defp maybe_put_status_filter(opts, value) when is_binary(value) do
    case Map.fetch(@read_model_status_filters, value) do
      {:ok, status} -> {:ok, Keyword.put(opts, :status, status)}
      :error -> {:error, :invalid_filter}
    end
  end

  defp maybe_put_status_filter(_opts, _value), do: {:error, :invalid_filter}

  defp maybe_put_pipeline_module_filter(opts, nil), do: {:ok, opts}
  defp maybe_put_pipeline_module_filter(opts, ""), do: {:ok, opts}

  defp maybe_put_pipeline_module_filter(opts, value) when is_binary(value) do
    case allowed_manifest_pipeline_module(value) do
      {:ok, module} -> {:ok, Keyword.put(opts, :pipeline_module, module)}
      {:error, :invalid_manifest_pipeline_module} -> {:error, :invalid_filter}
      {:error, {:manifest_filter_lookup_failed, _reason}} = error -> error
    end
  end

  defp maybe_put_pipeline_module_filter(_opts, _value), do: {:error, :invalid_filter}

  defp allowed_manifest_pipeline_module(value) when is_binary(value) do
    with {:ok, modules} <- manifest_pipeline_modules(),
         {:ok, module} <- match_allowed_module(value, modules) do
      {:ok, module}
    else
      {:error, :not_allowed} -> {:error, :invalid_manifest_pipeline_module}
      {:error, {:manifest_filter_lookup_failed, _reason}} = error -> error
    end
  end

  defp allowed_manifest_asset_ref(module_value, name_value)
       when is_binary(module_value) and is_binary(name_value) do
    with {:ok, refs} <- manifest_asset_refs(),
         {:ok, asset_ref} <- match_allowed_asset_ref(module_value, name_value, refs) do
      {:ok, asset_ref}
    else
      {:error, :not_allowed} -> {:error, :invalid_manifest_asset_ref}
      {:error, {:manifest_filter_lookup_failed, _reason}} = error -> error
    end
  end

  defp manifest_pipeline_modules do
    case FavnOrchestrator.list_manifests() do
      {:ok, versions} ->
        {:ok,
         versions
         |> Enum.flat_map(& &1.manifest.pipelines)
         |> Enum.map(& &1.module)
         |> Enum.uniq()}

      {:error, reason} ->
        {:error, {:manifest_filter_lookup_failed, reason}}
    end
  end

  defp manifest_asset_refs do
    case FavnOrchestrator.list_manifests() do
      {:ok, versions} ->
        {:ok,
         versions
         |> Enum.flat_map(& &1.manifest.assets)
         |> Enum.map(& &1.ref)
         |> Enum.uniq()}

      {:error, reason} ->
        {:error, {:manifest_filter_lookup_failed, reason}}
    end
  end

  defp match_allowed_module(value, modules) do
    Enum.find_value(modules, {:error, :not_allowed}, fn module ->
      if module_filter_match?(value, module), do: {:ok, module}
    end)
  end

  defp match_allowed_asset_ref(module_value, name_value, refs) do
    Enum.find_value(refs, {:error, :not_allowed}, fn {module, name} = ref ->
      if module_filter_match?(module_value, module) and name_value == Atom.to_string(name) do
        {:ok, ref}
      end
    end)
  end

  defp module_filter_match?(value, module) when is_atom(module) do
    value in module_filter_names(module)
  end

  defp module_filter_names(module) do
    module
    |> Atom.to_string()
    |> then(fn
      "Elixir." <> short_name = full_name -> [full_name, short_name]
      full_name -> [full_name]
    end)
  end

  defp data(conn, status, payload) do
    body = Jason.encode!(%{data: payload})

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, body)
  end

  defp page_response(page, mapper) when is_function(mapper, 1) do
    %{
      items: Enum.map(page.items, mapper),
      pagination: %{
        limit: page.limit,
        offset: page.offset,
        has_more: page.has_more?,
        next_offset: page.next_offset
      }
    }
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

  defp command_window_policy_error(reason) do
    case window_policy_error(reason) do
      {:ok, message, details} -> {:error, 422, "validation_failed", message, details}
      :error -> {:error, 400, "bad_request", "Request failed", %{}}
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

  defp command_backfill_range_error(reason) do
    case backfill_range_error(reason) do
      {:ok, message, details} -> {:error, 422, "validation_failed", message, details}
      :error -> {:error, 400, "bad_request", "Request failed", %{}}
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

  defp backfill_range_error({:too_many_backfill_windows, requested, max}) do
    {:ok, "Backfill range exceeds maximum window count", %{requested: requested, max: max}}
  end

  defp backfill_range_error({:unsupported_backfill_option, option}) do
    {:ok, "Unsupported backfill option", %{option: Atom.to_string(option)}}
  end

  defp backfill_range_error({:coverage_baseline_not_found, baseline_id}) do
    {:ok, "Coverage baseline was not found", %{coverage_baseline_id: baseline_id}}
  end

  defp backfill_range_error({:coverage_baseline_pipeline_mismatch, baseline, requested}) do
    {:ok, "Coverage baseline does not belong to requested pipeline",
     %{
       baseline_pipeline_module: module_name(baseline),
       requested_pipeline_module: module_name(requested)
     }}
  end

  defp backfill_range_error({:coverage_baseline_not_ok, status}) do
    {:ok, "Coverage baseline is not usable for relative range resolution",
     %{status: atom_name(status)}}
  end

  defp backfill_range_error({:coverage_baseline_window_kind_mismatch, baseline, requested}) do
    {:ok, "Coverage baseline window kind does not match requested range kind",
     %{baseline_window_kind: atom_name(baseline), requested_window_kind: atom_name(requested)}}
  end

  defp backfill_range_error({:coverage_baseline_timezone_mismatch, baseline, requested}) do
    {:ok, "Coverage baseline timezone does not match requested range timezone",
     %{baseline_timezone: baseline, requested_timezone: requested}}
  end

  defp backfill_range_error(_reason), do: :error

  defp request_id(conn) do
    case get_resp_header(conn, "x-request-id") do
      [value | _] -> value
      _ -> conn.assigns[:request_id]
    end
  end

  @sse_retry_ms 3_000
  @sse_heartbeat_ms 15_000
  @sse_replay_limit 200

  defp sse_stream(conn, stream) do
    if plug_test_conn?(conn) do
      case fetch_replay_events(stream) do
        {:ok, replay_events} ->
          sse_test_stream(conn, stream, replay_events)

        {:error, :cursor_invalid} ->
          error(conn, 410, "cursor_expired", "Cursor is invalid or no longer replayable")

        {:error, _reason} ->
          error(conn, 400, "bad_request", "Request failed")
      end
    else
      sse_live_stream(conn, stream)
    end
  end

  defp sse_live_stream(conn, stream) do
    with :ok <- subscribe_sse_stream(stream),
         {:ok, replay_events} <- fetch_replay_events(stream) do
      conn =
        conn
        |> put_resp_content_type("text/event-stream")
        |> put_resp_header("cache-control", "no-cache, no-transform")
        |> put_resp_header("x-accel-buffering", "no")
        |> send_chunked(200)

      heartbeat_ref = Process.send_after(self(), :sse_heartbeat, @sse_heartbeat_ms)

      try do
        with {:ok, conn} <- chunk(conn, "retry: #{@sse_retry_ms}\n\n"),
             {:ok, conn, cursor} <- chunk_replay_events(conn, stream, replay_events),
             {:ok, conn} <- chunk(conn, sse_ready_body(stream_name(stream), cursor)) do
          sse_live_loop(conn, stream, cursor, heartbeat_ref)
        else
          {:error, _reason} -> conn
        end
      after
        Process.cancel_timer(heartbeat_ref)
        unsubscribe_sse_stream(stream)
      end
    else
      {:error, :cursor_invalid} ->
        unsubscribe_sse_stream(stream)
        error(conn, 410, "cursor_expired", "Cursor is invalid or no longer replayable")

      {:error, _reason} ->
        unsubscribe_sse_stream(stream)
        error(conn, 400, "bad_request", "Request failed")
    end
  end

  defp fetch_replay_events({:run, run_id, sequence}) do
    run_id
    |> FavnOrchestrator.list_run_stream_events(
      after_sequence: sequence,
      limit: @sse_replay_limit + 1
    )
    |> reject_incomplete_replay_page()
  end

  defp fetch_replay_events({:global, nil}) do
    FavnOrchestrator.list_global_run_stream_events(
      after_global_sequence: nil,
      limit: @sse_replay_limit
    )
  end

  defp fetch_replay_events({:global, global_sequence}) do
    [after_global_sequence: global_sequence, limit: @sse_replay_limit + 1]
    |> FavnOrchestrator.list_global_run_stream_events()
    |> reject_incomplete_replay_page()
  end

  defp reject_incomplete_replay_page({:ok, events}) when length(events) > @sse_replay_limit do
    {:error, :cursor_invalid}
  end

  defp reject_incomplete_replay_page({:ok, events}), do: {:ok, events}
  defp reject_incomplete_replay_page({:error, _reason} = error), do: error

  defp sse_test_stream(conn, stream, replay_events) do
    {_status, body, cursor} =
      Enum.reduce(
        replay_events,
        {:ok, "retry: #{@sse_retry_ms}\n\n", initial_cursor(stream)},
        fn event, {:ok, body, _cursor} ->
          cursor = event_cursor(stream, event)
          {:ok, body <> sse_run_event_body(event, stream, cursor), cursor}
        end
      )

    body = body <> sse_ready_body(stream_name(stream), cursor) <> ": heartbeat\n\n"

    conn
    |> put_resp_content_type("text/event-stream")
    |> put_resp_header("cache-control", "no-cache, no-transform")
    |> put_resp_header("x-accel-buffering", "no")
    |> send_resp(200, body)
  end

  defp plug_test_conn?(conn) do
    match?({Plug.Adapters.Test.Conn, _}, conn.adapter)
  end

  defp chunk_replay_events(conn, stream, events) do
    Enum.reduce_while(events, {:ok, conn, initial_cursor(stream)}, fn event,
                                                                      {:ok, conn, _cursor} ->
      cursor = event_cursor(stream, event)

      case chunk(conn, sse_run_event_body(event, stream, cursor)) do
        {:ok, conn} -> {:cont, {:ok, conn, cursor}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp sse_live_loop(conn, stream, cursor, heartbeat_ref) do
    receive do
      {:favn_run_event, %RunEvent{} = event} ->
        case maybe_hydrate_live_event(stream, event, cursor) do
          {:ok, nil} ->
            sse_live_loop(conn, stream, cursor, heartbeat_ref)

          {:ok, hydrated} ->
            next_cursor = event_cursor(stream, hydrated)

            case chunk(conn, sse_run_event_body(hydrated, stream, next_cursor)) do
              {:ok, conn} -> sse_live_loop(conn, stream, next_cursor, heartbeat_ref)
              {:error, _reason} -> conn
            end

          {:error, _reason} ->
            sse_live_loop(conn, stream, cursor, heartbeat_ref)
        end

      :sse_heartbeat ->
        next_ref = Process.send_after(self(), :sse_heartbeat, @sse_heartbeat_ms)
        Process.cancel_timer(heartbeat_ref)

        case chunk(conn, ": heartbeat\n\n") do
          {:ok, conn} -> sse_live_loop(conn, stream, cursor, next_ref)
          {:error, _reason} -> conn
        end
    end
  end

  defp maybe_hydrate_live_event(
         {:run, run_id, _initial_sequence},
         %RunEvent{run_id: run_id} = event,
         cursor
       ) do
    if event.sequence > run_sequence_from_cursor(cursor), do: {:ok, event}, else: {:ok, nil}
  end

  defp maybe_hydrate_live_event({:run, _run_id, _initial_sequence}, _event, _cursor),
    do: {:ok, nil}

  defp maybe_hydrate_live_event({:global, _initial_sequence}, %RunEvent{} = event, cursor) do
    with {:ok, [hydrated | _]} <-
           FavnOrchestrator.list_run_stream_events(event.run_id,
             after_sequence: event.sequence - 1,
             limit: 1
           ) do
      if hydrated.global_sequence &&
           hydrated.global_sequence > global_sequence_from_cursor(cursor) do
        {:ok, hydrated}
      else
        {:ok, nil}
      end
    end
  end

  defp subscribe_sse_stream({:run, run_id, _sequence}), do: FavnOrchestrator.subscribe_run(run_id)
  defp subscribe_sse_stream({:global, _sequence}), do: FavnOrchestrator.subscribe_runs()

  defp unsubscribe_sse_stream({:run, run_id, _sequence}),
    do: FavnOrchestrator.unsubscribe_run(run_id)

  defp unsubscribe_sse_stream({:global, _sequence}), do: FavnOrchestrator.unsubscribe_runs()

  defp sse_run_event_body(event, stream, cursor) do
    event_name = event_name(event.event_type)
    payload = Jason.encode!(sse_event_payload(event, stream, cursor, event_name))

    "id: #{cursor}\nevent: #{event_name}\ndata: #{payload}\n\n"
  end

  defp sse_ready_body(stream, nil) when is_binary(stream) do
    payload =
      Jason.encode!(%{
        schema_version: 1,
        stream: stream,
        event_type: "stream.ready",
        cursor: nil,
        occurred_at: DateTime.utc_now()
      })

    "event: stream.ready\ndata: #{payload}\n\n"
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

    "event: stream.ready\ndata: #{payload}\n\n"
  end

  defp sse_event_payload(event, stream, cursor, event_name) do
    %{
      schema_version: 1,
      event_id: cursor,
      stream: stream_name(stream),
      event_type: event_name,
      run_id: event.run_id,
      status: event_status(event.status),
      occurred_at: datetime(event.occurred_at),
      sequence: event.sequence,
      global_sequence: event.global_sequence,
      cursor: cursor,
      summary: event_summary(event),
      details: %{
        entity: Atom.to_string(event.entity),
        manifest_version_id: event.manifest_version_id,
        asset_ref: ref_to_string(event.asset_ref),
        stage: event.stage
      }
    }
  end

  defp event_summary(event), do: "Run #{event.run_id} #{event_name(event.event_type)}"

  defp stream_name({:run, run_id, _sequence}), do: "run:" <> run_id
  defp stream_name({:global, _sequence}), do: "runs"

  defp initial_cursor({:run, _run_id, 0}), do: nil

  defp initial_cursor({:run, run_id, sequence}),
    do: "run:" <> run_id <> ":" <> Integer.to_string(sequence)

  defp initial_cursor({:global, nil}), do: nil
  defp initial_cursor({:global, sequence}), do: "global:" <> Integer.to_string(sequence)

  defp event_cursor({:run, _run_id, _sequence}, event), do: run_cursor(event)

  defp event_cursor({:global, _sequence}, event),
    do: "global:" <> Integer.to_string(event.global_sequence)

  defp run_sequence_from_cursor("run:" <> rest) do
    rest |> String.split(":") |> List.last() |> parse_positive_int(0)
  end

  defp run_sequence_from_cursor(_cursor), do: 0

  defp global_sequence_from_cursor("global:" <> sequence), do: parse_positive_int(sequence, 0)
  defp global_sequence_from_cursor(_cursor), do: 0

  defp parse_positive_int(value, default) do
    case Integer.parse(to_string(value)) do
      {int, ""} when int > 0 -> int
      _ -> default
    end
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
      target_refs: Enum.map(List.wrap(run.target_refs), &ref_to_string/1),
      asset_results: asset_results_dto(run.asset_results),
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
      params: normalize_data(run.params),
      trigger: normalize_data(run.trigger),
      metadata: normalize_data(run.metadata),
      result: normalize_data(run.result),
      pipeline: normalize_data(run.pipeline),
      pipeline_context: normalize_data(run.pipeline_context),
      asset_results: asset_results_dto(run.asset_results),
      node_results: node_results_dto(run.node_results),
      error: inspect_term(run.error)
    }
  end

  defp asset_results_dto(results) when is_map(results) do
    results
    |> Map.values()
    |> Enum.map(&asset_result_dto/1)
    |> Enum.sort_by(&{&1.stage || 0, &1.asset_ref || ""})
  end

  defp asset_result_dto(%AssetResult{} = result) do
    %{
      asset_ref: ref_to_string(result.ref),
      stage: result.stage,
      status: atom_name(result.status),
      started_at: datetime(result.started_at),
      finished_at: datetime(result.finished_at),
      duration_ms: result.duration_ms,
      meta: normalize_data(result.meta),
      error: normalize_data(result.error),
      attempt_count: result.attempt_count,
      max_attempts: result.max_attempts,
      attempts: normalize_data(result.attempts),
      next_retry_at: datetime(result.next_retry_at)
    }
  end

  defp asset_result_dto(result) when is_map(result) do
    result
    |> normalize_data()
    |> Map.put_new("asset_ref", ref_to_string(Map.get(result, :ref) || Map.get(result, "ref")))
  end

  defp asset_result_dto(result), do: %{asset_ref: nil, error: inspect_term(result)}

  defp node_results_dto(results) when is_map(results) do
    results
    |> Enum.map(fn {node_key, result} ->
      %{
        node_key: normalize_data(node_key),
        result: asset_result_dto(result)
      }
    end)
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

  defp inspection_result_dto(%RelationInspectionResult{} = result) do
    %{
      asset_ref: ref_to_string(result.asset_ref),
      relation_ref: relation_ref_dto(result.relation_ref),
      relation: sql_relation_dto(result.relation),
      columns: Enum.map(List.wrap(result.columns), &sql_column_dto/1),
      row_count: result.row_count,
      sample: normalize_data(result.sample),
      table_metadata: normalize_data(result.table_metadata),
      adapter: module_name(result.adapter),
      inspected_at: datetime(result.inspected_at),
      warnings: normalize_data(result.warnings),
      error: normalize_data(result.error)
    }
  end

  defp inspection_result_dto(result), do: normalize_data(result)

  defp relation_ref_dto(nil), do: nil

  defp relation_ref_dto(%Favn.RelationRef{} = ref) do
    %{
      connection: atom_name(ref.connection),
      catalog: ref.catalog,
      schema: ref.schema,
      name: ref.name
    }
  end

  defp sql_relation_dto(nil), do: nil

  defp sql_relation_dto(%{__struct__: _struct} = relation) do
    %{
      catalog: Map.get(relation, :catalog),
      schema: Map.get(relation, :schema),
      name: Map.get(relation, :name),
      type: atom_name(Map.get(relation, :type)),
      metadata: normalize_data(Map.get(relation, :metadata, %{}))
    }
  end

  defp sql_relation_dto(relation), do: normalize_data(relation)

  defp sql_column_dto(%{__struct__: _struct} = column) do
    %{
      name: Map.get(column, :name),
      position: Map.get(column, :position),
      data_type: Map.get(column, :data_type),
      nullable: Map.get(column, :nullable?),
      default: normalize_data(Map.get(column, :default)),
      comment: Map.get(column, :comment),
      metadata: normalize_data(Map.get(column, :metadata, %{}))
    }
  end

  defp sql_column_dto(column), do: normalize_data(column)

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

  defp normalize_data(%AssetResult{} = value), do: asset_result_dto(value)
  defp normalize_data(%DateTime{} = value), do: datetime(value)

  defp normalize_data(value) when is_map(value) do
    value
    |> Enum.map(fn {key, val} -> {to_string(key), normalize_data(val)} end)
    |> Map.new()
  end

  defp normalize_data(value) when is_list(value), do: Enum.map(value, &normalize_data/1)
  defp normalize_data({module, name}), do: ref_to_string({module, name})
  defp normalize_data(nil), do: nil
  defp normalize_data(value) when is_boolean(value), do: value
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

  defp ref_to_string(%{"module" => module, "name" => name})
       when is_binary(module) and is_binary(name) do
    module <> ":" <> name
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
