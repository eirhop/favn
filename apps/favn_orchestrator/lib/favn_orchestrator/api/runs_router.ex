defmodule FavnOrchestrator.API.RunsRouter do
  @moduledoc false

  use Plug.Router

  require Logger

  alias FavnOrchestrator
  alias FavnOrchestrator.API.Audit
  alias FavnOrchestrator.API.Authentication
  alias FavnOrchestrator.API.CommandErrors
  alias FavnOrchestrator.API.DTO
  alias FavnOrchestrator.API.Filters
  alias FavnOrchestrator.API.IdempotentCommand
  alias FavnOrchestrator.API.OperatorCommands
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.RunEvents.Query, as: RunEventQuery
  alias FavnOrchestrator.Runs, as: RunDomain
  alias Favn.Replay.InputMode
  alias Favn.Retry.Policy

  plug(:match)
  plug(:dispatch)

  get "/in-flight" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, context} <- in_flight_context(conn),
         {:ok, runs} <- list_in_flight_runs(context) do
      run_ids = Enum.map(runs, & &1.id)
      Response.data(conn, 200, %{count: length(run_ids), run_ids: run_ids})
    else
      {:error, reason} -> authentication_error(conn, reason)
    end
  end

  get "/" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor, context} <- actor_context(conn, :viewer),
         {:ok, filters} <- Filters.runs(conn.params),
         {:ok, runs} <- list_runs(context, filters) do
      Response.data(conn, 200, %{items: Enum.map(runs, &DTO.run_summary/1)})
    else
      {:error, :invalid_filter} ->
        validation_error(conn, "Invalid run query filters")

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Logger.error("persisted run list is unavailable")
        Response.error(conn, 500, "runs_unavailable", "Runs could not be loaded")
    end
  end

  get "/submissions/stats" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor, context} <- actor_context(conn, :viewer),
         {:ok, stats} <- FavnOrchestrator.run_submission_stats(context) do
      Response.data(conn, 200, %{stats: DTO.run_submission_stats(stats)})
    else
      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Response.error(
          conn,
          503,
          "run_submissions_unavailable",
          "Queue statistics are unavailable"
        )
    end
  end

  get "/submissions" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor, context} <- actor_context(conn, :viewer),
         {:ok, opts} <- submission_page_options(conn.params),
         {:ok, page} <- FavnOrchestrator.page_run_submissions(context, opts) do
      Response.data(conn, 200, %{
        items: Enum.map(page.items, &DTO.run_submission/1),
        next: DTO.normalize(page.next)
      })
    else
      {:error, :invalid_submission_filters} ->
        validation_error(conn, "Invalid run submission query filters")

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Response.error(
          conn,
          503,
          "run_submissions_unavailable",
          "Run submissions are unavailable"
        )
    end
  end

  get "/submissions/:run_id" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor, context} <- actor_context(conn, :viewer),
         {:ok, submission} <- FavnOrchestrator.get_run_submission(context, run_id) do
      Response.data(conn, 200, %{submission: DTO.run_submission(submission)})
    else
      {:error, %{kind: :not_found}} ->
        Response.error(conn, 404, "not_found", "Run submission was not found")

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Response.error(conn, 503, "run_submissions_unavailable", "Run submission is unavailable")
    end
  end

  get "/:run_id/events" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor, context} <- actor_context(conn, :viewer),
         {:ok, opts} <- RunEventQuery.from_params(conn.params),
         {:ok, events} <- list_run_events(context, run_id, opts) do
      Response.data(conn, 200, %{items: Enum.map(events, &DTO.run_event/1)})
    else
      {:error, :invalid_opts} ->
        validation_error(conn, "Invalid event query options")

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Logger.error("persisted run events are unavailable", run_id: run_id)
        Response.error(conn, 500, "run_events_unavailable", "Run events could not be loaded")
    end
  end

  get "/:run_id" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor, context} <- actor_context(conn, :viewer) do
      run_detail_response(conn, context, run_id)
    else
      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Logger.error("persisted run detail is unavailable", run_id: run_id)
        Response.error(conn, 500, "run_unavailable", "Run could not be loaded")
    end
  end

  post "/" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, session, actor, context} <- actor_context(conn, :operator) do
      params = conn.body_params

      idempotent_run(
        conn,
        context,
        "run.submit",
        actor.id,
        session.id,
        params,
        fn idempotency -> submit(conn, params, session, actor, context, idempotency) end
      )
    else
      {:error, reason} -> authentication_error(conn, reason)
    end
  end

  post "/:run_id/cancel" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, session, actor, context} <- actor_context(conn, :operator) do
      idempotent_run(
        conn,
        context,
        "run.cancel",
        actor.id,
        session.id,
        %{run_id: run_id},
        fn idempotency -> cancel(conn, run_id, session, actor, context, idempotency) end
      )
    else
      {:error, reason} -> authentication_error(conn, reason)
    end
  end

  post "/:run_id/rerun" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, session, actor, context} <- actor_context(conn, :operator) do
      params = if is_map(conn.body_params), do: conn.body_params, else: %{}

      idempotent_run(
        conn,
        context,
        "run.rerun",
        actor.id,
        session.id,
        %{run_id: run_id, options: params},
        fn idempotency ->
          rerun(conn, run_id, params, session, actor, context, idempotency)
        end
      )
    else
      {:error, reason} -> authentication_error(conn, reason)
    end
  end

  match _ do
    Response.error(conn, 404, "not_found", "Route was not found")
  end

  defp submit(conn, params, session, actor, context, idempotency) do
    command_context = context || %{actor: actor, session: session}

    opts = command_options(context, idempotency)

    case OperatorCommands.submit_run(params, command_context, opts) do
      {:ok, run_id} ->
        audit(conn, context, "run.submit", run_id, session, actor, idempotency)
        {:ok, 202, %{run: run_summary(context, run_id)}, "run", run_id}

      {:error, reason} ->
        submit_error(reason)
    end
  end

  defp submit_error(:invalid_target), do: validation_command_error("Invalid run target request")

  defp submit_error(:invalid_manifest_selection),
    do: validation_command_error("Invalid manifest selection")

  defp submit_error(:invalid_dependencies),
    do:
      {:error, 422, "validation_failed", "dependencies is only supported for asset targets",
       %{field: "dependencies"}}

  defp submit_error({:invalid_operator_timeout_ms, _value}),
    do: validation_command_error("Invalid timeout_ms")

  defp submit_error({reason, _value})
       when reason in [
              :invalid_operator_selection_source,
              :invalid_operator_selection_id,
              :invalid_operator_window
            ],
       do: validation_command_error("Invalid run window request")

  defp submit_error(:invalid_window_request),
    do: validation_command_error("Invalid run window request")

  defp submit_error(:invalid_asset_target),
    do: validation_command_error("Invalid asset target id")

  defp submit_error(:invalid_pipeline_target),
    do: validation_command_error("Invalid pipeline target id")

  defp submit_error(:ambiguous_asset_run_context),
    do:
      {:error, 422, "validation_failed", "Asset run context is required",
       %{field: "run_context_id"}}

  defp submit_error(:invalid_asset_run_context),
    do:
      {:error, 422, "validation_failed", "Invalid asset run context", %{field: "run_context_id"}}

  defp submit_error(:active_manifest_not_set),
    do: {:error, 404, "not_found", "Active manifest is not set", %{}}

  defp submit_error(reason) when is_tuple(reason),
    do:
      CommandErrors.admission(reason) || CommandErrors.operator(reason) ||
        CommandErrors.window(reason)

  defp submit_error(reason) do
    Logger.error("run.submit failed after request validation: #{inspect(reason)}")
    {:error, 400, "bad_request", "Request failed", %{}}
  end

  defp cancel(conn, run_id, session, actor, context, idempotency) do
    case cancel_run(context, run_id, %{actor_id: actor.id}, idempotency) do
      :ok ->
        audit(conn, context, "run.cancel", run_id, session, actor, idempotency)
        {:ok, 200, %{cancelled: true, run_id: run_id}, "run", run_id}

      {:error, :not_found} ->
        {:error, 404, "not_found", "Run was not found", %{}}

      {:error, :backfill_parent_cancel_not_supported} ->
        {:error, 409, "conflict",
         "Backfill parent runs cannot be cancelled through generic run cancellation", %{}}

      {:error, :idempotency_conflict} ->
        {:error, 409, "idempotency_conflict",
         "The idempotency key was already used with different request content", %{}}

      {:error, :run_already_terminal} ->
        audit(
          conn,
          context,
          "run.cancel",
          run_id,
          session,
          actor,
          idempotency,
          "already_terminal"
        )

        terminal_cancel_response(context, run_id)

      {:error, _reason} ->
        {:error, 400, "bad_request", "Request failed", %{}}
    end
  end

  defp terminal_cancel_response(context, run_id) do
    case RunDomain.get(context, run_id) do
      {:ok, run} ->
        {:ok, 200,
         %{
           cancelled: run.status == :cancelled,
           outcome: :already_terminal,
           run_id: run_id,
           status: run.status
         }, "run", run_id}

      {:error, _reason} ->
        {:error, 409, "conflict", "Run became terminal before cancellation", %{}}
    end
  end

  defp rerun(conn, run_id, params, session, actor, context, idempotency) do
    with {:ok, opts} <- rerun_options(params),
         {:ok, rerun_id} <-
           rerun(context, run_id, opts ++ rerun_command_options(context, idempotency)) do
      audit(conn, context, "run.rerun", rerun_id, session, actor, idempotency)
      {:ok, 202, %{run: run_summary(context, rerun_id)}, "run", rerun_id}
    else
      {:error, :invalid_input_mode} ->
        validation_command_error("Invalid input_mode")

      {:error, {:invalid_retry_policy, _reason}} ->
        validation_command_error("Invalid retry_policy")

      {:error, :not_found} ->
        {:error, 404, "not_found", "Run was not found", %{}}

      {:error, :backfill_parent_rerun_not_supported} ->
        {:error, 409, "conflict",
         "Backfill parent runs cannot be rerun through generic run rerun", %{}}

      {:error, _reason} ->
        {:error, 400, "bad_request", "Request failed", %{}}
    end
  end

  defp rerun_options(params) when is_map(params) do
    with {:ok, input_mode} <- optional_input_mode(Map.get(params, "input_mode")),
         {:ok, retry_policy} <- optional_retry_policy(Map.get(params, "retry_policy")) do
      {:ok,
       []
       |> put_optional(:input_mode, input_mode)
       |> put_optional(:retry_policy, retry_policy)}
    end
  end

  defp optional_input_mode(nil), do: {:ok, nil}
  defp optional_input_mode(value), do: InputMode.normalize(value)

  defp optional_retry_policy(nil), do: {:ok, nil}

  defp optional_retry_policy(value) do
    case Policy.new(value) do
      {:ok, policy} -> {:ok, policy}
      {:error, reason} -> {:error, {:invalid_retry_policy, reason}}
    end
  end

  defp put_optional(opts, _key, nil), do: opts
  defp put_optional(opts, key, value), do: Keyword.put(opts, key, value)

  defp audit(conn, context, action, run_id, session, actor, idempotency, outcome \\ "accepted") do
    %{
      action: action,
      actor_id: actor.id,
      session_id: session.id,
      resource_type: "run",
      resource_id: run_id,
      outcome: outcome,
      service_identity: Authentication.service_identity(conn)
    }
    |> Map.merge(IdempotentCommand.audit_metadata(idempotency, outcome))
    |> then(&Audit.put_best_effort(context, &1))
  end

  defp run_summary(context, run_id) do
    case get_run(context, run_id) do
      {:ok, run} ->
        DTO.run_summary(run)

      {:error, :not_found} ->
        case FavnOrchestrator.get_run_submission(context, run_id) do
          {:ok, submission} -> accepted_run_summary(submission)
          {:error, reason} -> fallback_run_summary(run_id, reason)
        end

      {:error, reason} ->
        fallback_run_summary(run_id, reason)
    end
  end

  defp run_detail_response(conn, context, run_id) do
    case get_run(context, run_id) do
      {:ok, run} ->
        Response.data(conn, 200, %{run: DTO.run_detail(run)})

      {:error, :not_found} ->
        case FavnOrchestrator.get_run_submission(context, run_id) do
          {:ok, submission} ->
            Response.data(conn, 200, %{
              run: accepted_run_summary(submission),
              submission: DTO.run_submission(submission)
            })

          {:error, %{kind: :not_found}} ->
            Response.error(conn, 404, "not_found", "Run was not found")

          {:error, _reason} ->
            Response.error(
              conn,
              503,
              "run_submissions_unavailable",
              "Run submission is unavailable"
            )
        end

      {:error, _reason} ->
        Logger.error("persisted run detail is unavailable", run_id: run_id)
        Response.error(conn, 500, "run_unavailable", "Run could not be loaded")
    end
  end

  defp actor_context(conn, role) do
    Authentication.workspace_or_service_context(conn, role)
  end

  defp in_flight_context(conn) do
    case Authentication.workspace_or_service_context(conn, :viewer) do
      {:ok, _session, _actor, context} -> {:ok, context}
      {:error, _reason} = error -> error
    end
  end

  defp list_in_flight_runs(context) do
    with {:ok, pending} <- FavnOrchestrator.list_runs(context, status: :pending, limit: 200),
         {:ok, running} <- FavnOrchestrator.list_runs(context, status: :running, limit: 200) do
      {:ok, pending ++ running}
    end
  end

  defp list_runs(context, filters), do: FavnOrchestrator.list_runs(context, filters)

  defp list_run_events(context, run_id, opts),
    do: FavnOrchestrator.list_run_events(context, run_id, opts)

  defp get_run(context, run_id), do: FavnOrchestrator.get_run(context, run_id)

  defp cancel_run(context, run_id, reason, idempotency),
    do:
      FavnOrchestrator.cancel_run(context, run_id, reason,
        idempotency: idempotency.command_idempotency
      )

  defp rerun(context, run_id, opts), do: FavnOrchestrator.rerun(context, run_id, opts)

  defp idempotent_run(conn, context, operation, actor_id, session_id, input, execute) do
    IdempotentCommand.run(
      conn,
      context,
      operation,
      actor_id,
      session_id,
      input,
      execute
    )
  end

  defp command_options(_context, idempotency) do
    [
      run_id: idempotency.run_id,
      idempotency: idempotency.command_idempotency
    ]
  end

  defp rerun_command_options(_context, idempotency) do
    [
      run_id: idempotency.run_id,
      _idempotency: idempotency.command_idempotency,
      submission_source: :api
    ]
  end

  defp fallback_run_summary(run_id, reason) do
    Logger.warning("run command accepted but summary lookup failed: #{inspect(reason)}")

    %{
      id: run_id,
      status: "accepted",
      submit_kind: "unknown",
      manifest_version_id: nil,
      event_seq: nil,
      started_at: nil,
      finished_at: nil,
      target_refs: [],
      asset_results: [],
      error: nil
    }
  end

  defp accepted_run_summary(submission) do
    %{
      id: submission.run_id,
      status: "accepted",
      submit_kind: submission.target_kind,
      manifest_version_id: submission.manifest_version_id,
      event_seq: nil,
      started_at: nil,
      finished_at: nil,
      target_refs: [submission.target_id],
      asset_results: [],
      error: submission.error
    }
  end

  defp submission_page_options(params) when is_map(params) do
    with {:ok, status} <- submission_status(Map.get(params, "status")),
         {:ok, limit} <- submission_limit(Map.get(params, "limit")),
         {:ok, after_cursor} <-
           submission_cursor(
             Map.get(params, "after_inserted_at"),
             Map.get(params, "after_submission_id")
           ) do
      {:ok,
       []
       |> put_optional(:status, status)
       |> put_optional(:limit, limit)
       |> put_optional(:after, after_cursor)}
    else
      _invalid -> {:error, :invalid_submission_filters}
    end
  end

  defp submission_status(nil), do: {:ok, nil}

  defp submission_status(status) when is_binary(status) do
    case status do
      "queued" -> {:ok, :queued}
      "preparing" -> {:ok, :preparing}
      "admitting" -> {:ok, :admitting}
      "submitted" -> {:ok, :submitted}
      "failed" -> {:ok, :failed}
      "cancelled" -> {:ok, :cancelled}
      "superseded" -> {:ok, :superseded}
      _invalid -> {:error, :invalid}
    end
  end

  defp submission_status(_invalid), do: {:error, :invalid}

  defp submission_limit(nil), do: {:ok, nil}

  defp submission_limit(value) when is_binary(value) do
    case Integer.parse(value) do
      {limit, ""} when limit in 1..200 -> {:ok, limit}
      _invalid -> {:error, :invalid}
    end
  end

  defp submission_limit(_invalid), do: {:error, :invalid}

  defp submission_cursor(nil, nil), do: {:ok, nil}

  defp submission_cursor(inserted_at, submission_id)
       when is_binary(inserted_at) and is_binary(submission_id) and
              byte_size(submission_id) in 1..255 do
    case DateTime.from_iso8601(inserted_at) do
      {:ok, datetime, 0} -> {:ok, %{inserted_at: datetime, submission_id: submission_id}}
      _invalid -> {:error, :invalid}
    end
  end

  defp submission_cursor(_inserted_at, _submission_id), do: {:error, :invalid}

  defp validation_command_error(message),
    do: {:error, 422, "validation_failed", message, %{}}

  defp validation_error(conn, message),
    do: Response.error(conn, 422, "validation_failed", message)

  defp authentication_error(conn, :forbidden),
    do: Response.error(conn, 403, "forbidden", "Actor does not have access")

  defp authentication_error(conn, :service_unauthorized),
    do: Response.error(conn, 401, "service_unauthorized", "Invalid service credentials")

  defp authentication_error(conn, :unauthenticated),
    do: Response.error(conn, 401, "unauthenticated", "Missing or invalid actor context")

  defp authentication_error(conn, _reason),
    do: Response.error(conn, 400, "bad_request", "Request failed")
end
