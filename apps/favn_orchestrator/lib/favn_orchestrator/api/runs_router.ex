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
        Response.error(conn, 400, "bad_request", "Request failed")
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
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/:run_id" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor, context} <- actor_context(conn, :viewer),
         {:ok, run} <- get_run(context, run_id) do
      Response.data(conn, 200, %{run: DTO.run_detail(run)})
    else
      {:error, :not_found} ->
        Response.error(conn, 404, "not_found", "Run was not found")

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
        {:ok, 201, %{run: run_summary(context, run_id)}, "run", run_id}

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

      {:error, _reason} ->
        {:error, 400, "bad_request", "Request failed", %{}}
    end
  end

  defp rerun(conn, run_id, params, session, actor, context, idempotency) do
    with {:ok, opts} <- rerun_options(params),
         {:ok, rerun_id} <-
           rerun(context, run_id, opts ++ rerun_command_options(context, idempotency)) do
      audit(conn, context, "run.rerun", rerun_id, session, actor, idempotency)
      {:ok, 201, %{run: run_summary(context, rerun_id)}, "run", rerun_id}
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

  defp audit(conn, context, action, run_id, session, actor, idempotency) do
    %{
      action: action,
      actor_id: actor.id,
      session_id: session.id,
      resource_type: "run",
      resource_id: run_id,
      outcome: "accepted",
      service_identity: Authentication.service_identity(conn)
    }
    |> Map.merge(IdempotentCommand.audit_metadata(idempotency, "accepted"))
    |> then(&Audit.put_best_effort(context, &1))
  end

  defp run_summary(context, run_id) do
    case get_run(context, run_id) do
      {:ok, run} -> DTO.run_summary(run)
      {:error, reason} -> fallback_run_summary(run_id, reason)
    end
  end

  defp actor_context(conn, role), do: Authentication.workspace_context(conn, role)

  defp in_flight_context(conn) do
    case Authentication.service_workspace_context(conn) do
      {:ok, _session, _actor, context} ->
        {:ok, context}

      {:error, _service_reason} ->
        case Authentication.workspace_context(conn, :viewer) do
          {:ok, _session, _actor, context} -> {:ok, context}
          {:error, _reason} = error -> error
        end
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
      _idempotency: idempotency.command_idempotency
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
