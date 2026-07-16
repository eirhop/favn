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

  plug(:match)
  plug(:dispatch)

  get "/in-flight" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, runs} <- FavnOrchestrator.list_in_flight_runs() do
      run_ids = Enum.map(runs, & &1.id)
      Response.data(conn, 200, %{count: length(run_ids), run_ids: run_ids})
    else
      {:error, reason} -> authentication_error(conn, reason)
    end
  end

  get "/" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor} <- Authentication.actor_context(conn, :viewer),
         {:ok, filters} <- Filters.runs(conn.params),
         {:ok, runs} <- FavnOrchestrator.list_runs(filters) do
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
         {:ok, _session, _actor} <- Authentication.actor_context(conn, :viewer),
         {:ok, opts} <- RunEventQuery.from_params(conn.params),
         {:ok, events} <- FavnOrchestrator.list_run_events(run_id, opts) do
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
         {:ok, _session, _actor} <- Authentication.actor_context(conn, :viewer),
         {:ok, run} <- FavnOrchestrator.get_run(run_id) do
      Response.data(conn, 200, %{run: DTO.run_detail(run)})
    else
      {:error, :not_found} ->
        Response.error(conn, 404, "not_found", "Run was not found")

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  post "/" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, session, actor} <- Authentication.actor_context(conn, :operator) do
      params = conn.body_params

      IdempotentCommand.run(conn, "run.submit", actor.id, session.id, params, fn idempotency ->
        submit(conn, params, session, actor, idempotency)
      end)
    else
      {:error, reason} -> authentication_error(conn, reason)
    end
  end

  post "/:run_id/cancel" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, session, actor} <- Authentication.actor_context(conn, :operator) do
      IdempotentCommand.run(conn, "run.cancel", actor.id, session.id, %{run_id: run_id}, fn
        idempotency -> cancel(conn, run_id, session, actor, idempotency)
      end)
    else
      {:error, reason} -> authentication_error(conn, reason)
    end
  end

  post "/:run_id/rerun" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, session, actor} <- Authentication.actor_context(conn, :operator) do
      IdempotentCommand.run(conn, "run.rerun", actor.id, session.id, %{run_id: run_id}, fn
        idempotency -> rerun(conn, run_id, session, actor, idempotency)
      end)
    else
      {:error, reason} -> authentication_error(conn, reason)
    end
  end

  match _ do
    Response.error(conn, 404, "not_found", "Route was not found")
  end

  defp submit(conn, params, session, actor, idempotency) do
    case OperatorCommands.submit_run(params, %{actor: actor, session: session}) do
      {:ok, run_id} ->
        audit(conn, "run.submit", run_id, session, actor, idempotency)
        {:ok, 201, %{run: run_summary(run_id)}, "run", run_id}

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

  defp submit_error(:active_manifest_not_set),
    do: {:error, 404, "not_found", "Active manifest is not set", %{}}

  defp submit_error(reason) when is_tuple(reason),
    do: CommandErrors.operator(reason) || CommandErrors.window(reason)

  defp submit_error(reason) do
    Logger.error("run.submit failed after request validation: #{inspect(reason)}")
    {:error, 400, "bad_request", "Request failed", %{}}
  end

  defp cancel(conn, run_id, session, actor, idempotency) do
    case FavnOrchestrator.cancel_run(run_id, %{actor_id: actor.id}) do
      :ok ->
        audit(conn, "run.cancel", run_id, session, actor, idempotency)
        {:ok, 200, %{cancelled: true, run_id: run_id}, "run", run_id}

      {:error, :not_found} ->
        {:error, 404, "not_found", "Run was not found", %{}}

      {:error, :backfill_parent_cancel_not_supported} ->
        {:error, 409, "conflict",
         "Backfill parent runs cannot be cancelled through generic run cancellation", %{}}

      {:error, _reason} ->
        {:error, 400, "bad_request", "Request failed", %{}}
    end
  end

  defp rerun(conn, run_id, session, actor, idempotency) do
    case FavnOrchestrator.rerun(run_id) do
      {:ok, rerun_id} ->
        audit(conn, "run.rerun", rerun_id, session, actor, idempotency)
        {:ok, 201, %{run: run_summary(rerun_id)}, "run", rerun_id}

      {:error, :not_found} ->
        {:error, 404, "not_found", "Run was not found", %{}}

      {:error, :backfill_parent_rerun_not_supported} ->
        {:error, 409, "conflict",
         "Backfill parent runs cannot be rerun through generic run rerun", %{}}

      {:error, _reason} ->
        {:error, 400, "bad_request", "Request failed", %{}}
    end
  end

  defp audit(conn, action, run_id, session, actor, idempotency) do
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
    |> Audit.put_best_effort()
  end

  defp run_summary(run_id) do
    case FavnOrchestrator.get_run(run_id) do
      {:ok, run} -> DTO.run_summary(run)
      {:error, reason} -> fallback_run_summary(run_id, reason)
    end
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
