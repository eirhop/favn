defmodule FavnOrchestrator.API.BackfillsRouter do
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

  plug(:match)
  plug(:dispatch)

  post "/" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, session, actor} <- Authentication.actor_context(conn, :operator) do
      params = conn.body_params

      IdempotentCommand.run(
        conn,
        "backfill.submit",
        actor.id,
        session.id,
        params,
        fn idempotency -> submit_backfill(conn, params, session, actor, idempotency) end
      )
    else
      {:error, reason} -> authentication_error(conn, reason)
    end
  end

  post "/plan" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor} <- Authentication.actor_context(conn, :operator),
         {:ok, plan} <- OperatorCommands.plan_backfill(conn.body_params) do
      Response.data(conn, 200, %{plan: DTO.normalize(plan)})
    else
      {:error, :invalid_target} ->
        validation_error(conn, "Invalid backfill target request")

      {:error, :invalid_manifest_selection} ->
        validation_error(conn, "Invalid manifest selection")

      {:error, :invalid_backfill_range_request} ->
        validation_error(conn, "Invalid backfill range request")

      {:error, reason} when is_tuple(reason) ->
        send_command_error(conn, CommandErrors.operator(reason), reason)

      {:error, :active_manifest_not_set} ->
        Response.error(conn, 404, "not_found", "Active manifest is not set")

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/:backfill_run_id/windows" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor} <- Authentication.actor_context(conn, :viewer),
         {:ok, filters} <- Filters.backfill_windows(conn.params, backfill_run_id),
         {:ok, page} <- FavnOrchestrator.list_backfill_windows(filters) do
      Response.data(conn, 200, Response.page(page, &DTO.backfill_window/1))
    else
      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, :invalid_filter} ->
        validation_error(conn, "Invalid backfill window filter")

      {:error, :invalid_pagination} ->
        validation_error(conn, "Invalid pagination parameters")

      {:error, {:manifest_filter_lookup_failed, reason}} ->
        Logger.error("backfill_window.filter_lookup failed: #{inspect(reason)}")
        Response.error(conn, 400, "bad_request", "Request failed")

      {:error, _reason} ->
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  post "/:backfill_run_id/windows/rerun" do
    params = conn.body_params

    with :ok <- Authentication.ensure_service(conn),
         {:ok, session, actor} <- Authentication.actor_context(conn, :operator),
         {:ok, window_key} <- required_string(params, "window_key"),
         {:ok, window} <- find_window(backfill_run_id, window_key) do
      request =
        params
        |> Map.take(["refresh", "refresh_policy", "allow_success"])
        |> Map.merge(%{"backfill_run_id" => backfill_run_id, "window_key" => window_key})

      IdempotentCommand.run(
        conn,
        "backfill.window.rerun",
        actor.id,
        session.id,
        request,
        fn idempotency ->
          rerun_window(conn, params, backfill_run_id, window, session, actor, idempotency)
        end
      )
    else
      {:error, {:missing_field, field}} ->
        Response.error(conn, 422, "validation_failed", "Missing required field", %{field: field})

      {:error, :not_found} ->
        Response.error(conn, 404, "not_found", "Backfill window was not found")

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  post "/projections/repair" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, session, actor} <- Authentication.actor_context(conn, :operator),
         {:ok, opts} <- Filters.backfill_repair(conn.body_params),
         {:ok, report} <- FavnOrchestrator.repair_backfill_projections(opts) do
      Audit.put_if(Keyword.get(opts, :apply, false), %{
        action: "backfill.projections.repair",
        actor_id: actor.id,
        session_id: session.id,
        resource_type: "backfill_projection",
        resource_id: Filters.repair_resource_id(opts),
        outcome: "accepted",
        service_identity: Authentication.service_identity(conn)
      })

      Response.data(conn, 200, %{repair: DTO.normalize(report)})
    else
      {:error, :invalid_repair_scope} ->
        validation_error(conn, "Invalid backfill projection repair scope")

      {:error, :invalid_filter} ->
        validation_error(conn, "Invalid backfill projection repair filter")

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/coverage-baselines" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor} <- Authentication.actor_context(conn, :viewer),
         {:ok, filters} <- Filters.coverage_baselines(conn.params),
         {:ok, page} <- FavnOrchestrator.list_coverage_baselines(filters) do
      Response.data(conn, 200, Response.page(page, &DTO.coverage_baseline/1))
    else
      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, :invalid_filter} ->
        validation_error(conn, "Invalid coverage baseline filter")

      {:error, :invalid_pagination} ->
        validation_error(conn, "Invalid pagination parameters")

      {:error, {:manifest_filter_lookup_failed, reason}} ->
        Logger.error("coverage_baseline.filter_lookup failed: #{inspect(reason)}")
        Response.error(conn, 400, "bad_request", "Request failed")

      {:error, _reason} ->
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  match _ do
    Response.error(conn, 404, "not_found", "Route was not found")
  end

  defp submit_backfill(conn, params, session, actor, idempotency) do
    case OperatorCommands.submit_backfill(params) do
      {:ok, run_id} ->
        audit_command(conn, "backfill.submit", run_id, session, actor, idempotency)
        {:ok, 201, %{run: run_summary(run_id)}, "run", run_id}

      {:error, :invalid_target} ->
        command_validation_error("Invalid backfill target request")

      {:error, :invalid_manifest_selection} ->
        command_validation_error("Invalid manifest selection")

      {:error, :invalid_backfill_range_request} ->
        command_validation_error("Invalid backfill range request")

      {:error, reason} when is_tuple(reason) ->
        CommandErrors.operator(reason) || CommandErrors.backfill(reason)

      {:error, :active_manifest_not_set} ->
        {:error, 404, "not_found", "Active manifest is not set", %{}}

      {:error, _reason} ->
        {:error, 400, "bad_request", "Request failed", %{}}
    end
  end

  defp rerun_window(conn, params, backfill_run_id, window, session, actor, idempotency) do
    case FavnOrchestrator.rerun_backfill_window(
           backfill_run_id,
           window.pipeline_module,
           window.window_key,
           rerun_options(params)
         ) do
      {:ok, run_id} ->
        audit_command(conn, "backfill.window.rerun", run_id, session, actor, idempotency)
        {:ok, 201, %{run: run_summary(run_id)}, "run", run_id}

      {:error, :backfill_window_not_rerunnable} ->
        {:error, 409, "conflict", "Backfill window is not rerunnable", %{}}

      {:error, :backfill_window_has_no_attempt} ->
        {:error, 409, "conflict", "Backfill window has no attempt to rerun", %{}}

      {:error, :successful_backfill_window_requires_force_refresh} ->
        {:error, 409, "conflict", "Successful backfill window rerun requires force refresh", %{}}

      {:error, _reason} ->
        {:error, 400, "bad_request", "Request failed", %{}}
    end
  end

  defp audit_command(conn, action, run_id, session, actor, idempotency) do
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
    Logger.warning("backfill command accepted but summary lookup failed: #{inspect(reason)}")

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

  defp rerun_options(params) do
    []
    |> put_optional(:refresh, Map.get(params, "refresh"))
    |> put_optional(:refresh_policy, Map.get(params, "refresh_policy"))
    |> Keyword.put(:allow_success, Map.get(params, "allow_success") == true)
  end

  defp put_optional(opts, _key, nil), do: opts
  defp put_optional(opts, _key, ""), do: opts
  defp put_optional(opts, key, value), do: Keyword.put(opts, key, value)

  defp find_window(backfill_run_id, window_key) do
    with {:ok, page} <-
           FavnOrchestrator.list_backfill_windows(
             backfill_run_id: backfill_run_id,
             window_key: window_key,
             limit: 1
           ) do
      case page.items do
        [window | _rest] -> {:ok, window}
        [] -> {:error, :not_found}
      end
    end
  end

  defp required_string(params, key) do
    case Map.get(params, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing_or_invalid -> {:error, {:missing_field, key}}
    end
  end

  defp send_command_error(conn, nil, reason), do: CommandErrors.send_backfill(conn, reason)

  defp send_command_error(conn, {:error, status, code, message, details}, _reason),
    do: Response.error(conn, status, code, message, details)

  defp authentication_error(conn, :forbidden),
    do: Response.error(conn, 403, "forbidden", "Actor does not have access")

  defp authentication_error(conn, :service_unauthorized),
    do: Response.error(conn, 401, "service_unauthorized", "Invalid service credentials")

  defp authentication_error(conn, :unauthenticated),
    do: Response.error(conn, 401, "unauthenticated", "Missing or invalid actor context")

  defp authentication_error(conn, _reason),
    do: Response.error(conn, 400, "bad_request", "Request failed")

  defp validation_error(conn, message),
    do: Response.error(conn, 422, "validation_failed", message)

  defp command_validation_error(message),
    do: {:error, 422, "validation_failed", message, %{}}
end
