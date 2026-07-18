defmodule FavnOrchestrator.API.BackfillsRouter do
  @moduledoc false

  use Plug.Router

  require Logger

  alias FavnOrchestrator
  alias FavnOrchestrator.API.Audit
  alias FavnOrchestrator.API.Authentication
  alias FavnOrchestrator.API.CommandErrors
  alias FavnOrchestrator.API.DTO
  alias FavnOrchestrator.API.IdempotentCommand
  alias FavnOrchestrator.API.OperatorCommands
  alias FavnOrchestrator.API.Response

  plug(:match)
  plug(:dispatch)

  post "/" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, session, actor, context} <- actor_context(conn, :operator) do
      params = conn.body_params

      idempotent_submit(conn, context, params, session, actor)
    else
      {:error, reason} -> authentication_error(conn, reason)
    end
  end

  post "/plan" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor, context} <- actor_context(conn, :operator),
         {:ok, plan} <- plan_backfill(conn.body_params, context) do
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
         {:ok, _session, _actor, context} <- actor_context(conn, :viewer),
         {:ok, page} <- list_windows(context, backfill_run_id, conn.params) do
      Response.data(conn, 200, backfill_page(page))
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

  match _ do
    Response.error(conn, 404, "not_found", "Route was not found")
  end

  defp submit_backfill(conn, params, session, actor, context, idempotency) do
    opts = [
      root_run_id: idempotency.run_id,
      idempotency: idempotency.command_idempotency
    ]

    case OperatorCommands.submit_backfill(params, context, opts) do
      {:ok, backfill} ->
        audit_command(
          conn,
          "backfill.submit",
          backfill.backfill_id,
          session,
          actor,
          context,
          idempotency
        )

        {:ok, 202, %{backfill: DTO.backfill(backfill)}, "backfill", backfill.backfill_id}

      {:error, :invalid_target} ->
        command_validation_error("Invalid backfill target request")

      {:error, :invalid_manifest_selection} ->
        command_validation_error("Invalid manifest selection")

      {:error, :invalid_backfill_range_request} ->
        command_validation_error("Invalid backfill range request")

      {:error, reason} when is_tuple(reason) ->
        CommandErrors.operator(reason) || CommandErrors.backfill(reason)

      {:error, _reason} ->
        {:error, 400, "bad_request", "Request failed", %{}}
    end
  end

  defp idempotent_submit(conn, context, params, session, actor) do
    IdempotentCommand.run(
      conn,
      context,
      "backfill.submit",
      actor.id,
      session.id,
      params,
      fn idempotency ->
        submit_backfill(conn, params, session, actor, context, idempotency)
      end
    )
  end

  defp actor_context(conn, role), do: Authentication.workspace_context(conn, role)

  defp plan_backfill(params, context),
    do: OperatorCommands.plan_backfill(params, context)

  defp list_windows(context, backfill_id, params) do
    with {:ok, opts} <- window_page_options(params) do
      FavnOrchestrator.Backfills.page_windows(context, backfill_id, opts)
    end
  end

  defp window_page_options(params) do
    with {:ok, limit} <- page_limit(Map.get(params, "limit", "100")),
         {:ok, status} <- window_status(Map.get(params, "status")),
         {:ok, cursor} <- window_cursor(Map.get(params, "cursor")) do
      {:ok, [limit: limit, status: status, after: cursor]}
    end
  end

  defp page_limit(value) when is_integer(value) and value in 1..200, do: {:ok, value}

  defp page_limit(value) when is_binary(value) do
    case Integer.parse(value) do
      {limit, ""} when limit in 1..200 -> {:ok, limit}
      _invalid -> {:error, :invalid_pagination}
    end
  end

  defp page_limit(_value), do: {:error, :invalid_pagination}

  defp window_status(nil), do: {:ok, nil}
  defp window_status("planned"), do: {:ok, :planned}
  defp window_status("ready"), do: {:ok, :ready}
  defp window_status("claimed"), do: {:ok, :claimed}
  defp window_status("running"), do: {:ok, :running}
  defp window_status("succeeded"), do: {:ok, :succeeded}
  defp window_status("failed"), do: {:ok, :failed}
  defp window_status("cancelled"), do: {:ok, :cancelled}
  defp window_status(_status), do: {:error, :invalid_filter}

  defp window_cursor(nil), do: {:ok, nil}

  defp window_cursor(value) when is_binary(value) do
    with {:ok, decoded} <- Base.url_decode64(value, padding: false),
         {:ok, %{"window_key" => key, "window_id" => id}} <- Jason.decode(decoded),
         true <- is_binary(key) and is_binary(id) do
      {:ok, %{window_key: key, window_id: id}}
    else
      _invalid -> {:error, :invalid_pagination}
    end
  end

  defp backfill_page(%FavnOrchestrator.Page{} = page),
    do: Response.page(page, &DTO.backfill_window/1)

  defp backfill_page(%FavnOrchestrator.Persistence.Results.CursorPage{} = page) do
    %{
      items: Enum.map(page.items, &DTO.backfill_window/1),
      pagination: %{
        limit: page.limit,
        has_more: page.has_more?,
        next_cursor: encode_cursor(page.next_cursor)
      }
    }
  end

  defp encode_cursor(nil), do: nil

  defp encode_cursor(cursor) do
    cursor
    |> Jason.encode!()
    |> Base.url_encode64(padding: false)
  end

  defp audit_command(conn, action, run_id, session, actor, context, idempotency) do
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
