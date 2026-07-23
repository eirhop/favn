defmodule FavnOrchestrator.API.RebuildsRouter do
  @moduledoc false

  use Plug.Router

  require Logger

  alias FavnOrchestrator.API.Audit
  alias FavnOrchestrator.API.Authentication
  alias FavnOrchestrator.API.IdempotentCommand
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.Operator.Rebuilds, as: RebuildDTO
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Rebuilds
  alias FavnOrchestrator.Redaction

  @states ~w(planned queued building validating activating activation_unknown reconciling cancelling succeeded failed cancelled)
  @item_statuses ~w(planned ready claimed running succeeded failed cancelled outcome_unknown)

  plug(:match)
  plug(:dispatch)

  post "/plan" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, target_id, reason} <- plan_request(conn.body_params),
         {:ok, session, actor, context} <- Authentication.workspace_context(conn, :operator) do
      IdempotentCommand.run(
        conn,
        context,
        "rebuild.plan",
        actor.id,
        session.id,
        conn.body_params,
        fn idempotency ->
          operation_id = rebuild_operation_id(idempotency)

          case Rebuilds.plan(context, target_id, reason,
                 operation_id: operation_id,
                 idempotency_key: idempotency.key_hash,
                 idempotency: idempotency.command_idempotency
               ) do
            {:ok, plan} ->
              audit(
                conn,
                context,
                session,
                actor,
                idempotency,
                "rebuild.plan",
                operation_id,
                %{target_id: target_id, reason: reason, plan_hash: plan.plan_hash},
                "accepted",
                plan.idempotency_replay?
              )

              {:ok, 201, %{plan: RebuildDTO.plan(plan, admin?(context))}, "rebuild", operation_id}

            {:error, failure} ->
              audit(
                conn,
                context,
                session,
                actor,
                idempotency,
                "rebuild.plan",
                operation_id,
                %{target_id: target_id, reason: reason, error_code: error_code(failure)},
                "rejected",
                false
              )

              command_error(failure)
          end
        end
      )
    else
      {:error, reason} -> respond_error(conn, reason)
    end
  end

  post "/" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, plan_id, plan_hash} <- start_request(conn.body_params),
         {:ok, session, actor, context} <- Authentication.workspace_context(conn, :admin) do
      mutate(conn, context, session, actor, "rebuild.start", plan_id, fn idempotency ->
        Rebuilds.start(context, plan_id, plan_hash, idempotency: idempotency.command_idempotency)
      end)
    else
      {:error, reason} -> respond_error(conn, reason)
    end
  end

  get "/" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, opts} <- operation_page_options(conn.params),
         {:ok, _session, _actor, context} <- Authentication.workspace_context(conn, :viewer),
         {:ok, page} <- Rebuilds.page(context, opts) do
      Response.data(conn, 200, %{
        items: Enum.map(page.items, &RebuildDTO.operation(&1, false, :summary)),
        page: page_metadata(page)
      })
    else
      {:error, reason} -> respond_error(conn, reason)
    end
  end

  get "/:operation_id/items" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, opts} <- item_page_options(conn.params),
         {:ok, _session, _actor, context} <- Authentication.workspace_context(conn, :viewer),
         {:ok, page} <- Rebuilds.page_items(context, operation_id, opts) do
      Response.data(conn, 200, %{
        items: Enum.map(page.items, &RebuildDTO.item/1),
        page: page_metadata(page)
      })
    else
      {:error, reason} -> respond_error(conn, reason)
    end
  end

  post "/:operation_id/cancel" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, reason} <- required_string(conn.body_params, "reason", :rebuild_reason_required),
         {:ok, session, actor, context} <- Authentication.workspace_context(conn, :admin) do
      mutate(conn, context, session, actor, "rebuild.cancel", operation_id, fn idempotency ->
        Rebuilds.cancel(context, operation_id, reason,
          idempotency: idempotency.command_idempotency
        )
      end)
    else
      {:error, reason} -> respond_error(conn, reason)
    end
  end

  post "/:operation_id/retry" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, plan_hash} <- required_hash(conn.body_params, "plan_hash"),
         {:ok, session, actor, context} <- Authentication.workspace_context(conn, :admin) do
      mutate(conn, context, session, actor, "rebuild.retry", operation_id, fn idempotency ->
        Rebuilds.retry(context, operation_id, plan_hash,
          idempotency: idempotency.command_idempotency
        )
      end)
    else
      {:error, reason} -> respond_error(conn, reason)
    end
  end

  post "/:operation_id/reconcile" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, session, actor, context} <- Authentication.workspace_context(conn, :admin) do
      mutate(conn, context, session, actor, "rebuild.reconcile", operation_id, fn idempotency ->
        Rebuilds.reconcile(context, operation_id, idempotency: idempotency.command_idempotency)
      end)
    else
      {:error, reason} -> respond_error(conn, reason)
    end
  end

  get "/:operation_id" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor, context} <- Authentication.workspace_context(conn, :viewer),
         {:ok, operation} <- Rebuilds.get(context, operation_id) do
      Response.data(conn, 200, %{rebuild: RebuildDTO.operation(operation, admin?(context))})
    else
      {:error, reason} -> respond_error(conn, reason)
    end
  end

  match _ do
    Response.error(conn, 404, "not_found", "Route was not found")
  end

  defp mutate(conn, context, session, actor, action, operation_id, execute) do
    IdempotentCommand.run(
      conn,
      context,
      action,
      actor.id,
      session.id,
      conn.body_params,
      fn idempotency ->
        case execute.(idempotency) do
          {:ok, operation} ->
            audit(
              conn,
              context,
              session,
              actor,
              idempotency,
              action,
              operation_id,
              Map.merge(%{state: operation.state, phase: operation.phase}, audit_request(conn)),
              "accepted",
              operation.idempotency_replay? == true
            )

            {:ok, 202, %{rebuild: RebuildDTO.operation(operation, true)}, "rebuild", operation_id}

          {:error, reason} ->
            audit(
              conn,
              context,
              session,
              actor,
              idempotency,
              action,
              operation_id,
              Map.put(audit_request(conn), :error_code, error_code(reason)),
              "rejected",
              false
            )

            command_error(reason)
        end
      end
    )
  end

  defp audit(
         conn,
         context,
         session,
         actor,
         idempotency,
         action,
         operation_id,
         detail,
         outcome,
         replayed?
       ) do
    entry = %{
      action: action,
      actor_id: actor.id,
      session_id: session.id,
      resource_type: "rebuild",
      resource_id: operation_id,
      detail: detail,
      outcome: outcome,
      service_identity: Authentication.service_identity(conn)
    }

    Audit.put_best_effort(
      context,
      Map.merge(entry, IdempotentCommand.audit_metadata(idempotency, outcome, replayed?))
    )
  end

  defp audit_request(conn) do
    conn.body_params
    |> Map.take(["reason", "plan_hash", "approved"])
    |> Redaction.redact_operational_bounded()
  end

  defp rebuild_operation_id(%{run_id: "run_api_" <> digest}) do
    "rebuild_api_" <> digest
  end

  defp error_code(reason) do
    {_status, code, _message, _details} = error_response(reason)
    code
  end

  defp plan_request(params) do
    with {:ok, target_id} <- required_string(params, "target_id", :invalid_rebuild_target),
         {:ok, reason} <- required_string(params, "reason", :rebuild_reason_required) do
      {:ok, target_id, reason}
    end
  end

  defp start_request(%{"approved" => true} = params) do
    with {:ok, plan_id} <- required_string(params, "plan_id", :invalid_rebuild_plan),
         {:ok, plan_hash} <- required_hash(params, "plan_hash") do
      {:ok, plan_id, plan_hash}
    end
  end

  defp start_request(_params), do: {:error, :rebuild_approval_required}

  defp required_string(params, key, error) when is_map(params) do
    case Map.get(params, key) do
      value when is_binary(value) and byte_size(value) in 1..4096 ->
        if String.trim(value) == "", do: {:error, error}, else: {:ok, value}

      _invalid ->
        {:error, error}
    end
  end

  defp required_hash(params, key) do
    with {:ok, value} <- required_string(params, key, :invalid_rebuild_plan_hash),
         true <- Regex.match?(~r/\A[0-9a-f]{64}\z/, value) do
      {:ok, value}
    else
      _invalid -> {:error, :invalid_rebuild_plan_hash}
    end
  end

  defp operation_page_options(params) do
    with {:ok, limit} <- limit(Map.get(params, "limit", "100")),
         {:ok, cursor} <- decode_cursor(Map.get(params, "cursor"), :operation),
         {:ok, state} <- optional_atom(Map.get(params, "state"), @states) do
      {:ok, [limit: limit, after: cursor, state: state]}
    end
  end

  defp item_page_options(params) do
    with {:ok, limit} <- limit(Map.get(params, "limit", "100")),
         {:ok, cursor} <- decode_cursor(Map.get(params, "cursor"), :item),
         {:ok, status} <- optional_atom(Map.get(params, "status"), @item_statuses) do
      opts = [limit: limit, after: cursor, status: status]

      case Map.get(params, "target_id") do
        nil ->
          {:ok, opts}

        value when is_binary(value) and byte_size(value) in 1..255 ->
          {:ok, Keyword.put(opts, :target_id, value)}

        _invalid ->
          {:error, :invalid_rebuild_page}
      end
    end
  end

  defp limit(value) when is_integer(value) and value in 1..200, do: {:ok, value}

  defp limit(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed in 1..200 -> {:ok, parsed}
      _invalid -> {:error, :invalid_rebuild_page}
    end
  end

  defp limit(_value), do: {:error, :invalid_rebuild_page}
  defp optional_atom(nil, _allowed), do: {:ok, nil}

  defp optional_atom(value, allowed) when is_binary(value) do
    if value in allowed,
      do: {:ok, String.to_existing_atom(value)},
      else: {:error, :invalid_rebuild_page}
  end

  defp optional_atom(_value, _allowed), do: {:error, :invalid_rebuild_page}

  defp page_metadata(page) do
    %{
      limit: page.limit,
      has_more: page.has_more?,
      next_cursor: encode_cursor(page.next_cursor)
    }
  end

  defp encode_cursor(nil), do: nil

  defp encode_cursor(cursor) do
    cursor
    |> Jason.encode!()
    |> Base.url_encode64(padding: false)
  end

  defp decode_cursor(nil, _kind), do: {:ok, nil}

  defp decode_cursor(value, kind) when is_binary(value) and byte_size(value) <= 4096 do
    with {:ok, json} <- Base.url_decode64(value, padding: false),
         {:ok, decoded} <- Jason.decode(json),
         {:ok, cursor} <- normalize_cursor(decoded, kind) do
      {:ok, cursor}
    else
      _invalid -> {:error, :invalid_rebuild_cursor}
    end
  end

  defp decode_cursor(_value, _kind), do: {:error, :invalid_rebuild_cursor}

  defp normalize_cursor(%{"inserted_at" => value, "operation_id" => operation_id}, :operation)
       when is_binary(operation_id) do
    case DateTime.from_iso8601(value) do
      {:ok, inserted_at, 0} -> {:ok, %{inserted_at: inserted_at, operation_id: operation_id}}
      _invalid -> {:error, :invalid_rebuild_cursor}
    end
  end

  defp normalize_cursor(
         %{"ordinal" => ordinal, "target_id" => target_id, "item_id" => item_id},
         :item
       )
       when is_integer(ordinal) and is_binary(target_id) and is_binary(item_id),
       do: {:ok, %{ordinal: ordinal, target_id: target_id, item_id: item_id}}

  defp normalize_cursor(_decoded, _kind), do: {:error, :invalid_rebuild_cursor}

  defp command_error(reason) do
    {status, code, message, details} = error_response(reason)
    {:error, status, code, message, details}
  end

  defp respond_error(conn, reason) do
    {status, code, message, details} = error_response(reason)

    if status >= 500 do
      Logger.error("rebuild API failed: #{inspect(Redaction.redact_operational_bounded(reason))}")
    end

    Response.error(conn, status, code, message, details)
  end

  @doc false
  def error_response(:service_unauthorized),
    do: {401, "service_unauthorized", "Invalid service credentials", %{}}

  def error_response(:unauthenticated),
    do: {401, "unauthenticated", "Missing or invalid actor context", %{}}

  def error_response(:forbidden), do: {403, "forbidden", "Actor does not have access", %{}}
  def error_response(%Error{kind: :forbidden}), do: error_response(:forbidden)

  def error_response(%Error{kind: :not_found}),
    do: {404, "not_found", "Rebuild was not found", %{}}

  def error_response(:not_found), do: {404, "not_found", "Rebuild was not found", %{}}

  def error_response(%Error{kind: :conflict, details: details} = error) do
    code = Map.get(details, :reason_code) || Map.get(details, "reason_code") || "rebuild_conflict"
    {409, code, error.message, %{}}
  end

  def error_response(%Error{kind: :invalid}),
    do: {422, "validation_failed", "Invalid rebuild request", %{}}

  def error_response(reason)
      when reason in [
             :invalid_rebuild_target,
             :invalid_rebuild_plan,
             :invalid_rebuild_plan_hash,
             :invalid_rebuild_page,
             :invalid_rebuild_cursor,
             :invalid_rebuild_options,
             :invalid_rebuild_evaluated_at,
             :invalid_rebuild_item_page,
             :rebuild_reason_required,
             :rebuild_approval_required
           ],
      do: {422, Atom.to_string(reason), "Invalid rebuild request", %{}}

  def error_response(reason)
      when reason in [
             :rebuild_not_supported,
             :operator_decision_required,
             :coverage_window_limit_exceeded
           ],
      do: {409, Atom.to_string(reason), "Rebuild cannot start", %{}}

  def error_response(reason)
      when reason in [:rebuild_target_not_supported, :coverage_required_for_windowed_rebuild],
      do: {409, "rebuild_not_supported", "Rebuild cannot start", %{}}

  def error_response(_reason), do: {500, "internal_error", "Rebuild request failed", %{}}

  defp admin?(%{roles: roles}), do: RebuildDTO.admin?(roles)
end
