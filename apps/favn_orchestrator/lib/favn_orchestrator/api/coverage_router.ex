defmodule FavnOrchestrator.API.CoverageRouter do
  @moduledoc false

  use Plug.Router

  require Logger

  alias FavnOrchestrator.API.Audit
  alias FavnOrchestrator.API.Authentication
  alias FavnOrchestrator.API.CommandErrors
  alias FavnOrchestrator.API.DTO
  alias FavnOrchestrator.API.IdempotentCommand
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.Persistence.Error

  plug(:match)
  plug(:dispatch)

  get "/assets/:target_id" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, session, actor, context} <- Authentication.workspace_context(conn, :viewer),
         {:ok, operator_context} <-
           FavnOrchestrator.operator_context(context.workspace_id, actor, session),
         {:ok, summary} <- FavnOrchestrator.get_asset_coverage(operator_context, target_id) do
      Response.data(conn, 200, %{coverage: DTO.coverage_summary(summary)})
    else
      {:error, reason} -> respond_error(conn, reason)
    end
  end

  get "/assets/:target_id/missing" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, opts} <- page_options(conn.params),
         {:ok, session, actor, context} <- Authentication.workspace_context(conn, :viewer),
         {:ok, operator_context} <-
           FavnOrchestrator.operator_context(context.workspace_id, actor, session),
         {:ok, page} <-
           FavnOrchestrator.page_asset_missing_coverage(operator_context, target_id, opts) do
      Response.data(conn, 200, DTO.coverage_page(page))
    else
      {:error, reason} -> respond_error(conn, reason)
    end
  end

  post "/assets/:target_id/backfill/plan" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, session, actor, context} <- Authentication.workspace_context(conn, :operator),
         {:ok, operator_context} <-
           FavnOrchestrator.operator_context(context.workspace_id, actor, session),
         {:ok, opts} <- plan_options(conn.body_params),
         {:ok, plan} <-
           FavnOrchestrator.plan_missing_coverage_backfill(operator_context, target_id, opts) do
      Response.data(conn, 200, %{plan: DTO.coverage_plan(plan)})
    else
      {:error, reason} -> respond_error(conn, reason)
    end
  end

  post "/assets/:target_id/backfill" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, plan} <- request_plan(conn.body_params),
         {:ok, session, actor, context} <- Authentication.workspace_context(conn, :operator),
         {:ok, operator_context} <-
           FavnOrchestrator.operator_context(context.workspace_id, actor, session) do
      IdempotentCommand.run(
        conn,
        context,
        "coverage.backfill.submit",
        actor.id,
        session.id,
        conn.body_params,
        fn idempotency ->
          submit_backfill(
            conn,
            operator_context,
            target_id,
            plan,
            session,
            actor,
            context,
            idempotency
          )
        end
      )
    else
      {:error, reason} -> respond_error(conn, reason)
    end
  end

  match _ do
    Response.error(conn, 404, "not_found", "Route was not found")
  end

  defp submit_backfill(
         conn,
         operator_context,
         target_id,
         plan,
         session,
         actor,
         context,
         idempotency
       ) do
    opts = [
      root_run_id: idempotency.run_id,
      idempotency: idempotency.command_idempotency
    ]

    case FavnOrchestrator.submit_missing_coverage_backfill(
           operator_context,
           target_id,
           plan,
           opts
         ) do
      {:ok, run_id} ->
        audit_submit(conn, target_id, plan, run_id, session, actor, context, idempotency)
        {:ok, 202, %{run_id: run_id}, "run", run_id}

      {:error, reason} ->
        command_error(reason)
    end
  end

  defp request_plan(%{"plan" => plan}) when is_map(plan), do: {:ok, plan}
  defp request_plan(_params), do: {:error, :invalid_coverage_backfill_plan}

  defp plan_options(params) when is_map(params) do
    if Map.has_key?(params, "cursor") or Map.has_key?(params, "limit") do
      with {:ok, limit} <- page_limit(Map.get(params, "limit", 500)),
           {:ok, cursor} <- page_cursor(Map.get(params, "cursor")) do
        {:ok, [limit: limit, cursor: cursor]}
      end
    else
      {:ok, []}
    end
  end

  defp page_options(params) do
    with {:ok, limit} <- page_limit(Map.get(params, "limit", "100")),
         {:ok, cursor} <- page_cursor(Map.get(params, "cursor")) do
      {:ok, [limit: limit, cursor: cursor]}
    end
  end

  defp page_limit(value) when is_integer(value) and value in 1..500, do: {:ok, value}

  defp page_limit(value) when is_binary(value) do
    case Integer.parse(value) do
      {limit, ""} when limit in 1..500 -> {:ok, limit}
      _invalid -> {:error, :invalid_coverage_page_limit}
    end
  end

  defp page_limit(_value), do: {:error, :invalid_coverage_page_limit}
  defp page_cursor(nil), do: {:ok, nil}
  defp page_cursor(value) when is_binary(value) and byte_size(value) <= 4096, do: {:ok, value}
  defp page_cursor(_value), do: {:error, :invalid_coverage_cursor}

  defp audit_submit(conn, target_id, plan, run_id, session, actor, context, idempotency) do
    conn
    |> audit_entry(target_id, plan, run_id, session, actor, idempotency)
    |> then(&Audit.put_best_effort(context, &1))
  end

  @doc false
  @spec audit_entry(Plug.Conn.t(), String.t(), map(), String.t(), map(), map(), map()) :: map()
  def audit_entry(conn, target_id, plan, run_id, session, actor, idempotency) do
    %{
      action: "coverage.backfill.submit",
      actor_id: actor.id,
      session_id: session.id,
      resource_type: "run",
      resource_id: run_id,
      target_id: target_id,
      coverage_plan: %{
        plan_id: plan_field(plan, :plan_id),
        plan_hash: plan_field(plan, :plan_hash),
        window_count: plan_field(plan, :window_count)
      },
      outcome: "accepted",
      service_identity: Authentication.service_identity(conn)
    }
    |> Map.merge(IdempotentCommand.audit_metadata(idempotency, "accepted"))
  end

  defp command_error(reason) do
    {status, code, message, details} = error_response(reason)
    {:error, status, code, message, details}
  end

  defp plan_field(plan, field),
    do: Map.get(plan, field, Map.get(plan, Atom.to_string(field)))

  defp respond_error(conn, reason) do
    {status, code, message, details} = error_response(reason)

    if status >= 500 do
      Logger.error("coverage request failed: #{inspect(reason)}")
    end

    Response.error(conn, status, code, message, details)
  end

  @doc false
  @spec error_response(term()) :: {pos_integer(), String.t(), String.t(), map()}
  def error_response(:service_unauthorized),
    do: {401, "service_unauthorized", "Invalid service credentials", %{}}

  def error_response(:unauthenticated),
    do: {401, "unauthenticated", "Missing or invalid actor context", %{}}

  def error_response(:forbidden),
    do: {403, "forbidden", "Actor does not have access", %{}}

  def error_response(%Error{kind: :forbidden}),
    do: {403, "forbidden", "Actor does not have access", %{}}

  def error_response(%Error{kind: :not_found}),
    do: {404, "not_found", "Asset was not found", %{}}

  def error_response(reason) when reason in [:not_found, :invalid_asset_target],
    do: {404, "not_found", "Asset was not found", %{}}

  def error_response(:coverage_selection_stale),
    do: {409, "coverage_selection_stale", "Coverage changed; refresh the plan", %{}}

  def error_response(:coverage_cursor_stale),
    do: {409, "coverage_cursor_stale", "Coverage changed; restart pagination", %{}}

  def error_response(:coverage_complete),
    do: {409, "coverage_complete", "Asset coverage is already complete", %{}}

  def error_response(:coverage_page_complete),
    do: {409, "coverage_page_complete", "The selected page has no missing windows", %{}}

  def error_response({:coverage_unknown, reason}),
    do: {409, "coverage_unknown", "Asset coverage cannot be evaluated", %{reason: reason}}

  def error_response({:too_many_backfill_windows, count, limit}),
    do:
      {409, "coverage_backfill_limit_exceeded", "Select at most #{limit} missing windows",
       %{count: count, limit: limit}}

  def error_response(:coverage_window_limit_exceeded),
    do: {409, "coverage_window_limit_exceeded", "Coverage exceeds the evaluation limit", %{}}

  def error_response(reason) when is_tuple(reason) do
    case CommandErrors.admission(reason) do
      {:error, status, code, message, details} -> {status, code, message, details}
      nil -> {500, "internal_error", "Coverage request failed", %{}}
    end
  end

  def error_response(reason)
      when reason in [
             :invalid_coverage_backfill_plan,
             :invalid_coverage_cursor,
             :invalid_coverage_page_limit,
             :invalid_operator_context
           ],
      do: {422, "validation_failed", "Invalid coverage request", %{}}

  def error_response(_reason), do: {500, "internal_error", "Coverage request failed", %{}}
end
