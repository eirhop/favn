defmodule FavnOrchestrator.API.TargetRecoveriesRouter do
  @moduledoc false

  use Plug.Router

  require Logger

  alias FavnOrchestrator.API.Authentication
  alias FavnOrchestrator.API.IdempotentCommand
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.Operator.TargetRecovery, as: RecoveryDTO
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.TargetRecovery

  plug(:match)
  plug(:dispatch)

  post "/plan" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, session, actor, context} <-
           Authentication.workspace_or_service_context(conn, :operator),
         {:ok, target_id, reason} <- plan_request(conn.body_params) do
      IdempotentCommand.run(
        conn,
        context,
        "target_recovery.plan",
        Authentication.command_principal(session, actor),
        fn idempotency -> {"target_recovery", recovery_operation_id(idempotency)} end,
        conn.body_params,
        fn idempotency ->
          operation_id = recovery_operation_id(idempotency)

          case TargetRecovery.plan(context, target_id, reason,
                 operation_id: operation_id,
                 idempotency_key: idempotency.key_hash,
                 session_id: session.id
               ) do
            {:ok, plan} ->
              {:ok, 201, %{plan: RecoveryDTO.plan(plan, admin?(context))}, "target_recovery",
               operation_id}

            {:error, failure} ->
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
         {:ok, session, actor, context} <-
           Authentication.workspace_or_service_context(conn, :admin),
         {:ok, operation_id, plan_hash} <- start_request(conn.body_params) do
      mutate(
        conn,
        context,
        session,
        actor,
        "target_recovery.start",
        operation_id,
        fn _idempotency -> TargetRecovery.start(context, operation_id, plan_hash) end
      )
    else
      {:error, reason} -> respond_error(conn, reason)
    end
  end

  post "/:operation_id/reconcile" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, session, actor, context} <-
           Authentication.workspace_or_service_context(conn, :admin) do
      mutate(
        conn,
        context,
        session,
        actor,
        "target_recovery.reconcile",
        operation_id,
        fn _idempotency -> TargetRecovery.reconcile(context, operation_id) end
      )
    else
      {:error, reason} -> respond_error(conn, reason)
    end
  end

  get "/:operation_id" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor, context} <-
           Authentication.workspace_or_service_context(conn, :viewer),
         {:ok, operation} <- TargetRecovery.get(context, operation_id) do
      Response.data(conn, 200, %{
        target_recovery: RecoveryDTO.operation(operation, admin?(context))
      })
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
      Authentication.command_principal(session, actor),
      {"target_recovery", operation_id},
      conn.body_params,
      fn idempotency ->
        case execute.(idempotency) do
          {:ok, operation} ->
            {:ok, 202, %{target_recovery: RecoveryDTO.operation(operation, true)},
             "target_recovery", operation_id}

          {:error, failure} ->
            command_error(failure)
        end
      end
    )
  end

  defp plan_request(params) do
    with {:ok, target_id} <- required_string(params, "target_id", :invalid_recovery_target),
         {:ok, reason} <- required_string(params, "reason", :recovery_reason_required) do
      {:ok, target_id, reason}
    end
  end

  defp start_request(%{"approved" => true} = params) do
    with {:ok, operation_id} <-
           required_string(params, "plan_id", :invalid_target_recovery_plan),
         {:ok, plan_hash} <- required_hash(params, "plan_hash") do
      {:ok, operation_id, plan_hash}
    end
  end

  defp start_request(_params), do: {:error, :target_recovery_approval_required}

  defp required_hash(params, key) do
    with {:ok, value} <- required_string(params, key, :invalid_target_recovery_plan_hash),
         true <- Regex.match?(~r/\A[0-9a-f]{64}\z/, value) do
      {:ok, value}
    else
      _invalid -> {:error, :invalid_target_recovery_plan_hash}
    end
  end

  defp required_string(params, key, error) when is_map(params) do
    case Map.get(params, key) do
      value when is_binary(value) and byte_size(value) in 1..4096 ->
        if String.trim(value) == "", do: {:error, error}, else: {:ok, value}

      _invalid ->
        {:error, error}
    end
  end

  defp recovery_operation_id(%{run_id: "run_api_" <> digest}),
    do: "target_recovery_api_" <> digest

  defp command_error(reason) do
    {status, code, message, details} = error_response(reason)
    {:error, status, code, message, details}
  end

  defp respond_error(conn, reason) do
    {status, code, message, details} = error_response(reason)

    if status >= 500 do
      Logger.error(
        "target recovery API failed: #{inspect(Redaction.redact_operational_bounded(reason))}"
      )
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
    do: {404, "not_found", "Target recovery was not found", %{}}

  def error_response(%Error{kind: :conflict, details: details} = error) do
    code =
      Map.get(details, :reason_code) ||
        Map.get(details, "reason_code") ||
        "target_recovery_conflict"

    {409, code, error.message, %{}}
  end

  def error_response(%Error{kind: :invalid}),
    do: {422, "validation_failed", "Invalid target recovery request", %{}}

  def error_response(reason)
      when reason in [
             :invalid_recovery_target,
             :recovery_reason_required,
             :invalid_target_recovery_plan,
             :invalid_target_recovery_plan_hash,
             :target_recovery_approval_required,
             :invalid_target_recovery_options,
             :invalid_target_recovery_evaluated_at
           ],
      do: {422, Atom.to_string(reason), "Invalid target recovery request", %{}}

  def error_response(_reason),
    do: {500, "internal_error", "Target recovery request failed", %{}}

  defp admin?(%{roles: roles}), do: RecoveryDTO.admin?(roles)
end
