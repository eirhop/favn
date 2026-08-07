defmodule FavnOrchestrator.API.IdempotentCommand do
  @moduledoc """
  Reserves, executes, persists, and replays private API commands.

  Once execution starts, an exception or malformed callback result is treated as
  an unknown outcome. The terminal response is persisted when possible so a
  retry cannot blindly repeat a command that may already have mutated state.
  """

  import Plug.Conn, only: [get_req_header: 2, get_resp_header: 2]

  require Logger

  alias FavnOrchestrator.API.DTO
  alias FavnOrchestrator.API.ErrorResponse
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.Idempotency
  alias FavnOrchestrator.Operator.Audit
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Redaction

  @type idempotency :: %{
          required(:operation) => String.t(),
          required(:key_hash) => String.t(),
          optional(:command_idempotency) => CommandIdempotency.t(),
          optional(:run_id) => String.t()
        }
  @type command_result ::
          {:ok, Plug.Conn.status(), term(), String.t(), String.t()}
          | {:error, Plug.Conn.status(), String.t(), String.t(), map()}

  @doc "Runs a command whose owning PostgreSQL store commits idempotency atomically."
  @spec run(
          Plug.Conn.t(),
          WorkspaceContext.t(),
          String.t(),
          Audit.principal(),
          {String.t(), String.t()} | (idempotency() -> {String.t(), String.t()}),
          term(),
          (idempotency() -> command_result())
        ) :: Plug.Conn.t()
  def run(
        conn,
        %WorkspaceContext{} = context,
        operation,
        %{kind: principal_kind, id: principal_id} = principal,
        resource,
        request_input,
        execute
      )
      when principal_kind in [:actor, :service] and is_binary(principal_id) and
             is_binary(operation) and is_function(execute, 1) do
    with {:ok, raw_key} <- idempotency_key(conn),
         key_hash <- Idempotency.key_hash(raw_key),
         run_id <- deterministic_run_id(context.workspace_id, operation, principal_id, key_hash),
         proposed <- %{operation: operation, key_hash: key_hash, run_id: run_id},
         {:ok, {resource_type, resource_id}} <- resolve_resource(resource, proposed),
         {:ok, intent} <-
           safe_audit(fn ->
             Audit.begin_command(
               context,
               principal,
               operation,
               resource_type,
               resource_id,
               request_input,
               raw_key
             )
           end) do
      idempotency = %{
        operation: operation,
        key_hash: intent.key_hash,
        command_idempotency: intent.idempotency,
        run_id: run_id
      }

      execute_audited(conn, context, intent, resource_type, resource_id, idempotency, execute)
    else
      {:error, :missing_idempotency_key} ->
        validation_error(conn, "Missing required Idempotency-Key header")

      {:error, :invalid_idempotency_key} ->
        validation_error(conn, "Invalid Idempotency-Key header")

      {:error, :invalid_idempotency_context} ->
        Response.error(conn, 500, "internal_error", "Idempotency context is invalid")

      {:error, :invalid_command_resource} ->
        Response.error(conn, 500, "internal_error", "Command resource is invalid")

      {:error, %Error{kind: :conflict}} ->
        Response.error(
          conn,
          409,
          "idempotency_conflict",
          "The idempotency key is already associated with another command"
        )

      {:error, %Error{kind: kind}} when kind in [:timeout, :unavailable] ->
        Response.error(conn, 503, "audit_unavailable", "Command audit is unavailable")

      {:error, reason} ->
        log_audit_failure(conn, operation, "reservation", reason)
        Response.error(conn, 500, "internal_error", "Command audit could not be reserved")
    end
  end

  defp execute_audited(
         conn,
         context,
         intent,
         reserved_resource_type,
         reserved_resource_id,
         idempotency,
         execute
       ) do
    result =
      try do
        execute.(idempotency)
      rescue
        exception ->
          {:unknown_outcome, %{kind: :exception, type: exception.__struct__ |> Atom.to_string()}}
      catch
        kind, reason ->
          {:unknown_outcome, %{kind: kind, reason: Redaction.redact_operational_bounded(reason)}}
      end

    {outcome, resource_type, resource_id, detail} =
      audit_result(result, reserved_resource_type, reserved_resource_id)

    case safe_audit(fn ->
           Audit.finish_command(
             context,
             intent,
             outcome,
             resource_type,
             resource_id,
             detail
           )
         end) do
      :ok ->
        render_result(conn, idempotency, result)

      {:error, reason} ->
        log_audit_failure(conn, idempotency.operation, "completion", reason)
        error_response(conn, ErrorResponse.response(:idempotency_completion_failed))
    end
  end

  defp render_result(conn, _idempotency, {:ok, status, payload, _resource_type, _resource_id}),
    do: Response.data(conn, status, DTO.normalize(payload))

  defp render_result(conn, _idempotency, {:error, status, code, message, _details})
       when status >= 500,
       do:
         Response.error(conn, status, code, message, %{
           outcome: "unknown",
           retry_with_same_idempotency_key: true
         })

  defp render_result(conn, _idempotency, {:error, status, code, message, details}),
    do: Response.error(conn, status, code, message, details)

  defp render_result(conn, idempotency, {:unknown_outcome, failure}) do
    Logger.error(
      "atomic idempotent command failed operation=#{inspect(idempotency.operation)} " <>
        "request_id=#{inspect(request_id(conn))} " <>
        "failure=#{inspect(Redaction.redact_operational_bounded(failure))}"
    )

    error_response(conn, ErrorResponse.response(:idempotency_completion_failed))
  end

  defp render_result(conn, idempotency, unexpected) do
    Logger.error(
      "atomic idempotent command returned invalid result " <>
        "operation=#{inspect(idempotency.operation)} " <>
        "result=#{inspect(Redaction.redact_operational_bounded(unexpected))}"
    )

    error_response(conn, ErrorResponse.response(:idempotency_completion_failed))
  end

  defp audit_result({:ok, status, _payload, resource_type, resource_id}, _type, _id)
       when is_binary(resource_type) and is_binary(resource_id),
       do: {"accepted", resource_type, resource_id, %{status: status}}

  defp audit_result({:error, status, code, _message, _details}, resource_type, resource_id) do
    outcome = if status >= 500, do: "unknown", else: "rejected"
    {outcome, resource_type, resource_id, %{status: status, code: code}}
  end

  defp audit_result({:unknown_outcome, _formatted}, resource_type, resource_id),
    do: {"unknown", resource_type, resource_id, %{reason: "command_execution_failed"}}

  defp audit_result(_unexpected, resource_type, resource_id),
    do: {"unknown", resource_type, resource_id, %{reason: "invalid_command_result"}}

  defp idempotency_key(conn) do
    case header(conn, "idempotency-key") do
      nil ->
        {:error, :missing_idempotency_key}

      value ->
        value = String.trim(value)

        if byte_size(value) in 1..512,
          do: {:ok, value},
          else: {:error, :invalid_idempotency_key}
    end
  end

  defp resolve_resource({type, id}, _idempotency)
       when is_binary(type) and type != "" and is_binary(id) and id != "",
       do: {:ok, {type, id}}

  defp resolve_resource(resource, idempotency) when is_function(resource, 1) do
    resolve_resource(resource.(idempotency), idempotency)
  rescue
    _error -> {:error, :invalid_command_resource}
  catch
    _kind, _reason -> {:error, :invalid_command_resource}
  end

  defp resolve_resource(_resource, _idempotency), do: {:error, :invalid_command_resource}

  defp safe_audit(fun) do
    fun.()
  rescue
    exception ->
      {:error, {:audit_call_failed, :exception, exception.__struct__ |> Atom.to_string()}}
  catch
    kind, reason ->
      {:error, {:audit_call_failed, kind, Redaction.redact_operational_bounded(reason)}}
  end

  defp log_audit_failure(conn, operation, stage, reason) do
    Logger.error(
      "durable command audit failed operation=#{inspect(operation)} stage=#{stage} " <>
        "request_id=#{inspect(request_id(conn))} reason=#{inspect(redacted_reason(reason))}"
    )
  end

  defp redacted_reason(%Error{} = error),
    do:
      Redaction.redact_operational_bounded(%{
        kind: error.kind,
        retryable?: error.retryable?,
        details: error.details
      })

  defp redacted_reason(reason), do: Redaction.redact_operational_bounded(reason)

  defp deterministic_run_id(workspace_id, operation, actor_id, key_hash) do
    digest =
      :crypto.hash(
        :sha256,
        workspace_id <> <<0>> <> operation <> <<0>> <> actor_id <> <<0>> <> key_hash
      )
      |> Base.encode16(case: :lower)

    "run_api_" <> String.slice(digest, 0, 32)
  end

  defp validation_error(conn, message) do
    Response.error(conn, 422, "validation_failed", message, %{header: "Idempotency-Key"})
  end

  defp error_response(conn, {status, code, message, details}) do
    Response.error(conn, status, code, message, details)
  end

  defp request_id(conn) do
    case get_resp_header(conn, "x-request-id") do
      [value | _] -> value
      _other -> conn.assigns[:request_id]
    end
  end

  defp header(conn, key) do
    case get_req_header(conn, key) do
      [value | _] -> value
      _other -> nil
    end
  end
end
