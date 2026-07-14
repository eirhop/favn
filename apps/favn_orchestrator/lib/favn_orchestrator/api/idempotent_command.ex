defmodule FavnOrchestrator.API.IdempotentCommand do
  @moduledoc """
  Reserves, executes, persists, and replays private API commands.

  Once execution starts, an exception or malformed callback result is treated as
  an unknown outcome. The terminal response is persisted when possible so a
  retry cannot blindly repeat a command that may already have mutated state.
  """

  import Plug.Conn, only: [get_req_header: 2, get_resp_header: 2]

  require Logger

  alias FavnOrchestrator.API.Authentication
  alias FavnOrchestrator.API.DTO
  alias FavnOrchestrator.API.ErrorResponse
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.Idempotency

  @type idempotency :: %{operation: String.t(), key_hash: String.t()}
  @type command_result ::
          {:ok, Plug.Conn.status(), term(), String.t(), String.t()}
          | {:error, Plug.Conn.status(), String.t(), String.t(), map()}

  @doc "Runs or replays an idempotent command scoped to an actor session."
  @spec run(
          Plug.Conn.t(),
          String.t(),
          String.t(),
          String.t(),
          term(),
          (idempotency() -> command_result())
        ) :: Plug.Conn.t()
  def run(conn, operation, actor_id, session_id, request_input, execute)
      when is_binary(operation) and is_binary(actor_id) and is_binary(session_id) and
             is_function(execute, 1) do
    case key_hash(conn) do
      {:ok, key_hash} ->
        reserve_and_run(conn, operation, actor_id, session_id, request_input, key_hash, execute)

      {:error, :missing_idempotency_key} ->
        validation_error(conn, "Missing required Idempotency-Key header")

      {:error, :invalid_idempotency_key} ->
        validation_error(conn, "Invalid Idempotency-Key header")
    end
  end

  @doc "Returns the redacted idempotency metadata stored with audit entries."
  @spec audit_metadata(idempotency(), String.t()) :: map()
  def audit_metadata(%{operation: operation, key_hash: key_hash}, outcome) do
    %{
      operation: operation,
      idempotency: %{outcome: outcome, key_hash: key_hash}
    }
  end

  defp reserve_and_run(conn, operation, actor_id, session_id, request, key_hash, execute) do
    fingerprint = Idempotency.request_fingerprint(%{operation: operation, request: request})

    scope = %{
      operation: operation,
      actor_id: actor_id,
      session_id: session_id,
      service_identity: Authentication.service_identity(conn),
      idempotency_key_hash: key_hash
    }

    case scope |> Idempotency.new_record(fingerprint) |> Idempotency.reserve() do
      {:ok, {:reserved, record}} ->
        execute(conn, record, %{operation: operation, key_hash: key_hash}, execute)

      {:ok, {:replay, record}} ->
        replay(conn, record)

      {:error, :idempotency_conflict} ->
        Response.error(
          conn,
          409,
          "idempotency_conflict",
          "Idempotency key was reused with different input"
        )

      {:error, :operation_in_progress} ->
        Response.error(
          conn,
          409,
          "operation_in_progress",
          "Original operation is still in progress"
        )

      {:error, reason} ->
        Logger.error("idempotency.reserve failed: #{inspect(reason)}")
        Response.error(conn, 500, "internal_error", "Idempotency reservation failed")
    end
  end

  defp execute(conn, record, idempotency, execute) do
    result =
      try do
        execute.(idempotency)
      rescue
        exception -> {:unknown_outcome, Exception.format(:error, exception, __STACKTRACE__)}
      catch
        kind, reason -> {:unknown_outcome, Exception.format(kind, reason, __STACKTRACE__)}
      end

    persist_result(conn, record, result)
  end

  defp persist_result(conn, record, {:ok, status, payload, resource_type, resource_id}) do
    response_body = DTO.normalize(payload)

    attrs = %{
      status: :completed,
      response_status: status,
      response_body: response_body,
      resource_type: resource_type,
      resource_id: resource_id
    }

    complete(conn, record, attrs, fn -> Response.data(conn, status, response_body) end)
  end

  defp persist_result(conn, record, {:error, status, code, message, details}) do
    attrs = %{
      status: :failed,
      response_status: status,
      response_body: DTO.normalize(%{code: code, message: message, details: details}),
      resource_type: nil,
      resource_id: nil
    }

    complete(conn, record, attrs, fn -> Response.error(conn, status, code, message, details) end)
  end

  defp persist_result(conn, record, {:unknown_outcome, formatted_exception}) do
    Logger.error(
      "idempotent command failed with unknown outcome operation=#{inspect(record.operation)} " <>
        "record_id=#{record.id} request_id=#{inspect(request_id(conn))}\n#{formatted_exception}"
    )

    persist_unknown_outcome(conn, record)
  end

  defp persist_result(conn, record, unexpected) do
    Logger.error(
      "idempotent command returned invalid result operation=#{inspect(record.operation)} " <>
        "record_id=#{record.id} request_id=#{inspect(request_id(conn))} " <>
        "result=#{inspect(unexpected)}"
    )

    persist_unknown_outcome(conn, record)
  end

  defp persist_unknown_outcome(conn, record) do
    {status, code, message, details} = ErrorResponse.response(:idempotency_completion_failed)

    attrs = %{
      status: :failed,
      response_status: status,
      response_body: DTO.normalize(%{code: code, message: message, details: details}),
      resource_type: nil,
      resource_id: nil
    }

    complete(conn, record, attrs, fn -> Response.error(conn, status, code, message, details) end)
  end

  defp complete(conn, record, attrs, respond) do
    case Idempotency.complete(record.id, attrs) do
      :ok ->
        respond.()

      {:error, reason} ->
        log_completion_failure(conn, record, attrs, reason)
        error_response(conn, ErrorResponse.response(:idempotency_completion_failed))
    end
  end

  defp replay(conn, %{status: :completed} = record) do
    Response.data(conn, record.response_status, record.response_body || %{})
  end

  defp replay(conn, %{status: :failed} = record) do
    body = record.response_body || %{}

    Response.error(
      conn,
      record.response_status,
      body_field(body, "code") || "bad_request",
      body_field(body, "message") || "Request failed",
      body_field(body, "details") || %{}
    )
  end

  defp replay(conn, record) do
    Logger.error("idempotency.reserve returned invalid replay record: #{inspect(record)}")
    Response.error(conn, 500, "internal_error", "Idempotency replay failed")
  end

  defp key_hash(conn) do
    case header(conn, "idempotency-key") do
      nil ->
        {:error, :missing_idempotency_key}

      value ->
        value = String.trim(value)

        if value != "" and byte_size(value) <= 512,
          do: {:ok, Idempotency.key_hash(value)},
          else: {:error, :invalid_idempotency_key}
    end
  end

  defp validation_error(conn, message) do
    Response.error(conn, 422, "validation_failed", message, %{header: "Idempotency-Key"})
  end

  defp log_completion_failure(conn, record, attrs, reason) do
    Logger.error(
      "idempotency.complete failed operation=#{inspect(record.operation)} " <>
        "record_id=#{record.id} resource_type=#{inspect(attrs.resource_type)} " <>
        "resource_id=#{inspect(attrs.resource_id)} request_id=#{inspect(request_id(conn))} " <>
        "reason=#{inspect(reason)}"
    )
  end

  defp error_response(conn, {status, code, message, details}) do
    Response.error(conn, status, code, message, details)
  end

  defp body_field(body, "code") when is_map(body),
    do: Map.get(body, "code") || Map.get(body, :code)

  defp body_field(body, "message") when is_map(body),
    do: Map.get(body, "message") || Map.get(body, :message)

  defp body_field(body, "details") when is_map(body),
    do: Map.get(body, "details") || Map.get(body, :details)

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
