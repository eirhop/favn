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
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.WorkspaceContext

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
          String.t(),
          String.t(),
          term(),
          (idempotency() -> command_result())
        ) :: Plug.Conn.t()
  def run(
        conn,
        %WorkspaceContext{} = context,
        operation,
        actor_id,
        session_id,
        request_input,
        execute
      )
      when is_binary(operation) and is_binary(actor_id) and is_binary(session_id) and
             is_function(execute, 1) do
    with {:ok, key_hash} <- key_hash(conn),
         fingerprint <-
           Idempotency.request_fingerprint(%{operation: operation, request: request_input}),
         {:ok, command_idempotency} <-
           CommandIdempotency.new(
             operation,
             :actor,
             actor_id,
             key_hash,
             fingerprint,
             DateTime.add(DateTime.utc_now(), 7 * 24 * 60 * 60, :second)
           ) do
      idempotency = %{
        operation: operation,
        key_hash: key_hash,
        command_idempotency: command_idempotency,
        run_id: deterministic_run_id(context.workspace_id, operation, actor_id, key_hash)
      }

      execute_atomic(conn, idempotency, execute)
    else
      {:error, :missing_idempotency_key} ->
        validation_error(conn, "Missing required Idempotency-Key header")

      {:error, :invalid_idempotency_key} ->
        validation_error(conn, "Invalid Idempotency-Key header")

      {:error, :invalid_idempotency_context} ->
        Response.error(conn, 500, "internal_error", "Idempotency context is invalid")
    end
  end

  @doc "Returns the redacted idempotency metadata stored with audit entries."
  @spec audit_metadata(idempotency(), String.t()) :: map()
  def audit_metadata(idempotency, outcome), do: audit_metadata(idempotency, outcome, false)

  @spec audit_metadata(idempotency(), String.t(), boolean()) :: map()
  def audit_metadata(%{operation: operation, key_hash: key_hash}, outcome, replayed?)
      when is_boolean(replayed?) do
    %{
      operation: operation,
      idempotency: %{outcome: outcome, key_hash: key_hash, replayed: replayed?}
    }
  end

  defp execute_atomic(conn, idempotency, execute) do
    result =
      try do
        execute.(idempotency)
      rescue
        exception -> {:unknown_outcome, Exception.format(:error, exception, __STACKTRACE__)}
      catch
        kind, reason -> {:unknown_outcome, Exception.format(kind, reason, __STACKTRACE__)}
      end

    case result do
      {:ok, status, payload, _resource_type, _resource_id} ->
        Response.data(conn, status, DTO.normalize(payload))

      {:error, status, code, message, details} ->
        Response.error(conn, status, code, message, details)

      {:unknown_outcome, formatted} ->
        Logger.error(
          "atomic idempotent command failed operation=#{inspect(idempotency.operation)} " <>
            "request_id=#{inspect(request_id(conn))}\n#{formatted}"
        )

        error_response(conn, ErrorResponse.response(:idempotency_completion_failed))

      unexpected ->
        Logger.error(
          "atomic idempotent command returned invalid result " <>
            "operation=#{inspect(idempotency.operation)} result=#{inspect(unexpected)}"
        )

        error_response(conn, ErrorResponse.response(:idempotency_completion_failed))
    end
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
