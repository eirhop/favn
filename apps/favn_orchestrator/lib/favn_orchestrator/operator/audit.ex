defmodule FavnOrchestrator.Operator.Audit do
  @moduledoc """
  Durable same-BEAM operator command intent.

  The intent is committed before the owning domain mutation starts. The
  domain's own PostgreSQL command receipt remains the accepted-result
  authority, while this record guarantees that no supported browser mutation
  can run without durable actor, session, workspace, request-fingerprint, and
  idempotency evidence.
  """

  alias FavnOrchestrator.Idempotency
  alias FavnOrchestrator.Identity
  alias FavnOrchestrator.OperatorContext
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.Commands.CompleteOperatorCommand
  alias FavnOrchestrator.Persistence.Commands.ReserveOperatorCommand
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Redaction

  @retention_seconds 7 * 24 * 60 * 60
  @fingerprint_key_context "favn.operator-command-request-fingerprint.v1"

  @enforce_keys [:operation, :key_hash, :request_fingerprint, :idempotency]
  defstruct [:operation, :key_hash, :request_fingerprint, :idempotency]

  @type intent :: %__MODULE__{
          operation: String.t(),
          key_hash: String.t(),
          request_fingerprint: String.t(),
          idempotency: CommandIdempotency.t()
        }

  @doc "Persists an exact, replay-safe operator intent before mutation."
  @spec begin_command(
          WorkspaceContext.t(),
          OperatorContext.t(),
          map(),
          String.t(),
          String.t(),
          String.t(),
          term(),
          String.t()
        ) :: {:ok, intent()} | {:error, term()}
  def begin_command(
        %WorkspaceContext{} = context,
        %OperatorContext{} = operator_context,
        %{id: actor_id},
        operation,
        resource_type,
        resource_id,
        request,
        raw_key
      )
      when is_binary(actor_id) and is_binary(operation) and is_binary(resource_type) and
             is_binary(resource_id) and is_binary(raw_key) do
    safe_request = request |> Redaction.redact() |> json_safe()

    with :ok <- validate_key(raw_key),
         {:ok, fingerprint_key} <- fingerprint_key(),
         proposed_key_hash <- Idempotency.key_hash(raw_key),
         request_fingerprint <-
           Idempotency.request_hmac(%{operation: operation, request: request}, fingerprint_key),
         now <- DateTime.utc_now(),
         expires_at <- DateTime.add(now, @retention_seconds, :second),
         {:ok, reservation} <-
           Identity.reserve_operator_command(%ReserveOperatorCommand{
             workspace_context: context,
             actor_id: actor_id,
             session_id: operator_context.session_id,
             operation: operation,
             resource_type: resource_type,
             resource_id: resource_id,
             key_hash: proposed_key_hash,
             request_fingerprint: request_fingerprint,
             detail: %{
               actor_id: actor_id,
               session_id: operator_context.session_id,
               service_identity: "same_beam_operator_ui",
               outcome: "requested",
               request: safe_request,
               request_fingerprint: request_fingerprint
             },
             expires_at: expires_at,
             occurred_at: now
           }),
         {:ok, idempotency} <-
           CommandIdempotency.new(
             operation,
             :actor,
             actor_id,
             reservation.key_hash,
             request_fingerprint,
             reservation.expires_at
           ) do
      {:ok,
       %__MODULE__{
         operation: operation,
         key_hash: reservation.key_hash,
         request_fingerprint: request_fingerprint,
         idempotency: idempotency
       }}
    end
  end

  @doc "Durably records the authoritative terminal result of an operator command."
  @spec finish_command(
          WorkspaceContext.t(),
          OperatorContext.t(),
          map(),
          intent(),
          String.t(),
          String.t(),
          String.t(),
          map()
        ) :: :ok | {:error, term()}
  def finish_command(
        %WorkspaceContext{} = context,
        %OperatorContext{} = operator_context,
        %{id: actor_id},
        %__MODULE__{} = intent,
        outcome,
        resource_type,
        resource_id,
        detail
      )
      when outcome in ["accepted", "partial", "rejected", "unknown"] and
             is_binary(resource_type) and is_binary(resource_id) and is_map(detail) do
    Identity.complete_operator_command(%CompleteOperatorCommand{
      workspace_context: context,
      actor_id: actor_id,
      session_id: operator_context.session_id,
      operation: intent.operation,
      key_hash: intent.key_hash,
      request_fingerprint: intent.request_fingerprint,
      outcome: outcome,
      resource_type: resource_type,
      resource_id: resource_id,
      detail: %{
        actor_id: actor_id,
        session_id: operator_context.session_id,
        service_identity: "same_beam_operator_ui",
        outcome: outcome,
        result: detail |> Redaction.redact() |> json_safe(),
        request_fingerprint: intent.request_fingerprint
      },
      occurred_at: DateTime.utc_now()
    })
  end

  @doc """
  Derives the operator command HMAC key from a deployment's secret key base.

  Every boot path that configures `:operator_command_hmac_key` must derive it
  through this function, so request fingerprints stay comparable across
  restarts of the same deployment.
  """
  @spec derive_command_hmac_key(String.t()) :: binary()
  def derive_command_hmac_key(secret_key_base)
      when is_binary(secret_key_base) and byte_size(secret_key_base) >= 32 do
    :crypto.mac(:hmac, :sha256, secret_key_base, @fingerprint_key_context)
  end

  @doc "Builds a stable, bounded resource ID from a persisted operator intent."
  @spec deterministic_id(intent(), String.t(), [term()]) :: String.t()
  def deterministic_id(%__MODULE__{} = intent, prefix, parts \\ [])
      when is_binary(prefix) and is_list(parts) do
    digest =
      Idempotency.request_fingerprint([
        intent.operation,
        intent.key_hash,
        parts
      ])

    prefix <> "_" <> String.slice(digest, 0, 32)
  end

  defp validate_key(key) do
    if byte_size(key) in 16..255,
      do: :ok,
      else: {:error, :invalid_idempotency_key}
  end

  defp fingerprint_key do
    case Application.get_env(:favn_orchestrator, :operator_command_hmac_key) do
      key when is_binary(key) and byte_size(key) >= 32 -> {:ok, key}
      _missing -> {:error, :operator_command_hmac_key_unavailable}
    end
  end

  defp json_safe(%DateTime{} = value), do: DateTime.to_iso8601(value)

  defp json_safe(value) when is_map(value) do
    Map.new(value, fn {key, child} -> {json_key(key), json_safe(child)} end)
  end

  defp json_safe(value) when is_list(value), do: Enum.map(value, &json_safe/1)
  defp json_safe(value) when is_tuple(value), do: value |> Tuple.to_list() |> json_safe()

  defp json_safe(value)
       when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value),
       do: value

  defp json_safe(value) when is_atom(value), do: Atom.to_string(value)
  defp json_safe(value), do: inspect(value)

  defp json_key(key) when is_binary(key), do: key
  defp json_key(key) when is_atom(key), do: Atom.to_string(key)
  defp json_key(key), do: inspect(key)
end
