defmodule FavnOrchestrator.Persistence.CommandIdempotency do
  @moduledoc """
  Hashed, workspace-scoped identity for an externally retried command.

  Raw idempotency keys and request bodies never cross the persistence boundary.
  Callers provide a digest of the key and a non-secret canonical request
  fingerprint. Commands that can contain credentials must fingerprint an
  operation-specific redacted form or use a keyed HMAC.
  """

  @enforce_keys [
    :operation,
    :principal_kind,
    :principal_id,
    :key_hash,
    :request_fingerprint,
    :expires_at
  ]
  defstruct [
    :operation,
    :principal_kind,
    :principal_id,
    :key_hash,
    :request_fingerprint,
    :expires_at
  ]

  @type t :: %__MODULE__{
          operation: String.t(),
          principal_kind: :actor | :service,
          principal_id: String.t(),
          key_hash: binary(),
          request_fingerprint: binary(),
          expires_at: DateTime.t()
        }

  @doc "Builds a validated command identity from already-hashed inputs."
  @spec new(String.t(), :actor | :service, String.t(), binary(), binary(), DateTime.t()) ::
          {:ok, t()} | {:error, :invalid_idempotency_context}
  def new(operation, principal_kind, principal_id, key_hash, request_fingerprint, expires_at)
      when is_binary(operation) and principal_kind in [:actor, :service] and
             is_binary(principal_id) and is_binary(key_hash) and
             is_binary(request_fingerprint) and is_struct(expires_at, DateTime) do
    if operation != "" and byte_size(operation) <= 128 and principal_id != "" and
         byte_size(principal_id) <= 255 and byte_size(key_hash) in 16..64 and
         byte_size(request_fingerprint) in 16..64 do
      {:ok,
       %__MODULE__{
         operation: operation,
         principal_kind: principal_kind,
         principal_id: principal_id,
         key_hash: key_hash,
         request_fingerprint: request_fingerprint,
         expires_at: expires_at
       }}
    else
      {:error, :invalid_idempotency_context}
    end
  end

  def new(_operation, _principal_kind, _principal_id, _key_hash, _fingerprint, _expires_at),
    do: {:error, :invalid_idempotency_context}
end
