defmodule FavnOrchestrator.Rebuild.Validation do
  @moduledoc "One bounded, explicitly requested rebuild validation attempt. Interrupted attempts cannot resume."

  @enforce_keys [:operation_id, :attempt_id, :purpose, :owner_id, :request_id, :deadline_at]
  defstruct @enforce_keys ++ [fencing_token: 0, status: "active", failure: nil, receipt: nil]

  @type t :: %__MODULE__{
          operation_id: String.t(),
          attempt_id: String.t(),
          purpose: :plan | :start | :retry,
          owner_id: String.t(),
          request_id: String.t(),
          deadline_at: DateTime.t(),
          fencing_token: non_neg_integer(),
          status: String.t(),
          failure: map() | nil,
          receipt: FavnOrchestrator.Persistence.CommandIdempotency.t() | nil
        }

  @doc "Creates a fresh request identity with an absolute five-minute deadline."
  @spec new(String.t(), :plan | :start | :retry, String.t(), DateTime.t()) :: t()
  def new(operation_id, purpose, request_id, now) do
    id = :crypto.strong_rand_bytes(18) |> Base.url_encode64(padding: false)

    %__MODULE__{
      operation_id: operation_id,
      attempt_id: id,
      purpose: purpose,
      owner_id: "rebuild-validation:" <> id,
      request_id: request_id,
      deadline_at: DateTime.add(now, 300, :second)
    }
  end

  @doc false
  def encode(%__MODULE__{} = v) do
    v
    |> Map.from_struct()
    |> Map.new(fn
      {:receipt, value} -> {"receipt", encode_receipt(value)}
      {:purpose, value} -> {"purpose", Atom.to_string(value)}
      {:deadline_at, value} -> {"deadline_at", DateTime.to_iso8601(value)}
      {key, value} -> {Atom.to_string(key), value}
    end)
  end

  @doc false
  def decode(nil), do: nil

  def decode(map) do
    {:ok, deadline, 0} = DateTime.from_iso8601(map["deadline_at"])

    %__MODULE__{
      operation_id: map["operation_id"],
      attempt_id: map["attempt_id"],
      purpose:
        Map.fetch!(%{"plan" => :plan, "start" => :start, "retry" => :retry}, map["purpose"]),
      owner_id: map["owner_id"],
      request_id: map["request_id"],
      deadline_at: deadline,
      fencing_token: map["fencing_token"],
      status: map["status"],
      failure: map["failure"],
      receipt: decode_receipt(map["receipt"])
    }
  end

  @doc false
  def task_context(%__MODULE__{} = v),
    do: %{
      kind: :rebuild_validation,
      attempt_id: v.attempt_id,
      owner_id: v.owner_id,
      fencing_token: v.fencing_token
    }

  defp encode_receipt(nil), do: nil

  defp encode_receipt(receipt) do
    receipt
    |> Map.from_struct()
    |> Map.new(fn
      {key, value} when key in [:key_hash, :request_fingerprint] ->
        {Atom.to_string(key), Base.encode16(value)}

      {:expires_at, value} ->
        {"expires_at", DateTime.to_iso8601(value)}

      {:principal_kind, value} ->
        {"principal_kind", Atom.to_string(value)}

      {key, value} ->
        {Atom.to_string(key), value}
    end)
  end

  defp decode_receipt(nil), do: nil

  defp decode_receipt(map) do
    {:ok, expiry, 0} = DateTime.from_iso8601(map["expires_at"])

    %FavnOrchestrator.Persistence.CommandIdempotency{
      operation: map["operation"],
      principal_kind:
        Map.fetch!(%{"actor" => :actor, "service" => :service}, map["principal_kind"]),
      principal_id: map["principal_id"],
      key_hash: Base.decode16!(map["key_hash"]),
      request_fingerprint: Base.decode16!(map["request_fingerprint"]),
      expires_at: expiry
    }
  end

  @doc false
  def failure(operation_id, purpose) do
    FavnOrchestrator.Persistence.Error.new(
      :invalid,
      "Rebuild input checks were interrupted. Retry manually.",
      details: %{
        reason_code:
          if(purpose == :plan, do: "rebuild_planning_failed", else: "rebuild_validation_failed"),
        operation_id: operation_id
      }
    )
  end

  @doc false
  def encode_error(error),
    do: %{
      "kind" => Atom.to_string(error.kind),
      "message" => error.message,
      "retryable" => error.retryable?,
      "details" => Map.new(error.details, fn {key, value} -> {to_string(key), value} end)
    }

  @doc false
  def decode_error(error) do
    kinds = %{"invalid" => :invalid, "conflict" => :conflict, "fenced" => :fenced}

    details =
      Map.new(error["details"], fn
        {"reason_code", value} -> {:reason_code, value}
        {"operation_id", value} -> {:operation_id, value}
      end)

    FavnOrchestrator.Persistence.Error.new(Map.fetch!(kinds, error["kind"]), error["message"],
      retryable?: error["retryable"],
      details: details
    )
  end
end
