defmodule FavnOrchestrator.Rebuild.Plan do
  @moduledoc "Immutable, canonical rebuild plan returned for manual approval."

  alias Favn.Manifest.Serializer

  @enforce_keys [:plan_id, :plan_hash, :expires_at, :payload]
  defstruct @enforce_keys ++ [idempotency_replay?: false]

  @type t :: %__MODULE__{
          plan_id: String.t(),
          plan_hash: String.t(),
          expires_at: DateTime.t(),
          payload: map(),
          idempotency_replay?: boolean()
        }

  @doc "Builds a plan envelope and hashes its complete canonical payload."
  @spec new(String.t(), DateTime.t(), map()) :: t()
  def new(plan_id, %DateTime{} = expires_at, payload)
      when is_binary(plan_id) and is_map(payload) do
    payload = Map.merge(payload, %{plan_id: plan_id, expires_at: expires_at})

    %__MODULE__{
      plan_id: plan_id,
      plan_hash: hash(payload),
      expires_at: expires_at,
      payload: payload
    }
  end

  @doc "Returns the lowercase SHA-256 hash of canonical plan bytes."
  @spec hash(map()) :: String.t()
  def hash(payload) when is_map(payload) do
    payload
    |> Serializer.encode_canonical!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end
