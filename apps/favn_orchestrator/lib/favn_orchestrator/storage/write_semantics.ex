defmodule FavnOrchestrator.Storage.WriteSemantics do
  @moduledoc false

  @type decision ::
          :insert | :replace | :idempotent | {:error, :conflicting_snapshot | :stale_write}

  @spec decide(non_neg_integer() | nil, String.t() | nil, non_neg_integer(), String.t()) ::
          decision()
  def decide(nil, _existing_hash, _incoming_event_seq, _incoming_hash), do: :insert

  def decide(existing_event_seq, existing_hash, incoming_event_seq, incoming_hash)
      when is_integer(existing_event_seq) and is_integer(incoming_event_seq) do
    cond do
      incoming_event_seq > existing_event_seq -> :replace
      incoming_event_seq < existing_event_seq -> {:error, :stale_write}
      existing_hash == incoming_hash -> :idempotent
      true -> {:error, :conflicting_snapshot}
    end
  end
end
