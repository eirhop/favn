defmodule Favn.Run do
  @moduledoc """
  Canonical in-memory representation of one Favn run.

  The runtime engine projects coordinator-owned runtime state into this public
  struct for callers and storage adapters.
  """

  alias Favn.Ref
  alias Favn.Run.AssetResult

  @type status :: :running | :ok | :error | :cancelled | :timed_out

  @type t :: %__MODULE__{
          id: String.t(),
          target_refs: [Ref.t()],
          plan: Favn.Plan.t(),
          status: status(),
          event_seq: non_neg_integer(),
          started_at: DateTime.t(),
          finished_at: DateTime.t() | nil,
          params: map(),
          retry_policy: map(),
          outputs: %{Ref.t() => term()},
          target_outputs: %{Ref.t() => term()},
          asset_results: %{Ref.t() => AssetResult.t()},
          error: term() | nil,
          terminal_reason: map() | nil
        }

  defstruct [
    :id,
    :target_refs,
    :plan,
    :started_at,
    status: :running,
    event_seq: 0,
    finished_at: nil,
    params: %{},
    retry_policy: %{},
    outputs: %{},
    target_outputs: %{},
    asset_results: %{},
    error: nil,
    terminal_reason: nil
  ]
end
