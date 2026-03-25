defmodule Flux.Run do
  @moduledoc """
  Canonical in-memory representation of one Flux run.

  The first runner stores run state in memory and returns this struct directly
  to callers. Persistence and subscriptions are intentionally deferred.
  """

  alias Flux.Ref
  alias Flux.Run.AssetResult

  @type status :: :running | :ok | :error

  @type t :: %__MODULE__{
          id: String.t(),
          target_refs: [Ref.t()],
          plan: Flux.Plan.t(),
          status: status(),
          started_at: DateTime.t(),
          finished_at: DateTime.t() | nil,
          params: map(),
          outputs: %{Ref.t() => term()},
          target_outputs: %{Ref.t() => term()},
          asset_results: %{Ref.t() => AssetResult.t()},
          error: term() | nil
        }

  defstruct [
    :id,
    :target_refs,
    :plan,
    :started_at,
    status: :running,
    finished_at: nil,
    params: %{},
    outputs: %{},
    target_outputs: %{},
    asset_results: %{},
    error: nil
  ]
end
