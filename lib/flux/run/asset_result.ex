defmodule Flux.Run.AssetResult do
  @moduledoc """
  Per-asset execution outcome captured during a run.
  """

  alias Flux.Ref

  @type error_kind :: :error | :exit | :throw

  @type error_details :: %{
          required(:kind) => error_kind(),
          required(:reason) => term(),
          required(:stacktrace) => [term()],
          optional(:message) => String.t()
        }

  @type status :: :ok | :error

  @type t :: %__MODULE__{
          ref: Ref.t(),
          stage: non_neg_integer(),
          status: status(),
          started_at: DateTime.t(),
          finished_at: DateTime.t(),
          duration_ms: non_neg_integer(),
          value: term() | nil,
          error: error_details() | nil
        }

  defstruct [
    :ref,
    :stage,
    :status,
    :started_at,
    :finished_at,
    :duration_ms,
    value: nil,
    error: nil
  ]
end
