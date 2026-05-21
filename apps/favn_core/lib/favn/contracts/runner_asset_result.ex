defmodule Favn.Contracts.RunnerAssetResult do
  @moduledoc """
  Runner-owned per-asset result envelope.
  """

  alias Favn.Contracts.RunnerError
  alias Favn.Ref

  @type status :: :ok | :error | :cancelled | :timed_out

  @type attempt_result :: %{
          attempt: pos_integer(),
          started_at: DateTime.t(),
          finished_at: DateTime.t(),
          duration_ms: non_neg_integer(),
          status: status(),
          meta: map(),
          error: RunnerError.t() | nil
        }

  @type t :: %__MODULE__{
          ref: Ref.t(),
          status: status(),
          started_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          duration_ms: non_neg_integer() | nil,
          meta: map(),
          error: RunnerError.t() | nil,
          attempt_count: non_neg_integer(),
          max_attempts: pos_integer(),
          attempts: [attempt_result()],
          asset_step_id: String.t() | nil
        }

  defstruct [
    :ref,
    :status,
    :started_at,
    :finished_at,
    :duration_ms,
    :error,
    :asset_step_id,
    meta: %{},
    attempt_count: 0,
    max_attempts: 1,
    attempts: []
  ]
end
