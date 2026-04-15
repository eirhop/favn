defmodule Favn.Run.AssetResult do
  @moduledoc """
  Per-asset execution outcome captured during a run.
  """

  alias Favn.Ref

  @type error_kind :: :error | :exit | :throw

  @type error_details :: %{
          required(:kind) => error_kind(),
          required(:reason) => term(),
          required(:stacktrace) => [term()],
          optional(:message) => String.t()
        }

  @type status :: :running | :retrying | :ok | :error | :cancelled | :timed_out

  @type attempt_result :: %{
          attempt: pos_integer(),
          started_at: DateTime.t(),
          finished_at: DateTime.t(),
          duration_ms: non_neg_integer(),
          status: :ok | :error | :cancelled | :timed_out,
          meta: map(),
          error: error_details() | nil
        }

  @type t :: %__MODULE__{
          ref: Ref.t(),
          stage: non_neg_integer(),
          status: status(),
          started_at: DateTime.t(),
          finished_at: DateTime.t(),
          duration_ms: non_neg_integer(),
          meta: map(),
          error: error_details() | nil,
          attempt_count: non_neg_integer(),
          max_attempts: pos_integer(),
          attempts: [attempt_result()],
          next_retry_at: DateTime.t() | nil
        }

  defstruct [
    :ref,
    :stage,
    :status,
    :started_at,
    :finished_at,
    :duration_ms,
    meta: %{},
    error: nil,
    attempt_count: 0,
    max_attempts: 1,
    attempts: [],
    next_retry_at: nil
  ]
end
