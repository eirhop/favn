defmodule Favn.Run.Context do
  @moduledoc """
  Runtime context passed to each asset invocation.
  """

  alias Favn.Ref

  @type t :: %__MODULE__{
          run_id: String.t(),
          target_refs: [Ref.t()],
          current_ref: Ref.t(),
          params: map(),
          window: map() | nil,
          pipeline: map() | nil,
          run_started_at: DateTime.t(),
          stage: non_neg_integer(),
          attempt: pos_integer(),
          max_attempts: pos_integer()
        }

  defstruct [
    :run_id,
    :target_refs,
    :current_ref,
    :params,
    :window,
    :run_started_at,
    :stage,
    :attempt,
    :max_attempts,
    pipeline: nil
  ]
end
