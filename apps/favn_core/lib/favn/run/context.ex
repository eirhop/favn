defmodule Favn.Run.Context do
  @moduledoc """
  Runtime context passed to each asset invocation.
  """

  alias Favn.Plan.NodeIdentity
  alias Favn.Run.AssetContext
  alias Favn.Run.PipelineContext
  alias Favn.Window.Runtime

  @type t :: %__MODULE__{
          run_id: String.t(),
          node_identity: NodeIdentity.t() | nil,
          target_refs: [Favn.Ref.t()],
          asset: AssetContext.t(),
          runtime_config: map(),
          params: map(),
          window: Runtime.t() | nil,
          pipeline: PipelineContext.t() | nil,
          run_started_at: DateTime.t(),
          deadline_at: DateTime.t() | nil,
          stage: non_neg_integer(),
          attempt: pos_integer(),
          max_attempts: pos_integer()
        }

  defstruct [
    :run_id,
    :node_identity,
    :target_refs,
    :asset,
    :runtime_config,
    :params,
    :window,
    :run_started_at,
    :deadline_at,
    :stage,
    :attempt,
    :max_attempts,
    pipeline: nil
  ]
end
