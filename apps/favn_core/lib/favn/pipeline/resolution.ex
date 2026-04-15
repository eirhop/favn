defmodule Favn.Pipeline.Resolution do
  @moduledoc """
  Resolved pipeline execution input ready for planner/runtime handoff.
  """

  alias Favn.Pipeline.Definition
  alias Favn.Ref

  @type dependencies_mode :: :all | :none

  @type t :: %__MODULE__{
          pipeline: Definition.t(),
          target_refs: [Ref.t()],
          dependencies: dependencies_mode(),
          pipeline_ctx: map()
        }

  defstruct [:pipeline, target_refs: [], dependencies: :all, pipeline_ctx: %{}]
end
