defmodule Favn.Diagnostic do
  @moduledoc """
  Normalized diagnostic emitted by compile, registry, or runtime phases.
  """

  @enforce_keys [:severity, :stage, :code, :message]
  defstruct [
    :severity,
    :stage,
    :code,
    :message,
    :asset_ref,
    :span,
    details: %{}
  ]

  @type severity :: :info | :warning | :error
  @type stage :: :compile | :registry | :render | :planner | :runtime

  @type t :: %__MODULE__{
          severity: severity(),
          stage: stage(),
          code: atom(),
          message: String.t(),
          asset_ref: Favn.Ref.t() | nil,
          span: map() | nil,
          details: map()
        }
end
