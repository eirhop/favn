defmodule Favn.Run.AssetContext do
  @moduledoc """
  Manifest-backed asset data exposed to runtime code.

  `settings` contains non-secret static values. Resolved environment and secret
  values live separately in `Favn.Run.Context.runtime_config`.
  """

  alias Favn.Ref
  alias Favn.RelationRef

  @enforce_keys [:ref]
  @type t :: %__MODULE__{
          ref: Ref.t(),
          relation: RelationRef.t() | nil,
          settings: Favn.Settings.t()
        }

  defstruct [:ref, relation: nil, settings: %{}]
end
