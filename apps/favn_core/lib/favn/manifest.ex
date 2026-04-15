defmodule Favn.Manifest do
  @moduledoc """
  Canonical runtime manifest payload.

  `%Favn.Manifest{}` is the stable payload that gets serialized, hashed, and
  pinned into `%Favn.Manifest.Version{}`. Build-only fields such as timestamps
  and diagnostics do not belong here.
  """

  alias Favn.Manifest.Graph

  @type t :: %__MODULE__{
          schema_version: pos_integer(),
          runner_contract_version: pos_integer(),
          assets: [Favn.Manifest.Asset.t()],
          pipelines: [Favn.Manifest.Pipeline.t()],
          schedules: [Favn.Manifest.Schedule.t()],
          graph: Graph.t(),
          metadata: map()
        }

  defstruct schema_version: 1,
            runner_contract_version: 1,
            assets: [],
            pipelines: [],
            schedules: [],
            graph: %Graph{},
            metadata: %{}
end
