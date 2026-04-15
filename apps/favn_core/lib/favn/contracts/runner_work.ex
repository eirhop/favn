defmodule Favn.Contracts.RunnerWork do
  @moduledoc """
  Runner work request contract pinned to an immutable manifest version.
  """

  @type t :: %__MODULE__{
          run_id: String.t() | nil,
          manifest_version_id: String.t(),
          manifest_content_hash: String.t(),
          asset_ref: {module(), atom()} | nil,
          asset_refs: [{module(), atom()}],
          params: map(),
          trigger: map(),
          metadata: map()
        }

  defstruct run_id: nil,
            manifest_version_id: nil,
            manifest_content_hash: nil,
            asset_ref: nil,
            asset_refs: [],
            params: %{},
            trigger: %{},
            metadata: %{}
end
