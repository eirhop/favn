defmodule FavnRunner.ManifestHandle do
  @moduledoc """
  Small immutable identity for one manifest compiled by the runner cache.

  The handle is safe to copy into work processes. Exact assets and SQL relation
  metadata are resolved before worker admission, so workers never receive or
  rescan the full compact manifest.
  """

  @enforce_keys [:manifest_version_id, :content_hash]
  defstruct [:manifest_version_id, :content_hash]

  @type t :: %__MODULE__{
          manifest_version_id: String.t(),
          content_hash: String.t()
        }
end
