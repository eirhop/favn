defmodule FavnOrchestrator.Persistence.VerifiedExecutionPackage do
  @moduledoc """
  Canonical byte envelope accepted by execution-package persistence.

  The Orchestrator verifies the package once. Persistence receives bounded
  identity metadata and canonical JSON bytes, so it does not decode another
  complete payload copy before insertion.
  """

  @enforce_keys [
    :content_hash,
    :asset_module,
    :asset_name,
    :canonical_json
  ]
  defstruct [
    :content_hash,
    :asset_module,
    :asset_name,
    :runtime_input_resolver,
    :canonical_json
  ]

  @type t :: %__MODULE__{
          content_hash: String.t(),
          asset_module: String.t(),
          asset_name: String.t(),
          runtime_input_resolver: String.t() | nil,
          canonical_json: binary()
        }
end
