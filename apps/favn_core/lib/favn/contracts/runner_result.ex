defmodule Favn.Contracts.RunnerResult do
  @moduledoc """
  Runner execution result contract for manifest-pinned work.
  """

  alias Favn.Contracts.RunnerAssetResult
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.ResourceOutcome

  @type status :: :ok | :error | :cancelled | :timed_out

  @type t :: %__MODULE__{
          run_id: String.t() | nil,
          manifest_version_id: String.t(),
          manifest_content_hash: String.t(),
          status: status(),
          asset_results: [RunnerAssetResult.t()],
          resource_outcomes: [ResourceOutcome.t()],
          error: RunnerError.t() | nil,
          metadata: map()
        }

  defstruct run_id: nil,
            manifest_version_id: nil,
            manifest_content_hash: nil,
            status: :ok,
            asset_results: [],
            resource_outcomes: [],
            error: nil,
            metadata: %{}
end
