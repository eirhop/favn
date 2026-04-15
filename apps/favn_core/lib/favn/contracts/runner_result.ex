defmodule Favn.Contracts.RunnerResult do
  @moduledoc """
  Runner execution result contract for manifest-pinned work.
  """

  @type status :: :ok | :error | :cancelled | :timed_out

  @type t :: %__MODULE__{
          run_id: String.t() | nil,
          manifest_version_id: String.t(),
          manifest_content_hash: String.t(),
          status: status(),
          asset_results: [map()],
          error: term() | nil,
          metadata: map()
        }

  defstruct run_id: nil,
            manifest_version_id: nil,
            manifest_content_hash: nil,
            status: :ok,
            asset_results: [],
            error: nil,
            metadata: %{}
end
