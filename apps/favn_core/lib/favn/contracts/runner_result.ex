defmodule Favn.Contracts.RunnerResult do
  @moduledoc """
  Runner execution result contract for manifest-pinned work.
  """

  alias Favn.Contracts.RunnerAssetResult
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerReleaseBinding
  alias Favn.Contracts.ResourceOutcome

  @type status :: :ok | :error | :cancelled | :timed_out

  @type t :: %__MODULE__{
          run_id: String.t() | nil,
          manifest_version_id: String.t(),
          manifest_content_hash: String.t(),
          required_runner_release_id: String.t(),
          status: status(),
          asset_results: [RunnerAssetResult.t()],
          resource_outcomes: [ResourceOutcome.t()],
          error: RunnerError.t() | nil,
          metadata: map()
        }

  defstruct run_id: nil,
            manifest_version_id: nil,
            manifest_content_hash: nil,
            required_runner_release_id: nil,
            status: :ok,
            asset_results: [],
            resource_outcomes: [],
            error: nil,
            metadata: %{}

  @doc "Validates the exact runner release identity echoed by this result."
  @spec validate_release_binding(t()) :: :ok | {:error, RunnerReleaseBinding.error()}
  def validate_release_binding(%__MODULE__{required_runner_release_id: release_id}),
    do: RunnerReleaseBinding.validate(release_id)
end
