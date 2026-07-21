defmodule Favn.Contracts.RunnerEvent do
  @moduledoc """
  Runner event contract emitted during manifest-pinned execution.
  """

  alias Favn.Contracts.RunnerReleaseBinding

  @type t :: %__MODULE__{
          run_id: String.t() | nil,
          manifest_version_id: String.t(),
          manifest_content_hash: String.t(),
          required_runner_release_id: String.t(),
          event_type: atom() | String.t(),
          occurred_at: DateTime.t() | nil,
          payload: map()
        }

  defstruct run_id: nil,
            manifest_version_id: nil,
            manifest_content_hash: nil,
            required_runner_release_id: nil,
            event_type: nil,
            occurred_at: nil,
            payload: %{}

  @doc "Validates the exact runner release identity echoed by this event."
  @spec validate_release_binding(t()) :: :ok | {:error, RunnerReleaseBinding.error()}
  def validate_release_binding(%__MODULE__{required_runner_release_id: release_id}),
    do: RunnerReleaseBinding.validate(release_id)
end
