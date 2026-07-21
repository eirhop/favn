defmodule Favn.Contracts.RunnerReleaseBinding do
  @moduledoc """
  Shared validation for contracts pinned to one exact runner release.

  This module validates only the immutable release identity. Owning runtime
  boundaries remain responsible for validating their other fields and for
  comparing the required ID with the connected runner's baked descriptor.
  """

  alias Favn.RunnerRelease

  @type error :: {:invalid_required_runner_release_id, term()}

  @doc "Validates a required runner release ID without creating atoms."
  @spec validate(term()) :: :ok | {:error, error()}
  def validate(value) do
    case RunnerRelease.validate_id(value) do
      :ok -> :ok
      {:error, _reason} -> {:error, {:invalid_required_runner_release_id, value}}
    end
  end
end
