defmodule FavnOrchestrator.RunnerIdentityVerifier do
  @moduledoc """
  Enforces immutable runner releases selected by manifests and runs.

  Runner availability is deliberately not an activation prerequisite. Durable
  pool/release demand may exist while the matching runner count is zero.
  Runner-owned results must still echo the exact release pinned to their task.
  """

  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.RunnerResult
  alias Favn.Manifest.Version
  alias Favn.RunnerRelease
  alias FavnOrchestrator.RunState

  @type error ::
          :invalid_runner_release_identity
          | {:runner_release_mismatch, term(), term()}
          | {:run_manifest_identity_mismatch, atom()}

  @doc "Requires a run's immutable deployment identity to match its manifest."
  @spec verify_run_manifest(RunState.t(), Version.t()) :: :ok | {:error, error()}
  def verify_run_manifest(%RunState{} = run, %Version{} = version) do
    cond do
      run.manifest_version_id != version.manifest_version_id ->
        {:error, {:run_manifest_identity_mismatch, :manifest_version_id}}

      run.manifest_content_hash != version.content_hash ->
        {:error, {:run_manifest_identity_mismatch, :manifest_content_hash}}

      run.runner_releases != version.runner_releases ->
        {:error, {:runner_release_mismatch, run.runner_releases, version.runner_releases}}

      true ->
        :ok
    end
  end

  @doc "Validates the release identity echoed by one runner result."
  @spec verify_result(String.t(), RunnerResult.t()) :: :ok | {:error, error()}
  def verify_result(required, %RunnerResult{required_runner_release_id: actual}),
    do: require_match(required, actual)

  @doc "Validates the release identity echoed by one relation-inspection result."
  @spec verify_inspection_result(String.t(), RelationInspectionResult.t()) ::
          :ok | {:error, error()}
  def verify_inspection_result(
        required,
        %RelationInspectionResult{required_runner_release_id: actual}
      ),
      do: require_match(required, actual)

  defp require_match(required, actual) do
    with :ok <- RunnerRelease.validate_id(required),
         :ok <- RunnerRelease.validate_id(actual) do
      if required == actual,
        do: :ok,
        else: {:error, {:runner_release_mismatch, required, actual}}
    else
      {:error, _reason} -> {:error, :invalid_runner_release_identity}
    end
  end
end
