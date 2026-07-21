defmodule FavnOrchestrator.RunnerReleaseCompatibility do
  @moduledoc """
  Enforces the immutable runner release selected by a manifest and run.

  Activation and dispatch use the configured runner client's bounded diagnostics
  path. Runner-owned results are accepted only when they echo the same release
  identity.
  """

  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.RunnerResult
  alias Favn.Manifest.Version
  alias Favn.RunnerRelease
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.RunnerClientValidator
  alias FavnOrchestrator.RunnerDiagnostics
  alias FavnOrchestrator.RunState

  @type error ::
          :legacy_runner_release_unbound
          | :invalid_runner_release_identity
          | :runner_client_not_available
          | :runner_release_info_unavailable
          | :runner_runtime_info_unavailable
          | :runner_node_identity_mismatch
          | :runner_not_ready
          | {:runner_release_mismatch, String.t(), term()}
          | {:run_manifest_identity_mismatch, atom()}

  @doc "Requires a ready runner with the exact release required by the manifest."
  @spec verify_runner(module(), Version.t() | String.t(), keyword()) :: :ok | {:error, error()}
  def verify_runner(
        client,
        %Version{
          manifest_version_id: manifest_version_id,
          required_runner_release_id: required
        },
        opts
      ) do
    verify_runner_and_emit(client, required, opts, %{manifest_version_id: manifest_version_id})
  end

  def verify_runner(client, required, opts)
      when is_atom(client) and is_binary(required) and is_list(opts) do
    verify_runner_and_emit(client, required, opts, %{})
  end

  defp verify_runner_and_emit(client, required, opts, metadata) do
    started_at = System.monotonic_time()
    result = do_verify_runner(client, required, opts)

    duration_ms =
      System.monotonic_time()
      |> Kernel.-(started_at)
      |> System.convert_time_unit(:native, :millisecond)

    OperationalEvents.emit(
      :runner_release_diagnostics_checked,
      %{duration_ms: duration_ms},
      Map.merge(metadata, diagnostics_metadata(required, result)),
      level: diagnostics_level(result)
    )

    result
  end

  defp do_verify_runner(client, required, opts) do
    with :ok <- RunnerClientValidator.validate(client),
         true <- function_exported?(client, :diagnostics, 1),
         {:ok, diagnostics} <- runner_diagnostics(client, opts),
         {:ok, actual} <- RunnerDiagnostics.validate_ready(diagnostics, opts),
         :ok <- require_match(required, actual) do
      :ok
    else
      false ->
        {:error, :runner_release_info_unavailable}

      {:error, :runner_client_not_available} = error ->
        error

      {:error, reason}
      when reason in [
             :runner_release_info_unavailable,
             :runner_runtime_info_unavailable,
             :runner_node_identity_mismatch,
             :runner_not_ready
           ] ->
        {:error, reason}

      {:error, {:runner_release_mismatch, _required, _actual}} = error ->
        error
    end
  end

  @doc "Requires a run's immutable deployment identity to match its manifest."
  @spec verify_run_manifest(RunState.t(), Version.t()) :: :ok | {:error, error()}
  def verify_run_manifest(
        %RunState{required_runner_release_id: nil},
        %Version{}
      ),
      do: {:error, :legacy_runner_release_unbound}

  def verify_run_manifest(%RunState{} = run, %Version{} = version) do
    cond do
      run.manifest_version_id != version.manifest_version_id ->
        {:error, {:run_manifest_identity_mismatch, :manifest_version_id}}

      run.manifest_content_hash != version.content_hash ->
        {:error, {:run_manifest_identity_mismatch, :manifest_content_hash}}

      run.required_runner_release_id != version.required_runner_release_id ->
        {:error,
         {:runner_release_mismatch, run.required_runner_release_id,
          version.required_runner_release_id}}

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

  defp runner_diagnostics(client, opts) do
    case client.diagnostics(opts) do
      {:ok, diagnostics} when is_map(diagnostics) -> {:ok, diagnostics}
      {:error, _reason} -> {:error, :runner_not_ready}
      _other -> {:error, :runner_not_ready}
    end
  rescue
    _exception -> {:error, :runner_not_ready}
  catch
    _kind, _reason -> {:error, :runner_not_ready}
  end

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

  defp diagnostics_metadata(required, :ok) do
    %{
      status: :ready,
      required_runner_release_id: required,
      runner_release_id: required
    }
  end

  defp diagnostics_metadata(required, {:error, {:runner_release_mismatch, _required, actual}}) do
    %{
      status: :rejected,
      reason: :runner_release_mismatch,
      required_runner_release_id: required,
      runner_release_id: actual
    }
  end

  defp diagnostics_metadata(required, {:error, reason}) when is_atom(reason) do
    %{
      status: :rejected,
      reason: reason,
      required_runner_release_id: required
    }
  end

  defp diagnostics_level(:ok), do: :info
  defp diagnostics_level({:error, _reason}), do: :warning
end
