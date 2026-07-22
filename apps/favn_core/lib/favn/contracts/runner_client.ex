defmodule Favn.Contracts.RunnerClient do
  @moduledoc """
  Shared orchestrator-to-runner client boundary.

  `favn_orchestrator` depends on this behaviour from `favn_core` so it can
  dispatch manifest-pinned work without taking a compile-time dependency on the
  concrete runner implementation app.
  """

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.GenerationActivationRequest
  alias Favn.Contracts.GenerationActivationResult
  alias Favn.Contracts.GenerationDiscardRequest
  alias Favn.Contracts.GenerationDiscardResult
  alias Favn.Contracts.GenerationMarker
  alias Favn.Contracts.GenerationMarkerInitializationRequest
  alias Favn.Contracts.GenerationMarkerInitializationResult
  alias Favn.Contracts.GenerationReconciliationRequest
  alias Favn.Contracts.GenerationReconciliationResult
  alias Favn.Contracts.RunnerCancellation
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias Favn.RuntimeInput.Resolution

  @type execution_id :: String.t()

  @callback register_manifest(Version.t(), keyword()) :: :ok | {:error, term()}

  @callback ensure_manifest(Version.t(), keyword()) :: :ok | :missing | {:error, term()}

  @callback acquire_manifest(
              Version.t(),
              String.t(),
              DateTime.t(),
              [Favn.Ref.t()],
              keyword()
            ) ::
              :ok | {:error, term()}

  @callback renew_manifest(String.t(), DateTime.t(), keyword()) :: :ok | {:error, term()}

  @callback release_manifest(String.t(), keyword()) :: :ok

  @doc """
  Submits work and returns its execution identity.

  A successful client must return the exact `execution_id` supplied in the
  `RunnerWork`. This keeps dispatch, idempotency, recovery, and cancellation on
  one orchestrator-selected identity.
  """
  @callback submit_work(RunnerWork.t(), keyword()) :: {:ok, execution_id()} | {:error, term()}

  @callback resolve_runtime_inputs(RunnerWork.t(), keyword()) ::
              {:ok, Resolution.t() | nil} | {:error, term()}

  @callback await_result(execution_id(), timeout(), keyword()) ::
              {:ok, RunnerResult.t()} | {:error, term()}

  @callback cancel_work(execution_id(), RunnerCancellation.t(), keyword()) ::
              {:ok, RunnerCancellation.outcome()} | {:error, RunnerError.t()}

  @callback subscribe_execution_logs(execution_id(), pid(), keyword()) :: :ok | {:error, term()}

  @callback unsubscribe_execution_logs(execution_id(), pid(), keyword()) :: :ok

  @callback inspect_relation(RelationInspectionRequest.t(), keyword()) ::
              {:ok, RelationInspectionResult.t()} | {:error, term()}

  @callback generation_capabilities(Version.t(), Favn.Ref.t(), keyword()) ::
              {:ok, map()} | {:error, term()}

  @callback initialize_generation_marker(GenerationMarkerInitializationRequest.t(), keyword()) ::
              {:ok, GenerationMarkerInitializationResult.t()} | {:error, term()}

  @callback generation_marker(Version.t(), Favn.Ref.t(), keyword()) ::
              {:ok, GenerationMarker.t() | nil} | {:error, term()}

  @callback activate_generation(GenerationActivationRequest.t(), keyword()) ::
              {:ok, GenerationActivationResult.t()} | {:error, term()}

  @callback reconcile_generation(GenerationReconciliationRequest.t(), keyword()) ::
              {:ok, GenerationReconciliationResult.t()} | {:error, term()}

  @callback discard_generation(GenerationDiscardRequest.t(), keyword()) ::
              {:ok, GenerationDiscardResult.t()} | {:error, term()}

  @callback diagnostics(keyword()) :: {:ok, map()} | {:error, term()}

  @optional_callbacks diagnostics: 1,
                      resolve_runtime_inputs: 2,
                      subscribe_execution_logs: 3,
                      unsubscribe_execution_logs: 3,
                      generation_capabilities: 3,
                      initialize_generation_marker: 2,
                      generation_marker: 3,
                      activate_generation: 2,
                      reconcile_generation: 2,
                      discard_generation: 2
end
