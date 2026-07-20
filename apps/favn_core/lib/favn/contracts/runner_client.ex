defmodule Favn.Contracts.RunnerClient do
  @moduledoc """
  Shared orchestrator-to-runner client boundary.

  `favn_orchestrator` depends on this behaviour from `favn_core` so it can
  dispatch manifest-pinned work without taking a compile-time dependency on the
  concrete runner implementation app.
  """

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.RunnerCancellation
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias Favn.RuntimeInput.Resolution

  @type execution_id :: String.t()

  @callback register_manifest(Version.t(), keyword()) :: :ok | {:error, term()}

  @callback ensure_manifest(String.t(), String.t(), keyword()) ::
              :ok | :missing | {:error, term()}

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

  @callback diagnostics(keyword()) :: {:ok, map()} | {:error, term()}

  @optional_callbacks diagnostics: 1,
                      resolve_runtime_inputs: 2,
                      subscribe_execution_logs: 3,
                      unsubscribe_execution_logs: 3
end
