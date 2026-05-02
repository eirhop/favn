defmodule Favn.Contracts.RunnerClient do
  @moduledoc """
  Shared orchestrator-to-runner client boundary.

  `favn_orchestrator` depends on this behaviour from `favn_core` so it can
  dispatch manifest-pinned work without taking a compile-time dependency on the
  concrete runner implementation app.
  """

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version

  @type execution_id :: String.t()

  @callback register_manifest(Version.t(), keyword()) :: :ok | {:error, term()}

  @callback submit_work(RunnerWork.t(), keyword()) :: {:ok, execution_id()} | {:error, term()}

  @callback await_result(execution_id(), timeout(), keyword()) ::
              {:ok, RunnerResult.t()} | {:error, term()}

  @callback cancel_work(execution_id(), map(), keyword()) :: :ok | {:error, term()}

  @callback inspect_relation(RelationInspectionRequest.t(), keyword()) ::
              {:ok, RelationInspectionResult.t()} | {:error, term()}

  @callback diagnostics(keyword()) :: {:ok, map()} | {:error, term()}

  @optional_callbacks diagnostics: 1
end
