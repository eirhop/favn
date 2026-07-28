defmodule FavnOrchestrator.Persistence.TargetRecoveryStore do
  @moduledoc "Persistence authority for immutable, evidence-backed target recovery."

  alias FavnOrchestrator.Persistence.Commands.ActivateRecoveredTargetGeneration
  alias FavnOrchestrator.Persistence.Commands.BeginTargetRecovery
  alias FavnOrchestrator.Persistence.Commands.CreateTargetRecoveryPlan
  alias FavnOrchestrator.Persistence.Commands.FailTargetRecovery
  alias FavnOrchestrator.Persistence.Commands.MarkTargetRecoveryUnknown
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetInitialTargetRecoveryCandidate
  alias FavnOrchestrator.Persistence.Queries.GetTargetRecovery
  alias FavnOrchestrator.Persistence.Results.InitialTargetRecoveryCandidate
  alias FavnOrchestrator.Persistence.Results.TargetRecoveryOperation

  @callback get_initial_candidate(GetInitialTargetRecoveryCandidate.t()) ::
              {:ok, InitialTargetRecoveryCandidate.t()} | {:error, Error.t()}
  @callback create_plan(CreateTargetRecoveryPlan.t()) ::
              {:ok, TargetRecoveryOperation.t()} | {:error, Error.t()}
  @callback begin_recovery(BeginTargetRecovery.t()) ::
              {:ok, TargetRecoveryOperation.t()} | {:error, Error.t()}
  @callback activate_generation(ActivateRecoveredTargetGeneration.t()) ::
              {:ok, TargetRecoveryOperation.t()} | {:error, Error.t()}
  @callback mark_unknown(MarkTargetRecoveryUnknown.t()) ::
              {:ok, TargetRecoveryOperation.t()} | {:error, Error.t()}
  @callback fail_recovery(FailTargetRecovery.t()) ::
              {:ok, TargetRecoveryOperation.t()} | {:error, Error.t()}
  @callback get(GetTargetRecovery.t()) ::
              {:ok, TargetRecoveryOperation.t()} | {:error, Error.t()}
end
