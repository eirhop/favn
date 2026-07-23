defmodule FavnOrchestrator.Persistence.TargetGenerationStore do
  @moduledoc """
  Persistence contract for logical target bindings and physical generations.

  The store owns atomic generation identity and binding changes. Compatibility
  classification remains orchestrator policy and is supplied through typed
  commands rather than inferred from database rows.
  """

  alias FavnOrchestrator.Persistence.Commands.EnsureWritableTargetGeneration
  alias FavnOrchestrator.Persistence.Commands.ReconcileInitialTargetGeneration
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetTargetBinding
  alias FavnOrchestrator.Persistence.Queries.GetTargetBindings
  alias FavnOrchestrator.Persistence.Results.TargetBinding
  alias FavnOrchestrator.Persistence.Results.InitialTargetGenerationReconciliation
  alias FavnOrchestrator.Persistence.Results.WritableTargetGeneration

  @callback ensure_writable(EnsureWritableTargetGeneration.t()) ::
              {:ok, WritableTargetGeneration.t()} | {:error, Error.t()}
  @callback reconcile_initial(ReconcileInitialTargetGeneration.t()) ::
              {:ok, InitialTargetGenerationReconciliation.t()} | {:error, Error.t()}
  @callback get_binding(GetTargetBinding.t()) ::
              {:ok, TargetBinding.t() | nil} | {:error, Error.t()}
  @callback get_bindings(GetTargetBindings.t()) ::
              {:ok, [TargetBinding.t()]} | {:error, Error.t()}
end
