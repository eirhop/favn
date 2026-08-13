defmodule FavnOrchestrator.Persistence.WorkspaceProvisioningStore do
  @moduledoc """
  Atomic persistence boundary for a workspace and its initial administrator.

  Implementations must commit the workspace, actor, authorization, selected
  authentication method, audit evidence, and durable operation receipt in one
  transaction. Exact replays return the committed result; conflicting reuse of
  an operation or workspace identity must not change authorization state.
  """

  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspaceAdministrator
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetWorkspaceProvisioning
  alias FavnOrchestrator.Persistence.Results.WorkspaceProvisioning

  @callback provision(ProvisionWorkspaceAdministrator.t()) ::
              {:ok, WorkspaceProvisioning.t()} | {:error, Error.t()}

  @callback get(GetWorkspaceProvisioning.t()) ::
              {:ok, WorkspaceProvisioning.t()} | {:error, Error.t()}
end
