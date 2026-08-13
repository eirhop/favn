defmodule FavnOrchestrator.Persistence.Results.WorkspaceProvisioning do
  @moduledoc """
  Redacted readiness result for an atomically provisioned workspace.
  """

  @enforce_keys [
    :operation_id,
    :workspace_id,
    :slug,
    :workspace_name,
    :actor_id,
    :username,
    :authentication_mode,
    :workspace_roles,
    :platform_roles,
    :status,
    :replayed?
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          operation_id: String.t(),
          workspace_id: String.t(),
          slug: String.t(),
          workspace_name: String.t(),
          actor_id: String.t(),
          username: String.t(),
          authentication_mode: :entra | :password,
          workspace_roles: [:workspace_admin],
          platform_roles: [:platform_admin],
          status: :ready,
          replayed?: boolean()
        }
end
