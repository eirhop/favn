defmodule FavnOrchestrator.Persistence.Queries.GetWorkspaceProvisioning do
  @moduledoc """
  Looks up the authoritative provisioning receipt for one workspace.
  """

  alias FavnOrchestrator.Persistence.PlatformContext

  @enforce_keys [:platform_context, :workspace_id]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          platform_context: PlatformContext.t(),
          workspace_id: String.t()
        }
end
