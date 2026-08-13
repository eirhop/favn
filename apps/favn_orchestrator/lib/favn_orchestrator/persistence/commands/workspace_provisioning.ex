defmodule FavnOrchestrator.Persistence.Commands.ProvisionWorkspaceAdministrator do
  @moduledoc """
  Complete normalized input for atomic workspace provisioning.

  Password material crosses this boundary only as an Argon2id hash. The keyed
  request fingerprint binds the protected password input without persisting a
  reusable password verifier in the operation receipt.
  """

  alias FavnOrchestrator.Persistence.PlatformContext

  @enforce_keys [
    :platform_context,
    :operation_id,
    :request_fingerprint,
    :workspace_id,
    :slug,
    :workspace_name,
    :actor_id,
    :username,
    :display_name,
    :authentication_mode,
    :occurred_at
  ]
  defstruct @enforce_keys ++ [:password_hash, :tenant_id, :object_id]

  @type authentication_mode :: :entra | :password
  @type t :: %__MODULE__{
          platform_context: PlatformContext.t(),
          operation_id: String.t(),
          request_fingerprint: String.t(),
          workspace_id: String.t(),
          slug: String.t(),
          workspace_name: String.t(),
          actor_id: String.t(),
          username: String.t(),
          display_name: String.t(),
          authentication_mode: authentication_mode(),
          password_hash: String.t() | nil,
          tenant_id: String.t() | nil,
          object_id: String.t() | nil,
          occurred_at: DateTime.t()
        }
end
