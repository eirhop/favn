defmodule FavnOrchestrator.Persistence.Results.ManifestDeployment do
  @moduledoc "Durable status for one caller-named manifest deployment operation."

  @type state :: :accepted | :activating | :succeeded | :needs_attention | :failed | :unknown

  @enforce_keys [
    :workspace_id,
    :operation_id,
    :archive_sha256,
    :request_fingerprint,
    :service_identity,
    :state,
    :inserted_at,
    :updated_at
  ]
  defstruct [
    :workspace_id,
    :operation_id,
    :archive_sha256,
    :request_fingerprint,
    :service_identity,
    :manifest_version_id,
    :manifest_content_hash,
    :runner_releases,
    :state,
    :deployment_id,
    :failure_class,
    :activation_diagnostics,
    :claim_owner,
    :claim_fence,
    :claim_expires_at,
    :inspection_total,
    :inspection_completed,
    :accepted_at,
    :activating_at,
    :terminal_at,
    :inserted_at,
    :updated_at
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          operation_id: String.t(),
          archive_sha256: String.t(),
          request_fingerprint: String.t(),
          service_identity: String.t(),
          manifest_version_id: String.t() | nil,
          manifest_content_hash: String.t() | nil,
          runner_releases: Favn.RunnerPool.releases() | nil,
          state: state(),
          deployment_id: String.t() | nil,
          failure_class: String.t() | nil,
          activation_diagnostics: map() | nil,
          claim_owner: String.t() | nil,
          claim_fence: pos_integer() | nil,
          claim_expires_at: DateTime.t() | nil,
          inspection_total: non_neg_integer(),
          inspection_completed: non_neg_integer(),
          accepted_at: DateTime.t() | nil,
          activating_at: DateTime.t() | nil,
          terminal_at: DateTime.t() | nil,
          inserted_at: DateTime.t(),
          updated_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.UpdateManifestDeploymentProgress do
  @moduledoc "Records bounded target-inspection progress under the current worker fence."
  alias FavnOrchestrator.Persistence.PlatformContext

  @enforce_keys [
    :platform_context,
    :workspace_id,
    :operation_id,
    :owner,
    :fence,
    :completed,
    :total,
    :occurred_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          platform_context: PlatformContext.t(),
          workspace_id: String.t(),
          operation_id: String.t(),
          owner: String.t(),
          fence: pos_integer(),
          completed: non_neg_integer(),
          total: non_neg_integer(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.AcquireManifestUploadLease do
  @moduledoc "Acquires distributed admission before a manifest archive body is read."
  alias FavnOrchestrator.ManifestDeploymentContext

  @enforce_keys [:context, :lease_id, :expires_at, :occurred_at]
  defstruct [:context, :lease_id, :expires_at, :occurred_at]

  @type t :: %__MODULE__{
          context: ManifestDeploymentContext.t(),
          lease_id: String.t(),
          expires_at: DateTime.t(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RenewManifestUploadLease do
  @moduledoc "Renews one owned manifest archive upload lease."
  alias FavnOrchestrator.ManifestDeploymentContext

  @enforce_keys [:context, :lease_id, :expires_at]
  defstruct [:context, :lease_id, :expires_at]

  @type t :: %__MODULE__{
          context: ManifestDeploymentContext.t(),
          lease_id: String.t(),
          expires_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ReleaseManifestUploadLease do
  @moduledoc "Releases one owned manifest archive upload lease."
  alias FavnOrchestrator.ManifestDeploymentContext

  @enforce_keys [:context, :lease_id]
  defstruct [:context, :lease_id]

  @type t :: %__MODULE__{
          context: ManifestDeploymentContext.t(),
          lease_id: String.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.AcceptManifestDeployment do
  @moduledoc "Atomically registers a manifest and accepts its durable deployment intent."
  alias Favn.Manifest.Version
  alias FavnOrchestrator.ManifestDeploymentContext
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :context,
    :platform_context,
    :workspace_context,
    :operation_id,
    :upload_lease_id,
    :archive_sha256,
    :request_fingerprint,
    :version,
    :occurred_at
  ]
  defstruct [
    :context,
    :platform_context,
    :workspace_context,
    :operation_id,
    :upload_lease_id,
    :archive_sha256,
    :request_fingerprint,
    :version,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          context: ManifestDeploymentContext.t(),
          platform_context: PlatformContext.t(),
          workspace_context: WorkspaceContext.t(),
          operation_id: String.t(),
          upload_lease_id: String.t(),
          archive_sha256: String.t(),
          request_fingerprint: String.t(),
          version: Version.t(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetManifestDeployment do
  @moduledoc "Reads one workspace-authorized manifest deployment operation."
  alias FavnOrchestrator.ManifestDeploymentContext

  @enforce_keys [:context, :operation_id]
  defstruct [:context, :operation_id]

  @type t :: %__MODULE__{
          context: ManifestDeploymentContext.t(),
          operation_id: String.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ClaimManifestDeployment do
  @moduledoc "Claims the next recoverable manifest deployment under a fenced lease."
  alias FavnOrchestrator.Persistence.PlatformContext

  @enforce_keys [:platform_context, :owner, :expires_at, :occurred_at]
  defstruct [:platform_context, :owner, :expires_at, :occurred_at]

  @type t :: %__MODULE__{
          platform_context: PlatformContext.t(),
          owner: String.t(),
          expires_at: DateTime.t(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RenewManifestDeploymentClaim do
  @moduledoc "Renews one fenced manifest deployment worker claim."
  alias FavnOrchestrator.Persistence.PlatformContext

  @enforce_keys [
    :platform_context,
    :workspace_id,
    :operation_id,
    :owner,
    :fence,
    :expires_at
  ]
  defstruct [
    :platform_context,
    :workspace_id,
    :operation_id,
    :owner,
    :fence,
    :expires_at
  ]

  @type t :: %__MODULE__{
          platform_context: PlatformContext.t(),
          workspace_id: String.t(),
          operation_id: String.t(),
          owner: String.t(),
          fence: pos_integer(),
          expires_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ReleaseManifestDeploymentClaim do
  @moduledoc "Returns one busy manifest deployment to the accepted queue."
  alias FavnOrchestrator.Persistence.PlatformContext

  @enforce_keys [:platform_context, :workspace_id, :operation_id, :owner, :fence, :occurred_at]
  defstruct [:platform_context, :workspace_id, :operation_id, :owner, :fence, :occurred_at]

  @type t :: %__MODULE__{
          platform_context: PlatformContext.t(),
          workspace_id: String.t(),
          operation_id: String.t(),
          owner: String.t(),
          fence: pos_integer(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.CompleteManifestDeployment do
  @moduledoc "Commits one fenced manifest deployment terminal result."
  alias FavnOrchestrator.Persistence.PlatformContext

  @enforce_keys [
    :platform_context,
    :workspace_id,
    :operation_id,
    :owner,
    :fence,
    :state,
    :occurred_at
  ]
  defstruct [
    :platform_context,
    :workspace_id,
    :operation_id,
    :owner,
    :fence,
    :state,
    :deployment_id,
    :failure_class,
    :activation_diagnostics,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          platform_context: PlatformContext.t(),
          workspace_id: String.t(),
          operation_id: String.t(),
          owner: String.t(),
          fence: pos_integer(),
          state: FavnOrchestrator.Persistence.Results.ManifestDeployment.state(),
          deployment_id: String.t() | nil,
          failure_class: String.t() | nil,
          activation_diagnostics: map() | nil,
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.AcquireManifestActivationLease do
  @moduledoc "Serializes manifest activation for one workspace."
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :operation_id, :owner, :expires_at, :occurred_at]
  defstruct [:workspace_context, :operation_id, :owner, :expires_at, :occurred_at]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          operation_id: String.t(),
          owner: String.t(),
          expires_at: DateTime.t(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RenewManifestActivationLease do
  @moduledoc "Renews one fenced workspace activation lease."
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :operation_id, :owner, :fence, :expires_at]
  defstruct [:workspace_context, :operation_id, :owner, :fence, :expires_at]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          operation_id: String.t(),
          owner: String.t(),
          fence: pos_integer(),
          expires_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ReleaseManifestActivationLease do
  @moduledoc "Releases one fenced workspace activation lease."
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :operation_id, :owner, :fence]
  defstruct [:workspace_context, :operation_id, :owner, :fence]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          operation_id: String.t(),
          owner: String.t(),
          fence: pos_integer()
        }
end
