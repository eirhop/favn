defmodule FavnOrchestrator.Persistence.Commands.ProvisionWorkspace do
  @moduledoc "Creates one explicit customer workspace and its runtime-state root."

  alias FavnOrchestrator.Persistence.PlatformContext

  @enforce_keys [:platform_context, :workspace_id, :slug, :display_name, :occurred_at]
  defstruct [:platform_context, :workspace_id, :slug, :display_name, :occurred_at]

  @type t :: %__MODULE__{
          platform_context: PlatformContext.t(),
          workspace_id: String.t(),
          slug: String.t(),
          display_name: String.t(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RegisterManifest do
  @moduledoc "Registers one immutable platform-global manifest release."

  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.PlatformContext

  @enforce_keys [:platform_context, :version]
  defstruct [:platform_context, :version]

  @type t :: %__MODULE__{platform_context: PlatformContext.t(), version: Version.t()}
end

defmodule FavnOrchestrator.Persistence.Commands.RegisterExecutionPackages do
  @moduledoc "Registers immutable content-addressed execution packages under platform authority."

  alias Favn.Manifest.ExecutionPackage
  alias FavnOrchestrator.Persistence.PlatformContext

  @enforce_keys [:platform_context, :packages]
  defstruct [:platform_context, packages: []]

  @type t :: %__MODULE__{
          platform_context: PlatformContext.t(),
          packages: [ExecutionPackage.t()]
        }
end

defmodule FavnOrchestrator.Persistence.Commands.DeploymentTarget do
  @moduledoc "One exact target granted to an immutable workspace deployment."

  @enforce_keys [:target_kind, :target_id, :selection_source, :customer_visible]
  defstruct [:target_kind, :target_id, :selection_source, :customer_visible]

  @type t :: %__MODULE__{
          target_kind: :asset | :pipeline,
          target_id: String.t(),
          selection_source: :common | :explicit | :dependency,
          customer_visible: boolean()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.DeployManifest do
  @moduledoc "Activates one immutable, exact manifest deployment for a workspace."

  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.Commands.DeploymentTarget
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :deployment_id,
    :manifest_version_id,
    :configuration,
    :targets,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :deployment_id,
    :manifest_version_id,
    :configuration,
    :targets,
    :occurred_at,
    :idempotency,
    schedules: [],
    capacity_scopes: [],
    configuration_version: 1
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          deployment_id: String.t(),
          manifest_version_id: String.t(),
          configuration: map(),
          configuration_version: pos_integer(),
          targets: [DeploymentTarget.t()],
          schedules: [FavnOrchestrator.Persistence.Commands.DeploymentSchedule.t()],
          capacity_scopes: [FavnOrchestrator.Persistence.Commands.DeploymentCapacityScope.t()],
          occurred_at: DateTime.t(),
          idempotency: CommandIdempotency.t() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Commands.DeploymentCapacityScope do
  @moduledoc "One workspace capacity limit activated with a deployment."
  @enforce_keys [:scope_id, :scope_kind, :scope_key, :capacity_limit]
  defstruct [:scope_id, :scope_kind, :scope_key, :capacity_limit]

  @type t :: %__MODULE__{
          scope_id: String.t(),
          scope_kind: :workspace | :pool | :pipeline | :run,
          scope_key: String.t(),
          capacity_limit: pos_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.DeploymentSchedule do
  @moduledoc "One exact schedule cursor initialized for an immutable workspace deployment."
  @enforce_keys [
    :pipeline_target_id,
    :schedule_id,
    :schedule_fingerprint,
    :definition,
    :next_due_at,
    :cursor
  ]
  defstruct [
    :pipeline_target_id,
    :schedule_id,
    :schedule_fingerprint,
    :definition,
    :next_due_at,
    :cursor
  ]

  @type t :: %__MODULE__{
          pipeline_target_id: String.t(),
          schedule_id: String.t(),
          schedule_fingerprint: String.t(),
          definition: map(),
          next_due_at: DateTime.t(),
          cursor: map()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.ManifestSelector do
  @moduledoc "Typed selectors for immutable global manifest releases."

  defmodule ById do
    @moduledoc "Selects a manifest by operator-visible release ID."
    @enforce_keys [:manifest_version_id]
    defstruct [:manifest_version_id]
    @type t :: %__MODULE__{manifest_version_id: String.t()}
  end

  defmodule ByContentHash do
    @moduledoc "Selects a manifest by its canonical content hash."
    @enforce_keys [:content_hash]
    defstruct [:content_hash]
    @type t :: %__MODULE__{content_hash: String.t()}
  end

  @type t :: ById.t() | ByContentHash.t()
end

defmodule FavnOrchestrator.Persistence.Queries.GetRuntimeState do
  @moduledoc "Fetches the active immutable deployment for one workspace."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context]
  defstruct [:workspace_context]
  @type t :: %__MODULE__{workspace_context: WorkspaceContext.t()}
end

defmodule FavnOrchestrator.Persistence.Queries.MissingExecutionPackageHashes do
  @moduledoc "Finds content hashes that are not present in the execution-package registry."

  alias FavnOrchestrator.Persistence.PlatformContext

  @enforce_keys [:platform_context, :hashes]
  defstruct [:platform_context, hashes: []]

  @type t :: %__MODULE__{platform_context: PlatformContext.t(), hashes: [String.t()]}
end

defmodule FavnOrchestrator.Persistence.Queries.GetExecutionPackage do
  @moduledoc "Fetches one execution package for an authorized workspace runtime."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :content_hash]
  defstruct [:workspace_context, :content_hash]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          content_hash: String.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetDeploymentTargets do
  @moduledoc "Fetches the exact immutable target catalog for one workspace deployment."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :deployment_id]
  defstruct [:workspace_context, :deployment_id, customer_visible_only: false]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          deployment_id: String.t(),
          customer_visible_only: boolean()
        }
end

defmodule FavnOrchestrator.Persistence.Results.RuntimeState do
  @moduledoc "Committed active deployment state for one workspace."

  @enforce_keys [:workspace_id, :deployment_id, :manifest_version_id, :revision]
  defstruct [:workspace_id, :deployment_id, :manifest_version_id, :revision, :activated_at]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          deployment_id: String.t(),
          manifest_version_id: String.t(),
          revision: non_neg_integer(),
          activated_at: DateTime.t() | nil
        }
end
