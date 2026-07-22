defmodule FavnOrchestrator.Persistence.Commands.EnsureWritableTargetGeneration do
  @moduledoc """
  Ensures one persisted SQL target has a generation that ordinary work may pin.

  An uninitialized target receives one reusable building generation. A target
  with an active generation returns that generation. Blocked compatibility
  states are never bypassed by this command.
  """

  alias Favn.Manifest.TargetDescriptor
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :target_id,
    :manifest_version_id,
    :descriptor,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :target_id,
    :manifest_version_id,
    :descriptor,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          target_id: String.t(),
          manifest_version_id: String.t(),
          descriptor: TargetDescriptor.t(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetTargetBinding do
  @moduledoc "Fetches one logical target's current generation binding."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :target_id]
  defstruct [:workspace_context, :target_id]

  @type t :: %__MODULE__{workspace_context: WorkspaceContext.t(), target_id: String.t()}
end

defmodule FavnOrchestrator.Persistence.Commands.ReconcileInitialTargetGeneration do
  @moduledoc """
  Activates an initial building generation after one exact successful write.

  `materialization_id` is authoritative control-plane evidence that the pinned
  generation completed successfully. The physical fingerprint is supplied by
  runner inspection. A generation-capable adapter initializes an exact
  sidecar marker before this command makes the binding active.
  """

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :target_id,
    :manifest_version_id,
    :target_generation_id,
    :materialization_id,
    :physical_schema_fingerprint,
    :data_plane_marker,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :target_id,
    :manifest_version_id,
    :target_generation_id,
    :materialization_id,
    :physical_schema_fingerprint,
    :data_plane_marker,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          target_id: String.t(),
          manifest_version_id: String.t(),
          target_generation_id: String.t(),
          materialization_id: String.t(),
          physical_schema_fingerprint: String.t(),
          data_plane_marker: map() | nil,
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetTargetBindings do
  @moduledoc "Batch-fetches current generation bindings for exact logical target ids."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :target_ids]
  defstruct [:workspace_context, :target_ids]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          target_ids: [String.t()]
        }
end

defmodule FavnOrchestrator.Persistence.Results.TargetBinding do
  @moduledoc "Current desired and active generation binding for one logical SQL target."

  @type compatibility_status ::
          :ready
          | :uninitialized
          | :rebuild_available
          | :rebuild_required
          | :unexpected_drift
          | :operator_decision

  @enforce_keys [
    :workspace_id,
    :target_id,
    :desired_manifest_id,
    :desired_descriptor_hash,
    :compatibility_status,
    :reason_code,
    :compatibility_diff,
    :version,
    :updated_at
  ]
  defstruct [
    :workspace_id,
    :target_id,
    :active_generation_id,
    :active_manifest_id,
    :active_descriptor_hash,
    :desired_manifest_id,
    :desired_descriptor_hash,
    :compatibility_status,
    :reason_code,
    :compatibility_diff,
    :active_physical_fingerprint,
    :version,
    :updated_at
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          target_id: String.t(),
          active_generation_id: String.t() | nil,
          active_manifest_id: String.t() | nil,
          active_descriptor_hash: String.t() | nil,
          desired_manifest_id: String.t(),
          desired_descriptor_hash: String.t(),
          compatibility_status: compatibility_status(),
          reason_code: String.t(),
          compatibility_diff: map(),
          active_physical_fingerprint: String.t() | nil,
          version: pos_integer(),
          updated_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.WritableTargetGeneration do
  @moduledoc "Generation and binding pinned for one ordinary persisted-target write."

  alias Favn.TargetGeneration
  alias FavnOrchestrator.Persistence.Results.TargetBinding

  @enforce_keys [:generation, :binding]
  defstruct [:generation, :binding]

  @type t :: %__MODULE__{generation: TargetGeneration.t(), binding: TargetBinding.t()}
end

defmodule FavnOrchestrator.Persistence.Results.InitialTargetGenerationReconciliation do
  @moduledoc "Generation and binding atomically activated from successful materialization evidence."

  alias Favn.TargetGeneration
  alias FavnOrchestrator.Persistence.Results.TargetBinding

  @enforce_keys [:generation, :binding, :materialization_id]
  defstruct [:generation, :binding, :materialization_id]

  @type t :: %__MODULE__{
          generation: TargetGeneration.t(),
          binding: TargetBinding.t(),
          materialization_id: String.t()
        }
end
