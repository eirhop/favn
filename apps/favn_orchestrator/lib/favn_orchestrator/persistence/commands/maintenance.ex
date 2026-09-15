defmodule FavnOrchestrator.Persistence.Commands.BackfillMissingProjection do
  @moduledoc "Starts or resumes a bounded replay that fills missing projection rows."

  alias FavnOrchestrator.Persistence.PlatformContext
  @enforce_keys [:platform_context, :job_id, :projection, :workspace_id]
  defstruct [:platform_context, :job_id, :projection, :workspace_id, limit: 100]

  @type t :: %__MODULE__{
          platform_context: PlatformContext.t(),
          job_id: String.t(),
          projection:
            :execution_groups | :backfills | :target_statuses | :asset_attempts | :freshness,
          workspace_id: String.t(),
          limit: 1..250
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ReconcilePersistence do
  @moduledoc "Checks and optionally repairs one named authoritative invariant in bounded scope."

  alias FavnOrchestrator.Persistence.PlatformContext
  @enforce_keys [:platform_context, :job_id, :invariant]
  defstruct [:platform_context, :job_id, :invariant, :workspace_id, repair?: false, limit: 100]

  @type t :: %__MODULE__{
          platform_context: PlatformContext.t(),
          job_id: String.t(),
          invariant: :capacity_counters,
          workspace_id: String.t() | nil,
          repair?: boolean(),
          limit: 1..1_000
        }
end

defmodule FavnOrchestrator.Persistence.Results.MaintenanceOutcome do
  @moduledoc "One observable bounded maintenance job outcome."

  @enforce_keys [:job_id, :status, :processed_count, :batch_count]
  defstruct [:job_id, :status, :processed_count, :batch_count, :cursor, :details]

  @type t :: %__MODULE__{
          job_id: String.t(),
          status: :running | :completed | :failed,
          processed_count: non_neg_integer(),
          batch_count: non_neg_integer(),
          cursor: map() | nil,
          details: map() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RetentionBatch do
  @moduledoc "Executes at most one retention transaction at an expected job version."
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Retention.Policy
  @enforce_keys [:platform_context, :policy, :expected_version]
  defstruct [:platform_context, :policy, :expected_version, scheduled?: false]

  @type t :: %__MODULE__{
          platform_context: PlatformContext.t(),
          policy: Policy.t(),
          expected_version: non_neg_integer(),
          scheduled?: boolean()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ConfigureRetention do
  @moduledoc "Changes the effective platform retention policy under the batch lock."
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Retention.Policy
  @enforce_keys [:platform_context, :policy, :expected_version]
  defstruct [:platform_context, :policy, :expected_version]

  @type t :: %__MODULE__{
          platform_context: PlatformContext.t(),
          policy: Policy.t(),
          expected_version: non_neg_integer()
        }
end
