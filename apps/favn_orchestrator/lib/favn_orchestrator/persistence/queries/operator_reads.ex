defmodule FavnOrchestrator.Persistence.Queries.PageManifests do
  @moduledoc "Keyset-pages immutable manifest releases for an authorized platform operator."

  alias FavnOrchestrator.Persistence.PlatformContext
  @enforce_keys [:platform_context]
  defstruct [:platform_context, :after, limit: 100]

  @type t :: %__MODULE__{
          platform_context: PlatformContext.t(),
          after: %{inserted_at: DateTime.t(), manifest_version_id: String.t()} | nil,
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Queries.PageExecutionGroups do
  @moduledoc "Keyset-pages compact execution-group overviews under an explicit scope."

  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:scope]
  defstruct [:scope, :status, :after, limit: 100]

  @type t :: %__MODULE__{
          scope: WorkspaceContext.t() | PlatformContext.t(),
          status: atom() | nil,
          after:
            %{
              latest_event_id: pos_integer(),
              workspace_id: String.t(),
              root_run_id: String.t()
            }
            | nil,
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetExecutionGroup do
  @moduledoc "Fetches one overview and independent first pages of its bounded details."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :root_run_id]
  defstruct [:workspace_context, :root_run_id, detail_limit: 50]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          root_run_id: String.t(),
          detail_limit: 1..200
        }
end

defmodule FavnOrchestrator.Persistence.Queries.PageGroupRuns do
  @moduledoc "Keyset-pages canonical run summaries in one execution group."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :root_run_id]
  defstruct [:workspace_context, :root_run_id, :after, limit: 100]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          root_run_id: String.t(),
          after: %{submitted_event_id: pos_integer(), run_id: String.t()} | nil,
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Queries.PageGroupWindows do
  @moduledoc "Keyset-pages backfill windows associated with one execution group."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :root_run_id]
  defstruct [:workspace_context, :root_run_id, :after, limit: 100]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          root_run_id: String.t(),
          after: %{window_start: DateTime.t(), window_id: String.t()} | nil,
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetTargetStatuses do
  @moduledoc "Batch-fetches exact target status identities for one manifest and kind."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :manifest_version_id, :target_kind, :target_ids]
  defstruct [:workspace_context, :manifest_version_id, :target_kind, :target_ids]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          manifest_version_id: String.t(),
          target_kind: :asset | :pipeline,
          target_ids: [String.t()]
        }
end

defmodule FavnOrchestrator.Persistence.Queries.PageTargetRuns do
  @moduledoc "Keyset-pages target run history in submission order."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :deployment_id, :target_kind, :target_id]
  defstruct [
    :workspace_context,
    :deployment_id,
    :target_kind,
    :target_id,
    :after,
    limit: 100
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          deployment_id: String.t(),
          target_kind: :asset | :pipeline,
          target_id: String.t(),
          after: %{submitted_event_id: pos_integer(), run_id: String.t()} | nil,
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Queries.FreshnessIdentity do
  @moduledoc "One exact asset freshness read-model identity."

  @enforce_keys [:deployment_id, :target_id, :freshness_key]
  defstruct [:deployment_id, :target_id, :freshness_key]

  @type t :: %__MODULE__{
          deployment_id: String.t(),
          target_id: String.t(),
          freshness_key: String.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetFreshnessMany do
  @moduledoc "Batch-fetches exact asset freshness identities without a table scan."

  alias FavnOrchestrator.Persistence.Queries.FreshnessIdentity
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :identities]
  defstruct [:workspace_context, :identities]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          identities: [FreshnessIdentity.t()]
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetAssetDetailState do
  @moduledoc "Fetches bounded freshness and window projections for one deployed asset."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :deployment_id, :manifest_version_id, :target_id]
  defstruct [:workspace_context, :deployment_id, :manifest_version_id, :target_id, limit: 200]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          deployment_id: String.t(),
          manifest_version_id: String.t(),
          target_id: String.t(),
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Results.ManifestSummary do
  @moduledoc "Compact immutable manifest release metadata."
  @enforce_keys [:manifest_version_id, :content_hash, :inserted_at]
  defstruct [
    :manifest_version_id,
    :content_hash,
    :schema_version,
    :runner_contract_version,
    :inserted_at
  ]

  @type t :: %__MODULE__{
          manifest_version_id: String.t(),
          content_hash: String.t(),
          schema_version: pos_integer(),
          runner_contract_version: pos_integer(),
          inserted_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.ExecutionGroupOverview do
  @moduledoc "Compact bounded execution-group projection."
  @enforce_keys [:workspace_id, :root_run_id, :status, :run_count, :latest_event_id]
  defstruct [
    :workspace_id,
    :root_run_id,
    :status,
    :run_count,
    :pending_count,
    :running_count,
    :succeeded_count,
    :failed_count,
    :latest_event_id,
    :source_publication_id,
    :updated_at
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          root_run_id: String.t(),
          status: atom(),
          run_count: non_neg_integer(),
          pending_count: non_neg_integer(),
          running_count: non_neg_integer(),
          succeeded_count: non_neg_integer(),
          failed_count: non_neg_integer(),
          latest_event_id: pos_integer(),
          source_publication_id: pos_integer(),
          updated_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunSummary do
  @moduledoc "Canonical run metadata and decoded snapshot for bounded operator pages."
  @enforce_keys [:workspace_id, :run_id, :root_run_id, :status, :submitted_event_id, :run]
  defstruct [
    :workspace_id,
    :run_id,
    :root_run_id,
    :parent_run_id,
    :deployment_id,
    :manifest_version_id,
    :status,
    :submit_kind,
    :trigger_type,
    :submitted_event_id,
    :latest_event_id,
    :inserted_at,
    :updated_at,
    :run
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          run_id: String.t(),
          root_run_id: String.t(),
          parent_run_id: String.t() | nil,
          deployment_id: String.t(),
          manifest_version_id: String.t(),
          status: atom(),
          submit_kind: atom(),
          trigger_type: atom(),
          submitted_event_id: pos_integer(),
          latest_event_id: pos_integer(),
          inserted_at: DateTime.t(),
          updated_at: DateTime.t(),
          run: FavnOrchestrator.RunState.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.ExecutionGroup do
  @moduledoc "One execution-group overview with independently pageable detail slices."

  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.ExecutionGroupOverview

  @enforce_keys [:overview, :runs, :windows, :failures]
  defstruct [:overview, :runs, :windows, :failures]

  @type t :: %__MODULE__{
          overview: ExecutionGroupOverview.t(),
          runs: CursorPage.t(),
          windows: CursorPage.t(),
          failures: CursorPage.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.TargetStatus do
  @moduledoc "Current projected target status guarded by publication order."
  @enforce_keys [:workspace_id, :deployment_id, :target_kind, :target_id, :status]
  defstruct [
    :workspace_id,
    :deployment_id,
    :target_kind,
    :target_id,
    :status,
    :run_id,
    :event_id,
    :source_publication_id,
    :updated_at
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          deployment_id: String.t(),
          target_kind: :asset | :pipeline,
          target_id: String.t(),
          status: atom(),
          run_id: String.t() | nil,
          event_id: pos_integer() | nil,
          source_publication_id: pos_integer(),
          updated_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.FreshnessState do
  @moduledoc "Projected exact asset freshness state."
  @enforce_keys [:workspace_id, :deployment_id, :target_id, :freshness_key, :status]
  defstruct [
    :workspace_id,
    :deployment_id,
    :target_id,
    :freshness_key,
    :latest_attempt_materialization_id,
    :latest_success_materialization_id,
    :status,
    :payload,
    :source_publication_id,
    :updated_at
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          deployment_id: String.t(),
          target_id: String.t(),
          freshness_key: String.t(),
          latest_attempt_materialization_id: String.t() | nil,
          latest_success_materialization_id: String.t() | nil,
          status: atom(),
          payload: map(),
          source_publication_id: pos_integer(),
          updated_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.AssetWindowState do
  @moduledoc "Projected exact asset window state."
  @enforce_keys [:workspace_id, :manifest_version_id, :target_id, :window_key, :status]
  defstruct [
    :workspace_id,
    :manifest_version_id,
    :target_id,
    :window_key,
    :window_start,
    :window_end,
    :status,
    :run_id,
    :materialization_id,
    :payload,
    :source_publication_id,
    :updated_at
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          manifest_version_id: String.t(),
          target_id: String.t(),
          window_key: String.t(),
          window_start: DateTime.t(),
          window_end: DateTime.t(),
          status: atom(),
          run_id: String.t() | nil,
          materialization_id: String.t() | nil,
          payload: map(),
          source_publication_id: pos_integer(),
          updated_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.AssetDetailState do
  @moduledoc "Bounded persisted state needed to render one asset detail."
  @enforce_keys [:freshness_states, :window_states]
  defstruct [:freshness_states, :window_states]

  @type t :: %__MODULE__{
          freshness_states: [FavnOrchestrator.Persistence.Results.FreshnessState.t()],
          window_states: [FavnOrchestrator.Persistence.Results.AssetWindowState.t()]
        }
end
