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
  @moduledoc """
  Keyset-pages compact execution-group overviews under an explicit scope.

  Every field but `scope` narrows the page, and they compose: a query carrying a
  status, a search term, and a start window returns only groups that satisfy all
  three. Narrowing has to happen here rather than in the caller, because a page
  filtered after the fact reports the size of the page instead of the size of the
  answer.

  Groups are ordered by when their root run started, which is the question a runs
  list is asked, and `started_after`/`started_before` bound that same instant. The
  cursor follows the same key, so ordering, filtering, and paging cannot disagree
  about what "recent" means.

  `search` matches the group's root run id and the module or name of any target
  any run in the group declared. It does not reach physical target coordinates —
  connection, catalogue, or schema — because those are not part of this read
  model.
  """

  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:scope]
  defstruct [
    :scope,
    :status,
    :search,
    :trigger_type,
    :started_after,
    :started_before,
    :after,
    order: :started_desc,
    limit: 100
  ]

  @type t :: %__MODULE__{
          scope: WorkspaceContext.t() | PlatformContext.t(),
          status: atom() | [atom()] | nil,
          search: String.t() | nil,
          trigger_type: atom() | nil,
          started_after: DateTime.t() | nil,
          started_before: DateTime.t() | nil,
          after:
            %{
              started_at: DateTime.t(),
              workspace_id: String.t(),
              root_run_id: String.t()
            }
            | nil,
          order: :started_desc | :started_asc,
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Queries.CountExecutionGroups do
  @moduledoc """
  Counts execution groups per status, in one pass over one narrowed scope.

  A runs list has to say how much is running, how much failed, and how much
  succeeded before the operator has clicked anything, and a count taken from the
  first page of a list is only ever the count of that page.

  Every field except the status narrows the same way `PageExecutionGroups` does,
  because a count offered next to a filter has to be the size of what that filter
  would return. Only the status axis is left open, and the result reports one
  count per value it could take.
  """

  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:scope]
  defstruct [:scope, :search, :trigger_type, :started_after, :started_before]

  @type t :: %__MODULE__{
          scope: WorkspaceContext.t() | PlatformContext.t(),
          search: String.t() | nil,
          trigger_type: atom() | nil,
          started_after: DateTime.t() | nil,
          started_before: DateTime.t() | nil
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

defmodule FavnOrchestrator.Persistence.Queries.GetOperatorRunOverview do
  @moduledoc """
  Fetches a compact run overview without snapshots, plans, or event payloads.
  """

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :run_id]
  defstruct [:workspace_context, :run_id, limit: 200]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          run_id: String.t(),
          limit: 1..500
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

  @enforce_keys [:evidence_generation_id, :target_id, :freshness_key]
  defstruct [:evidence_generation_id, :target_id, :freshness_key]

  @type t :: %__MODULE__{
          evidence_generation_id: String.t(),
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

defmodule FavnOrchestrator.Persistence.Queries.GetAssetWindowStates do
  @moduledoc "Fetches bounded window projections for one deployed asset."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :evidence_generation_id, :target_id]
  defstruct [:workspace_context, :evidence_generation_id, :target_id, limit: 200]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          evidence_generation_id: String.t(),
          target_id: String.t(),
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Queries.CountSuccessfulAssetWindows do
  @moduledoc "Counts successful windows in one exact evidence-generation range."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :evidence_generation_id,
    :target_id,
    :first_window_start,
    :last_window_start
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          evidence_generation_id: String.t(),
          target_id: String.t(),
          first_window_start: DateTime.t(),
          last_window_start: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetSuccessfulAssetWindowKeys do
  @moduledoc "Batch-fetches successful keys for one exact evidence generation."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :evidence_generation_id, :target_id, :window_keys]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          evidence_generation_id: String.t(),
          target_id: String.t(),
          window_keys: [String.t()]
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
    :runner_releases,
    :inserted_at
  ]

  @type t :: %__MODULE__{
          manifest_version_id: String.t(),
          content_hash: String.t(),
          schema_version: pos_integer(),
          runner_contract_version: pos_integer(),
          runner_releases: Favn.RunnerPool.releases(),
          inserted_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.ExecutionGroupOverview do
  @moduledoc """
  Compact bounded execution-group projection.

  The counts come from the group's own row. Identity and timing — what the run
  was for, what triggered it, and how long it took — come from the group's root
  run, so a store that can resolve them fills them and one that cannot leaves
  them `nil`. A list of runs that cannot say what each run targeted is not a
  list of runs, which is why these are part of the projection rather than
  something the caller fetches per row.

  `target_refs` and `pipeline_refs` are separate because a pipeline run declares
  every asset it will touch, and fourteen asset refs do not tell an operator what
  was submitted. The pipeline does.

  `asset_counts` is how much of the work actually happened: one count per asset
  step the group has recorded, across every run in it. The group's own counters
  count *runs*, which for a backfill is its windows and for everything else is
  one — a number that answers nothing.
  """
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
    :updated_at,
    :trigger_type,
    :started_at,
    :finished_at,
    target_refs: [],
    pipeline_refs: [],
    asset_counts: %{total: 0, completed: 0, failed: 0, running: 0, queued: 0}
  ]

  @type asset_counts :: %{
          total: non_neg_integer(),
          completed: non_neg_integer(),
          failed: non_neg_integer(),
          running: non_neg_integer(),
          queued: non_neg_integer()
        }

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
          updated_at: DateTime.t(),
          trigger_type: atom() | nil,
          started_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          target_refs: [String.t()],
          pipeline_refs: [String.t()],
          asset_counts: asset_counts()
        }
end

defmodule FavnOrchestrator.Persistence.Results.ExecutionGroupCounts do
  @moduledoc """
  How many execution groups the queried scope holds, per status.

  `active` counts pending and running together, because a queued run is one the
  operator is waiting on. `total` is every status, including the ones with no
  count of their own.
  """
  @enforce_keys [:active, :failed, :succeeded, :total]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          active: non_neg_integer(),
          failed: non_neg_integer(),
          succeeded: non_neg_integer(),
          total: non_neg_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunSummary do
  @moduledoc "Compact relational run history row; it never contains a run snapshot."
  @enforce_keys [:workspace_id, :run_id, :status, :event_sequence, :inserted_at]
  defstruct [
    :workspace_id,
    :run_id,
    :root_run_id,
    :parent_run_id,
    :deployment_id,
    :manifest_version_id,
    :runner_releases,
    :status,
    :submit_kind,
    :trigger_type,
    :submitted_event_id,
    :latest_event_id,
    :event_sequence,
    :inserted_at,
    :updated_at,
    :terminal_at,
    :rerun_of_run_id,
    :target_label,
    target_refs: []
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          run_id: String.t(),
          root_run_id: String.t() | nil,
          parent_run_id: String.t() | nil,
          deployment_id: String.t(),
          manifest_version_id: String.t(),
          runner_releases: Favn.RunnerPool.releases(),
          status: atom(),
          submit_kind: atom(),
          trigger_type: atom(),
          submitted_event_id: pos_integer(),
          latest_event_id: pos_integer(),
          event_sequence: pos_integer(),
          inserted_at: DateTime.t(),
          updated_at: DateTime.t(),
          terminal_at: DateTime.t() | nil,
          rerun_of_run_id: String.t() | nil,
          target_label: String.t() | nil,
          target_refs: [String.t()]
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

defmodule FavnOrchestrator.Persistence.Results.AssetAttemptOverview do
  @moduledoc "Compact projected asset attempt with one concrete runtime window identity."

  @enforce_keys [
    :workspace_id,
    :root_run_id,
    :run_id,
    :asset_step_id,
    :asset_ref,
    :window_identity,
    :status
  ]
  defstruct [
    :workspace_id,
    :root_run_id,
    :run_id,
    :asset_step_id,
    :asset_ref,
    :window_identity,
    :window,
    :status,
    :stage,
    :attempt_number,
    :execution_pool,
    :queue_reason,
    :started_at,
    :finished_at,
    :duration_ms,
    :error,
    :output_metadata,
    :source_publication_id,
    :updated_at
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          root_run_id: String.t(),
          run_id: String.t(),
          asset_step_id: String.t(),
          asset_ref: String.t(),
          window_identity: String.t(),
          window: map() | nil,
          status: atom(),
          stage: non_neg_integer() | nil,
          attempt_number: pos_integer() | nil,
          execution_pool: String.t() | nil,
          queue_reason: String.t() | nil,
          started_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          duration_ms: non_neg_integer() | nil,
          error: term(),
          output_metadata: map() | nil,
          source_publication_id: pos_integer(),
          updated_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.OperatorRunOverview do
  @moduledoc "Bounded compact slices required by the run Overview view."

  alias FavnOrchestrator.Persistence.Results.AssetAttemptOverview
  alias FavnOrchestrator.Persistence.Results.BackfillWindow
  alias FavnOrchestrator.Persistence.Results.ExecutionGroupOverview
  alias FavnOrchestrator.Persistence.Results.RunSummary

  @enforce_keys [
    :overview,
    :root_run,
    :runs,
    :requested_windows,
    :requested_windows_truncated?,
    :requested_window_counts,
    :attempts,
    :attempt_counts,
    :attempts_truncated?,
    :runs_truncated?,
    :target_refs
  ]
  defstruct [
    :overview,
    :root_run,
    :runs,
    :requested_windows,
    :requested_windows_truncated?,
    :requested_window_counts,
    :attempts,
    :attempt_counts,
    :attempts_truncated?,
    :runs_truncated?,
    :target_refs
  ]

  @type t :: %__MODULE__{
          overview: ExecutionGroupOverview.t(),
          root_run: RunSummary.t(),
          runs: [RunSummary.t()],
          requested_windows: [BackfillWindow.t()],
          requested_windows_truncated?: boolean(),
          requested_window_counts: %{
            required(:total) => non_neg_integer(),
            required(:completed) => non_neg_integer(),
            required(:failed) => non_neg_integer()
          },
          attempts: [AssetAttemptOverview.t()],
          attempt_counts: %{
            required(:total) => non_neg_integer(),
            required(:completed) => non_neg_integer(),
            required(:failed) => non_neg_integer(),
            required(:running) => non_neg_integer(),
            required(:queued) => non_neg_integer(),
            required(:effective_windows) => non_neg_integer()
          },
          attempts_truncated?: boolean(),
          runs_truncated?: boolean(),
          target_refs: [String.t()]
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
  @enforce_keys [
    :workspace_id,
    :evidence_generation_id,
    :deployment_id,
    :manifest_version_id,
    :target_id,
    :freshness_key,
    :status
  ]
  defstruct [
    :workspace_id,
    :evidence_generation_id,
    :deployment_id,
    :manifest_version_id,
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
          evidence_generation_id: String.t(),
          deployment_id: String.t(),
          manifest_version_id: String.t(),
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
  @enforce_keys [
    :workspace_id,
    :evidence_generation_id,
    :manifest_version_id,
    :target_id,
    :window_key,
    :status
  ]
  defstruct [
    :workspace_id,
    :evidence_generation_id,
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
          evidence_generation_id: String.t(),
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
