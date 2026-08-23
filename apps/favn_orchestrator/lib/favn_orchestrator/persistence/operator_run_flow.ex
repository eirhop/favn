defmodule FavnOrchestrator.Persistence.Queries.ResolveRunSubscription do
  @moduledoc "Resolves only the durable identities needed to authorize a run topic."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :run_id]
  defstruct @enforce_keys

  @type t :: %__MODULE__{workspace_context: WorkspaceContext.t(), run_id: String.t()}
end

defmodule FavnOrchestrator.Persistence.Results.RunSubscriptionIdentity do
  @moduledoc "Scalar run identities used by subscription authorization."

  @enforce_keys [:run_id, :root_run_id]
  defstruct @enforce_keys

  @type t :: %__MODULE__{run_id: String.t(), root_run_id: String.t()}
end

defmodule FavnOrchestrator.Persistence.Queries.GetRunFlowPage do
  @moduledoc "Reads one bounded, exact-run Flow page from the compact projection."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :run_id]
  defstruct [:workspace_context, :run_id, :asset_prefix, :after, :before, limit: 200]

  @type key :: %{asset_ref: String.t(), asset_step_id: String.t()}
  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          run_id: String.t(),
          asset_prefix: String.t() | nil,
          after: key() | nil,
          before: key() | nil,
          limit: 1..200
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetRunFlowDelta do
  @moduledoc "Reads changed summaries only for the Flow rows retained by one caller."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :run_id,
    :asset_step_ids,
    :asset_prefix,
    :after_publication_id,
    :through_publication_id
  ]
  defstruct @enforce_keys ++ [:after, limit: 200]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          run_id: String.t(),
          asset_step_ids: [String.t()],
          asset_prefix: String.t() | nil,
          after_publication_id: non_neg_integer(),
          through_publication_id: non_neg_integer(),
          after: %{source_publication_id: non_neg_integer(), asset_step_id: String.t()} | nil,
          limit: 1..200
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunFlowDelta do
  @moduledoc "One bounded retained-row delta under a frozen publication watermark."
  alias FavnOrchestrator.Persistence.Results.RunFlowHeader
  alias FavnOrchestrator.Persistence.Results.RunFlowStep
  @enforce_keys [:header, :items, :through_publication_id, :has_more?]
  defstruct @enforce_keys ++ [:next_cursor]

  @type t :: %__MODULE__{
          header: RunFlowHeader.t(),
          items: [RunFlowStep.t()],
          through_publication_id: non_neg_integer(),
          has_more?: boolean(),
          next_cursor: map() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetRunAssetAttempt do
  @moduledoc "Reads exactly one asset-step detail for an exact run."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :run_id, :asset_step_id]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          run_id: String.t(),
          asset_step_id: String.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunFlowStep do
  @moduledoc "Lean browser-safe Flow row; complete error and output payloads are excluded."

  @enforce_keys [:run_id, :asset_step_id, :asset_ref, :status, :source_publication_id]
  defstruct [
    :run_id,
    :asset_step_id,
    :target_id,
    :asset_ref,
    :status,
    :stage,
    :window_kind,
    :window_start_at,
    :window_end_at,
    :window_timezone,
    :started_at,
    :finished_at,
    :duration_ms,
    :attempt_number,
    :execution_pool,
    :queue_reason,
    :failure_summary,
    :source_publication_id
  ]

  @type t :: %__MODULE__{
          run_id: String.t(),
          asset_step_id: String.t(),
          target_id: String.t() | nil,
          asset_ref: String.t(),
          status: atom(),
          stage: non_neg_integer() | nil,
          window_kind: atom() | nil,
          window_start_at: DateTime.t() | nil,
          window_end_at: DateTime.t() | nil,
          window_timezone: String.t() | nil,
          started_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          duration_ms: non_neg_integer() | nil,
          attempt_number: pos_integer() | nil,
          execution_pool: String.t() | nil,
          queue_reason: String.t() | nil,
          failure_summary: String.t() | nil,
          source_publication_id: non_neg_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunFlowHeader do
  @moduledoc "Compact header and exact lifecycle counts for one selected run."

  @enforce_keys [
    :run_id,
    :root_run_id,
    :status,
    :trigger_type,
    :started_at,
    :updated_at,
    :counts,
    :filtered_total,
    :unfiltered_total,
    :projection_cursor
  ]
  defstruct @enforce_keys ++
              [
                :finished_at,
                :target_id,
                :target_label,
                :manifest_version_id,
                :parent_run_id,
                :rerun_of_run_id,
                :window_counts,
                :window_failure_total,
                :window_failures
              ]

  @type counts :: %{required(atom()) => non_neg_integer()}
  @type t :: %__MODULE__{
          run_id: String.t(),
          root_run_id: String.t(),
          status: atom(),
          trigger_type: atom() | nil,
          started_at: DateTime.t(),
          updated_at: DateTime.t(),
          finished_at: DateTime.t() | nil,
          target_id: String.t() | nil,
          target_label: String.t() | nil,
          manifest_version_id: String.t(),
          parent_run_id: String.t() | nil,
          rerun_of_run_id: String.t() | nil,
          counts: counts(),
          filtered_total: non_neg_integer(),
          unfiltered_total: non_neg_integer(),
          projection_cursor: non_neg_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunFlowPage do
  @moduledoc "One consistent Flow header and bounded exact-run step page."

  alias FavnOrchestrator.Persistence.Results.RunFlowHeader
  alias FavnOrchestrator.Persistence.Results.RunFlowStep

  @enforce_keys [:header, :items, :has_next?, :has_previous?]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          header: RunFlowHeader.t(),
          items: [RunFlowStep.t()],
          has_next?: boolean(),
          has_previous?: boolean()
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunAssetAttempt do
  @moduledoc "One keyed asset-step detail, including its bounded diagnostic payloads."

  alias FavnOrchestrator.Persistence.Results.RunFlowStep

  @enforce_keys [:summary]
  defstruct [:summary, :error, :output_metadata, :window]

  @type t :: %__MODULE__{
          summary: RunFlowStep.t(),
          error: term(),
          output_metadata: map() | nil,
          window: map() | nil
        }
end
