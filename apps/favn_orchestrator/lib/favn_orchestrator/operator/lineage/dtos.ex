defmodule FavnOrchestrator.Operator.Lineage.Error do
  @moduledoc """
  Normalized error returned by the operator lineage facade.
  """

  @type code ::
          :active_manifest_not_found
          | :manifest_not_found
          | :invalid_scope
          | :node_not_found
          | :query_timeout
          | :storage_unavailable
          | :lineage_projection_unavailable

  @type t :: %__MODULE__{
          code: code(),
          message: String.t(),
          retryable?: boolean(),
          details: map()
        }

  @enforce_keys [:code, :message]
  defstruct [:code, :message, retryable?: false, details: %{}]
end

defmodule FavnOrchestrator.Operator.Lineage.Limits do
  @moduledoc """
  Bounded payload limits for lineage graph and inspector requests.
  """

  @type t :: %__MODULE__{
          max_visible_groups: pos_integer(),
          max_preview_assets_per_group: pos_integer(),
          max_visible_asset_nodes: pos_integer(),
          max_visible_edges: pos_integer(),
          max_dependency_previews_per_edge: pos_integer(),
          max_inspector_adjacent_groups: pos_integer(),
          group_asset_page_size: pos_integer(),
          search_page_size: pos_integer(),
          timeout_ms: pos_integer()
        }

  defstruct max_visible_groups: 40,
            max_preview_assets_per_group: 4,
            max_visible_asset_nodes: 160,
            max_visible_edges: 300,
            max_dependency_previews_per_edge: 5,
            max_inspector_adjacent_groups: 12,
            group_asset_page_size: 50,
            search_page_size: 20,
            timeout_ms: 250
end

defmodule FavnOrchestrator.Operator.Lineage.Summary do
  @moduledoc """
  Aggregate lineage graph counts used by the operator UI.
  """

  @type status_counts :: %{
          fresh: non_neg_integer(),
          stale: non_neg_integer(),
          failed: non_neg_integer(),
          running: non_neg_integer(),
          unknown: non_neg_integer()
        }

  @type t :: %__MODULE__{
          total_assets: non_neg_integer(),
          visible_assets: non_neg_integer(),
          total_groups: non_neg_integer(),
          visible_groups: non_neg_integer(),
          total_edges: non_neg_integer(),
          visible_edges: non_neg_integer(),
          status_counts: status_counts(),
          truncated?: boolean()
        }

  defstruct total_assets: 0,
            visible_assets: 0,
            total_groups: 0,
            visible_groups: 0,
            total_edges: 0,
            visible_edges: 0,
            status_counts: %{fresh: 0, stale: 0, failed: 0, running: 0, unknown: 0},
            truncated?: false
end

defmodule FavnOrchestrator.Operator.Lineage.AssetNode do
  @moduledoc """
  Operator-facing asset node or bounded asset preview.
  """

  @type t :: %__MODULE__{
          id: String.t(),
          label: String.t(),
          asset_ref: Favn.Ref.t(),
          asset_ref_text: String.t(),
          group_id: String.t(),
          schema: String.t() | nil,
          layer: atom(),
          kind: atom(),
          freshness_status: atom(),
          run_status: atom() | nil,
          latest_run_id: String.t() | nil,
          selected?: boolean(),
          position_hint: map() | nil
        }

  @enforce_keys [:id, :label, :asset_ref, :asset_ref_text, :group_id, :layer, :kind]
  defstruct [
    :id,
    :label,
    :asset_ref,
    :asset_ref_text,
    :group_id,
    :schema,
    :layer,
    :kind,
    :latest_run_id,
    :position_hint,
    freshness_status: :unknown,
    run_status: nil,
    selected?: false
  ]
end

defmodule FavnOrchestrator.Operator.Lineage.GroupNode do
  @moduledoc """
  Grouped lineage node rendered as the default bounded graph unit.
  """

  alias FavnOrchestrator.Operator.Lineage.AssetNode
  alias FavnOrchestrator.Operator.Lineage.Summary

  @type state :: :collapsed | :expanded_preview | :expanded_full
  @type group_type :: :source_system | :schema | :domain | :dashboard | :asset_group

  @type t :: %__MODULE__{
          id: String.t(),
          label: String.t(),
          system: String.t() | nil,
          schema: String.t() | nil,
          layer: atom(),
          type: group_type(),
          state: state(),
          asset_count: non_neg_integer(),
          preview_asset_ids: [String.t()],
          preview_assets: [AssetNode.t()],
          hidden_asset_count: non_neg_integer(),
          status_counts: Summary.status_counts(),
          top_issues: [map()],
          position_hint: map(),
          selected?: boolean()
        }

  @enforce_keys [:id, :label, :layer, :type, :position_hint]
  defstruct [
    :id,
    :label,
    :system,
    :schema,
    :layer,
    :type,
    :position_hint,
    state: :collapsed,
    asset_count: 0,
    preview_asset_ids: [],
    preview_assets: [],
    hidden_asset_count: 0,
    status_counts: %{fresh: 0, stale: 0, failed: 0, running: 0, unknown: 0},
    top_issues: [],
    selected?: false
  ]
end

defmodule FavnOrchestrator.Operator.Lineage.Edge do
  @moduledoc """
  Aggregated or concrete dependency edge in the lineage graph.
  """

  @type t :: %__MODULE__{
          id: String.t(),
          from: String.t(),
          to: String.t(),
          kind: atom(),
          dependency_count: pos_integer(),
          status: atom(),
          aggregated?: boolean(),
          preview_dependencies: [map()],
          hidden_dependency_count: non_neg_integer(),
          selected?: boolean()
        }

  @enforce_keys [:id, :from, :to]
  defstruct [
    :id,
    :from,
    :to,
    kind: :dependency,
    dependency_count: 1,
    status: :healthy,
    aggregated?: true,
    preview_dependencies: [],
    hidden_dependency_count: 0,
    selected?: false
  ]
end

defmodule FavnOrchestrator.Operator.Lineage.Graph do
  @moduledoc """
  Bounded lineage graph read model for operator pages.
  """

  alias FavnOrchestrator.Operator.Lineage.Edge
  alias FavnOrchestrator.Operator.Lineage.GroupNode
  alias FavnOrchestrator.Operator.Lineage.Limits
  alias FavnOrchestrator.Operator.Lineage.Summary

  @type view_mode :: :all
  @type scope :: :global

  @type t :: %__MODULE__{
          manifest_version_id: String.t(),
          scope: scope(),
          selected_id: String.t() | nil,
          view_mode: view_mode(),
          nodes: [GroupNode.t() | FavnOrchestrator.Operator.Lineage.AssetNode.t()],
          edges: [Edge.t()],
          groups: [GroupNode.t()],
          summary: Summary.t(),
          limits: Limits.t(),
          layout: map(),
          generated_at: DateTime.t()
        }

  @enforce_keys [:manifest_version_id, :scope, :view_mode, :summary, :limits, :generated_at]
  defstruct [
    :manifest_version_id,
    :scope,
    :selected_id,
    :summary,
    :limits,
    :generated_at,
    view_mode: :all,
    nodes: [],
    edges: [],
    groups: [],
    layout: %{direction: :left_to_right, layers: [:raw, :staging, :core, :marts, :dashboards]}
  ]
end

defmodule FavnOrchestrator.Operator.Lineage.GroupInspector do
  @moduledoc """
  Inspector read model for a selected lineage group.
  """

  alias FavnOrchestrator.Operator.Lineage.GroupNode

  @type t :: %__MODULE__{
          id: String.t(),
          title: String.t(),
          group: GroupNode.t(),
          about: map(),
          health_summary: map(),
          top_issues: [map()],
          upstream: [map()],
          downstream: [map()],
          hidden_upstream_count: non_neg_integer(),
          hidden_downstream_count: non_neg_integer(),
          actions: [map()]
        }

  @enforce_keys [:id, :title, :group]
  defstruct [
    :id,
    :title,
    :group,
    about: %{},
    health_summary: %{},
    top_issues: [],
    upstream: [],
    downstream: [],
    hidden_upstream_count: 0,
    hidden_downstream_count: 0,
    actions: []
  ]
end

defmodule FavnOrchestrator.Operator.Lineage.AssetInspector do
  @moduledoc """
  Inspector read model for a selected lineage asset.
  """

  alias FavnOrchestrator.Operator.Lineage.AssetNode

  @type t :: %__MODULE__{
          id: String.t(),
          title: String.t(),
          asset: AssetNode.t(),
          latest_run: map() | nil,
          upstream: [map()],
          downstream: [map()],
          hidden_upstream_count: non_neg_integer(),
          hidden_downstream_count: non_neg_integer(),
          actions: [map()]
        }

  @enforce_keys [:id, :title, :asset]
  defstruct [
    :id,
    :title,
    :asset,
    :latest_run,
    upstream: [],
    downstream: [],
    hidden_upstream_count: 0,
    hidden_downstream_count: 0,
    actions: []
  ]
end

defmodule FavnOrchestrator.Operator.Lineage.EdgeInspector do
  @moduledoc """
  Inspector read model for an aggregated dependency edge.
  """

  alias FavnOrchestrator.Operator.Lineage.Edge

  @type t :: %__MODULE__{
          id: String.t(),
          title: String.t(),
          edge: Edge.t(),
          upstream_label: String.t() | nil,
          downstream_label: String.t() | nil,
          affected_statuses: map(),
          dependencies: [map()],
          actions: [map()]
        }

  @enforce_keys [:id, :title, :edge]
  defstruct [
    :id,
    :title,
    :edge,
    :upstream_label,
    :downstream_label,
    affected_statuses: %{},
    dependencies: [],
    actions: []
  ]
end

defmodule FavnOrchestrator.Operator.Lineage.SearchResult do
  @moduledoc """
  Search result returned by the lineage operator facade.
  """

  @type kind :: :group | :asset | :schema

  @type t :: %__MODULE__{
          id: String.t(),
          kind: kind(),
          label: String.t(),
          subtitle: String.t() | nil,
          status: atom() | nil
        }

  @enforce_keys [:id, :kind, :label]
  defstruct [:id, :kind, :label, :subtitle, :status]
end
