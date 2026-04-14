defmodule Favn.Run do
  @moduledoc """
  Canonical in-memory representation of one Favn run.

  The runtime engine projects coordinator-owned runtime state into this public
  struct for callers and storage adapters.
  """

  alias Favn.Ref
  alias Favn.Run.AssetResult

  @type status :: :queued | :running | :ok | :error | :cancelled | :timed_out

  @type t :: %__MODULE__{
          id: String.t(),
          target_refs: [Ref.t()],
          plan: Favn.Plan.t() | nil,
          pipeline: map() | nil,
          pipeline_context: map() | nil,
          submit_kind: :asset | :pipeline | :backfill_asset | :backfill_pipeline | :rerun,
          submit_ref: term() | nil,
          max_concurrency: pos_integer(),
          timeout_ms: pos_integer() | nil,
          status: status(),
          event_seq: non_neg_integer(),
          queued_at: DateTime.t() | nil,
          admitted_at: DateTime.t() | nil,
          queue_seq: pos_integer() | nil,
          started_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          params: map(),
          retry_policy: map(),
          replay_mode: :none | :resume_from_failure | :exact_replay,
          backfill: map() | nil,
          rerun_of_run_id: String.t() | nil,
          parent_run_id: String.t() | nil,
          root_run_id: String.t() | nil,
          lineage_depth: non_neg_integer(),
          operator_reason: term() | nil,
          asset_results: %{Ref.t() => AssetResult.t()},
          node_results: %{Favn.Plan.node_key() => AssetResult.t()},
          error: term() | nil,
          terminal_reason: map() | nil
        }

  defstruct [
    :id,
    :target_refs,
    :plan,
    :queued_at,
    :admitted_at,
    :queue_seq,
    :started_at,
    status: :running,
    event_seq: 0,
    finished_at: nil,
    params: %{},
    retry_policy: %{},
    max_concurrency: 1,
    timeout_ms: nil,
    asset_results: %{},
    node_results: %{},
    error: nil,
    terminal_reason: nil,
    pipeline: nil,
    pipeline_context: nil,
    submit_kind: :asset,
    submit_ref: nil,
    replay_mode: :none,
    backfill: nil,
    rerun_of_run_id: nil,
    parent_run_id: nil,
    root_run_id: nil,
    lineage_depth: 0,
    operator_reason: nil
  ]
end
