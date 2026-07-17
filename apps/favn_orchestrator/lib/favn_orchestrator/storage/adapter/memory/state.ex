defmodule FavnOrchestrator.Storage.Adapter.Memory.State do
  @moduledoc """
  Internal state owned by the in-memory storage process.

  The struct makes the adapter's storage contract explicit and prevents callbacks
  from silently adding misspelled state keys.
  """

  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Version
  alias Favn.RuntimeInput.Pin
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Backfill.Progress, as: BackfillProgress
  alias FavnOrchestrator.MaterializationClaim
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.TargetStatus

  @type t :: %__MODULE__{
          manifest_versions: %{optional(String.t()) => Version.t()},
          manifest_version_ids_by_content_hash: %{optional(String.t()) => String.t()},
          execution_packages: %{optional(String.t()) => ExecutionPackage.t()},
          manifest_package_hashes: %{optional(String.t()) => MapSet.t(String.t())},
          active_manifest_version_id: String.t() | nil,
          runs: %{optional(String.t()) => RunState.t()},
          runtime_input_pins: %{optional({String.t(), Favn.Plan.node_key()}) => Pin.t()},
          execution_group_run_ids: %{optional(String.t()) => MapSet.t(String.t())},
          execution_group_summaries: %{optional(String.t()) => map()},
          run_events: %{optional(String.t()) => [map()]},
          run_event_global_sequence: non_neg_integer(),
          execution_leases: %{optional(String.t()) => map()},
          execution_lease_ids_by_run: %{optional(String.t()) => MapSet.t(String.t())},
          execution_admission_waiters: %{optional(String.t()) => map()},
          execution_ownerships: %{optional(String.t()) => map()},
          materialization_claims: %{optional(String.t()) => MaterializationClaim.t()},
          log_entries: [Favn.Log.Entry.t()],
          log_entries_by_producer_sequence: %{
            optional({String.t(), non_neg_integer()}) => Favn.Log.Entry.t()
          },
          log_global_sequence: non_neg_integer(),
          scheduler_states: %{optional({module(), atom() | nil}) => map()},
          coverage_baselines: %{optional(String.t()) => CoverageBaseline.t()},
          backfill_windows: %{
            optional({String.t(), module(), String.t()}) => BackfillWindow.t()
          },
          backfill_window_keys_by_run: %{
            optional(String.t()) => MapSet.t({String.t(), module(), String.t()})
          },
          backfill_progress: %{optional(String.t()) => BackfillProgress.t()},
          asset_window_states: %{
            optional({module(), atom(), String.t()}) => AssetWindowState.t()
          },
          asset_freshness_states: %{
            optional({module(), atom(), String.t()}) => AssetFreshnessState.t()
          },
          target_statuses: %{
            optional({String.t(), TargetStatus.target_kind(), String.t()}) => TargetStatus.t()
          },
          auth_actors: %{optional(String.t()) => map()},
          auth_usernames: %{optional(String.t()) => String.t()},
          auth_credentials: %{optional(String.t()) => map()},
          auth_sessions: %{optional(String.t()) => map()},
          auth_session_hashes: %{optional(String.t()) => String.t()},
          auth_audits: [map()],
          idempotency_records: %{optional(String.t()) => map()}
        }

  defstruct manifest_versions: %{},
            manifest_version_ids_by_content_hash: %{},
            execution_packages: %{},
            manifest_package_hashes: %{},
            active_manifest_version_id: nil,
            runs: %{},
            runtime_input_pins: %{},
            execution_group_run_ids: %{},
            execution_group_summaries: %{},
            run_events: %{},
            run_event_global_sequence: 0,
            execution_leases: %{},
            execution_lease_ids_by_run: %{},
            execution_admission_waiters: %{},
            execution_ownerships: %{},
            materialization_claims: %{},
            log_entries: [],
            log_entries_by_producer_sequence: %{},
            log_global_sequence: 0,
            scheduler_states: %{},
            coverage_baselines: %{},
            backfill_windows: %{},
            backfill_window_keys_by_run: %{},
            backfill_progress: %{},
            asset_window_states: %{},
            asset_freshness_states: %{},
            target_statuses: %{},
            auth_actors: %{},
            auth_usernames: %{},
            auth_credentials: %{},
            auth_sessions: %{},
            auth_session_hashes: %{},
            auth_audits: [],
            idempotency_records: %{}

  @doc "Returns empty in-memory adapter state."
  @spec new() :: t()
  def new, do: %__MODULE__{}
end
