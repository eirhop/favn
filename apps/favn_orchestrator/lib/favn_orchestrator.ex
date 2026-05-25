defmodule FavnOrchestrator do
  @moduledoc """
  Runtime orchestrator control-plane facade for manifest-pinned operations.

  `FavnOrchestrator` is the boundary used by runtime apps, operator tooling, and
  the thin `Favn` runtime helpers. Same-BEAM operator UI code uses the operator
  wrappers here, including run submission, backfill submission, and
  `cancel_operator_run/2`, so authz stays in the control plane. It is not the
  stable authoring-time API that most application code should build against.
  """

  alias Favn.Assets.Planner
  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RunnerClient
  alias Favn.Backfill.RangeRequest
  alias Favn.Manifest.Index
  alias Favn.Manifest.PipelineResolver
  alias Favn.Manifest.Version
  alias Favn.Window.Anchor
  alias Favn.Window.Policy
  alias Favn.Window.Request, as: WindowRequest
  alias Favn.Window.Runtime, as: RuntimeWindow
  alias Favn.Window.Spec, as: WindowSpec
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.Repair, as: BackfillRepair
  alias FavnOrchestrator.BackfillManager
  alias FavnOrchestrator.Diagnostics
  alias FavnOrchestrator.Events
  alias FavnOrchestrator.Freshness.Decider, as: FreshnessDecider
  alias FavnOrchestrator.Freshness.Query, as: FreshnessQuery
  alias FavnOrchestrator.LogWriter
  alias FavnOrchestrator.Logs
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.OperatorCommands.AssetBackfillRequest
  alias FavnOrchestrator.OperatorCommands.AssetRunRequest
  alias FavnOrchestrator.OperatorCommands.PipelineBackfillRequest
  alias FavnOrchestrator.OperatorCommands.PipelineRunRequest
  alias FavnOrchestrator.OperatorErrorDTO
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.RunReadModel
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunEvents.Query, as: RunEventQuery
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.Scheduler.ManifestEntries
  alias FavnOrchestrator.Scheduler.Runtime, as: SchedulerRuntime
  alias FavnOrchestrator.Scheduler.Cron
  alias FavnOrchestrator.ScheduleOccurrencePreview
  alias FavnOrchestrator.ScheduleListEntry
  alias FavnOrchestrator.SchedulerEntry
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.TargetStatus

  @type run_id :: String.t()
  @type operator_actor :: Auth.actor()
  @type operator_session :: Auth.session()
  @type operator_actor_context :: %{
          required(:actor) => operator_actor(),
          required(:session) => operator_session()
        }
  @type manifest_summary :: %{
          required(:manifest_version_id) => String.t(),
          required(:content_hash) => String.t(),
          required(:asset_count) => non_neg_integer(),
          required(:pipeline_count) => non_neg_integer(),
          required(:schedule_count) => non_neg_integer()
        }

  @type manifest_target_option :: %{
          required(:target_id) => String.t(),
          required(:label) => String.t(),
          optional(:asset_ref) => String.t(),
          optional(:type) => String.t(),
          optional(:relation) => map() | nil,
          optional(:metadata) => map(),
          optional(:runtime_config) => map(),
          optional(:depends_on) => [String.t()],
          optional(:materialization) => map() | nil,
          optional(:window) => map() | nil
        }

  @type manifest_targets :: %{
          required(:manifest_version_id) => String.t(),
          required(:assets) => [manifest_target_option()],
          required(:pipelines) => [manifest_target_option()]
        }

  @type operator_error_context ::
          :load
          | :schedule_occurrences
          | :schedule_activation
          | :run_cancel
          | :run_failure_detail

  @type asset_catalogue_entry :: %{
          required(:target_id) => String.t(),
          required(:label) => String.t(),
          optional(:asset_ref) => String.t(),
          optional(:type) => String.t(),
          optional(:relation) => map() | nil,
          optional(:metadata) => map(),
          required(:status) => :healthy | :running | :failed | :unknown,
          required(:latest_run_id) => String.t() | nil,
          required(:latest_run_status) => atom() | nil,
          required(:latest_run_at) => DateTime.t() | nil
        }

  @type pipeline_catalogue_entry :: %{
          required(:target_id) => String.t(),
          required(:label) => String.t(),
          required(:name) => String.t(),
          required(:selected_assets) => [String.t()],
          required(:dependencies) => :all | :none | :unknown,
          required(:window) => map() | nil,
          required(:can_run_without_window?) => boolean(),
          required(:can_backfill?) => boolean(),
          required(:status) => :healthy | :running | :failed | :unknown,
          required(:latest_run_id) => String.t() | nil,
          required(:latest_run_status) => atom() | nil,
          required(:latest_run_at) => DateTime.t() | nil,
          required(:latest_run_duration_ms) => non_neg_integer() | nil
        }

  @type pipeline_run_history_entry :: %{
          required(:id) => String.t(),
          required(:status) => atom(),
          required(:submit_kind) => atom() | nil,
          required(:started_at) => DateTime.t() | nil,
          required(:finished_at) => DateTime.t() | nil,
          required(:duration_ms) => non_neg_integer() | nil,
          required(:scope) => map() | nil,
          required(:window) => map() | String.t() | nil
        }

  @type pipeline_detail :: %{
          required(:target_id) => String.t(),
          required(:manifest_version_id) => String.t(),
          required(:label) => String.t(),
          required(:name) => String.t(),
          required(:selected_assets) => [String.t()],
          required(:dependencies) => :all | :none | :unknown,
          required(:window) => map() | nil,
          required(:can_run_without_window?) => boolean(),
          required(:can_backfill?) => boolean(),
          required(:status) => :healthy | :running | :failed | :unknown,
          required(:latest_run_id) => String.t() | nil,
          required(:latest_run_status) => atom() | nil,
          required(:latest_run_at) => DateTime.t() | nil,
          required(:latest_run_duration_ms) => non_neg_integer() | nil,
          required(:runs) => [pipeline_run_history_entry()]
        }

  @type asset_timeline_window :: %{
          required(:id) => String.t(),
          required(:kind) => :hour | :day | :month | :year,
          required(:value) => String.t(),
          required(:label) => String.t(),
          required(:date) => Date.t(),
          required(:range) => String.t(),
          required(:status) => :healthy | :running | :failed | :unknown,
          required(:latest_run_id) => String.t() | nil,
          required(:latest_run_status) => atom() | nil,
          required(:latest_run_at) => DateTime.t() | nil,
          required(:run_enabled?) => boolean(),
          required(:run_disabled_reason) => atom() | nil,
          required(:run_label) => String.t()
        }

  @type asset_detail :: %{
          required(:target_id) => String.t(),
          required(:manifest_version_id) => String.t(),
          required(:label) => String.t(),
          required(:name) => String.t(),
          required(:asset_ref) => String.t() | nil,
          required(:canonical_asset_ref) => Favn.Ref.t(),
          required(:relation) => map() | nil,
          required(:type) => String.t() | nil,
          required(:status) => :healthy | :running | :failed | :unknown,
          required(:latest_run_id) => String.t() | nil,
          required(:latest_run_status) => atom() | nil,
          required(:latest_run_at) => DateTime.t() | nil,
          required(:window) => map() | nil,
          required(:refresh_timeline) => [asset_timeline_window()],
          required(:data_coverage_timeline) => [asset_timeline_window()] | nil,
          required(:has_data_windows?) => boolean(),
          required(:can_run_asset?) => boolean(),
          required(:freshness) => asset_freshness_detail(),
          required(:timeline) => [asset_timeline_window()]
        }

  @type asset_freshness_reason :: %{
          required(:kind) => atom(),
          required(:message) => String.t(),
          optional(:upstream_ref) => String.t() | nil,
          optional(:previous_version) => String.t() | nil,
          optional(:current_version) => String.t() | nil,
          optional(:run_id) => String.t() | nil
        }

  @type asset_freshness_detail :: %{
          required(:state) => :fresh | :stale | :unknown | :always_run,
          required(:policy) => %{required(:kind) => atom(), required(:label) => String.t()},
          required(:latest_success) => map() | nil,
          required(:explanation) => String.t(),
          required(:reasons) => [asset_freshness_reason()]
        }

  @type run_summary :: RunReadModel.run_summary()
  @type run_detail :: RunReadModel.run_detail()
  @type execution_group_summary :: RunReadModel.execution_group_summary()
  @type execution_group_detail :: RunReadModel.execution_group_detail()
  @type operator_run_detail :: RunReadModel.operator_run_detail()
  @type schedule_list_entry :: ScheduleListEntry.t()
  @type schedule_occurrence_preview :: ScheduleOccurrencePreview.t()

  @doc """
  Returns redacted operator diagnostics for the orchestrator runtime.
  """
  @spec diagnostics(keyword()) :: map()
  def diagnostics(opts \\ []) when is_list(opts), do: Diagnostics.report(opts)

  @doc """
  Returns orchestrator process liveness diagnostics.
  """
  @spec liveness() :: map()
  def liveness, do: FavnOrchestrator.Readiness.liveness()

  @doc """
  Returns orchestrator readiness diagnostics through the public orchestrator facade.
  """
  @spec readiness() :: map()
  def readiness, do: FavnOrchestrator.Readiness.readiness()

  @doc """
  Authenticates an operator browser user with username and password.

  Returns a session containing the one-time raw session token for caller storage.
  All auth failures, including login backoff, disabled actors, missing users, and
  invalid passwords, are returned as `{:error, :invalid_credentials}`.
  """
  @spec operator_password_login(String.t(), String.t()) ::
          {:ok, operator_session(), operator_actor()} | {:error, :invalid_credentials}
  @spec operator_password_login(String.t(), String.t(), keyword() | map()) ::
          {:ok, operator_session(), operator_actor()} | {:error, :invalid_credentials}
  def operator_password_login(username, password, opts \\ [])
      when is_binary(username) and is_binary(password) and (is_list(opts) or is_map(opts)) do
    case Auth.password_login(username, password, opts) do
      {:ok, session, actor} -> {:ok, session, actor}
      {:error, _reason} -> {:error, :invalid_credentials}
    end
  end

  @doc """
  Resolves an operator session token into the persisted session and actor.

  Invalid, expired, revoked, or actor-disabled sessions are normalized to
  `{:error, :invalid_session}` for browser-facing auth glue.
  """
  @spec introspect_operator_session(String.t()) ::
          {:ok, operator_session(), operator_actor()} | {:error, :invalid_session}
  def introspect_operator_session(session_token) when is_binary(session_token) do
    case Auth.introspect_session(session_token) do
      {:ok, session, actor} -> {:ok, session, actor}
      {:error, _reason} -> {:error, :invalid_session}
    end
  end

  @doc """
  Revokes an operator session by session id.
  """
  @spec revoke_operator_session(String.t()) :: :ok | {:error, term()}
  def revoke_operator_session(session_id) when is_binary(session_id) do
    Auth.revoke_session(session_id)
  end

  @doc """
  Returns whether an operator actor has at least the required role.
  """
  @spec operator_has_role?(operator_actor(), :viewer | :operator | :admin) :: boolean()
  def operator_has_role?(actor, role) when role in [:viewer, :operator, :admin] do
    Auth.has_role?(actor, role)
  end

  @doc """
  Registers one manifest version in orchestrator storage.
  """
  @spec register_manifest(Version.t()) :: :ok | {:error, term()}
  def register_manifest(%Version{} = version), do: ManifestStore.register_manifest(version)

  @doc """
  Publishes one manifest version, returning the canonical stored version for duplicate content.
  """
  @spec publish_manifest(Version.t()) ::
          {:ok, :published | :already_published, Version.t()} | {:error, term()}
  def publish_manifest(%Version{} = version), do: ManifestStore.publish_manifest(version)

  @doc """
  Returns one persisted manifest version.
  """
  @spec get_manifest(String.t()) :: {:ok, Version.t()} | {:error, term()}
  def get_manifest(manifest_version_id) when is_binary(manifest_version_id) do
    ManifestStore.get_manifest(manifest_version_id)
  end

  @doc """
  Returns one persisted manifest version by content hash.
  """
  @spec get_manifest_by_content_hash(String.t()) :: {:ok, Version.t()} | {:error, term()}
  def get_manifest_by_content_hash(content_hash) when is_binary(content_hash) do
    ManifestStore.get_manifest_by_content_hash(content_hash)
  end

  @doc """
  Registers a persisted manifest version with the configured runner boundary.
  """
  @spec register_manifest_with_runner(String.t()) :: {:ok, map()} | {:error, term()}
  def register_manifest_with_runner(manifest_version_id) when is_binary(manifest_version_id) do
    runner_client = configured_runner_client()
    runner_opts = configured_runner_opts()

    with {:ok, version} <- get_manifest(manifest_version_id),
         :ok <- validate_runner_client(runner_client) do
      content_hash = version.content_hash

      case runner_client.register_manifest(version, runner_opts) do
        :ok ->
          {:ok, runner_manifest_registration(version, runner_client, :accepted)}

        {:error, {:manifest_version_conflict, ^manifest_version_id, ^content_hash, ^content_hash}} ->
          {:ok, runner_manifest_registration(version, runner_client, :already_registered)}

        {:error, {:manifest_version_conflict, ^manifest_version_id, _existing, _incoming}} ->
          {:error, :runner_manifest_conflict}

        {:error, reason} ->
          {:error, runner_registration_error(reason)}
      end
    end
  end

  @doc """
  Lists persisted manifest versions.
  """
  @spec list_manifests() :: {:ok, [Version.t()]} | {:error, term()}
  def list_manifests, do: ManifestStore.list_manifests()

  @doc """
  Lists stable operator-facing manifest summaries.
  """
  @spec list_manifest_summaries() :: {:ok, [manifest_summary()]} | {:error, term()}
  def list_manifest_summaries do
    with {:ok, versions} <- list_manifests() do
      {:ok,
       versions
       |> Enum.map(&manifest_summary/1)
       |> Enum.sort_by(& &1.manifest_version_id)}
    end
  end

  @doc """
  Returns one stable operator-facing manifest summary.
  """
  @spec get_manifest_summary(String.t()) :: {:ok, manifest_summary()} | {:error, term()}
  def get_manifest_summary(manifest_version_id) when is_binary(manifest_version_id) do
    with {:ok, version} <- get_manifest(manifest_version_id) do
      {:ok, manifest_summary(version)}
    end
  end

  @doc """
  Returns manifest-scoped submit targets for one persisted manifest version.
  """
  @spec manifest_targets(String.t()) :: {:ok, manifest_targets()} | {:error, term()}
  def manifest_targets(manifest_version_id) when is_binary(manifest_version_id) do
    with {:ok, version} <- get_manifest(manifest_version_id) do
      {:ok,
       %{
         manifest_version_id: manifest_version_id,
         assets: manifest_asset_targets(version),
         pipelines: manifest_pipeline_targets(version)
       }}
    end
  end

  @doc """
  Returns submit targets for the currently active manifest version.
  """
  @spec active_manifest_targets() :: {:ok, manifest_targets()} | {:error, term()}
  def active_manifest_targets do
    with {:ok, manifest_version_id} <- active_manifest() do
      manifest_targets(manifest_version_id)
    end
  end

  @doc """
  Rebuilds persisted current target statuses for one manifest version.

  The target-status table is a repairable read model for operator catalogue and
  detail pages. This function rebuilds it from authoritative persisted run and
  freshness state and replaces rows for the manifest scope.
  """
  @spec rebuild_target_statuses(String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def rebuild_target_statuses(manifest_version_id) when is_binary(manifest_version_id) do
    with {:ok, version} <- get_manifest(manifest_version_id) do
      TargetStatus.Projector.rebuild_manifest(version)
    end
  end

  @doc """
  Returns operator-facing catalogue entries for the currently active manifest.

  Entries are manifest target metadata enriched with latest known freshness/run
  state. Missing runtime state is represented explicitly as `:unknown` with no
  latest run timestamp.
  """
  @spec active_asset_catalogue() :: {:ok, [asset_catalogue_entry()]} | {:error, term()}
  def active_asset_catalogue do
    with {:ok, manifest_version_id} <- active_manifest(),
         {:ok, version} <- get_manifest(manifest_version_id),
         targets <- Enum.map(List.wrap(version.manifest.assets), &manifest_asset_target/1),
         {:ok, statuses} <- target_statuses(manifest_version_id, :asset, targets) do
      {:ok, asset_catalogue_entries(version, statuses)}
    end
  end

  @doc """
  Returns operator-facing catalogue entries for pipelines in the active manifest.

  Entries include manifest-level pipeline selection metadata enriched with the
  latest persisted run that can be associated with each pipeline.
  """
  @spec active_pipeline_catalogue() :: {:ok, [pipeline_catalogue_entry()]} | {:error, term()}
  def active_pipeline_catalogue do
    with {:ok, manifest_version_id} <- active_manifest(),
         {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         targets <-
           Enum.map(List.wrap(version.manifest.pipelines), &manifest_pipeline_target(index, &1)),
         {:ok, statuses} <- target_statuses(manifest_version_id, :pipeline, targets) do
      {:ok, pipeline_catalogue_entries(version, index, statuses)}
    end
  end

  @doc """
  Returns an operator-facing detail read model for one active pipeline target.

  The detail is built at the orchestrator boundary and includes manifest target
  metadata, selected assets, latest run state, and persisted run history matched
  to the pipeline submit ref.
  """
  @spec active_pipeline_detail(String.t()) :: {:ok, pipeline_detail()} | {:error, term()}
  def active_pipeline_detail(target_id) when is_binary(target_id) do
    with {:ok, manifest_version_id} <- active_manifest(),
         {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         {:ok, status} <- target_status(manifest_version_id, :pipeline, target_id),
         {:ok, pipeline} <- pipeline_for_target(version, target_id),
         {:ok, recent_runs} <-
           Storage.list_target_runs(manifest_version_id, :pipeline, pipeline.module, limit: 50) do
      case pipeline_detail_entry(version, index, target_id, status, recent_runs) do
        nil -> {:error, :not_found}
        detail -> {:ok, detail}
      end
    end
  end

  @doc """
  Returns an operator-facing detail read model for one active asset target.

  The detail is a DTO built at the orchestrator boundary. It includes manifest
  target metadata, latest known freshness/run state, and a conservative 30-window
  timeline matching the asset window policy. Missing runtime evidence is
  represented as `:unknown`.
  """
  @spec active_asset_detail(String.t(), keyword()) :: {:ok, asset_detail()} | {:error, term()}
  def active_asset_detail(target_id, opts \\ []) when is_binary(target_id) and is_list(opts) do
    with {:ok, manifest_version_id} <- active_manifest(),
         {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, freshness_states} <- detail_freshness_states(manifest_version_id),
         {:ok, asset_window_states} <- detail_asset_window_states(manifest_version_id),
         {:ok, asset} <- asset_for_target(version, target_id),
         {:ok, recent_runs} <-
           Storage.list_target_runs(manifest_version_id, :asset, asset.ref, limit: 50),
         {:ok, status} <- target_status(manifest_version_id, :asset, target_id) do
      case asset_detail_entry(
             version,
             target_id,
             status,
             freshness_states,
             asset_window_states,
             recent_runs,
             opts
           ) do
        nil -> {:error, :not_found}
        detail -> {:ok, detail}
      end
    end
  end

  @doc """
  Persists and publishes one trusted backend log entry.
  """
  @spec emit_log(term()) :: {:ok, [Favn.Log.Entry.t()]} | {:error, term()}
  def emit_log(entry), do: LogWriter.write(entry)

  @doc """
  Persists and publishes trusted backend log entries as one batch.
  """
  @spec emit_logs([term()]) :: {:ok, [Favn.Log.Entry.t()]} | {:error, term()}
  def emit_logs(entries) when is_list(entries), do: LogWriter.write(entries)

  @doc """
  Lists persisted backend logs matching the given filter.
  """
  @spec list_logs(term(), keyword()) :: {:ok, Page.t(Favn.Log.Entry.t())} | {:error, term()}
  def list_logs(filter \\ default_log_filter(), opts \\ []) when is_list(opts) do
    Storage.list_logs(filter, opts)
  end

  @doc """
  Replays persisted backend logs after an authoritative log cursor.
  """
  @spec replay_logs(term(), term(), keyword()) :: {:ok, [Favn.Log.Entry.t()]} | {:error, term()}
  def replay_logs(cursor, filter \\ default_log_filter(), opts \\ []) when is_list(opts) do
    Storage.replay_logs_after(cursor, filter, opts)
  end

  @doc """
  Subscribes the caller to live backend logs matching the given filter.

  Run-scoped filters subscribe to the run topic. Filters with both `run_id` and
  `asset_step_id` subscribe to the asset-step topic, then apply the remaining
  `Favn.Log.Filter` fields before forwarding entries to the caller.
  """
  @spec subscribe_logs(term()) :: {:ok, term()} | {:error, term()}
  def subscribe_logs(filter \\ default_log_filter()), do: Logs.subscribe_logs(filter)

  @doc """
  Unsubscribes the caller from a prior backend log subscription.

  Callers must pass the subscription returned by `subscribe_logs/1`; equivalent
  filters cannot stop the owned forwarding process.
  """
  @spec unsubscribe_logs(term()) :: :ok | {:error, :invalid_log_subscription}
  def unsubscribe_logs(subscription), do: Logs.unsubscribe_logs(subscription)

  @doc """
  Submits one asset run by manifest-scoped target id.
  """
  @spec submit_asset_run_for_manifest(String.t(), String.t(), keyword()) ::
          {:ok, run_id()} | {:error, term()}
  @spec submit_asset_run_for_manifest(String.t(), String.t(), map()) ::
          {:ok, run_id()} | {:error, term()}
  def submit_asset_run_for_manifest(manifest_version_id, target_id, opts_or_request \\ [])

  def submit_asset_run_for_manifest(manifest_version_id, target_id, request)
      when is_binary(manifest_version_id) and is_binary(target_id) and is_map(request) do
    with {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, asset} <- resolve_asset_target(version, target_id),
         {:ok, opts} <- asset_run_request_opts(asset, request) do
      submit_asset_run(asset.ref, Keyword.put(opts, :manifest_version_id, manifest_version_id))
    end
  end

  def submit_asset_run_for_manifest(manifest_version_id, target_id, opts)
      when is_binary(manifest_version_id) and is_binary(target_id) and is_list(opts) do
    with {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, asset_ref} <- resolve_asset_target_ref(version, target_id) do
      submit_asset_run(asset_ref, Keyword.put(opts, :manifest_version_id, manifest_version_id))
    end
  end

  @doc """
  Submits one asset run command for an authenticated operator actor context.

  This is the same-BEAM boundary for browser, API, and CLI operator actions.
  Callers pass operator intent, such as dependency mode, refresh mode, and
  selected timeline window. The orchestrator validates that intent and translates
  it into runtime submit options after resolving the manifest target.

  Missing or incomplete actor/session context returns `{:error,
  :unauthenticated}`; authenticated actors without the operator role return
  `{:error, :forbidden}`.

  TODO: add a narrow audit event for accepted LiveView operator commands once the
  audit shape for same-BEAM browser actions is finalized.
  """
  @spec submit_operator_asset_run(
          operator_actor_context(),
          String.t(),
          String.t()
        ) :: {:ok, run_id()} | {:error, term()}
  @spec submit_operator_asset_run(
          operator_actor_context(),
          String.t(),
          String.t(),
          AssetRunRequest.t() | map() | keyword() | nil
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_operator_asset_run(
        actor_context,
        manifest_version_id,
        target_id,
        command_input \\ %{}
      ) do
    with :ok <- require_operator_context(actor_context),
         {:ok, request} <- AssetRunRequest.from_input(command_input),
         {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, asset} <- resolve_asset_target(version, target_id),
         {:ok, opts} <- operator_asset_run_opts(asset, request) do
      submit_asset_run(asset.ref, Keyword.put(opts, :manifest_version_id, manifest_version_id))
    end
  end

  @doc """
  Submits one manifest target run for an authenticated operator actor context.

  This is the shared command boundary for browser, API, and CLI callers. The
  target decides whether the request is normalized as an asset run or a pipeline
  run; callers should not dispatch to asset/pipeline-specific submit functions.
  """
  @spec submit_operator_run(
          operator_actor_context(),
          String.t(),
          map(),
          AssetRunRequest.t() | PipelineRunRequest.t() | map() | keyword() | nil
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_operator_run(actor_context, manifest_version_id, target, command_input \\ %{}) do
    with :ok <- require_operator_context(actor_context),
         {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, normalized_target} <- normalize_operator_run_target(version, target) do
      submit_operator_run_target(version, normalized_target, command_input)
    end
  end

  @doc """
  Submits an asset backfill command for an authenticated operator actor context.

  Thin callers pass operator intent for the range, dependency mode, and refresh
  mode. The orchestrator owns range expansion, parent/child grouping, child
  refresh defaults, selected-asset refresh translation, and partial submission
  compensation.
  """
  @spec submit_operator_asset_backfill(
          operator_actor_context(),
          String.t(),
          String.t()
        ) :: {:ok, run_id()} | {:error, term()}
  @spec submit_operator_asset_backfill(
          operator_actor_context(),
          String.t(),
          String.t(),
          AssetBackfillRequest.t() | map() | keyword()
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_operator_asset_backfill(
        actor_context,
        manifest_version_id,
        target_id,
        command_input \\ %{}
      ) do
    with :ok <- require_operator_context(actor_context),
         {:ok, request} <- AssetBackfillRequest.from_input(command_input),
         {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, asset} <- resolve_asset_target(version, target_id),
         {:ok, opts} <- operator_asset_backfill_opts(asset, request) do
      BackfillManager.submit_asset_backfill(
        asset.ref,
        Keyword.put(opts, :manifest_version_id, manifest_version_id)
      )
    end
  end

  @doc """
  Submits a manifest-pinned asset run for one stable asset detail window id.
  """
  @spec submit_asset_window_run(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, run_id()} | {:error, term()}
  def submit_asset_window_run(manifest_version_id, target_id, window_id, opts \\ [])
      when is_binary(manifest_version_id) and is_binary(target_id) and is_binary(window_id) and
             is_list(opts) do
    submit_asset_run_for_manifest(manifest_version_id, target_id, %{
      selection: %{source: :data_coverage_timeline, id: window_id},
      config: Map.new(opts)
    })
  end

  defp asset_run_request_opts(asset, request) do
    config = Map.get(request, :config) || Map.get(request, "config") || %{}
    selection = Map.get(request, :selection) || Map.get(request, "selection")

    with {:ok, opts} <- asset_run_config_opts(asset, config),
         {:ok, opts} <- put_asset_run_selection_opts(opts, asset, selection) do
      {:ok, opts}
    end
  end

  defp asset_run_config_opts(asset, config) when is_map(config) do
    dependencies_value = Map.get(config, :dependencies) || Map.get(config, "dependencies") || :all
    refresh_value = Map.get(config, :refresh) || Map.get(config, "refresh") || :auto

    with {:ok, dependencies} <- request_dependency_option(dependencies_value),
         {:ok, refresh} <- request_refresh_option(refresh_value, asset.ref, dependencies) do
      opts = [dependencies: dependencies, refresh: refresh]

      opts =
        cond do
          Map.has_key?(config, :metadata) ->
            Keyword.put(opts, :metadata, Map.get(config, :metadata))

          Map.has_key?(config, "metadata") ->
            Keyword.put(opts, :metadata, Map.get(config, "metadata"))

          true ->
            opts
        end

      {:ok, opts}
    end
  end

  defp asset_run_config_opts(_asset, _config), do: {:error, :invalid_asset_run_config}

  defp put_asset_run_selection_opts(opts, _asset, nil), do: {:ok, opts}

  defp put_asset_run_selection_opts(opts, asset, %{} = selection) do
    source = Map.get(selection, :source) || Map.get(selection, "source")
    id = Map.get(selection, :id) || Map.get(selection, "id")

    case normalize_selection_source(source) do
      {:ok, :data_coverage_timeline} ->
        put_data_coverage_selection_opts(opts, asset, id, selection)

      {:ok, :refresh_timeline} ->
        put_refresh_selection_opts(opts, id, selection)

      {:error, _reason} = error ->
        error
    end
  end

  defp put_asset_run_selection_opts(_opts, _asset, _selection),
    do: {:error, :invalid_asset_run_selection}

  defp put_data_coverage_selection_opts(opts, asset, id, selection) when is_binary(id) do
    with {:ok, window_request} <- window_request_from_id(id),
         {:ok, anchor_window} <- resolve_asset_window(asset, window_request),
         {:ok, runtime_window} <- runtime_window_from_anchor(anchor_window) do
      opts =
        opts
        |> Keyword.put(:anchor_window, anchor_window)
        |> Keyword.put(:exact_windows, %{asset.ref => [runtime_window]})
        |> put_window_run_metadata(id, anchor_window)
        |> put_selection_metadata(:data_coverage_timeline, selection)

      {:ok, opts}
    end
  end

  defp put_data_coverage_selection_opts(_opts, _asset, _id, _selection),
    do: {:error, :invalid_asset_run_selection}

  defp runtime_window_from_anchor(%Anchor{} = anchor_window) do
    RuntimeWindow.new(
      anchor_window.kind,
      anchor_window.start_at,
      anchor_window.end_at,
      anchor_window.key,
      timezone: anchor_window.timezone
    )
  end

  defp put_refresh_selection_opts(opts, id, selection) when is_binary(id) do
    with {:ok, window_request} <- refresh_request_from_id(id),
         {:ok, anchor_window} <-
           WindowRequest.to_anchor(window_request, selection_timezone(selection)) do
      opts =
        opts
        |> Keyword.put(:anchor_window, anchor_window)
        |> put_selection_metadata(:refresh_timeline, selection)

      {:ok, opts}
    end
  end

  defp put_refresh_selection_opts(_opts, _id, _selection),
    do: {:error, :invalid_asset_run_selection}

  defp normalize_selection_source(source) when source in [:refresh_timeline, "refresh_timeline"],
    do: {:ok, :refresh_timeline}

  defp normalize_selection_source(source)
       when source in [:data_coverage_timeline, "data_coverage_timeline"],
       do: {:ok, :data_coverage_timeline}

  defp normalize_selection_source(source), do: {:error, {:invalid_selection_source, source}}

  defp selection_timezone(selection),
    do: Map.get(selection, :timezone) || Map.get(selection, "timezone") || "Etc/UTC"

  defp request_dependency_option(value) when value in [:all, "all"], do: {:ok, :all}
  defp request_dependency_option(value) when value in [:none, "none"], do: {:ok, :none}
  defp request_dependency_option(value), do: {:error, {:invalid_dependencies_mode, value}}

  defp request_refresh_option(value, _asset_ref, _dependencies) when value in [:auto, "auto"],
    do: {:ok, :auto}

  defp request_refresh_option(value, _asset_ref, _dependencies)
       when value in [:missing, "missing"], do: {:ok, :missing}

  defp request_refresh_option(value, _asset_ref, _dependencies)
       when value in [:force, :force_all, "force", "force_all"], do: {:ok, :force}

  defp request_refresh_option({:force_assets, refs}, _asset_ref, _dependencies)
       when is_list(refs),
       do: {:ok, {:force_assets, refs}}

  defp request_refresh_option({:force_assets, refs, opts}, _asset_ref, _dependencies)
       when is_list(refs) and is_list(opts),
       do: {:ok, {:force_assets, refs, opts}}

  defp request_refresh_option(value, asset_ref, _dependencies)
       when value in [:force_selected, "force_selected"] and is_tuple(asset_ref),
       do: {:ok, {:force_assets, [asset_ref]}}

  defp request_refresh_option(value, _asset_ref, :none)
       when value in [:force_selected_upstream, "force_selected_upstream"],
       do: {:error, {:refresh_include_upstream_requires_dependencies, :all}}

  defp request_refresh_option(value, asset_ref, :all)
       when value in [:force_selected_upstream, "force_selected_upstream"] and is_tuple(asset_ref),
       do: {:ok, {:force_assets, [asset_ref], include_upstream: true}}

  defp request_refresh_option(value, _asset_ref, _dependencies),
    do: {:error, {:invalid_refresh_policy, value}}

  defp operator_asset_run_opts(asset, %AssetRunRequest{} = request) do
    with {:ok, refresh} <-
           operator_asset_refresh_option(request.refresh_mode, asset.ref, request.dependency_mode) do
      opts =
        []
        |> Keyword.put(:dependencies, request.dependency_mode)
        |> Keyword.put(:refresh, refresh)
        |> maybe_put_opt(:metadata, request.metadata)
        |> maybe_put_opt(:timeout_ms, request.timeout_ms)

      put_asset_run_selection_opts(opts, asset, request.selection)
    end
  end

  defp operator_asset_backfill_opts(asset, %AssetBackfillRequest{} = request) do
    with {:ok, refresh} <-
           operator_asset_backfill_refresh_option(
             request.refresh_mode,
             asset.ref,
             request.dependency_mode
           ) do
      opts =
        []
        |> Keyword.put(:range_request, request.range)
        |> Keyword.put(:dependencies, request.dependency_mode)
        |> maybe_put_opt(:refresh, refresh)
        |> maybe_put_opt(:metadata, request.metadata)
        |> maybe_put_opt(:max_attempts, request.max_attempts)
        |> maybe_put_opt(:retry_backoff_ms, request.retry_backoff_ms)
        |> maybe_put_opt(:timeout_ms, request.timeout_ms)

      {:ok, opts}
    end
  end

  defp operator_pipeline_run_opts(%PipelineRunRequest{} = request) do
    opts =
      []
      |> maybe_put_opt(:window_request, request.window)
      |> maybe_put_opt(:refresh, operator_pipeline_refresh_option(request.refresh_mode))
      |> maybe_put_opt(:metadata, request.metadata)
      |> maybe_put_opt(:timeout_ms, request.timeout_ms)

    {:ok, opts}
  end

  defp normalize_operator_run_target(%Version{} = version, %{type: type, id: target_id})
       when type in [:asset, "asset"] and is_binary(target_id) do
    with {:ok, asset} <- resolve_asset_target(version, target_id) do
      {:ok, %{type: :asset, asset: asset}}
    end
  end

  defp normalize_operator_run_target(%Version{} = version, %{type: type, id: target_id})
       when type in [:pipeline, "pipeline"] and is_binary(target_id) do
    with {:ok, pipeline_module} <- resolve_pipeline_target_module(version, target_id) do
      {:ok, %{type: :pipeline, pipeline_module: pipeline_module}}
    end
  end

  defp normalize_operator_run_target(%Version{} = version, %{type: type, module: pipeline_module})
       when type in [:pipeline, "pipeline"] and is_atom(pipeline_module) do
    if Enum.any?(List.wrap(version.manifest.pipelines), &(&1.module == pipeline_module)) do
      {:ok, %{type: :pipeline, pipeline_module: pipeline_module}}
    else
      {:error, :invalid_pipeline_target}
    end
  end

  defp normalize_operator_run_target(_version, _target), do: {:error, :invalid_target}

  defp submit_operator_run_target(%Version{} = version, %{type: :asset, asset: asset}, input) do
    with {:ok, request} <- AssetRunRequest.from_input(input),
         {:ok, opts} <- operator_asset_run_opts(asset, request) do
      submit_asset_run(
        asset.ref,
        Keyword.put(opts, :manifest_version_id, version.manifest_version_id)
      )
    end
  end

  defp submit_operator_run_target(
         %Version{} = version,
         %{type: :pipeline, pipeline_module: pipeline_module},
         input
       ) do
    with {:ok, request} <- PipelineRunRequest.from_input(input),
         {:ok, opts} <- operator_pipeline_run_opts(request) do
      submit_pipeline_run(
        pipeline_module,
        Keyword.put(opts, :manifest_version_id, version.manifest_version_id)
      )
    end
  end

  defp operator_pipeline_backfill_opts(%PipelineBackfillRequest{} = request) do
    opts =
      []
      |> Keyword.put(:range_request, request.range)
      |> maybe_put_opt(:coverage_baseline_id, request.coverage_baseline_id)
      |> maybe_put_opt(:refresh, operator_pipeline_backfill_refresh_option(request.refresh_mode))
      |> maybe_put_opt(:metadata, request.metadata)
      |> maybe_put_opt(:max_attempts, request.max_attempts)
      |> maybe_put_opt(:retry_backoff_ms, request.retry_backoff_ms)
      |> maybe_put_opt(:timeout_ms, request.timeout_ms)

    {:ok, opts}
  end

  defp operator_asset_refresh_option(:auto, _asset_ref, _dependencies), do: {:ok, :auto}
  defp operator_asset_refresh_option(:missing, _asset_ref, _dependencies), do: {:ok, :missing}
  defp operator_asset_refresh_option(:force_all, _asset_ref, _dependencies), do: {:ok, :force}

  defp operator_asset_refresh_option(:force_selected, asset_ref, _dependencies),
    do: {:ok, {:force_assets, [asset_ref]}}

  defp operator_asset_refresh_option(:force_selected_upstream, _asset_ref, :none),
    do: {:error, {:refresh_include_upstream_requires_dependencies, :all}}

  defp operator_asset_refresh_option(:force_selected_upstream, asset_ref, :all),
    do: {:ok, {:force_assets, [asset_ref], include_upstream: true}}

  defp operator_asset_backfill_refresh_option(:auto, _asset_ref, _dependencies), do: {:ok, nil}

  defp operator_asset_backfill_refresh_option(refresh_mode, asset_ref, dependencies),
    do: operator_asset_refresh_option(refresh_mode, asset_ref, dependencies)

  defp operator_pipeline_refresh_option(:auto), do: nil
  defp operator_pipeline_refresh_option(:missing), do: :missing
  defp operator_pipeline_refresh_option(:force_all), do: :force

  defp operator_pipeline_backfill_refresh_option(:auto), do: nil
  defp operator_pipeline_backfill_refresh_option(:missing), do: :missing
  defp operator_pipeline_backfill_refresh_option(:force_all), do: :force

  defp put_window_run_metadata(opts, window_id, %Anchor{} = anchor_window) do
    selected_window_metadata = window_run_metadata(window_id, anchor_window)

    case Keyword.fetch(opts, :metadata) do
      :error ->
        Keyword.put(opts, :metadata, selected_window_metadata)

      {:ok, metadata} when is_map(metadata) ->
        Keyword.put(opts, :metadata, Map.merge(metadata, selected_window_metadata))

      {:ok, _invalid_metadata} ->
        opts
    end
  end

  defp put_selection_metadata(opts, source, selection) do
    selection_metadata = %{
      timeline_selection: %{
        source: source,
        id: Map.get(selection, :id) || Map.get(selection, "id"),
        kind: Map.get(selection, :kind) || Map.get(selection, "kind"),
        value: Map.get(selection, :value) || Map.get(selection, "value"),
        run_id: Map.get(selection, :run_id) || Map.get(selection, "run_id")
      }
    }

    case Keyword.fetch(opts, :metadata) do
      :error ->
        Keyword.put(opts, :metadata, selection_metadata)

      {:ok, metadata} when is_map(metadata) ->
        Keyword.put(opts, :metadata, Map.merge(metadata, selection_metadata))

      {:ok, _invalid_metadata} ->
        opts
    end
  end

  defp window_run_metadata(window_id, %Anchor{} = anchor_window) do
    %{
      selected_window: %{
        id: window_id,
        kind: anchor_window.kind,
        key: anchor_window.key,
        start_at: anchor_window.start_at,
        end_at: anchor_window.end_at,
        timezone: anchor_window.timezone
      }
    }
  end

  @doc """
  Submits one pipeline run by manifest-scoped target id.

  Thin callers may pass plain map input with an optional `:window` map. The
  orchestrator validates and translates it into the runtime window request.
  """
  @spec submit_pipeline_run_for_manifest(String.t(), String.t(), keyword() | map()) ::
          {:ok, run_id()} | {:error, term()}
  def submit_pipeline_run_for_manifest(manifest_version_id, target_id, opts \\ [])
      when is_binary(manifest_version_id) and is_binary(target_id) and
             (is_list(opts) or is_map(opts)) do
    with {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, pipeline_module} <- resolve_pipeline_target_module(version, target_id),
         {:ok, opts} <- normalize_pipeline_run_submit_opts(opts) do
      submit_pipeline_run(
        pipeline_module,
        Keyword.put(opts, :manifest_version_id, manifest_version_id)
      )
    end
  end

  @doc """
  Submits one pipeline run command for an authenticated operator actor context.

  This is the same-BEAM boundary for browser, API, and CLI operator actions.
  Callers pass operator intent and the orchestrator translates it into runtime
  submit options after resolving the manifest pipeline target.

  Missing or incomplete actor/session context returns `{:error,
  :unauthenticated}`; authenticated actors without the operator role return
  `{:error, :forbidden}`.

  TODO: add a narrow audit event for accepted LiveView operator commands once the
  audit shape for same-BEAM browser actions is finalized.
  """
  @spec submit_operator_pipeline_run(
          operator_actor_context(),
          String.t(),
          String.t()
        ) :: {:ok, run_id()} | {:error, term()}
  @spec submit_operator_pipeline_run(
          operator_actor_context(),
          String.t(),
          String.t(),
          PipelineRunRequest.t() | map() | keyword() | nil
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_operator_pipeline_run(
        actor_context,
        manifest_version_id,
        target_id,
        command_input \\ %{}
      ) do
    with :ok <- require_operator_context(actor_context),
         {:ok, request} <- PipelineRunRequest.from_input(command_input),
         {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, pipeline_module} <- resolve_pipeline_target_module(version, target_id),
         {:ok, opts} <- operator_pipeline_run_opts(request) do
      submit_pipeline_run(
        pipeline_module,
        Keyword.put(opts, :manifest_version_id, manifest_version_id)
      )
    end
  end

  @doc """
  Sets the active manifest version used by default for new runs.
  """
  @spec activate_manifest(String.t()) :: :ok | {:error, term()}
  def activate_manifest(manifest_version_id) when is_binary(manifest_version_id) do
    ManifestStore.set_active_manifest(manifest_version_id)
  end

  @doc """
  Returns the currently active manifest version id.
  """
  @spec active_manifest() :: {:ok, String.t()} | {:error, term()}
  def active_manifest, do: ManifestStore.get_active_manifest()

  @doc """
  Submits one asset run pinned to a manifest version.

  When `:manifest_version_id` is not provided, the active manifest version is used.

  Freshness-related options:

  - `:refresh` or `:refresh_policy` accepts `:auto`, `:force`, `:missing`,
    `{:force_assets, refs}`, `{:force_assets, refs, include_upstream: true}`, or
    equivalent maps. See `FavnOrchestrator.RefreshPolicy`.
  """
  @spec submit_asset_run(Favn.Ref.t(), keyword()) :: {:ok, run_id()} | {:error, term()}
  def submit_asset_run(asset_ref, opts \\ [])

  def submit_asset_run({module, name} = asset_ref, opts)
      when is_atom(module) and is_atom(name) and is_list(opts) do
    RunManager.submit_asset_run(asset_ref, opts)
  end

  def submit_asset_run(_asset_ref, _opts), do: {:error, :invalid_target_ref}

  @doc """
  Submits one pipeline run from explicit target refs or a persisted manifest pipeline module.

  Freshness-related options:

  - `:refresh` or `:refresh_policy` accepts `:auto`, `:force`, `:missing`,
    `{:force_assets, refs}`, `{:force_assets, refs, include_upstream: true}`, or
    equivalent maps. See `FavnOrchestrator.RefreshPolicy`.

  Under `:auto`, manifest freshness policies decide which planned nodes run or
  skip. `:missing` skips nodes with prior successful freshness state. `:force`
  runs every planned node.

  """
  @spec submit_pipeline_run([Favn.Ref.t()], keyword()) :: {:ok, run_id()} | {:error, term()}
  @spec submit_pipeline_run(module(), keyword()) :: {:ok, run_id()} | {:error, term()}
  def submit_pipeline_run(target_or_module, opts \\ [])

  def submit_pipeline_run(target_refs, opts) when is_list(target_refs) and is_list(opts) do
    RunManager.submit_pipeline_run(target_refs, opts)
  end

  def submit_pipeline_run(pipeline_module, opts)
      when is_atom(pipeline_module) and is_list(opts) do
    RunManager.submit_pipeline_module_run(pipeline_module, opts)
  end

  @doc """
  Inspects one manifest-owned asset relation through the configured runner boundary.
  """
  @spec inspect_manifest_asset(String.t(), String.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def inspect_manifest_asset(manifest_version_id, target_id, opts \\ [])
      when is_binary(manifest_version_id) and is_binary(target_id) and is_list(opts) do
    with {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, asset_ref} <- resolve_asset_target_ref(version, target_id),
         :ok <- validate_runner_client(configured_runner_client()),
         :ok <- configured_runner_client().register_manifest(version, configured_runner_opts()) do
      request = %RelationInspectionRequest{
        manifest_version_id: manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: asset_ref,
        sample_limit: Keyword.get(opts, :sample_limit, 20)
      }

      configured_runner_client().inspect_relation(request, configured_runner_opts())
    end
  end

  @doc """
  Submits a parent pipeline backfill run and child pipeline runs for each resolved anchor.

  This control-plane entrypoint is used by the private HTTP surface and internal
  callers. It resolves `:range_request` through `Favn.Backfill.RangeResolver`,
  persists a parent `:backfill_pipeline` run, creates one normalized backfill
  window row per anchor, and submits one child pipeline run per window.

  Required options:

  - `:range_request` - `Favn.Backfill.RangeRequest.t/0`, map, or keyword input.

  Common options:

  - `:manifest_version_id` - defaults to the active manifest.
  - `:coverage_baseline_id` - associates requested windows with a projected
    coverage baseline.
  - `:refresh` or `:refresh_policy` - forwarded to child pipeline runs. Defaults
    to `:missing` when neither option is provided.
  - `:metadata` - user metadata merged into the parent run metadata.
  - `:max_attempts`, `:retry_backoff_ms`, and `:timeout_ms` - forwarded to child
    runs.
  """
  @spec submit_pipeline_backfill(module(), keyword()) :: {:ok, run_id()} | {:error, term()}
  def submit_pipeline_backfill(pipeline_module, opts \\ [])

  def submit_pipeline_backfill(pipeline_module, opts)
      when is_atom(pipeline_module) and is_list(opts) do
    BackfillManager.submit_pipeline_backfill(pipeline_module, opts)
  end

  def submit_pipeline_backfill(_pipeline_module, _opts), do: {:error, :invalid_pipeline_module}

  @doc """
  Plans one pipeline backfill without persisting a parent run or submitting child runs.

  The returned DTO contains the selected manifest, target, resolved range bounds,
  and concrete window keys that a later submit would use.
  """
  @spec plan_pipeline_backfill(module(), keyword()) :: {:ok, map()} | {:error, term()}
  def plan_pipeline_backfill(pipeline_module, opts \\ [])

  def plan_pipeline_backfill(pipeline_module, opts)
      when is_atom(pipeline_module) and is_list(opts) do
    BackfillManager.plan_pipeline_backfill(pipeline_module, opts)
  end

  def plan_pipeline_backfill(_pipeline_module, _opts), do: {:error, :invalid_pipeline_module}

  @doc """
  Submits one pipeline backfill by manifest-scoped target id.

  Thin callers may pass plain map input with `:range` containing `:from`, `:to`,
  `:kind`, and `:timezone`. The orchestrator validates and translates it into
  the runtime backfill range request.
  """
  @spec submit_pipeline_backfill_for_manifest(String.t(), String.t(), keyword() | map()) ::
          {:ok, run_id()} | {:error, term()}
  def submit_pipeline_backfill_for_manifest(manifest_version_id, target_id, opts \\ [])
      when is_binary(manifest_version_id) and is_binary(target_id) and
             (is_list(opts) or is_map(opts)) do
    with {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, pipeline_module} <- resolve_pipeline_target_module(version, target_id),
         {:ok, opts} <- normalize_pipeline_backfill_submit_opts(opts) do
      submit_pipeline_backfill(
        pipeline_module,
        Keyword.put(opts, :manifest_version_id, manifest_version_id)
      )
    end
  end

  @doc """
  Plans one pipeline backfill by manifest-scoped target id.
  """
  @spec plan_pipeline_backfill_for_manifest(String.t(), String.t(), keyword() | map()) ::
          {:ok, map()} | {:error, term()}
  def plan_pipeline_backfill_for_manifest(manifest_version_id, target_id, opts \\ [])
      when is_binary(manifest_version_id) and is_binary(target_id) and
             (is_list(opts) or is_map(opts)) do
    with {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, pipeline_module} <- resolve_pipeline_target_module(version, target_id),
         {:ok, opts} <- normalize_pipeline_backfill_submit_opts(opts) do
      plan_pipeline_backfill(
        pipeline_module,
        Keyword.put(opts, :manifest_version_id, manifest_version_id)
      )
    end
  end

  @doc """
  Submits one pipeline backfill command for an authenticated operator actor context.

  This is the same-BEAM boundary for browser, API, and CLI operator actions.
  Callers pass operator intent for the range and refresh mode. The orchestrator
  validates and translates that intent before submitting the runtime backfill.

  Missing or incomplete actor/session context returns `{:error,
  :unauthenticated}`; authenticated actors without the operator role return
  `{:error, :forbidden}`.

  TODO: add a narrow audit event for accepted LiveView operator commands once the
  audit shape for same-BEAM browser actions is finalized.
  """
  @spec submit_operator_pipeline_backfill(
          operator_actor_context(),
          String.t(),
          String.t()
        ) :: {:ok, run_id()} | {:error, term()}
  @spec submit_operator_pipeline_backfill(
          operator_actor_context(),
          String.t(),
          String.t(),
          PipelineBackfillRequest.t() | map() | keyword()
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_operator_pipeline_backfill(
        actor_context,
        manifest_version_id,
        target_id,
        command_input \\ %{}
      ) do
    with :ok <- require_operator_context(actor_context),
         {:ok, request} <- PipelineBackfillRequest.from_input(command_input),
         {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, pipeline_module} <- resolve_pipeline_target_module(version, target_id),
         {:ok, opts} <- operator_pipeline_backfill_opts(request) do
      submit_pipeline_backfill(
        pipeline_module,
        Keyword.put(opts, :manifest_version_id, manifest_version_id)
      )
    end
  end

  @doc """
  Lists normalized backfill-window ledger rows.
  """
  @spec list_backfill_windows(keyword()) ::
          {:ok, FavnOrchestrator.Page.t(FavnOrchestrator.Backfill.BackfillWindow.t())}
          | {:error, term()}
  def list_backfill_windows(filters \\ []) when is_list(filters) do
    Storage.list_backfill_windows(filters)
  end

  @doc """
  Lists projected coverage baselines.
  """
  @spec list_coverage_baselines(keyword()) ::
          {:ok, FavnOrchestrator.Page.t(FavnOrchestrator.Backfill.CoverageBaseline.t())}
          | {:error, term()}
  def list_coverage_baselines(filters \\ []) when is_list(filters) do
    Storage.list_coverage_baselines(filters)
  end

  @doc """
  Lists latest asset/window states.
  """
  @spec list_asset_window_states(keyword()) ::
          {:ok, FavnOrchestrator.Page.t(FavnOrchestrator.Backfill.AssetWindowState.t())}
          | {:error, term()}
  def list_asset_window_states(filters \\ []) when is_list(filters) do
    Storage.list_asset_window_states(filters)
  end

  @doc """
  Returns one internal freshness state for an asset/freshness key.

  `freshness_key` must come from `Favn.Freshness.Key`, for example `"latest"`, a
  calendar key, or a window key. This is an orchestrator control-plane API, not a
  `favn_view` public endpoint.
  """
  @spec get_asset_freshness(Favn.Ref.t(), String.t()) ::
          {:ok, FavnOrchestrator.AssetFreshnessState.t()} | {:error, term()}
  def get_asset_freshness(asset_ref, freshness_key),
    do: FreshnessQuery.get_asset_freshness(asset_ref, freshness_key)

  @doc """
  Lists internal asset freshness states.

  Common filters are `:asset_ref_module`, `:asset_ref_name`, `:freshness_key`,
  `:status`, `:manifest_version_id`, `:limit`, and `:offset`.
  """
  @spec list_asset_freshness(keyword()) ::
          {:ok, FavnOrchestrator.Page.t(FavnOrchestrator.AssetFreshnessState.t())}
          | {:error, term()}
  def list_asset_freshness(filters \\ []) when is_list(filters),
    do: FreshnessQuery.list_asset_freshness(filters)

  @doc """
  Explains whether a stored freshness state is stale against current upstream versions.

  Options:

  - `:freshness_key` - downstream freshness key, defaults to `"latest"`.
  - `:upstream_node_keys` - concrete planned upstream node keys to compare.

  Returns `status: :fresh` with no reasons when stored input versions still match
  the current upstream freshness versions. Returns `status: :stale` with explicit
  stale reasons when an upstream version is missing or changed.
  """
  @spec explain_asset_staleness(Favn.Ref.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def explain_asset_staleness(asset_ref, opts \\ []) when is_list(opts),
    do: FreshnessQuery.explain_asset_staleness(asset_ref, opts)

  @doc """
  Repairs derived operational-backfill read models from persisted run snapshots.

  The repair source is authoritative run state. By default this returns a dry-run
  report; pass `apply: true` to replace scoped derived read models. Repair never
  appends run events or rewrites run snapshots.
  """
  @spec repair_backfill_projections(keyword()) :: {:ok, map()} | {:error, term()}
  def repair_backfill_projections(opts \\ []) when is_list(opts) do
    BackfillRepair.repair(opts)
  end

  @doc """
  Reruns the latest attempt for one failed backfill window.

  Pass `allow_success: true` with an explicit refresh policy such as
  `refresh: :force` to intentionally repair a successful window whose external
  side effects need recomputation.
  """
  @spec rerun_backfill_window(String.t(), module(), String.t(), keyword()) ::
          {:ok, run_id()} | {:error, term()}
  def rerun_backfill_window(backfill_run_id, pipeline_module, window_key, opts \\ [])
      when is_binary(backfill_run_id) and is_atom(pipeline_module) and is_binary(window_key) and
             is_list(opts) do
    with {:ok, window} <-
           Storage.get_backfill_window(backfill_run_id, pipeline_module, window_key),
         :ok <- ensure_window_rerunnable(window, opts),
         source_run_id when is_binary(source_run_id) <- window.latest_attempt_run_id,
         {:ok, anchor} <-
           Anchor.new(window.window_kind, window.window_start_at, window.window_end_at,
             timezone: window.timezone
           ) do
      rerun(
        source_run_id,
        opts
        |> Keyword.put(:anchor_window, anchor)
        |> Keyword.put(:parent_run_id, backfill_run_id)
        |> Keyword.put(:root_run_id, backfill_run_id)
        |> Keyword.put(:trigger, %{
          kind: :backfill,
          backfill_run_id: backfill_run_id,
          window_key: window_key,
          rerun: true
        })
        |> Keyword.update(:metadata, %{backfill_window_rerun: true}, fn metadata ->
          Map.merge(metadata, %{backfill_window_rerun: true})
        end)
      )
    else
      nil -> {:error, :backfill_window_has_no_attempt}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Requests cancellation for one run and forwards cancellation to the runner when work is in flight.
  """
  @spec cancel_run(run_id(), map()) :: :ok | {:error, term()}
  def cancel_run(run_id, reason \\ %{}) when is_binary(run_id) and is_map(reason) do
    RunManager.cancel_run(run_id, reason)
  end

  @doc """
  Requests cancellation for one run on behalf of an authenticated operator.

  This is the same-BEAM boundary for browser operator actions. The orchestrator
  validates the actor/session context before forwarding cancellation to the
  run-manager lifecycle contract.
  """
  @spec cancel_operator_run(operator_actor_context(), run_id()) :: :ok | {:error, term()}
  def cancel_operator_run(actor_context, run_id)
      when is_map(actor_context) and is_binary(run_id) do
    with :ok <- require_operator_context(actor_context),
         {:ok, actor} <- actor_context_object(actor_context, :actor) do
      cancel_run(run_id, %{actor_id: context_id(actor), requested_by: :operator})
    end
  end

  def cancel_operator_run(_actor_context, _run_id), do: {:error, :unauthenticated}

  @doc "Returns a browser-safe operator error DTO for a public UI context."
  @spec operator_error(operator_error_context(), term()) :: OperatorErrorDTO.t()
  def operator_error(:load, reason), do: OperatorErrorDTO.load(reason)

  def operator_error(:schedule_occurrences, reason),
    do: OperatorErrorDTO.schedule_occurrences(reason)

  def operator_error(:schedule_activation, reason),
    do: OperatorErrorDTO.schedule_activation(reason)

  def operator_error(:run_cancel, reason), do: OperatorErrorDTO.run_cancel(reason)
  def operator_error(:run_failure_detail, reason), do: OperatorErrorDTO.run_failure_detail(reason)

  @doc """
  Submits a rerun pinned to the source run's manifest version.
  """
  @spec rerun(run_id(), keyword()) :: {:ok, run_id()} | {:error, term()}
  def rerun(source_run_id, opts \\ []) when is_binary(source_run_id) and is_list(opts) do
    RunManager.rerun(source_run_id, opts)
  end

  defp ensure_window_rerunnable(window, opts) when is_list(opts) do
    if Keyword.get(opts, :allow_success, false) == true do
      ensure_success_rerun_refresh(opts)
    else
      ensure_window_rerunnable(window)
    end
  end

  defp ensure_window_rerunnable(window, _opts), do: ensure_window_rerunnable(window)

  defp ensure_success_rerun_refresh(opts) do
    case RefreshPolicy.from_opts(opts) do
      {:ok, %RefreshPolicy{mode: :force}} -> :ok
      {:ok, _policy} -> {:error, :successful_backfill_window_requires_force_refresh}
      {:error, _reason} = error -> error
    end
  end

  defp ensure_window_rerunnable(%FavnOrchestrator.Backfill.BackfillWindow{status: status})
       when status in [:error, :cancelled, :timed_out, :partial],
       do: :ok

  defp ensure_window_rerunnable(_window), do: {:error, :backfill_window_not_rerunnable}

  defp require_operator_context(actor_context) when is_map(actor_context) do
    with {:ok, actor} <- actor_context_object(actor_context, :actor),
         {:ok, session} <- actor_context_object(actor_context, :session),
         {:ok, persisted_actor, persisted_session} <- load_operator_context(actor, session),
         :ok <- ensure_operator_session_context(persisted_actor, persisted_session) do
      if Auth.has_role?(persisted_actor, :operator), do: :ok, else: {:error, :forbidden}
    else
      {:error, :missing_context} -> {:error, :unauthenticated}
      {:error, :invalid_session} -> {:error, :unauthenticated}
    end
  end

  defp require_operator_context(_actor_context), do: {:error, :unauthenticated}

  defp actor_context_object(actor_context, key) do
    case Map.get(actor_context, key) || Map.get(actor_context, Atom.to_string(key)) do
      %{id: id} = object when is_binary(id) and id != "" -> {:ok, object}
      %{"id" => id} = object when is_binary(id) and id != "" -> {:ok, object}
      _object -> {:error, :missing_context}
    end
  end

  defp load_operator_context(actor, session) do
    actor_id = context_id(actor)
    session_id = context_id(session)

    with true <- is_binary(actor_id) and actor_id != "",
         true <- is_binary(session_id) and session_id != "",
         {:ok, persisted_session} <- Storage.get_auth_session(session_id),
         true <- context_actor_id(persisted_session) == actor_id,
         {:ok, persisted_actor} <- Storage.get_auth_actor(actor_id) do
      {:ok, persisted_actor, Map.drop(persisted_session, [:token_hash])}
    else
      _error -> {:error, :invalid_session}
    end
  end

  defp context_id(%{id: id}), do: id
  defp context_id(%{"id" => id}), do: id
  defp context_id(_context), do: nil

  defp context_actor_id(%{actor_id: actor_id}), do: actor_id
  defp context_actor_id(%{"actor_id" => actor_id}), do: actor_id
  defp context_actor_id(_context), do: nil

  defp ensure_operator_session_context(actor, session) do
    actor_id = Map.get(actor, :id) || Map.get(actor, "id")
    session_actor_id = Map.get(session, :actor_id) || Map.get(session, "actor_id")
    revoked_at = Map.get(session, :revoked_at) || Map.get(session, "revoked_at")
    expires_at = Map.get(session, :expires_at) || Map.get(session, "expires_at")

    cond do
      (Map.get(actor, :status) || Map.get(actor, "status")) not in [:active, "active"] ->
        {:error, :invalid_session}

      actor_id != session_actor_id ->
        {:error, :invalid_session}

      not is_nil(revoked_at) ->
        {:error, :invalid_session}

      not match?(%DateTime{}, expires_at) ->
        {:error, :invalid_session}

      match?(%DateTime{}, expires_at) and DateTime.compare(expires_at, DateTime.utc_now()) == :lt ->
        {:error, :invalid_session}

      true ->
        :ok
    end
  end

  @doc """
  Returns one persisted run snapshot.
  """
  @spec get_run(run_id()) :: {:ok, Favn.Run.t()} | {:error, term()}
  def get_run(run_id) when is_binary(run_id) do
    case Storage.get_run(run_id) do
      {:ok, run_state} -> {:ok, Projector.project_run(run_state)}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Lists persisted run snapshots.
  """
  @spec list_runs(keyword()) :: {:ok, [Favn.Run.t()]} | {:error, term()}
  def list_runs(opts \\ []) when is_list(opts) do
    case Storage.list_runs(opts) do
      {:ok, runs} -> {:ok, Projector.project_runs(runs)}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Lists public run summaries with orchestrator-owned classification.
  """
  @spec list_run_summaries(keyword()) :: {:ok, [run_summary()]} | {:error, term()}
  def list_run_summaries(opts \\ []) when is_list(opts) do
    RunReadModel.list_run_summaries(opts)
  end

  @doc """
  Returns one public run detail with orchestrator-owned classification.
  """
  @spec get_run_detail(run_id()) :: {:ok, run_detail()} | {:error, term()}
  def get_run_detail(run_id) when is_binary(run_id) do
    RunReadModel.get_run_detail(run_id)
  end

  @doc """
  Returns bounded operator-facing run detail for the run detail page.

  By default this does not include full event streams. Pass `include: [:events]`
  with `event_limit` to request bounded events explicitly.
  """
  @spec get_operator_run_detail(run_id(), keyword()) ::
          {:ok, operator_run_detail()} | {:error, term()}
  def get_operator_run_detail(run_id, opts \\ [])

  def get_operator_run_detail(run_id, opts) when is_binary(run_id) and is_list(opts) do
    RunReadModel.get_operator_run_detail(run_id, opts)
  end

  @doc """
  Lists execution groups with orchestrator-owned run/backfill aggregation.
  """
  @spec list_execution_groups(keyword()) :: {:ok, [execution_group_summary()]} | {:error, term()}
  def list_execution_groups(filters \\ []) when is_list(filters) do
    RunReadModel.list_execution_groups(filters)
  end

  @doc """
  Returns a bounded page of execution groups with orchestrator-owned semantics.
  """
  @spec page_execution_groups(keyword()) ::
          {:ok, FavnOrchestrator.Page.t(execution_group_summary())} | {:error, term()}
  def page_execution_groups(filters \\ []) when is_list(filters) do
    RunReadModel.page_execution_groups(filters)
  end

  @doc """
  Returns one execution group detail for redesigned run views.
  """
  @spec get_execution_group_detail(run_id(), keyword()) ::
          {:ok, execution_group_detail()} | {:error, term()}
  def get_execution_group_detail(group_id, filters \\ [])
      when is_binary(group_id) and is_list(filters) do
    RunReadModel.get_execution_group_detail(group_id, filters)
  end

  @doc """
  Returns execution group detail for any run id in that group.
  """
  @spec get_execution_group_detail_for_run(run_id(), keyword()) ::
          {:ok, execution_group_detail()} | {:error, term()}
  def get_execution_group_detail_for_run(run_id, filters \\ [])
      when is_binary(run_id) and is_list(filters) do
    RunReadModel.get_execution_group_detail_for_run(run_id, filters)
  end

  @doc """
  Lists asset attempts for one execution group.
  """
  @spec list_execution_group_asset_attempts(run_id(), keyword()) ::
          {:ok, [RunReadModel.asset_attempt_summary()]} | {:error, term()}
  def list_execution_group_asset_attempts(group_id, filters \\ [])
      when is_binary(group_id) and is_list(filters) do
    RunReadModel.list_execution_group_asset_attempts(group_id, filters)
  end

  @doc """
  Lists window summaries for one execution group.
  """
  @spec list_execution_group_windows(run_id(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def list_execution_group_windows(group_id, filters \\ [])
      when is_binary(group_id) and is_list(filters) do
    RunReadModel.list_execution_group_windows(group_id, filters)
  end

  @doc """
  Lists wall-clock timeline entries for one execution group.
  """
  @spec list_execution_group_timeline(run_id(), keyword()) ::
          {:ok, [RunReadModel.timeline_entry()]} | {:error, term()}
  def list_execution_group_timeline(group_id, filters \\ [])
      when is_binary(group_id) and is_list(filters) do
    RunReadModel.list_execution_group_timeline(group_id, filters)
  end

  @doc """
  Lists persisted events for an execution group, including child/window runs.
  """
  @spec list_execution_group_events(run_id(), keyword()) ::
          {:ok, [RunEvent.t()]} | {:error, term()}
  def list_execution_group_events(group_id, filters \\ [])
      when is_binary(group_id) and is_list(filters) do
    RunReadModel.list_execution_group_events(group_id, filters)
  end

  @doc """
  Returns public log-page context for one asset step in a run.
  """
  @spec get_asset_step_log_context(run_id(), String.t()) ::
          {:ok, RunReadModel.asset_step_log_context()} | {:error, term()}
  def get_asset_step_log_context(run_id, asset_step_id)
      when is_binary(run_id) and is_binary(asset_step_id) do
    RunReadModel.get_asset_step_log_context(run_id, asset_step_id)
  end

  @doc """
  Lists persisted runs that have not reached a terminal status.
  """
  @spec list_in_flight_runs() :: {:ok, [Favn.Run.t()]} | {:error, term()}
  def list_in_flight_runs do
    with {:ok, pending} <- list_runs(status: :pending),
         {:ok, running} <- list_runs(status: :running) do
      {:ok, Enum.sort_by(pending ++ running, &run_updated_at_sort_key/1, :desc)}
    end
  end

  @doc """
  Lists persisted run events for one run.

  This public facade is bounded for operator/API callers: `:limit` defaults to
  `100` and is capped at `500`; `:after_sequence` is an optional non-negative
  cursor. Orchestrator-internal repair/projection code that intentionally needs
  an unbounded adapter read should call the storage boundary directly.
  """
  @spec list_run_events(run_id(), keyword()) :: {:ok, [RunEvent.t()]} | {:error, term()}
  def list_run_events(run_id, opts \\ []) when is_binary(run_id) and is_list(opts) do
    with {:ok, opts} <- RunEventQuery.normalize_opts(opts),
         {:ok, events} <- Storage.list_run_events(run_id, opts) do
      {:ok, Enum.map(events, &RunEvent.from_map/1)}
    end
  end

  @doc """
  Lists replayable events for one run stream after an optional persisted cursor sequence.
  """
  @spec list_run_stream_events(run_id(), keyword()) :: {:ok, [RunEvent.t()]} | {:error, term()}
  def list_run_stream_events(run_id, opts \\ []) when is_binary(run_id) and is_list(opts) do
    after_sequence = Keyword.get(opts, :after_sequence)
    limit = Keyword.get(opts, :limit, 200)

    with true <- is_integer(limit) and limit > 0,
         {:ok, cursor_valid?} <- run_event_cursor_valid?(run_id, after_sequence),
         true <- cursor_valid?,
         {:ok, events} <-
           list_run_events(run_id, after_sequence: after_sequence || 0, limit: limit) do
      case after_sequence do
        nil ->
          {:ok, events}

        sequence when is_integer(sequence) and sequence >= 0 ->
          {:ok, events}

        _ ->
          {:error, :cursor_invalid}
      end
    else
      false -> {:error, :cursor_invalid}
      {:error, _reason} = error -> error
    end
  end

  defp run_updated_at_sort_key(%{updated_at: %DateTime{} = updated_at}),
    do: DateTime.to_unix(updated_at, :microsecond)

  defp run_updated_at_sort_key(_run), do: 0

  @doc """
  Lists replayable events for the global runs stream after an optional persisted cursor.
  """
  @spec list_global_run_stream_events(keyword()) :: {:ok, [RunEvent.t()]} | {:error, term()}
  def list_global_run_stream_events(opts \\ []) when is_list(opts) do
    after_global_sequence = Keyword.get(opts, :after_global_sequence)
    limit = Keyword.get(opts, :limit, 200)

    with true <- is_integer(limit) and limit > 0,
         true <- is_nil(after_global_sequence) or is_integer(after_global_sequence),
         {:ok, events} <-
           Storage.list_global_run_events(
             after_global_sequence: after_global_sequence,
             limit: limit
           ) do
      {:ok, Enum.map(events, &RunEvent.from_map/1)}
    else
      false -> {:error, :cursor_invalid}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Subscribes the current process to one run-scoped live event stream.
  """
  @spec subscribe_run(run_id()) :: :ok | {:error, term()}
  def subscribe_run(run_id) when is_binary(run_id), do: Events.subscribe_run(run_id)
  def subscribe_run(_run_id), do: {:error, :invalid_run_id}

  @doc """
  Unsubscribes the current process from one run-scoped live event stream.
  """
  @spec unsubscribe_run(run_id()) :: :ok
  def unsubscribe_run(run_id) when is_binary(run_id), do: Events.unsubscribe_run(run_id)
  def unsubscribe_run(_run_id), do: :ok

  @doc """
  Subscribes the current process to the global runs live event stream.
  """
  @spec subscribe_runs() :: :ok | {:error, term()}
  def subscribe_runs, do: Events.subscribe_runs()

  @doc """
  Unsubscribes the current process from the global runs live event stream.
  """
  @spec unsubscribe_runs() :: :ok
  def unsubscribe_runs, do: Events.unsubscribe_runs()

  @doc """
  Reloads scheduler entries from the active manifest.
  """
  @spec reload_scheduler() :: :ok | {:error, term()}
  def reload_scheduler do
    SchedulerRuntime.reload()
  catch
    :exit, {:noproc, _} -> {:error, :scheduler_not_running}
  end

  @doc """
  Forces one scheduler evaluation tick.
  """
  @spec tick_scheduler() :: :ok | {:error, term()}
  def tick_scheduler do
    SchedulerRuntime.tick()
  catch
    :exit, {:noproc, _} -> {:error, :scheduler_not_running}
  end

  @doc """
  Lists scheduler runtime entries derived from the active manifest.
  """
  @spec scheduled_entries() :: [SchedulerEntry.t()] | {:error, term()}
  def scheduled_entries do
    SchedulerRuntime.inspect_entries()
  catch
    :exit, {:noproc, _} -> {:error, :scheduler_not_running}
  end

  @doc """
  Lists operator-facing schedule inspection entries.

  Falls back to active-manifest schedule descriptors when scheduler runtime is not running.
  """
  @spec list_schedule_entries() :: {:ok, [SchedulerEntry.t()]} | {:error, term()}
  def list_schedule_entries do
    case scheduled_entries() do
      entries when is_list(entries) -> {:ok, entries}
      {:error, :scheduler_not_running} -> list_schedule_entries_from_active_manifest()
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Returns a bounded page of operator schedule list entries.

  Supported filters are `:search`, `:activation_state`, `:runtime_state`,
  `:pipeline_module`, `:window`, `:limit`, and `:offset`.
  """
  @spec page_schedule_list_entries(keyword()) ::
          {:ok, Page.t(schedule_list_entry())} | {:error, term()}
  def page_schedule_list_entries(filters \\ []) when is_list(filters) do
    with {:ok, page_opts} <- Page.normalize_opts(filters),
         {:ok, entries} <- list_schedule_entries() do
      items =
        entries
        |> Enum.map(&ScheduleListEntry.from_scheduler_entry(schedule_entry_id(&1), &1))
        |> filter_schedule_list_entries(filters)
        |> Enum.sort_by(&{inspect(&1.pipeline_module), inspect(&1.schedule_id)})
        |> Enum.drop(Keyword.fetch!(page_opts, :offset))

      {:ok, Page.from_fetched(items, page_opts)}
    end
  end

  @doc """
  Returns one schedule inspection entry by remote schedule id.
  """
  @spec get_schedule_entry(String.t()) :: {:ok, SchedulerEntry.t()} | {:error, term()}
  def get_schedule_entry(schedule_id) when is_binary(schedule_id) do
    with {:ok, entries} <- list_schedule_entries() do
      case Enum.find(entries, &(schedule_entry_id(&1) == schedule_id)) do
        nil -> {:error, :schedule_not_found}
        %SchedulerEntry{} = entry -> {:ok, entry}
      end
    end
  end

  @doc """
  Enables a schedule for future submissions through the orchestrator facade.

  Enabling starts from the next due occurrence by moving the scheduler cursor to
  the latest due occurrence observed at command time. It does not submit missed
  catch-up work automatically.
  """
  @spec enable_schedule(String.t(), keyword()) :: {:ok, SchedulerEntry.t()} | {:error, term()}
  def enable_schedule(schedule_id, opts \\ []) when is_binary(schedule_id) and is_list(opts) do
    update_schedule_activation(schedule_id, :enabled)
  end

  @doc """
  Disables a schedule for future submissions through the orchestrator facade.

  Existing in-flight runs are not cancelled. Any queued due occurrence is cleared
  because disabled schedules do not submit future work.
  """
  @spec disable_schedule(String.t(), keyword()) :: {:ok, SchedulerEntry.t()} | {:error, term()}
  def disable_schedule(schedule_id, opts \\ []) when is_binary(schedule_id) and is_list(opts) do
    update_schedule_activation(schedule_id, :disabled)
  end

  @doc """
  Returns an orchestrator-owned preview of upcoming occurrences for one schedule.

  Options:

    * `:limit` - number of preview rows, defaults to `10` and is capped at `100`.
  """
  @spec preview_schedule_occurrences(String.t(), keyword()) ::
          {:ok, [schedule_occurrence_preview()]} | {:error, term()}
  def preview_schedule_occurrences(schedule_id, opts \\ [])
      when is_binary(schedule_id) and is_list(opts) do
    with {:ok, limit} <- normalize_occurrence_preview_limit(opts),
         {:ok, %SchedulerEntry{} = entry} <- get_schedule_entry(schedule_id),
         :ok <- validate_preview_schedule(entry) do
      now = DateTime.utc_now()

      occurrences =
        entry.cron
        |> preview_due_ats(entry.timezone, now, limit)
        |> Enum.map(&schedule_occurrence_preview(schedule_id, entry, &1))

      {:ok, occurrences}
    end
  end

  @doc """
  Returns the stable remote id for one schedule inspection entry.
  """
  @spec schedule_entry_id(SchedulerEntry.t()) :: String.t()
  def schedule_entry_id(%SchedulerEntry{} = entry) do
    pipeline_module = entry.pipeline_module || Favn.Unknown
    schedule_name = entry.schedule_id || :unknown

    "schedule:" <> Atom.to_string(pipeline_module) <> ":" <> Atom.to_string(schedule_name)
  end

  @doc """
  Returns a summary for the current schedule list page.
  """
  @spec schedule_list_summary([schedule_list_entry()]) :: map()
  def schedule_list_summary(entries) when is_list(entries) do
    %{
      total: length(entries),
      enabled: Enum.count(entries, &(&1.activation_state == :enabled)),
      pending_activation: Enum.count(entries, &(&1.activation_state == :pending_activation)),
      disabled: Enum.count(entries, &(&1.activation_state == :disabled)),
      needs_review: Enum.count(entries, &(&1.activation_state == :needs_review)),
      running: Enum.count(entries, &(&1.runtime_state == :running)),
      queued: Enum.count(entries, &(&1.runtime_state == :queued))
    }
  end

  defp run_event_cursor_valid?(_run_id, nil), do: {:ok, true}
  defp run_event_cursor_valid?(_run_id, 0), do: {:ok, true}

  defp run_event_cursor_valid?(run_id, sequence) when is_integer(sequence) and sequence > 0 do
    with {:ok, events} <- list_run_events(run_id, after_sequence: sequence - 1, limit: 1) do
      {:ok, match?([%RunEvent{sequence: ^sequence}], events)}
    end
  end

  defp run_event_cursor_valid?(_run_id, _sequence), do: {:ok, false}

  defp list_schedule_entries_from_active_manifest do
    with {:ok, manifest_version_id} <- active_manifest(),
         {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         {:ok, runtime_entries} <- ManifestEntries.discover(version, index) do
      now = DateTime.utc_now()

      result =
        runtime_entries
        |> Map.values()
        |> Enum.reduce_while({:ok, []}, fn runtime_entry, {:ok, acc} ->
          case load_schedule_entry_state(runtime_entry, now) do
            {:ok, state} ->
              runtime_entry =
                Map.put(runtime_entry, :next_due_at, next_schedule_due_at(runtime_entry, now))

              {:cont, {:ok, [SchedulerEntry.from_runtime(runtime_entry, state) | acc]}}

            {:error, reason} ->
              {:halt, {:error, reason}}
          end
        end)

      case result do
        {:ok, entries} ->
          {:ok, Enum.sort_by(entries, &{inspect(&1.pipeline_module), inspect(&1.schedule_id)})}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp load_schedule_entry_state(entry, now) do
    key = {entry.module, entry.schedule.name}

    case Storage.get_scheduler_state(key) do
      {:ok, nil} ->
        state = %Favn.Scheduler.State{
          pipeline_module: entry.module,
          schedule_id: entry.schedule.name,
          schedule_fingerprint: entry.schedule_fingerprint,
          activation_state: :pending_activation,
          updated_at: now,
          version: 1
        }

        with :ok <- Storage.put_scheduler_state(key, state) do
          {:ok, state}
        end

      {:ok, %Favn.Scheduler.State{} = state} ->
        reconcile_schedule_entry_state(key, state, entry, now)

      {:ok, state} when is_map(state) ->
        reconcile_schedule_entry_state(key, struct(Favn.Scheduler.State, state), entry, now)

      {:error, _reason} = error ->
        error
    end
  end

  defp reconcile_schedule_entry_state(key, state, entry, now) do
    next =
      cond do
        is_nil(state.activation_state) ->
          %{
            state
            | activation_state: initial_schedule_activation_state(state, entry),
              version: next_scheduler_version(state)
          }

        state.schedule_fingerprint != entry.schedule_fingerprint ->
          %Favn.Scheduler.State{
            pipeline_module: entry.module,
            schedule_id: entry.schedule.name,
            schedule_fingerprint: entry.schedule_fingerprint,
            activation_state: :needs_review,
            updated_at: now,
            version: next_scheduler_version(state)
          }

        true ->
          %{state | schedule_id: entry.schedule.name}
      end

    if next != state do
      persisted = %{next | version: next.version || next_scheduler_version(state)}

      with :ok <- Storage.put_scheduler_state(key, persisted) do
        {:ok, persisted}
      end
    else
      {:ok, next}
    end
  end

  defp initial_schedule_activation_state(state, entry) do
    has_runtime_cursor? =
      not is_nil(state.last_due_at) or not is_nil(state.last_submitted_due_at) or
        not is_nil(state.in_flight_run_id) or not is_nil(state.queued_due_at)

    cond do
      entry.schedule.active != true -> :disabled
      has_runtime_cursor? -> :enabled
      true -> :pending_activation
    end
  end

  defp next_scheduler_version(%Favn.Scheduler.State{version: version})
       when is_integer(version) and version > 0,
       do: version + 1

  defp next_scheduler_version(_state), do: 1

  defp update_schedule_activation(schedule_id, activation_state)
       when activation_state in [:enabled, :disabled] do
    with {:ok, %SchedulerEntry{} = entry} <- get_schedule_entry(schedule_id),
         :ok <- validate_activation_target(entry),
         {:ok, state} <- current_schedule_state(entry),
         :ok <- persist_schedule_activation(entry, state, activation_state),
         :ok <- reload_scheduler_if_running() do
      get_schedule_entry(schedule_id)
    end
  end

  defp validate_activation_target(%SchedulerEntry{active: true}), do: :ok
  defp validate_activation_target(%SchedulerEntry{}), do: {:error, :schedule_inactive_in_manifest}

  defp current_schedule_state(%SchedulerEntry{} = entry) do
    key = {entry.pipeline_module, entry.schedule_id}

    case Storage.get_scheduler_state(key) do
      {:ok, %Favn.Scheduler.State{} = state} -> {:ok, state}
      {:ok, state} when is_map(state) -> {:ok, struct(Favn.Scheduler.State, state)}
      {:ok, nil} -> {:ok, scheduler_state_from_entry(entry)}
      {:error, _reason} = error -> error
    end
  end

  defp scheduler_state_from_entry(%SchedulerEntry{} = entry) do
    %Favn.Scheduler.State{
      pipeline_module: entry.pipeline_module,
      schedule_id: entry.schedule_id,
      schedule_fingerprint: entry.schedule_fingerprint,
      activation_state: entry.activation_state,
      last_evaluated_at: entry.last_evaluated_at,
      last_due_at: entry.last_due_at,
      last_submitted_due_at: entry.last_submitted_due_at,
      in_flight_run_id: entry.in_flight_run_id,
      queued_due_at: entry.queued_due_at,
      last_scheduler_error: entry.last_scheduler_error,
      updated_at: entry.updated_at,
      version: 1
    }
  end

  defp persist_schedule_activation(entry, state, :enabled) do
    now = DateTime.utc_now()

    next = %{
      state
      | activation_state: :enabled,
        last_due_at: latest_schedule_due_at(entry, now) || state.last_due_at,
        queued_due_at: nil,
        last_scheduler_error: nil,
        updated_at: now,
        version: next_scheduler_version(state)
    }

    Storage.put_scheduler_state({entry.pipeline_module, entry.schedule_id}, next)
  end

  defp persist_schedule_activation(entry, state, :disabled) do
    now = DateTime.utc_now()

    next = %{
      state
      | activation_state: :disabled,
        queued_due_at: nil,
        updated_at: now,
        version: next_scheduler_version(state)
    }

    Storage.put_scheduler_state({entry.pipeline_module, entry.schedule_id}, next)
  end

  defp latest_schedule_due_at(%SchedulerEntry{cron: cron, timezone: timezone}, now)
       when is_binary(cron) and is_binary(timezone),
       do: Cron.latest_due(cron, timezone, now)

  defp latest_schedule_due_at(_entry, _now), do: nil

  defp reload_scheduler_if_running do
    case reload_scheduler() do
      :ok -> :ok
      {:error, :scheduler_not_running} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp normalize_occurrence_preview_limit(opts) do
    case Keyword.get(opts, :limit, 10) do
      value when is_integer(value) and value > 0 -> {:ok, min(value, 100)}
      value -> {:error, {:invalid_schedule_occurrence_preview_limit, value}}
    end
  end

  defp validate_preview_schedule(%SchedulerEntry{cron: cron, timezone: timezone})
       when is_binary(cron) and is_binary(timezone),
       do: :ok

  defp validate_preview_schedule(%SchedulerEntry{}), do: {:error, :schedule_preview_unavailable}

  defp preview_due_ats(cron, timezone, now, limit) do
    do_preview_due_ats(cron, timezone, now, limit, [])
  end

  defp do_preview_due_ats(_cron, _timezone, _after_at, 0, acc), do: Enum.reverse(acc)

  defp do_preview_due_ats(cron, timezone, after_at, remaining, acc) do
    case Cron.next_due(cron, timezone, after_at) do
      %DateTime{} = due_at ->
        do_preview_due_ats(cron, timezone, DateTime.add(due_at, 1, :second), remaining - 1, [
          due_at | acc
        ])

      nil ->
        Enum.reverse(acc)
    end
  end

  defp schedule_occurrence_preview(schedule_id, entry, due_at) do
    {window, window_error} = occurrence_window(entry.window, due_at, entry.timezone)

    %ScheduleOccurrencePreview{
      schedule_entry_id: schedule_id,
      due_at: due_at,
      timezone: entry.timezone,
      window: window,
      status: occurrence_status(entry, due_at, window_error),
      notes: occurrence_notes(entry, window_error)
    }
  end

  defp occurrence_window(nil, _due_at, _timezone), do: {nil, nil}

  defp occurrence_window(%Policy{} = policy, due_at, timezone) do
    with {:ok, policy} <- Policy.validate(policy),
         {:ok, window} <- Policy.resolve_scheduled(policy, due_at, timezone) do
      {window_to_map(window), nil}
    else
      {:error, reason} -> {nil, reason}
    end
  end

  defp occurrence_window(_window, _due_at, _timezone), do: {nil, :invalid_scheduler_window}

  defp window_to_map(window) when is_map(window) do
    %{
      kind: Map.get(window, :kind),
      start_at: Map.get(window, :start_at),
      end_at: Map.get(window, :end_at),
      timezone: Map.get(window, :timezone),
      key: Map.get(window, :key),
      anchor_key: Map.get(window, :anchor_key)
    }
  end

  defp occurrence_status(_entry, _due_at, window_error) when not is_nil(window_error),
    do: :blocked

  defp occurrence_status(%SchedulerEntry{effective_enabled?: false}, _due_at, _window_error),
    do: :disabled

  defp occurrence_status(
         %SchedulerEntry{queued_due_at: %DateTime{} = queued_due_at},
         due_at,
         _window_error
       ) do
    if DateTime.compare(queued_due_at, due_at) == :eq, do: :queued, else: :upcoming
  end

  defp occurrence_status(
         %SchedulerEntry{
           in_flight_run_id: run_id,
           last_submitted_due_at: %DateTime{} = submitted_at
         },
         due_at,
         _window_error
       )
       when is_binary(run_id) do
    if DateTime.compare(submitted_at, due_at) == :eq, do: :running, else: :upcoming
  end

  defp occurrence_status(_entry, _due_at, _window_error), do: :upcoming

  defp occurrence_notes(entry, window_error) do
    []
    |> maybe_note(entry.effective_enabled? == false, "Will not submit until enabled")
    |> maybe_note(entry.activation_state == :needs_review, "Schedule change needs review")
    |> maybe_note(
      not is_nil(window_error),
      "Window preview unavailable: #{inspect(window_error)}"
    )
    |> Enum.reverse()
  end

  defp maybe_note(notes, true, note), do: [note | notes]
  defp maybe_note(notes, false, _note), do: notes

  defp next_schedule_due_at(%{schedule: %{cron: cron, timezone: timezone}}, now)
       when is_binary(cron) and is_binary(timezone),
       do: Cron.next_due(cron, timezone, now)

  defp next_schedule_due_at(_entry, _now), do: nil

  defp filter_schedule_list_entries(entries, filters) do
    Enum.filter(entries, fn entry ->
      schedule_matches_search?(entry, Keyword.get(filters, :search)) and
        schedule_matches_atom?(entry.activation_state, Keyword.get(filters, :activation_state)) and
        schedule_matches_atom?(entry.runtime_state, Keyword.get(filters, :runtime_state)) and
        schedule_matches_pipeline?(entry, Keyword.get(filters, :pipeline_module)) and
        schedule_matches_window?(entry, Keyword.get(filters, :window))
    end)
  end

  defp schedule_matches_search?(_entry, value) when value in [nil, "", :all], do: true

  defp schedule_matches_search?(entry, value) do
    search = value |> to_string() |> String.downcase() |> String.trim()

    [entry.id, inspect(entry.pipeline_module), inspect(entry.schedule_id)]
    |> Enum.join(" ")
    |> String.downcase()
    |> String.contains?(search)
  end

  defp schedule_matches_atom?(_actual, value) when value in [nil, "", :all, "all"], do: true
  defp schedule_matches_atom?(actual, value) when is_atom(value), do: actual == value
  defp schedule_matches_atom?(actual, value), do: Atom.to_string(actual) == to_string(value)

  defp schedule_matches_pipeline?(_entry, value) when value in [nil, "", :all, "all"], do: true

  defp schedule_matches_pipeline?(entry, value),
    do: inspect(entry.pipeline_module) == to_string(value)

  defp schedule_matches_window?(_entry, value) when value in [nil, "", :all, "all"], do: true
  defp schedule_matches_window?(%{window: nil}, value), do: to_string(value) == "none"

  defp schedule_matches_window?(%{window: %{kind: kind}}, value),
    do: Atom.to_string(kind) == to_string(value)

  defp schedule_matches_window?(%{window: window}, value), do: inspect(window) == to_string(value)

  defp manifest_summary(%Version{} = version) do
    manifest = version.manifest

    %{
      manifest_version_id: version.manifest_version_id,
      content_hash: version.content_hash,
      asset_count: list_count(manifest.assets),
      pipeline_count: list_count(manifest.pipelines),
      schedule_count: list_count(manifest.schedules)
    }
  end

  defp manifest_asset_targets(%Version{} = version) do
    version.manifest.assets
    |> List.wrap()
    |> Enum.map(&manifest_asset_target/1)
    |> Enum.sort_by(& &1.label)
  end

  defp target_statuses(manifest_version_id, target_kind, targets) do
    target_ids = Enum.map(targets, & &1.target_id)

    with {:ok, statuses} <-
           Storage.list_target_statuses(manifest_version_id, target_kind, target_ids) do
      {:ok,
       Map.new(targets, fn target ->
         status =
           Map.get(statuses, target.target_id) ||
             TargetStatus.unknown(
               manifest_version_id,
               target_kind,
               target.target_id,
               target_ref_text(target_kind, target)
             )

         {target.target_id, status}
       end)}
    end
  end

  defp target_status(manifest_version_id, target_kind, target_id) do
    case Storage.get_target_status(manifest_version_id, target_kind, target_id) do
      {:ok, %TargetStatus{} = status} -> {:ok, status}
      {:error, :not_found} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp target_ref_text(:asset, target), do: Map.fetch!(target, :asset_ref)
  defp target_ref_text(:pipeline, target), do: Map.fetch!(target, :label)

  defp pipeline_for_target(%Version{} = version, target_id) do
    version.manifest.pipelines
    |> List.wrap()
    |> Enum.find(&(manifest_pipeline_target(&1).target_id == target_id))
    |> case do
      nil -> {:error, :not_found}
      pipeline -> {:ok, pipeline}
    end
  end

  defp asset_for_target(%Version{} = version, target_id) do
    version.manifest.assets
    |> List.wrap()
    |> Enum.find(&(manifest_asset_target(&1).target_id == target_id))
    |> case do
      nil -> {:error, :not_found}
      asset -> {:ok, asset}
    end
  end

  defp detail_freshness_states(manifest_version_id) do
    case list_asset_freshness(manifest_version_id: manifest_version_id, limit: Page.max_limit()) do
      {:ok, page} -> {:ok, page.items}
      {:error, :asset_freshness_state_not_supported} -> {:ok, []}
      {:error, reason} -> {:error, reason}
    end
  end

  defp detail_asset_window_states(manifest_version_id) do
    case list_asset_window_states(
           manifest_version_id: manifest_version_id,
           limit: Page.max_limit()
         ) do
      {:ok, page} -> {:ok, page.items}
      {:error, reason} -> {:error, reason}
    end
  end

  defp asset_catalogue_entries(%Version{} = version, statuses) do
    version.manifest.assets
    |> List.wrap()
    |> Enum.map(fn asset ->
      target = manifest_asset_target(asset)
      status = Map.fetch!(statuses, target.target_id)
      put_target_status(target, status)
    end)
    |> Enum.sort_by(& &1.label)
  end

  defp pipeline_catalogue_entries(%Version{} = version, %Index{} = index, statuses) do
    version.manifest.pipelines
    |> List.wrap()
    |> Enum.map(fn pipeline ->
      target = manifest_pipeline_target(index, pipeline)
      status = Map.fetch!(statuses, target.target_id)
      put_target_status(target, status)
    end)
    |> Enum.sort_by(& &1.label)
  end

  defp pipeline_detail_entry(%Version{} = version, %Index{} = index, target_id, status, runs) do
    version.manifest.pipelines
    |> List.wrap()
    |> Enum.find(&(manifest_pipeline_target(&1).target_id == target_id))
    |> case do
      nil ->
        nil

      pipeline ->
        target = manifest_pipeline_target(index, pipeline)
        pipeline_runs = pipeline_runs(pipeline, target, runs)

        status =
          status ||
            TargetStatus.unknown(version.manifest_version_id, :pipeline, target_id, target.label)

        target
        |> Map.put(:manifest_version_id, version.manifest_version_id)
        |> put_target_status(status)
        |> Map.put(:runs, Enum.map(pipeline_runs, &pipeline_run_history_entry/1))
    end
  end

  defp asset_detail_entry(
         %Version{} = version,
         target_id,
         status,
         freshness_states,
         asset_window_states,
         runs,
         opts
       ) do
    version.manifest.assets
    |> List.wrap()
    |> Enum.find(&(manifest_asset_target(&1).target_id == target_id))
    |> case do
      nil ->
        nil

      asset ->
        target = manifest_asset_target(asset)
        ref_string = ref_to_string(asset.ref)
        latest_freshness = latest_freshness_for_ref(freshness_states, ref_string)
        latest_run = latest_run_for_ref(runs, ref_string)
        runs_by_id = Map.new(runs, &{&1.id, &1})

        status =
          status ||
            TargetStatus.unknown(version.manifest_version_id, :asset, target_id, ref_string)

        {refresh_kind, refresh_timezone} = detail_refresh_policy(version, asset)
        {data_coverage_kind, _data_coverage_timezone} = detail_timeline_policy(asset)

        refresh_timeline =
          asset_refresh_timeline(
            version,
            asset,
            latest_freshness,
            latest_run,
            freshness_states,
            runs_by_id,
            opts
          )

        data_coverage_timeline =
          asset_data_coverage_timeline(
            asset,
            latest_freshness,
            latest_run,
            freshness_states,
            asset_window_states,
            runs_by_id,
            opts
          )

        target
        |> Map.take([:target_id, :label, :asset_ref, :relation, :type, :window])
        |> Map.put(:manifest_version_id, version.manifest_version_id)
        |> Map.put(:canonical_asset_ref, asset.ref)
        |> Map.put(:name, asset_detail_name(target))
        |> put_target_status(status)
        |> Map.put(:freshness, asset_freshness_detail(asset, version, freshness_states, opts))
        |> Map.put(:refresh_timeline_label, timeline_kind_label(refresh_kind, "refresh periods"))
        |> Map.put(
          :refresh_cadence_label,
          "#{timeline_kind_label(refresh_kind, "refresh")} #{refresh_timezone}"
        )
        |> Map.put(
          :data_coverage_timeline_label,
          timeline_kind_label(data_coverage_kind, "data windows")
        )
        |> Map.put(:refresh_timeline, refresh_timeline)
        |> Map.put(:data_coverage_timeline, data_coverage_timeline)
        |> Map.put(:has_data_windows?, not is_nil(data_coverage_timeline))
        |> Map.put(:can_run_asset?, true)
        |> Map.put(:timeline, data_coverage_timeline || refresh_timeline)
    end
  end

  defp asset_freshness_detail(asset, version, freshness_states, opts) do
    policy = asset_freshness_policy(asset)
    now = Keyword.get(opts, :now, DateTime.utc_now())

    cond do
      policy.kind == :always ->
        freshness_detail(
          :always_run,
          policy,
          nil,
          "Freshness is intentionally bypassed; this asset runs whenever it is planned.",
          [
            %{kind: :always_run, message: "Manifest policy is always run."}
          ]
        )

      policy.kind == :none ->
        freshness_detail(
          :unknown,
          policy,
          nil,
          "No freshness policy is declared for this asset.",
          [
            %{kind: :no_freshness_policy, message: "No freshness policy is declared."}
          ]
        )

      true ->
        classify_asset_freshness(asset, version, freshness_states, policy, now)
    end
  end

  defp classify_asset_freshness(asset, version, freshness_states, policy, now) do
    with {:ok, plan} <- asset_freshness_plan(asset, version, now),
         {:ok, target_node_key} <- asset_freshness_target_node_key(plan, asset.ref) do
      states = freshness_state_lookup(freshness_states)
      assets_by_ref = Map.new(version.manifest.assets, &{&1.ref, &1})

      decision =
        FreshnessDecider.decide(plan, target_node_key,
          assets_by_ref: assets_by_ref,
          prior_states: states,
          current_states: states,
          now: now
        )

      state = Map.get(states, {asset.ref, Map.fetch!(decision, :freshness_key)})
      freshness_detail_from_decision(asset, policy, state, decision)
    else
      {:error, _reason} ->
        freshness_detail(
          :unknown,
          policy,
          nil,
          "Freshness state exists, but backend could not explain whether it is stale.",
          [
            %{
              kind: :insufficient_state,
              message: "Backend could not build a staleness explanation from available state."
            }
          ]
        )
    end
  end

  defp freshness_detail_from_decision(_asset, policy, state, %{decision: :skipped_fresh}) do
    freshness_detail(
      :fresh,
      policy,
      latest_success_detail(state),
      "Backend freshness state currently satisfies this asset's policy.",
      [
        %{
          kind: :policy_fresh,
          message: "Backend freshness state satisfies the declared policy."
        }
      ]
    )
  end

  defp freshness_detail_from_decision(_asset, policy, state, %{decision: :run})
       when is_nil(state) or is_nil(state.latest_success_run_id) do
    freshness_detail(
      :unknown,
      policy,
      nil,
      "No successful freshness evidence exists for this asset yet.",
      [
        %{
          kind: :never_run,
          message: "No successful freshness-producing run has been recorded."
        }
      ]
    )
  end

  defp freshness_detail_from_decision(asset, policy, state, %{
         decision: :run,
         reason: :upstream_version_changed,
         stale_reasons: stale_reasons
       }) do
    reasons = Enum.map(stale_reasons, &asset_freshness_reason/1)

    freshness_detail(
      :stale,
      policy,
      latest_success_detail(state),
      stale_explanation(asset, reasons),
      reasons
    )
  end

  defp freshness_detail_from_decision(_asset, policy, state, %{
         decision: :run,
         reason: :freshness_expired
       }) do
    freshness_detail(
      :stale,
      policy,
      latest_success_detail(state),
      "Stored freshness evidence no longer satisfies this asset's policy.",
      [
        %{
          kind: :freshness_expired,
          message: "Stored freshness evidence no longer satisfies the declared policy."
        }
      ]
    )
  end

  defp freshness_detail_from_decision(_asset, policy, state, %{decision: :run, reason: reason}) do
    freshness_detail(
      :stale,
      policy,
      latest_success_detail(state),
      "Backend freshness policy requires this asset to run.",
      [
        %{
          kind: reason,
          message: "Backend freshness policy requires this asset to run."
        }
      ]
    )
  end

  defp asset_freshness_plan(asset, version, now) do
    with {:ok, index} <- Index.build_from_version(version) do
      opts = [dependencies: :all, planning_index: index.planning_index]

      opts =
        case asset_current_anchor_window(asset, now) do
          {:ok, anchor_window} -> Keyword.put(opts, :anchor_window, anchor_window)
          :error -> opts
        end

      Planner.plan(asset.ref, opts)
    end
  end

  defp asset_current_anchor_window(%{window: %WindowSpec{} = spec}, now) do
    with {:ok, period} <- Favn.TimePeriod.current(spec.kind, now, spec.timezone) do
      {:ok, Anchor.new!(period.kind, period.start_at, period.end_at, timezone: period.timezone)}
    end
  end

  defp asset_current_anchor_window(_asset, _now), do: :error

  defp asset_freshness_target_node_key(plan, asset_ref) do
    Enum.find(plan.target_node_keys, fn {ref, _window_key} -> ref == asset_ref end)
    |> case do
      nil -> {:error, :target_node_key_not_found}
      node_key -> {:ok, node_key}
    end
  end

  defp freshness_state_lookup(freshness_states) do
    Enum.reduce(freshness_states, %{}, fn %AssetFreshnessState{} = state, acc ->
      ref = {state.asset_ref_module, state.asset_ref_name}

      acc
      |> Map.put(state.latest_success_node_key, state)
      |> Map.put({ref, state.freshness_key}, state)
      |> Map.put(ref_to_string(ref) <> ":" <> state.freshness_key, state)
    end)
  end

  defp freshness_detail(state, policy, latest_success, explanation, reasons) do
    %{
      state: state,
      policy: policy,
      latest_success: latest_success,
      explanation: explanation,
      reasons: reasons
    }
  end

  defp latest_success_detail(nil), do: nil

  defp latest_success_detail(%AssetFreshnessState{latest_success_run_id: nil}), do: nil

  defp latest_success_detail(%AssetFreshnessState{} = state) do
    %{
      run_id: state.latest_success_run_id,
      at: state.latest_success_at,
      freshness_key: state.freshness_key
    }
  end

  defp asset_freshness_policy(%{freshness: nil}), do: %{kind: :none, label: "no freshness policy"}

  defp asset_freshness_policy(%{
         freshness: %Favn.Freshness.Policy{mode: :calendar_period, kind: :day, timezone: timezone}
       }),
       do: %{kind: :daily, label: "daily #{timezone || "Etc/UTC"}"}

  defp asset_freshness_policy(%{
         freshness: %Favn.Freshness.Policy{mode: :max_age, amount: amount, unit: unit}
       }),
       do: %{kind: :max_age, label: "max age #{amount} #{pluralize(unit, amount)}"}

  defp asset_freshness_policy(%{freshness: %Favn.Freshness.Policy{mode: :window_success}}),
    do: %{kind: :window_success, label: "window success"}

  defp asset_freshness_policy(%{freshness: %Favn.Freshness.Policy{mode: :always}}),
    do: %{kind: :always, label: "always run"}

  defp asset_freshness_policy(_asset), do: %{kind: :none, label: "no freshness policy"}

  defp asset_freshness_reason(%{type: :upstream_version_changed} = reason) do
    upstream_ref = Map.get(reason, :upstream_ref)
    label = ref_display_name(upstream_ref)

    %{
      kind: :upstream_version_changed,
      message: "#{label} refreshed after this asset last consumed it.",
      upstream_ref: ref_to_string(upstream_ref),
      previous_version: Map.get(reason, :consumed_version),
      current_version: Map.get(reason, :current_version),
      run_id: Map.get(reason, :current_success_run_id)
    }
  end

  defp asset_freshness_reason(%{type: :missing_upstream_version} = reason) do
    upstream_ref = Map.get(reason, :upstream_ref)
    label = ref_display_name(upstream_ref)

    %{
      kind: :upstream_missing,
      message: "#{label} has no current upstream freshness version available.",
      upstream_ref: ref_to_string(upstream_ref),
      previous_version: Map.get(reason, :consumed_version),
      current_version: nil,
      run_id: nil
    }
  end

  defp asset_freshness_reason(reason) do
    %{
      kind: :unknown,
      message: "Backend returned an unrecognized stale reason: #{inspect(reason, limit: 5)}."
    }
  end

  defp stale_explanation(asset, []),
    do: "#{ref_display_name(asset.ref)} is stale according to backend freshness state."

  defp stale_explanation(asset, [reason | _reasons]) do
    "#{ref_display_name(asset.ref)} is stale because #{String.downcase(reason.message)}"
  end

  defp ref_display_name({module, name}) when is_atom(module) and is_atom(name) do
    module
    |> inspect()
    |> String.split(".")
    |> List.last()
    |> Kernel.<>(".#{name}")
  end

  defp ref_display_name(ref), do: ref_to_string(ref)

  defp pluralize(unit, 1), do: Atom.to_string(unit)
  defp pluralize(unit, _amount), do: Atom.to_string(unit) <> "s"

  defp latest_freshness_for_ref(freshness_states, ref_string) do
    Enum.find(freshness_states, fn state ->
      freshness_ref_string(state) == ref_string &&
        state.freshness_key == Favn.Freshness.Key.latest()
    end)
  end

  defp latest_run_for_ref(runs, ref_string) do
    runs
    |> Enum.flat_map(&run_ref_entries/1)
    |> Enum.filter(fn {run_ref_string, _run} -> run_ref_string == ref_string end)
    |> Enum.map(fn {_run_ref_string, run} -> run end)
    |> latest_run()
  end

  defp asset_data_coverage_timeline(
         %{window: nil},
         _latest_freshness,
         _latest_run,
         _freshness_states,
         _asset_window_states,
         _runs_by_id,
         _opts
       ),
       do: nil

  defp asset_data_coverage_timeline(
         asset,
         latest_freshness,
         latest_run,
         freshness_states,
         asset_window_states,
         runs_by_id,
         opts
       ) do
    {kind, timezone} = detail_timeline_policy(asset)

    selected_value =
      detail_timeline_selected_value(kind, timezone, latest_freshness, latest_run, opts)

    window_states = asset_window_state_by_date(asset, asset_window_states, kind, timezone)
    freshness_states = asset_window_freshness_by_date(asset, freshness_states, kind)

    latest_run_value =
      latest_run_at(latest_freshness, latest_run)
      |> detail_value_from_datetime(kind, timezone)

    for offset <- 0..29 do
      value = shift_timeline_value(kind, timezone, selected_value, offset - 29)
      date = timeline_value_date(kind, value)
      window_state = Map.get(window_states, value)
      window_freshness = Map.get(freshness_states, value)

      %{
        id: timeline_window_id(kind, value),
        kind: kind,
        value: value,
        timezone: timezone,
        label: timeline_window_label(kind, value),
        date: date,
        range: timeline_window_range(kind, value),
        status:
          data_coverage_timeline_status(
            window_state,
            window_freshness,
            latest_freshness,
            latest_run,
            value,
            latest_run_value
          ),
        latest_run_id:
          asset_window_latest_run_id(window_state) || latest_run_id(window_freshness, nil),
        latest_run_status:
          asset_window_latest_run_status(window_state) || latest_run_status(window_freshness, nil),
        latest_run_at:
          asset_window_latest_run_at(window_state) || latest_run_at(window_freshness, nil),
        run_label: "Run this window"
      }
      |> put_window_run_state(asset)
      |> maybe_put_latest_run(latest_freshness, latest_run, value, latest_run_value)
      |> Map.put(:source, :data_coverage_timeline)
      |> Map.put(
        :default_run_config,
        default_timeline_run_config(:data_coverage_timeline, kind, value, timezone)
      )
      |> put_latest_run_config(runs_by_id)
    end
  end

  defp asset_refresh_timeline(
         version,
         asset,
         latest_freshness,
         latest_run,
         freshness_states,
         runs_by_id,
         opts
       ) do
    {kind, timezone} = detail_refresh_policy(version, asset)

    selected_value =
      detail_timeline_selected_value(kind, timezone, latest_freshness, latest_run, opts)

    freshness_by_value = asset_window_freshness_by_date(asset, freshness_states, kind)

    latest_run_value =
      latest_run_at(latest_freshness, latest_run)
      |> detail_value_from_datetime(kind, timezone)

    for offset <- 0..29 do
      value = shift_timeline_value(kind, timezone, selected_value, offset - 29)
      freshness = Map.get(freshness_by_value, value)

      %{
        id: "refresh:#{kind}:#{value}",
        source: :refresh_timeline,
        kind: kind,
        value: value,
        timezone: timezone,
        label: timeline_window_label(kind, value),
        date: timeline_value_date(kind, value),
        range: timeline_window_range(kind, value),
        status:
          refresh_timeline_status(
            freshness,
            latest_freshness,
            latest_run,
            value,
            latest_run_value
          ),
        latest_run_id: latest_run_id(freshness, nil),
        latest_run_status: latest_run_status(freshness, nil),
        latest_run_at: latest_run_at(freshness, nil),
        run_enabled?: true,
        run_disabled_reason: nil,
        run_label: "Run asset",
        default_run_config: default_timeline_run_config(:refresh_timeline, kind, value, timezone)
      }
      |> maybe_put_latest_run(latest_freshness, latest_run, value, latest_run_value)
      |> put_latest_run_config(runs_by_id)
    end
  end

  defp timeline_kind_label(:hour, suffix), do: "Hourly #{suffix}"
  defp timeline_kind_label(:day, suffix), do: "Daily #{suffix}"
  defp timeline_kind_label(:month, suffix), do: "Monthly #{suffix}"
  defp timeline_kind_label(:year, suffix), do: "Yearly #{suffix}"
  defp timeline_kind_label(_kind, suffix), do: "#{String.capitalize(suffix)}"

  defp detail_refresh_policy(version, asset) do
    version.manifest.pipelines
    |> List.wrap()
    |> Enum.find(fn pipeline -> asset.ref in List.wrap(pipeline.selectors) end)
    |> case do
      %{window: %Policy{kind: kind, timezone: timezone}} -> {kind, timezone || "Etc/UTC"}
      %{window: nil} -> {:day, "Etc/UTC"}
      _pipeline -> {:day, "Etc/UTC"}
    end
  end

  defp refresh_timeline_status(
         %AssetFreshnessState{status: :ok},
         _latest_freshness,
         _latest_run,
         _value,
         _latest_run_value
       ),
       do: :fresh

  defp refresh_timeline_status(
         %AssetFreshnessState{status: :error},
         _latest_freshness,
         _latest_run,
         _value,
         _latest_run_value
       ),
       do: :failed

  defp refresh_timeline_status(
         %AssetFreshnessState{status: status},
         _latest_freshness,
         _latest_run,
         _value,
         _latest_run_value
       )
       when status in [:running, :pending], do: :running

  defp refresh_timeline_status(nil, latest_freshness, latest_run, value, value),
    do: refresh_status_from_latest(latest_freshness, latest_run)

  defp refresh_timeline_status(nil, _latest_freshness, _latest_run, _value, _latest_run_value),
    do: :missing

  defp refresh_status_from_latest(%AssetFreshnessState{status: :ok}, _run), do: :fresh
  defp refresh_status_from_latest(%AssetFreshnessState{status: :error}, _run), do: :failed

  defp refresh_status_from_latest(_freshness, %{status: status})
       when status in [:running, :pending], do: :running

  defp refresh_status_from_latest(_freshness, %{status: :ok}), do: :fresh

  defp refresh_status_from_latest(_freshness, %{status: status})
       when status in [:partial, :error, :cancelled, :timed_out],
       do: :failed

  defp refresh_status_from_latest(_freshness, _run), do: :unknown

  defp default_timeline_run_config(source, kind, value, timezone) do
    %{
      source: source,
      kind: kind,
      value: value,
      timezone: timezone,
      dependencies: :all,
      refresh: :auto
    }
  end

  defp put_latest_run_config(%{latest_run_id: run_id} = window, runs_by_id)
       when is_binary(run_id) do
    case Map.get(runs_by_id, run_id) do
      nil ->
        window

      run ->
        Map.put(window, :latest_run_config, run_config_from_run(run, window.default_run_config))
    end
  end

  defp put_latest_run_config(window, _runs_by_id), do: window

  defp run_config_from_run(run, default_config) do
    metadata = Map.get(run, :metadata, %{}) || %{}

    default_config
    |> Map.put(:dependencies, Map.get(metadata, :asset_dependencies, default_config.dependencies))
    |> Map.put(
      :refresh,
      refresh_config_from_metadata(Map.get(metadata, :refresh_policy), default_config.refresh)
    )
  end

  defp refresh_config_from_metadata(%{mode: :auto}, _default), do: :auto
  defp refresh_config_from_metadata(%{mode: :missing}, _default), do: :missing
  defp refresh_config_from_metadata(%{mode: :force}, _default), do: :force

  defp refresh_config_from_metadata(%{mode: :force_assets, include_upstream?: true}, _default),
    do: :force_selected_upstream

  defp refresh_config_from_metadata(%{mode: :force_assets}, _default), do: :force_selected
  defp refresh_config_from_metadata(_refresh_policy, default), do: default

  defp detail_timeline_policy(%{window: %WindowSpec{kind: kind, timezone: timezone}}),
    do: {kind, timezone}

  defp detail_timeline_policy(%{window: window}) when is_atom(window) do
    case normalize_window_kind(window) do
      {:ok, kind} -> {kind, "Etc/UTC"}
      {:error, _reason} -> {:day, "Etc/UTC"}
    end
  end

  defp detail_timeline_policy(%{window: %{} = window}) do
    kind = Map.get(window, :kind) || Map.get(window, "kind")
    timezone = Map.get(window, :timezone) || Map.get(window, "timezone") || "Etc/UTC"

    case normalize_window_kind(kind) do
      {:ok, kind} -> {kind, timezone}
      {:error, _reason} -> {:day, "Etc/UTC"}
    end
  end

  defp detail_timeline_policy(_asset), do: {:day, "Etc/UTC"}

  defp put_window_run_state(window, %{window: nil}) do
    window
    |> Map.put(:run_enabled?, false)
    |> Map.put(:run_disabled_reason, :asset_has_no_window_policy)
  end

  defp put_window_run_state(%{id: window_id} = window, asset) do
    with {:ok, window_request} <- window_request_from_id(window_id),
         {:ok, _anchor_window} <- resolve_asset_window(asset, window_request) do
      window
      |> Map.put(:run_enabled?, true)
      |> Map.put(:run_disabled_reason, nil)
    else
      {:error, reason} ->
        window
        |> Map.put(:run_enabled?, false)
        |> Map.put(:run_disabled_reason, run_disabled_reason(reason))
    end
  end

  defp run_disabled_reason({:window_request_without_policy, _kind}),
    do: :asset_has_no_window_policy

  defp run_disabled_reason(_reason), do: :invalid_window

  defp detail_timeline_selected_value(kind, timezone, latest_freshness, latest_run, opts) do
    case {opts[:now], opts[:today], latest_run_at(latest_freshness, latest_run)} do
      {%DateTime{} = now, _today, _latest_run_at} ->
        timeline_value_from_datetime(kind, timezone, now)

      {_now, %Date{} = date, _latest_run_at} ->
        timeline_value_from_date(kind, date)

      {_now, _today, %DateTime{} = datetime} ->
        timeline_value_from_datetime(kind, timezone, datetime)

      _other ->
        timeline_value_from_date(kind, Date.utc_today())
    end
  end

  defp asset_window_freshness_by_date(asset, freshness_states, timeline_kind) do
    asset_ref_string = ref_to_string(asset.ref)

    freshness_states
    |> Enum.filter(&(freshness_ref_string(&1) == asset_ref_string))
    |> Enum.flat_map(fn state ->
      case window_date_from_freshness_key(state.freshness_key) do
        {^timeline_kind, value} -> [{value, state}]
        _other -> []
      end
    end)
    |> Map.new(fn {date, state} -> {date, state} end)
  end

  defp asset_window_state_by_date(asset, asset_window_states, timeline_kind, timezone) do
    {asset_ref_module, asset_ref_name} = asset.ref

    asset_window_states
    |> Enum.filter(fn
      %AssetWindowState{
        asset_ref_module: ^asset_ref_module,
        asset_ref_name: ^asset_ref_name,
        window_kind: ^timeline_kind
      } ->
        true

      _other ->
        false
    end)
    |> Map.new(fn %AssetWindowState{} = state ->
      {timeline_value_from_datetime(timeline_kind, timezone, state.window_start_at), state}
    end)
  end

  defp data_coverage_timeline_status(
         %AssetWindowState{status: :ok},
         _window_freshness,
         _latest_freshness,
         _latest_run,
         _value,
         _latest_run_value
       ),
       do: :covered

  defp data_coverage_timeline_status(
         %AssetWindowState{status: status},
         _window_freshness,
         _latest_freshness,
         _latest_run,
         _value,
         _latest_run_value
       )
       when status in [:running, :pending],
       do: :running

  defp data_coverage_timeline_status(
         %AssetWindowState{status: status},
         _window_freshness,
         _latest_freshness,
         _latest_run,
         _value,
         _latest_run_value
       )
       when status in [:partial, :error, :cancelled, :timed_out],
       do: :failed

  defp data_coverage_timeline_status(
         _window_state,
         window_freshness,
         latest_freshness,
         latest_run,
         value,
         latest_run_value
       ) do
    timeline_status(window_freshness, latest_freshness, latest_run, value, latest_run_value)
  end

  defp asset_window_latest_run_id(%AssetWindowState{latest_run_id: run_id})
       when is_binary(run_id),
       do: run_id

  defp asset_window_latest_run_id(_state), do: nil

  defp asset_window_latest_run_status(%AssetWindowState{status: status}) when not is_nil(status),
    do: status

  defp asset_window_latest_run_status(_state), do: nil

  defp asset_window_latest_run_at(%AssetWindowState{updated_at: %DateTime{} = updated_at}),
    do: updated_at

  defp asset_window_latest_run_at(_state), do: nil

  defp window_date_from_freshness_key("calendar:day:" <> rest) do
    rest
    |> String.split(":")
    |> List.last()
    |> case do
      <<_::binary-size(10)>> = value -> {:day, value}
      _other -> nil
    end
  end

  defp window_date_from_freshness_key("calendar:month:" <> rest) do
    rest
    |> String.split(":")
    |> List.last()
    |> case do
      <<_year::binary-size(4), "-", _month::binary-size(2)>> = value ->
        {:month, value}

      _other ->
        nil
    end
  end

  defp window_date_from_freshness_key("calendar:year:" <> rest) do
    rest
    |> String.split(":")
    |> List.last()
    |> case do
      <<_year::binary-size(4)>> = value -> {:year, value}
      _other -> nil
    end
  end

  defp window_date_from_freshness_key("calendar:hour:" <> rest) do
    rest
    |> String.split(":")
    |> List.last()
    |> case do
      <<_date::binary-size(10), "T", _hour::binary-size(2)>> = value -> {:hour, value}
      _other -> nil
    end
  end

  defp window_date_from_freshness_key(_key), do: nil

  defp detail_value_from_datetime(nil, _kind, _timezone), do: nil

  defp detail_value_from_datetime(%DateTime{} = datetime, kind, timezone),
    do: timeline_value_from_datetime(kind, timezone, datetime)

  defp timeline_value_from_datetime(:hour, timezone, %DateTime{} = datetime) do
    datetime
    |> DateTime.shift_zone!(timezone, Favn.Timezone.database!())
    |> then(&"#{Date.to_iso8601(DateTime.to_date(&1))}T#{pad2(&1.hour)}")
  end

  defp timeline_value_from_datetime(:day, timezone, %DateTime{} = datetime) do
    datetime
    |> DateTime.shift_zone!(timezone, Favn.Timezone.database!())
    |> DateTime.to_date()
    |> Date.to_iso8601()
  end

  defp timeline_value_from_datetime(:month, timezone, %DateTime{} = datetime) do
    datetime
    |> DateTime.shift_zone!(timezone, Favn.Timezone.database!())
    |> then(&format_month(&1.year, &1.month))
  end

  defp timeline_value_from_datetime(:year, timezone, %DateTime{} = datetime) do
    datetime
    |> DateTime.shift_zone!(timezone, Favn.Timezone.database!())
    |> then(&Integer.to_string(&1.year))
  end

  defp timeline_value_from_date(:hour, %Date{} = date), do: "#{Date.to_iso8601(date)}T00"
  defp timeline_value_from_date(:day, %Date{} = date), do: Date.to_iso8601(date)
  defp timeline_value_from_date(:month, %Date{} = date), do: format_month(date.year, date.month)
  defp timeline_value_from_date(:year, %Date{} = date), do: Integer.to_string(date.year)

  defp shift_timeline_value(kind, timezone, value, 0),
    do: normalize_timeline_value(kind, timezone, value)

  defp shift_timeline_value(kind, timezone, value, count) do
    {:ok, period} = Favn.TimePeriod.bounds(kind, value, timezone)
    {:ok, shifted} = Favn.TimePeriod.shift(period.start_at, kind, count)
    timeline_value_from_datetime(kind, timezone, shifted)
  end

  defp normalize_timeline_value(kind, timezone, value) do
    {:ok, period} = Favn.TimePeriod.bounds(kind, value, timezone)
    timeline_value_from_datetime(kind, timezone, period.start_at)
  end

  defp timeline_value_date(:hour, <<date::binary-size(10), "T", _hour::binary-size(2)>>),
    do: Date.from_iso8601!(date)

  defp timeline_value_date(:day, value), do: Date.from_iso8601!(value)

  defp timeline_value_date(:month, <<year::binary-size(4), "-", month::binary-size(2)>>) do
    Date.new!(String.to_integer(year), String.to_integer(month), 1)
  end

  defp timeline_value_date(:year, value), do: Date.new!(String.to_integer(value), 1, 1)

  defp timeline_window_id(kind, value), do: "window:#{kind}:#{value}"

  defp timeline_window_label(:hour, <<date::binary-size(10), "T", hour::binary-size(2)>>) do
    date
    |> Date.from_iso8601!()
    |> Calendar.strftime("%b %-d")
    |> then(&"#{&1} #{hour}:00")
  end

  defp timeline_window_label(:day, value),
    do: value |> Date.from_iso8601!() |> Calendar.strftime("%b %-d")

  defp timeline_window_label(:month, value) do
    :month
    |> timeline_value_date(value)
    |> Calendar.strftime("%b %Y")
  end

  defp timeline_window_label(:year, value), do: value

  defp timeline_window_range(:hour, <<date::binary-size(10), "T", hour::binary-size(2)>>) do
    date
    |> Date.from_iso8601!()
    |> Calendar.strftime("%b %-d, %Y")
    |> then(&"#{&1} #{hour}:00")
  end

  defp timeline_window_range(:day, value),
    do: value |> Date.from_iso8601!() |> Calendar.strftime("%b %-d, %Y")

  defp timeline_window_range(:month, value) do
    :month
    |> timeline_value_date(value)
    |> Calendar.strftime("%B %Y")
  end

  defp timeline_window_range(:year, value), do: value

  defp format_month(year, month), do: "#{year}-#{pad2(month)}"

  defp pad2(value), do: value |> Integer.to_string() |> String.pad_leading(2, "0")

  defp window_request_from_id("window:day:" <> date) do
    case WindowRequest.parse("day:#{date}") do
      {:ok, request} -> {:ok, request}
      {:error, reason} -> {:error, {:invalid_window_id, reason}}
    end
  end

  defp window_request_from_id("window:hour:" <> hour) do
    case WindowRequest.parse("hour:#{hour}") do
      {:ok, request} -> {:ok, request}
      {:error, reason} -> {:error, {:invalid_window_id, reason}}
    end
  end

  defp window_request_from_id("window:month:" <> month) do
    case WindowRequest.parse("month:#{month}") do
      {:ok, request} -> {:ok, request}
      {:error, reason} -> {:error, {:invalid_window_id, reason}}
    end
  end

  defp window_request_from_id("window:year:" <> year) do
    case WindowRequest.parse("year:#{year}") do
      {:ok, request} -> {:ok, request}
      {:error, reason} -> {:error, {:invalid_window_id, reason}}
    end
  end

  defp window_request_from_id(window_id), do: {:error, {:invalid_window_id, window_id}}

  defp refresh_request_from_id("refresh:hour:" <> hour), do: parse_window_request(:hour, hour)
  defp refresh_request_from_id("refresh:day:" <> date), do: parse_window_request(:day, date)
  defp refresh_request_from_id("refresh:month:" <> month), do: parse_window_request(:month, month)
  defp refresh_request_from_id("refresh:year:" <> year), do: parse_window_request(:year, year)
  defp refresh_request_from_id(id), do: {:error, {:invalid_refresh_id, id}}

  defp parse_window_request(kind, value) do
    case WindowRequest.parse("#{kind}:#{value}") do
      {:ok, request} -> {:ok, request}
      {:error, reason} -> {:error, {:invalid_window_id, reason}}
    end
  end

  defp resolve_asset_window(%{window: nil}, %WindowRequest{kind: kind}) do
    {:error, {:window_request_without_policy, kind}}
  end

  defp resolve_asset_window(%{window: %WindowSpec{} = spec}, %WindowRequest{kind: kind} = request) do
    if spec.kind == kind do
      WindowRequest.to_anchor(request, spec.timezone)
    else
      {:error, {:window_kind_mismatch, spec.kind, kind}}
    end
  end

  defp resolve_asset_window(asset, %WindowRequest{} = request) when is_atom(asset.window) do
    with {:ok, spec} <- WindowSpec.new(asset.window) do
      resolve_asset_window(%{asset | window: spec}, request)
    end
  end

  defp resolve_asset_window(%{window: %{} = window}, %WindowRequest{} = request) do
    case {Map.get(window, :kind) || Map.get(window, "kind"),
          Map.get(window, :timezone) || Map.get(window, "timezone")} do
      {kind, timezone} when not is_nil(kind) ->
        with {:ok, normalized_kind} <- normalize_window_kind(kind),
             {:ok, spec} <- WindowSpec.new(normalized_kind, timezone: timezone || "Etc/UTC") do
          resolve_asset_window(%{window: spec}, request)
        end

      _other ->
        {:error, :invalid_window_policy}
    end
  end

  defp normalize_window_kind(kind) do
    case Policy.from_value(kind) do
      {:ok, %Policy{kind: normalized_kind}} -> {:ok, normalized_kind}
      {:ok, nil} -> {:error, {:invalid_window_policy_kind, kind}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp timeline_status(
         %AssetFreshnessState{} = freshness,
         _latest_freshness,
         _latest_run,
         _date,
         _latest_run_date
       ) do
    data_coverage_status_from_catalogue(catalogue_status(freshness, nil))
  end

  defp timeline_status(nil, latest_freshness, latest_run, date, date) do
    data_coverage_status_from_catalogue(catalogue_status(latest_freshness, latest_run))
  end

  defp timeline_status(nil, _latest_freshness, _latest_run, _date, _latest_run_date), do: :missing

  defp data_coverage_status_from_catalogue(:healthy), do: :covered
  defp data_coverage_status_from_catalogue(:failed), do: :failed
  defp data_coverage_status_from_catalogue(:running), do: :running
  defp data_coverage_status_from_catalogue(_status), do: :unknown

  defp maybe_put_latest_run(window, latest_freshness, latest_run, date, date) do
    window
    |> Map.put_new(:latest_run_id, latest_run_id(latest_freshness, latest_run))
    |> Map.put_new(:latest_run_status, latest_run_status(latest_freshness, latest_run))
    |> Map.put_new(:latest_run_at, latest_run_at(latest_freshness, latest_run))
    |> put_latest_run_if_missing(:latest_run_id, latest_run_id(latest_freshness, latest_run))
    |> put_latest_run_if_missing(
      :latest_run_status,
      latest_run_status(latest_freshness, latest_run)
    )
    |> put_latest_run_if_missing(:latest_run_at, latest_run_at(latest_freshness, latest_run))
  end

  defp maybe_put_latest_run(window, _latest_freshness, _latest_run, _date, _latest_run_date),
    do: window

  defp put_latest_run_if_missing(window, key, value) do
    if is_nil(window[key]), do: Map.put(window, key, value), else: window
  end

  defp asset_detail_name(%{relation: relation, asset_ref: asset_ref, label: label}) do
    relation_name(relation) || asset_ref_name(asset_ref) || label
  end

  defp relation_name(%{name: name}) when is_binary(name), do: name
  defp relation_name(%{"name" => name}) when is_binary(name), do: name
  defp relation_name(_relation), do: nil

  defp asset_ref_name(asset_ref) when is_binary(asset_ref) do
    asset_ref
    |> String.split(":")
    |> List.last()
  end

  defp asset_ref_name(_asset_ref), do: nil

  defp manifest_asset_target(asset) do
    target_ref = asset.ref

    %{
      target_id: target_id_for_asset(target_ref),
      label: inspect(target_ref),
      asset_ref: ref_to_string(target_ref),
      type: atom_name(asset.type),
      relation: relation_dto(asset.relation),
      metadata: normalize_map(asset.metadata),
      execution_pool: atom_name(asset.execution_pool),
      runtime_config: normalize_data(asset.runtime_config),
      depends_on: Enum.map(List.wrap(asset.depends_on), &ref_to_string/1),
      materialization: normalize_data(asset.materialization),
      window: normalize_data(asset.window)
    }
  end

  defp manifest_pipeline_targets(%Version{} = version) do
    version.manifest.pipelines
    |> List.wrap()
    |> Enum.map(&manifest_pipeline_target/1)
    |> Enum.sort_by(& &1.label)
  end

  defp manifest_pipeline_target(pipeline) do
    target_module = pipeline.module

    %{
      target_id: target_id_for_pipeline(target_module),
      label: inspect(target_module),
      window: window_policy_dto(pipeline.window),
      max_concurrency: pipeline.max_concurrency,
      execution_pool: atom_name(pipeline.execution_pool),
      can_run_without_window?: pipeline_can_run_without_window?(pipeline.window),
      can_backfill?: pipeline_can_backfill?(pipeline.window)
    }
  end

  defp pipeline_can_run_without_window?(nil), do: true
  defp pipeline_can_run_without_window?(%Policy{allow_full_load: true}), do: true
  defp pipeline_can_run_without_window?(_window), do: false

  defp pipeline_can_backfill?(nil), do: false
  defp pipeline_can_backfill?(_window), do: true

  defp manifest_pipeline_target(%Index{} = index, pipeline) do
    base = manifest_pipeline_target(pipeline)
    resolved_refs = resolve_pipeline_refs(index, pipeline)

    base
    |> Map.put(:name, pipeline_name(pipeline))
    |> Map.put(:selected_assets, Enum.map(resolved_refs, &ref_to_string/1))
    |> Map.put(:dependencies, pipeline_dependencies(pipeline))
  end

  defp resolve_pipeline_refs(%Index{} = index, pipeline) do
    case PipelineResolver.resolve(index, pipeline, trigger: %{kind: :catalogue}) do
      {:ok, resolution} -> resolution.target_refs
      {:error, _reason} -> raw_pipeline_selector_refs(index, pipeline)
    end
  end

  defp raw_pipeline_selector_refs(%Index{} = index, pipeline) do
    pipeline.selectors
    |> List.wrap()
    |> Enum.map(&raw_pipeline_selector_ref/1)
    |> Enum.filter(&(not is_nil(&1) and Map.has_key?(index.assets_by_ref, &1)))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp raw_pipeline_selector_ref({:asset, ref}), do: ref

  defp raw_pipeline_selector_ref({module, name} = ref) when is_atom(module) and is_atom(name),
    do: ref

  defp raw_pipeline_selector_ref(%{"module" => module, "name" => name})
       when is_binary(module) and is_binary(name) do
    {String.to_existing_atom(module), String.to_existing_atom(name)}
  rescue
    ArgumentError -> nil
  end

  defp raw_pipeline_selector_ref(_selector), do: nil

  defp pipeline_name(%{name: name}) when is_atom(name), do: Atom.to_string(name)

  defp pipeline_name(%{module: module}) when is_atom(module),
    do: module |> Atom.to_string() |> String.split(".") |> List.last()

  defp pipeline_name(_pipeline), do: "pipeline"

  defp pipeline_dependencies(%{deps: deps}) when deps in [:all, :none], do: deps
  defp pipeline_dependencies(_pipeline), do: :unknown

  defp freshness_ref_string(%AssetFreshnessState{} = state) do
    ref_to_string({state.asset_ref_module, state.asset_ref_name})
  end

  defp pipeline_runs(pipeline, %{selected_assets: selected_assets}, runs) do
    selected_assets = Enum.sort(selected_assets)

    runs
    |> Enum.filter(fn run ->
      pipeline_submit_ref_matches?(run, pipeline) ||
        legacy_pipeline_targets_match?(run, selected_assets)
    end)
    |> Enum.sort_by(&DateTime.to_unix(run_time_sort_key(&1), :microsecond), :desc)
  end

  defp run_ref_entries(run) do
    refs =
      [run.asset_ref | List.wrap(run.target_refs)] ++
        ((Map.get(run, :asset_results) || %{})
         |> Map.keys()
         |> List.wrap())

    refs
    |> Enum.filter(&match?({_module, _name}, &1))
    |> Enum.uniq()
    |> Enum.map(&{ref_to_string(&1), run})
  end

  defp latest_run(runs) do
    Enum.max_by(
      runs,
      &DateTime.to_unix(run_time_sort_key(&1), :microsecond),
      &>=/2,
      fn -> nil end
    )
  end

  defp pipeline_run_history_entry(run) do
    scope = run_history_scope(run)

    %{
      id: run.id,
      status: run.status,
      submit_kind: Map.get(run, :submit_kind),
      started_at: Map.get(run, :started_at),
      finished_at: Map.get(run, :finished_at),
      duration_ms: run_duration_ms(run),
      scope: scope,
      window: legacy_run_history_window(scope)
    }
  end

  defp run_history_scope(run) do
    params = Map.get(run, :params, %{}) || %{}
    metadata = Map.get(run, :metadata, %{}) || %{}

    cond do
      backfill = Map.get(metadata, :backfill) || Map.get(metadata, "backfill") ->
        Map.put(normalize_scope_map(backfill), :type, :range)

      window =
          Map.get(params, :window) || Map.get(params, "window") ||
            Map.get(metadata, :selected_window) || Map.get(metadata, "selected_window") ||
            Map.get(metadata, :window) || Map.get(metadata, "window") ->
        Map.put(normalize_scope_map(window), :type, :window)

      true ->
        nil
    end
  end

  defp legacy_run_history_window(%{type: :window} = scope), do: Map.delete(scope, :type)
  defp legacy_run_history_window(_scope), do: nil

  defp normalize_scope_map(value) when is_map(value), do: Map.new(value, &normalize_scope_pair/1)
  defp normalize_scope_map(value), do: %{label: to_string(value)}

  defp normalize_scope_pair({"type", value}), do: {:type, value}
  defp normalize_scope_pair({"kind", value}), do: {:kind, value}
  defp normalize_scope_pair({"timezone", value}), do: {:timezone, value}
  defp normalize_scope_pair({"range_start_at", value}), do: {:range_start_at, value}
  defp normalize_scope_pair({"range_end_at", value}), do: {:range_end_at, value}
  defp normalize_scope_pair({"requested_count", value}), do: {:requested_count, value}
  defp normalize_scope_pair({"window_keys", value}), do: {:window_keys, value}
  defp normalize_scope_pair({"id", value}), do: {:id, value}
  defp normalize_scope_pair({"key", value}), do: {:key, value}
  defp normalize_scope_pair({"label", value}), do: {:label, value}
  defp normalize_scope_pair(pair), do: pair

  defp normalize_pipeline_run_submit_opts(opts) when is_list(opts) do
    with {:ok, window_request} <- normalize_window_request(Keyword.get(opts, :window_request)) do
      {:ok, Keyword.put(opts, :window_request, window_request)}
    end
  end

  defp normalize_pipeline_run_submit_opts(opts) when is_map(opts) do
    with {:ok, window_request} <- normalize_window_request(field_value(opts, :window)) do
      opts =
        []
        |> maybe_put_opt(:metadata, field_value(opts, :metadata))
        |> maybe_put_opt(:window_request, window_request)
        |> maybe_put_opt(:refresh, field_value(opts, :refresh))
        |> maybe_put_opt(:refresh_policy, field_value(opts, :refresh_policy))
        |> maybe_put_opt(:timeout_ms, field_value(opts, :timeout_ms))

      {:ok, opts}
    end
  end

  defp normalize_pipeline_backfill_submit_opts(opts) when is_list(opts) do
    with {:ok, range_request} <- RangeRequest.from_value(Keyword.get(opts, :range_request)) do
      {:ok, Keyword.put(opts, :range_request, range_request)}
    end
  end

  defp normalize_pipeline_backfill_submit_opts(opts) when is_map(opts) do
    range = field_value(opts, :range) || field_value(opts, :range_request)

    with {:ok, range_request} <- RangeRequest.from_value(range) do
      submit_opts =
        []
        |> Keyword.put(:range_request, range_request)
        |> maybe_put_opt(:metadata, field_value(opts, :metadata))
        |> maybe_put_opt(:coverage_baseline_id, field_value(opts, :coverage_baseline_id))
        |> maybe_put_opt(:refresh, field_value(opts, :refresh))
        |> maybe_put_opt(:refresh_policy, field_value(opts, :refresh_policy))
        |> maybe_put_opt(:max_attempts, field_value(opts, :max_attempts))
        |> maybe_put_opt(:retry_backoff_ms, field_value(opts, :retry_backoff_ms))
        |> maybe_put_opt(:timeout_ms, field_value(opts, :timeout_ms))

      {:ok, submit_opts}
    end
  end

  defp normalize_window_request(nil), do: {:ok, nil}
  defp normalize_window_request(%WindowRequest{} = request), do: WindowRequest.from_value(request)

  defp normalize_window_request(value) when is_binary(value), do: WindowRequest.parse(value)
  defp normalize_window_request(value) when is_map(value), do: WindowRequest.from_value(value)

  defp normalize_window_request(value), do: {:error, {:invalid_window_request, value}}

  defp maybe_put_opt(opts, _key, nil), do: opts
  defp maybe_put_opt(opts, _key, ""), do: opts
  defp maybe_put_opt(opts, key, value), do: Keyword.put(opts, key, value)

  defp field_value(value, field) when is_map(value) do
    Map.get(value, field) || Map.get(value, Atom.to_string(field))
  end

  defp pipeline_submit_ref_matches?(run, pipeline) do
    case pipeline_submit_ref(run) do
      nil -> false
      submit_ref -> same_pipeline_ref?(submit_ref, pipeline.module)
    end
  end

  defp legacy_pipeline_targets_match?(run, selected_assets) do
    is_nil(pipeline_submit_ref(run)) && pipeline_origin?(run) &&
      selected_assets != [] &&
      run
      |> Map.get(:target_refs, [])
      |> Enum.map(&ref_to_string/1)
      |> Enum.sort()
      |> Kernel.==(selected_assets)
  end

  defp pipeline_origin?(run) do
    Map.get(run, :submit_kind) in [:pipeline, :backfill_pipeline] ||
      not is_nil(pipeline_metadata_value(run, :pipeline_submit_ref)) ||
      not is_nil(pipeline_metadata_value(run, :pipeline_target_refs))
  end

  defp pipeline_submit_ref(run) do
    pipeline_metadata_value(run, :pipeline_submit_ref) || direct_pipeline_submit_ref(run)
  end

  defp direct_pipeline_submit_ref(run) do
    if Map.get(run, :submit_kind) in [:pipeline, :backfill_pipeline] do
      Map.get(run, :submit_ref)
    end
  end

  defp pipeline_metadata_value(run, key) do
    metadata = Map.get(run, :metadata, %{}) || %{}
    Map.get(metadata, key) || Map.get(metadata, Atom.to_string(key))
  end

  defp same_pipeline_ref?(module, module) when is_atom(module), do: true

  defp same_pipeline_ref?(value, module) when is_atom(module),
    do: to_string(value) == Atom.to_string(module)

  defp same_pipeline_ref?(_value, _module), do: false

  defp run_time_sort_key(run),
    do:
      Map.get(run, :finished_at) || Map.get(run, :started_at) || Map.get(run, :updated_at) ||
        Map.get(run, :inserted_at) || DateTime.from_unix!(0)

  defp run_duration_ms(%{
         started_at: %DateTime{} = started_at,
         finished_at: %DateTime{} = finished_at
       }) do
    max(DateTime.diff(finished_at, started_at, :millisecond), 0)
  end

  defp run_duration_ms(_run), do: nil

  defp catalogue_status(%AssetFreshnessState{} = freshness, _run) do
    case freshness.latest_attempt_status || freshness.status do
      status when status in [:ok, :skipped_fresh] -> :healthy
      :running -> :running
      status when status in [:error, :cancelled, :timed_out, :blocked] -> :failed
      _other -> :unknown
    end
  end

  defp catalogue_status(nil, run), do: run_status(run)

  defp run_status(nil), do: :unknown
  defp run_status(%{status: status}) when status in [:pending, :running], do: :running
  defp run_status(%{status: :ok}), do: :healthy

  defp run_status(%{status: status}) when status in [:partial, :error, :cancelled, :timed_out],
    do: :failed

  defp run_status(_run), do: :unknown

  defp put_target_status(target, %TargetStatus{} = status) do
    target
    |> Map.put(:status, status.status)
    |> Map.put(:latest_run_id, status.latest_run_id)
    |> Map.put(:latest_run_status, status.latest_run_status)
    |> Map.put(:latest_run_at, status.latest_run_at)
    |> Map.put(:latest_run_duration_ms, status.latest_run_duration_ms)
  end

  defp latest_run_id(%AssetFreshnessState{latest_attempt_run_id: id}, _run) when is_binary(id),
    do: id

  defp latest_run_id(%AssetFreshnessState{latest_success_run_id: id}, _run) when is_binary(id),
    do: id

  defp latest_run_id(_freshness, %{id: id}) when is_binary(id), do: id
  defp latest_run_id(_freshness, _run), do: nil

  defp latest_run_status(%AssetFreshnessState{latest_attempt_status: status}, _run)
       when not is_nil(status),
       do: status

  defp latest_run_status(%AssetFreshnessState{status: status}, _run) when not is_nil(status),
    do: status

  defp latest_run_status(_freshness, %{status: status}), do: status
  defp latest_run_status(_freshness, _run), do: nil

  defp latest_run_at(%AssetFreshnessState{latest_attempt_at: at}, _run) when not is_nil(at),
    do: at

  defp latest_run_at(%AssetFreshnessState{latest_success_at: at}, _run) when not is_nil(at),
    do: at

  defp latest_run_at(_freshness, run) when not is_nil(run), do: run_time_sort_key(run)
  defp latest_run_at(_freshness, _run), do: nil

  defp window_policy_dto(nil), do: nil

  defp window_policy_dto(%Policy{} = policy) do
    %{
      kind: Atom.to_string(policy.kind),
      anchor: Atom.to_string(policy.anchor),
      timezone: policy.timezone,
      allow_full_load: policy.allow_full_load
    }
  end

  defp window_policy_dto(policy) do
    policy
    |> Policy.from_value!()
    |> window_policy_dto()
  end

  defp relation_dto(nil), do: nil

  defp relation_dto(relation) do
    case Favn.RelationRef.new!(relation) do
      ref ->
        %{
          connection: atom_name(ref.connection),
          catalog: ref.catalog,
          schema: ref.schema,
          name: ref.name
        }
    end
  rescue
    ArgumentError -> normalize_data(relation)
  end

  defp normalize_map(value) when is_map(value), do: value
  defp normalize_map(_value), do: %{}

  defp normalize_data(%Favn.RuntimeConfig.Ref{} = ref) do
    %{
      provider: atom_name(ref.provider),
      key: ref.key,
      secret: ref.secret?,
      required: ref.required?
    }
  end

  defp normalize_data(%Favn.Window.Spec{} = spec) do
    %{
      kind: atom_name(spec.kind),
      lookback: spec.lookback,
      refresh_from: normalize_data(spec.refresh_from),
      required: spec.required,
      timezone: spec.timezone
    }
  end

  defp normalize_data(%DateTime{} = value), do: DateTime.to_iso8601(value)

  defp normalize_data(value) when is_struct(value) do
    value
    |> Map.from_struct()
    |> normalize_data()
  end

  defp normalize_data(value) when is_map(value) do
    value
    |> Enum.map(fn {key, val} -> {to_string(key), normalize_data(val)} end)
    |> Map.new()
  end

  defp normalize_data(value) when is_list(value), do: Enum.map(value, &normalize_data/1)
  defp normalize_data({module, name}), do: ref_to_string({module, name})
  defp normalize_data(value) when is_atom(value), do: atom_name(value)
  defp normalize_data(value), do: value

  defp resolve_asset_target_ref(%Version{} = version, target_id) when is_binary(target_id) do
    with {:ok, asset} <- resolve_asset_target(version, target_id), do: {:ok, asset.ref}
  end

  defp resolve_asset_target(%Version{} = version, target_id) when is_binary(target_id) do
    case Enum.find(
           List.wrap(version.manifest.assets),
           &(target_id_for_asset(&1.ref) == target_id)
         ) do
      %{ref: _target_ref} = asset -> {:ok, asset}
      _ -> {:error, :invalid_asset_target}
    end
  end

  defp resolve_pipeline_target_module(%Version{} = version, target_id)
       when is_binary(target_id) do
    case Enum.find(
           List.wrap(version.manifest.pipelines),
           &(target_id_for_pipeline(&1.module) == target_id)
         ) do
      %{module: target_module} -> {:ok, target_module}
      _ -> {:error, :invalid_pipeline_target}
    end
  end

  defp target_id_for_asset({module, name}) when is_atom(module) and is_atom(name) do
    "asset:" <> Atom.to_string(module) <> ":" <> Atom.to_string(name)
  end

  defp target_id_for_pipeline(module) when is_atom(module) do
    "pipeline:" <> Atom.to_string(module)
  end

  defp configured_runner_client do
    RuntimeConfig.current().runner_client
  end

  defp configured_runner_opts do
    RuntimeConfig.current().runner_client_opts
  end

  defp default_log_filter do
    case Code.ensure_loaded(Favn.Log.Filter) do
      {:module, Favn.Log.Filter} -> struct(Favn.Log.Filter)
      _other -> %{}
    end
  end

  defp validate_runner_client(module) when is_atom(module) do
    callbacks =
      RunnerClient.behaviour_info(:callbacks) -- RunnerClient.behaviour_info(:optional_callbacks)

    with {:module, ^module} <- Code.ensure_loaded(module),
         true <-
           Enum.all?(callbacks, fn {name, arity} -> function_exported?(module, name, arity) end) do
      :ok
    else
      _ -> {:error, :runner_client_not_available}
    end
  end

  defp validate_runner_client(_module), do: {:error, :runner_client_not_available}

  defp runner_manifest_registration(%Version{} = version, runner_client, status) do
    %{
      manifest_version_id: version.manifest_version_id,
      content_hash: version.content_hash,
      runner_client: atom_name(runner_client),
      status: atom_name(status)
    }
  end

  defp runner_registration_error(:runner_client_not_available), do: :runner_client_not_available
  defp runner_registration_error({:runner_node_unreachable, _node}), do: :runner_unavailable
  defp runner_registration_error({:runner_node_ignored, _node}), do: :runner_unavailable

  defp runner_registration_error({:runner_function_undefined, _module, _function, _arity}),
    do: :runner_client_not_available

  defp runner_registration_error({:runner_dispatch_failed, _details}), do: :runner_unavailable

  defp runner_registration_error({:manifest_version_conflict, _id, _existing, _incoming}),
    do: :runner_manifest_conflict

  defp runner_registration_error(_reason), do: :runner_unavailable

  defp ref_to_string({module, name}) when is_atom(module) and is_atom(name) do
    Atom.to_string(module) <> ":" <> Atom.to_string(name)
  end

  defp ref_to_string(value), do: inspect(value)

  defp atom_name(nil), do: nil
  defp atom_name(value) when is_atom(value), do: Atom.to_string(value)
  defp atom_name(value), do: to_string(value)

  defp list_count(value) when is_list(value), do: length(value)
  defp list_count(_value), do: 0
end
