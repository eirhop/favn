defmodule FavnOrchestrator do
  @moduledoc """
  Runtime orchestrator control-plane facade for manifest-pinned operations.

  `FavnOrchestrator` is the boundary used by runtime apps, operator tooling, and
  the thin `Favn` runtime helpers. Same-BEAM operator UI code uses the operator
  wrappers here, including run submission, backfill submission, and
  `cancel_operator_run/2`, so authz stays in the control plane. It is not the
  stable authoring-time API that most application code should build against.
  """

  alias Favn.Backfill.RangeRequest
  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RunnerClient
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Version
  alias Favn.RuntimeInput.Pin
  alias Favn.Window.Anchor
  alias Favn.Window.Request, as: WindowRequest
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Backfill.Repair, as: BackfillRepair
  alias FavnOrchestrator.BackfillManager
  alias FavnOrchestrator.Diagnostics
  alias FavnOrchestrator.Events
  alias FavnOrchestrator.Freshness.Query, as: FreshnessQuery
  alias FavnOrchestrator.Logs
  alias FavnOrchestrator.LogWriter
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.Operator.Authorization
  alias FavnOrchestrator.Operator.Catalogue
  alias FavnOrchestrator.Operator.Commands, as: OperatorCommands
  alias FavnOrchestrator.Operator.Schedules
  alias FavnOrchestrator.OperatorCommands.AssetBackfillRequest
  alias FavnOrchestrator.OperatorCommands.AssetRunRequest
  alias FavnOrchestrator.OperatorCommands.PipelineBackfillRequest
  alias FavnOrchestrator.OperatorCommands.PipelineRunRequest
  alias FavnOrchestrator.OperatorErrorDTO
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunEvents.Query, as: RunEventQuery
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunReadModel
  alias FavnOrchestrator.RunRetryPlanner
  alias FavnOrchestrator.RunSubmission.AssetOptions
  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.ScheduleListEntry
  alias FavnOrchestrator.ScheduleOccurrencePreview
  alias FavnOrchestrator.Scheduler.Runtime, as: SchedulerRuntime
  alias FavnOrchestrator.SchedulerEntry
  alias FavnOrchestrator.Storage

  @type run_id :: String.t()
  @type operator_actor :: Auth.actor()
  @type operator_session :: Auth.session()
  @type operator_actor_context :: %{
          required(:actor) => operator_actor(),
          required(:session) => operator_session()
        }
  @type manifest_summary :: Catalogue.manifest_summary()
  @type manifest_target_option :: Catalogue.manifest_target_option()
  @type manifest_targets :: Catalogue.manifest_targets()
  @type asset_catalogue_entry :: Catalogue.asset_catalogue_entry()
  @type pipeline_catalogue_entry :: Catalogue.pipeline_catalogue_entry()
  @type pipeline_run_history_entry :: Catalogue.pipeline_run_history_entry()
  @type pipeline_detail :: Catalogue.pipeline_detail()
  @type asset_timeline_window :: Catalogue.asset_timeline_window()
  @type asset_detail :: Catalogue.asset_detail()
  @type asset_freshness_reason :: Catalogue.asset_freshness_reason()
  @type asset_freshness_detail :: Catalogue.asset_freshness_detail()

  @type operator_error_context ::
          :load
          | :schedule_occurrences
          | :schedule_activation
          | :run_cancel
          | :run_failure_detail

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

  @doc "Stores immutable execution packages before their manifest index is registered."
  @spec register_execution_packages([ExecutionPackage.t()]) :: :ok | {:error, term()}
  def register_execution_packages(packages) when is_list(packages) do
    Storage.put_execution_packages(packages)
  end

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
  def list_manifest_summaries, do: Catalogue.list_manifest_summaries()

  @doc """
  Returns one stable operator-facing manifest summary.
  """
  @spec get_manifest_summary(String.t()) :: {:ok, manifest_summary()} | {:error, term()}
  def get_manifest_summary(manifest_version_id) when is_binary(manifest_version_id),
    do: Catalogue.get_manifest_summary(manifest_version_id)

  @doc """
  Returns manifest-scoped submit targets for one persisted manifest version.
  """
  @spec manifest_targets(String.t()) :: {:ok, manifest_targets()} | {:error, term()}
  def manifest_targets(manifest_version_id) when is_binary(manifest_version_id),
    do: Catalogue.manifest_targets(manifest_version_id)

  @doc """
  Returns submit targets for the currently active manifest version.
  """
  @spec active_manifest_targets() :: {:ok, manifest_targets()} | {:error, term()}
  def active_manifest_targets, do: Catalogue.active_manifest_targets()

  @doc """
  Rebuilds persisted current target statuses for one manifest version.

  The target-status table is a repairable read model for operator catalogue and
  detail pages. This function rebuilds it from authoritative persisted run and
  freshness state and replaces rows for the manifest scope.
  """
  @spec rebuild_target_statuses(String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def rebuild_target_statuses(manifest_version_id) when is_binary(manifest_version_id),
    do: Catalogue.rebuild_target_statuses(manifest_version_id)

  @doc """
  Returns operator-facing catalogue entries for the currently active manifest.

  Entries are manifest target metadata enriched with latest known freshness/run
  state. Missing runtime state is represented explicitly as `:unknown` with no
  latest run timestamp.
  """
  @spec active_asset_catalogue() :: {:ok, [asset_catalogue_entry()]} | {:error, term()}
  def active_asset_catalogue, do: Catalogue.active_asset_catalogue()

  @doc """
  Returns operator-facing catalogue entries for pipelines in the active manifest.

  Entries include manifest-level pipeline selection metadata enriched with the
  latest persisted run that can be associated with each pipeline.
  """
  @spec active_pipeline_catalogue() :: {:ok, [pipeline_catalogue_entry()]} | {:error, term()}
  def active_pipeline_catalogue, do: Catalogue.active_pipeline_catalogue()

  @doc """
  Returns an operator-facing detail read model for one active pipeline target.

  The detail is built at the orchestrator boundary and includes manifest target
  metadata, selected assets, latest run state, and persisted run history matched
  to the pipeline submit ref.
  """
  @spec active_pipeline_detail(String.t()) :: {:ok, pipeline_detail()} | {:error, term()}
  def active_pipeline_detail(target_id) when is_binary(target_id),
    do: Catalogue.active_pipeline_detail(target_id)

  @doc """
  Returns an operator-facing detail read model for one active asset target.

  The detail is a DTO built at the orchestrator boundary. It includes manifest
  target metadata, latest known freshness/run state, and a conservative 30-window
  timeline matching the asset window policy. Missing runtime evidence is
  represented as `:unknown`.
  """
  @spec active_asset_detail(String.t(), keyword()) :: {:ok, asset_detail()} | {:error, term()}
  def active_asset_detail(target_id, opts \\ []) when is_binary(target_id) and is_list(opts),
    do: Catalogue.active_asset_detail(target_id, opts)

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
         {:ok, asset} <- ManifestTarget.resolve_asset(version, target_id),
         {:ok, opts} <- AssetOptions.from_input(asset, request) do
      submit_asset_run(asset.ref, Keyword.put(opts, :manifest_version_id, manifest_version_id))
    end
  end

  def submit_asset_run_for_manifest(manifest_version_id, target_id, opts)
      when is_binary(manifest_version_id) and is_binary(target_id) and is_list(opts) do
    with {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, asset_ref} <- ManifestTarget.resolve_asset_ref(version, target_id) do
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
    OperatorCommands.submit_asset_run(
      actor_context,
      manifest_version_id,
      target_id,
      command_input
    )
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
    OperatorCommands.submit_run(actor_context, manifest_version_id, target, command_input)
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
    OperatorCommands.submit_asset_backfill(
      actor_context,
      manifest_version_id,
      target_id,
      command_input
    )
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
         {:ok, pipeline_module} <- ManifestTarget.resolve_pipeline_module(version, target_id),
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
    OperatorCommands.submit_pipeline_run(
      actor_context,
      manifest_version_id,
      target_id,
      command_input
    )
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

  `:retry_policy` accepts `%Favn.Retry.Policy{}` or its map/keyword input form
  and is the explicit operator override for every planned node. It takes
  precedence over asset and pipeline policy. The override changes attempt
  count/timing only and never makes an unknown outcome safe to repeat.
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

  `:retry_policy` accepts `%Favn.Retry.Policy{}` or its map/keyword input form
  and overrides asset/pipeline policy for this run only. `max_attempts` includes
  the initial attempt. The effective policy and source are frozen per node and
  exposed in run details.

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
         {:ok, asset_ref} <- ManifestTarget.resolve_asset_ref(version, target_id),
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
  - `:retry_policy` and `:timeout_ms` - forwarded to child runs. Retry policy is
    one typed node policy and does not alter schedule overlap or backfill child
    identity.
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
         {:ok, pipeline_module} <- ManifestTarget.resolve_pipeline_module(version, target_id),
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
         {:ok, pipeline_module} <- ManifestTarget.resolve_pipeline_module(version, target_id),
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
    OperatorCommands.submit_pipeline_backfill(
      actor_context,
      manifest_version_id,
      target_id,
      command_input
    )
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
    with {:ok, actor} <- Authorization.authorize(actor_context, :operator) do
      cancel_run(run_id, %{actor_id: actor.id, requested_by: :operator})
    end
  end

  def cancel_operator_run(_actor_context, _run_id), do: {:error, :unauthenticated}

  @doc """
  Plans remaining retry work for a failed run or backfill execution group.
  """
  @spec plan_remaining_retry(run_id()) :: {:ok, RunRetryPlanner.retry_plan()} | {:error, term()}
  def plan_remaining_retry(run_id) when is_binary(run_id), do: RunRetryPlanner.remaining(run_id)

  @doc """
  Submits retry runs for failed or not-started assets, preserving source run config.
  """
  @spec retry_remaining(run_id(), keyword()) ::
          {:ok, map()} | {:partial, map()} | {:error, term()}
  def retry_remaining(run_id, opts \\ []) when is_binary(run_id) and is_list(opts) do
    with :ok <- validate_remaining_retry_opts(opts),
         {:ok, plan} <- RunRetryPlanner.remaining(run_id) do
      submit_remaining_retry_plan(plan, opts)
    end
  end

  @doc """
  Submits remaining retry work on behalf of an authenticated operator.
  """
  @spec retry_operator_run_remaining(operator_actor_context(), run_id()) ::
          {:ok, map()} | {:partial, map()} | {:error, term()}
  def retry_operator_run_remaining(actor_context, run_id)
      when is_map(actor_context) and is_binary(run_id) do
    with {:ok, actor} <- Authorization.authorize(actor_context, :operator) do
      retry_remaining(run_id,
        metadata: %{operator_retry: true, actor_id: actor.id}
      )
    end
  end

  def retry_operator_run_remaining(_actor_context, _run_id), do: {:error, :unauthenticated}

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

  Exact replay defaults to `input_mode: :pinned`: every selected SQL asset with
  runtime inputs must reuse a source-run pin. `:inherit` copies available pins
  and resolves nodes the source run never reached; `:fresh` deliberately
  resolves again and therefore is not exact replay. A `:retry_policy` option is
  an explicit run-only operator override.
  """
  @spec rerun(run_id(), keyword()) :: {:ok, run_id()} | {:error, term()}
  def rerun(source_run_id, opts \\ []) when is_binary(source_run_id) and is_list(opts) do
    RunManager.rerun(source_run_id, opts)
  end

  defp submit_remaining_retry_plan(%{children: children, asset_count: asset_count} = plan, opts) do
    with {:ok, submissions} <- prepare_remaining_retry_submissions(plan, children, opts) do
      admit_remaining_retry_submissions(plan, submissions, asset_count)
    end
  end

  defp prepare_remaining_retry_submissions(plan, children, opts) do
    children
    |> Enum.reduce_while({:ok, []}, fn child, {:ok, acc} ->
      submit_opts = remaining_retry_opts(plan, child, opts)

      case RunManager.prepare_rerun(child.source_run_id, submit_opts) do
        {:ok, submission} -> {:cont, {:ok, [submission | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, submissions} -> {:ok, Enum.reverse(submissions)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp admit_remaining_retry_submissions(plan, submissions, asset_count) do
    submissions
    |> Enum.reduce_while({:ok, []}, fn submission, {:ok, acc} ->
      case RunManager.admit_prepared_submission(submission) do
        {:ok, run_id} ->
          {:cont, {:ok, [run_id | acc]}}

        {:error, reason} when acc == [] ->
          {:halt, {:error, reason}}

        {:error, reason} ->
          {:halt,
           {:partial,
            %{
              source_run_id: plan.source_run_id,
              run_ids: Enum.reverse(acc),
              failed_run_id: submission.run_state.id,
              reason: reason,
              asset_count: asset_count
            }}}
      end
    end)
    |> case do
      {:ok, run_ids} ->
        {:ok,
         %{
           source_run_id: plan.source_run_id,
           run_ids: Enum.reverse(run_ids),
           asset_count: asset_count
         }}

      {:error, reason} ->
        {:error, reason}

      {:partial, result} ->
        {:partial, result}
    end
  end

  defp remaining_retry_opts(plan, child, opts) do
    metadata =
      opts
      |> Keyword.get(:metadata, %{})
      |> Map.merge(%{
        retry_mode: :remaining,
        retry_source_run_id: plan.source_run_id,
        retry_asset_count: length(child.target_refs)
      })

    []
    |> Keyword.put(:target_refs, child.target_refs)
    |> Keyword.put(:replay_node_keys, child.node_keys)
    |> Keyword.put(:replay_mode, :resume_from_failure)
    |> Keyword.put(:metadata, metadata)
    |> Keyword.put(:trigger, remaining_retry_trigger(plan, child))
    |> maybe_put_opt(:refresh_policy, Map.get(child, :refresh_policy))
    |> maybe_put_opt(:anchor_window, Map.get(child, :anchor_window))
    |> maybe_put_opt(:parent_run_id, Map.get(child, :backfill_run_id))
    |> maybe_put_opt(:root_run_id, Map.get(child, :backfill_run_id))
  end

  defp validate_remaining_retry_opts(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, :invalid_retry_remaining_options}

      Keyword.keys(opts) -- [:metadata] != [] ->
        {:error, {:invalid_retry_remaining_options, Keyword.keys(opts) -- [:metadata]}}

      not is_map(Keyword.get(opts, :metadata, %{})) ->
        {:error, :invalid_retry_remaining_metadata}

      true ->
        :ok
    end
  end

  defp remaining_retry_trigger(plan, child) do
    %{
      kind: :rerun,
      retry_mode: :remaining,
      source_run_id: child.source_run_id,
      retry_source_run_id: plan.source_run_id,
      backfill_run_id: Map.get(child, :backfill_run_id),
      window_key: Map.get(child, :window_key)
    }
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp ensure_window_rerunnable(window, opts) when is_list(opts) do
    if Keyword.get(opts, :allow_success, false) == true do
      ensure_success_rerun_refresh(opts)
    else
      ensure_window_rerunnable(window)
    end
  end

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

  @doc """
  Returns one persisted run snapshot.
  """
  @spec get_run(run_id()) :: {:ok, Favn.Run.t()} | {:error, term()}
  def get_run(run_id) when is_binary(run_id) do
    with {:ok, run_state} <- Storage.get_run(run_id),
         {:ok, pins} <- runtime_input_pins_for_read(run_id) do
      run = Projector.project_run(run_state)
      metadata = maybe_put_runtime_input_lineage(run.metadata, pins)
      {:ok, %{run | metadata: metadata}}
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
  def list_schedule_entries, do: Schedules.list_entries()

  @doc """
  Returns a bounded page of operator schedule list entries.

  Supported filters are `:search`, `:activation_state`, `:runtime_state`,
  `:pipeline_module`, `:window`, `:limit`, and `:offset`.
  """
  @spec page_schedule_list_entries(keyword()) ::
          {:ok, Page.t(schedule_list_entry())} | {:error, term()}
  def page_schedule_list_entries(filters \\ []) when is_list(filters),
    do: Schedules.page_entries(filters)

  @doc """
  Returns one schedule inspection entry by remote schedule id.
  """
  @spec get_schedule_entry(String.t()) :: {:ok, SchedulerEntry.t()} | {:error, term()}
  def get_schedule_entry(schedule_id) when is_binary(schedule_id),
    do: Schedules.get_entry(schedule_id)

  @doc """
  Enables a schedule for future submissions through the orchestrator facade.

  Enabling starts from the next due occurrence by moving the scheduler cursor to
  the latest due occurrence observed at command time. It does not submit missed
  catch-up work automatically.
  """
  @spec enable_schedule(String.t()) :: {:ok, SchedulerEntry.t()} | {:error, term()}
  def enable_schedule(schedule_id) when is_binary(schedule_id), do: Schedules.enable(schedule_id)

  @doc """
  Disables a schedule for future submissions through the orchestrator facade.

  Existing in-flight runs are not cancelled. Any queued due occurrence is cleared
  because disabled schedules do not submit future work.
  """
  @spec disable_schedule(String.t()) :: {:ok, SchedulerEntry.t()} | {:error, term()}
  def disable_schedule(schedule_id) when is_binary(schedule_id),
    do: Schedules.disable(schedule_id)

  @doc """
  Returns an orchestrator-owned preview of upcoming occurrences for one schedule.

  Options:

    * `:limit` - number of preview rows, defaults to `10` and is capped at `100`.
    * `:now` - `DateTime` from which to preview, defaulting to the current UTC time.
  """
  @spec preview_schedule_occurrences(String.t(), keyword()) ::
          {:ok, [schedule_occurrence_preview()]} | {:error, term()}
  def preview_schedule_occurrences(schedule_id, opts \\ [])
      when is_binary(schedule_id) and is_list(opts),
      do: Schedules.preview_occurrences(schedule_id, opts)

  @doc """
  Returns the stable remote id for one schedule inspection entry.
  """
  @spec schedule_entry_id(SchedulerEntry.t()) :: String.t()
  def schedule_entry_id(%SchedulerEntry{} = entry), do: Schedules.entry_id(entry)

  @doc """
  Returns a summary for the current schedule list page.
  """
  @spec schedule_list_summary([schedule_list_entry()]) :: map()
  def schedule_list_summary(entries) when is_list(entries), do: Schedules.summary(entries)

  defp run_event_cursor_valid?(_run_id, nil), do: {:ok, true}
  defp run_event_cursor_valid?(_run_id, 0), do: {:ok, true}

  defp run_event_cursor_valid?(run_id, sequence) when is_integer(sequence) and sequence > 0 do
    with {:ok, events} <- list_run_events(run_id, after_sequence: sequence - 1, limit: 1) do
      {:ok, match?([%RunEvent{sequence: ^sequence}], events)}
    end
  end

  defp run_event_cursor_valid?(_run_id, _sequence), do: {:ok, false}

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
        |> maybe_put_opt(:retry_policy, field_value(opts, :retry_policy))
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

  defp configured_runner_client do
    RuntimeConfig.current().runner_client
  end

  defp configured_runner_opts do
    RuntimeConfig.current().runner_client_opts
  end

  defp runtime_input_pins_for_read(run_id) do
    case Storage.list_runtime_input_pins(run_id) do
      {:ok, pins} -> {:ok, pins}
      {:error, :runtime_input_pins_not_supported} -> {:ok, []}
      {:error, _reason} = error -> error
    end
  end

  defp maybe_put_runtime_input_lineage(metadata, []), do: metadata

  defp maybe_put_runtime_input_lineage(metadata, pins),
    do: Map.put(metadata, :runtime_input_lineage, Enum.map(pins, &Pin.lineage/1))

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

  defp atom_name(nil), do: nil
  defp atom_name(value) when is_atom(value), do: Atom.to_string(value)
  defp atom_name(value), do: to_string(value)
end
