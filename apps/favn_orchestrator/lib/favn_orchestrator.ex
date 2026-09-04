defmodule FavnOrchestrator do
  @moduledoc """
  Runtime orchestrator control-plane facade for manifest-pinned operations.

  `FavnOrchestrator` is the boundary used by runtime apps, operator tooling, and
  the thin `Favn` runtime helpers. Operator UI code uses the operator wrappers
  here locally or through the View's distributed-Erlang adapter, including run
  submission, backfill submission, and
  `cancel_operator_run/2`, so authz stays in the control plane. It is not the
  stable authoring-time API that most application code should build against.
  """

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Manifest.Version
  alias Favn.RuntimeInput.Pin
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Coverage
  alias FavnOrchestrator.Diagnostics
  alias FavnOrchestrator.Events
  alias FavnOrchestrator.Idempotency
  alias FavnOrchestrator.Logs
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.Manifests
  alias FavnOrchestrator.Operator.Catalogue
  alias FavnOrchestrator.Operator.Audit, as: OperatorAudit
  alias FavnOrchestrator.Operator.Lineage
  alias FavnOrchestrator.Operator.Rebuilds, as: OperatorRebuilds
  alias FavnOrchestrator.Operator.TargetRecovery, as: OperatorTargetRecovery
  alias FavnOrchestrator.Operator.Commands, as: OperatorCommands
  alias FavnOrchestrator.OperatorContext
  alias FavnOrchestrator.OperatorRunView
  alias FavnOrchestrator.Operator.Schedules
  alias FavnOrchestrator.OperatorCommands.AssetBackfillRequest
  alias FavnOrchestrator.OperatorCommands.AssetRunRequest
  alias FavnOrchestrator.OperatorCommands.PipelineBackfillRequest
  alias FavnOrchestrator.OperatorCommands.PipelineRunRequest
  alias FavnOrchestrator.OperatorErrorDTO
  alias FavnOrchestrator.OperationRunnerTasks
  alias FavnOrchestrator.Persistence.Error, as: PersistenceError
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries.CountExecutionGroups
  alias FavnOrchestrator.Persistence.Queries.GetExecutionGroup
  alias FavnOrchestrator.Persistence.Queries.PageExecutionGroups
  alias FavnOrchestrator.Persistence.Results.Backfill, as: PersistedBackfill
  alias FavnOrchestrator.Persistence.Results.ExecutionGroupCounts
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunEvents.Query, as: RunEventQuery
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunnerIdentityVerifier
  alias FavnOrchestrator.RunnerOverview
  alias FavnOrchestrator.RunReadModel
  alias FavnOrchestrator.Rebuilds
  alias FavnOrchestrator.TargetRecovery
  alias FavnOrchestrator.WorkspaceConfiguration
  alias FavnOrchestrator.RunRetryPlanner
  alias FavnOrchestrator.RunSubmission.AssetOptions
  alias FavnOrchestrator.RunSubmissions
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.Shutdown
  alias FavnOrchestrator.ScheduleListEntry
  alias FavnOrchestrator.ScheduleOccurrencePreview
  alias FavnOrchestrator.SchedulerEntry

  @type run_id :: String.t()
  @type operator_actor :: Auth.actor()
  @type operator_session :: Auth.session()
  @type operator_actor_context :: OperatorContext.t()

  # The identity store is selected from the boot-validated persistence registry.
  # Dialyzer cannot follow successful callbacks through that dynamic module and
  # therefore erases the successful authorization branch at each of these
  # definitions. Keep the suppression scoped to the affected definitions so the
  # public success/error contracts and runtime authorization failures stay intact.
  @dialyzer {:no_match,
             [
               get_operator_lineage_graph: 2,
               get_operator_lineage_group: 3,
               get_operator_lineage_asset: 3,
               get_operator_lineage_edge: 3,
               operator_password_login: 4,
               operator_external_login: 2,
               introspect_operator_session: 2,
               revoke_operator_session: 1,
               list_operator_workspaces: 1,
               switch_operator_workspace: 2,
               page_operator_actors: 2,
               create_operator_actor: 5,
               attach_operator_actor: 3,
               update_operator_actor_membership: 4,
               page_operator_sessions: 2,
               revoke_operator_managed_session: 2,
               change_operator_password: 3,
               page_operator_audit: 2,
               active_workspace_configuration: 1,
               active_asset_catalogue: 1,
               active_pipeline_catalogue: 1,
               active_pipeline_detail: 2,
               active_asset_detail: 3,
               active_asset_documentation: 2,
               active_asset_run_detail: 3,
               get_asset_coverage: 2,
               active_asset_coverage_windows: 3,
               page_asset_missing_coverage: 3,
               plan_missing_coverage_backfill: 3,
               submit_missing_coverage_backfill: 4,
               plan_operator_rebuild: 4,
               start_operator_rebuild: 4,
               get_operator_rebuild: 2,
               page_operator_rebuilds: 2,
               page_operator_rebuild_items: 3,
               cancel_operator_rebuild: 4,
               retry_operator_rebuild: 4,
               reconcile_operator_rebuild: 3,
               plan_operator_target_recovery: 4,
               start_operator_target_recovery: 4,
               get_operator_target_recovery: 2,
               reconcile_operator_target_recovery: 3,
               list_logs: 3,
               replay_logs: 4,
               authorize_logs_subscription: 2,
               subscribe_logs: 2,
               submit_operator_run: 5,
               submit_operator_asset_backfill: 5,
               submit_operator_pipeline_backfill: 5,
               plan_operator_pipeline_backfill: 4,
               get_operator_backfill: 2,
               page_operator_backfill_windows: 3,
               list_operator_manifests: 1,
               get_operator_manifest: 2,
               cancel_operator_run: 3,
               retry_operator_run_remaining: 3,
               get_run_detail: 2,
               get_operator_run_flow: 2,
               get_operator_run_events: 2,
               list_operator_run_windows: 2,
               get_operator_run_asset_attempt: 3,
               get_operator_runner_overview: 2,
               get_operator_runner_session_tasks: 2,
               page_execution_groups: 2,
               count_execution_groups: 2,
               get_execution_group_detail: 3,
               get_asset_step_log_context: 3,
               list_run_stream_events: 3,
               authorize_run_subscription: 2,
               subscribe_run: 2,
               unsubscribe_run: 2,
               unsubscribe_run_wakeups: 1,
               authorize_runs_subscription: 1,
               subscribe_runs: 1,
               unsubscribe_runs: 1,
               page_schedule_list_entries: 2,
               get_schedule_entry: 2,
               enable_schedule: 3,
               disable_schedule: 3,
               preview_schedule_occurrences: 3,
               authorize_operator_context: 2
             ]}

  # These helpers are reached from the successful definitions above. The same
  # dynamic-dispatch blind spot otherwise reports them as unused.
  @dialyzer {:no_unused,
             [
               admin?: 1,
               operator_backfill_opts: 3,
               operator_refresh: 1,
               merge_coverage_metadata: 2,
               remaining_retry_plan_fingerprint: 1,
               begin_operator_command: 8,
               idempotency_key_opts: 1,
               put_rebuild_plan_idempotency: 2,
               put_rebuild_command_idempotency: 2,
               run_command_opts: 1,
               get_operator_submission: 2,
               finish_operator_result: 8,
               finish_operator_audit: 9,
               operator_error_code: 1,
               execution_group_status: 1
             ]}

  @doc "Builds browser-safe, non-authoritative operator identity hints."
  @spec operator_context(String.t(), operator_actor(), operator_session()) ::
          {:ok, OperatorContext.t()} | {:error, :invalid_operator_context}
  def operator_context(workspace_id, actor, session),
    do: OperatorContext.new(workspace_id, actor, session)

  @type manifest_summary :: Catalogue.manifest_summary()
  @type manifest_target_option :: Catalogue.manifest_target_option()
  @type manifest_targets :: Catalogue.manifest_targets()
  @type asset_catalogue_entry :: Catalogue.asset_catalogue_entry()
  @type pipeline_catalogue_entry :: Catalogue.pipeline_catalogue_entry()
  @type pipeline_run_history_entry :: Catalogue.pipeline_run_history_entry()
  @type pipeline_detail :: Catalogue.pipeline_detail()
  @type asset_run_config :: Catalogue.asset_run_config()
  @type asset_detail :: Catalogue.asset_detail()
  @type asset_dependency :: Catalogue.asset_dependency()
  @type asset_documentation :: Catalogue.asset_documentation()
  @type asset_run_history_entry :: Catalogue.asset_run_history_entry()
  @type asset_run_detail :: Catalogue.asset_run_detail()
  @type asset_freshness_reason :: Catalogue.asset_freshness_reason()
  @type asset_freshness_detail :: Catalogue.asset_freshness_detail()
  @type coverage_summary :: Favn.Coverage.Summary.t()
  @type missing_coverage_page :: Coverage.missing_page()
  @type coverage_window_states :: Coverage.window_states()

  @doc "Returns a customer-visible lineage graph after operator reauthorization."
  @spec get_operator_lineage_graph(OperatorContext.t(), keyword()) ::
          {:ok, Lineage.Graph.t()} | {:error, term()}
  def get_operator_lineage_graph(%OperatorContext{} = operator_context, opts)
      when is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Lineage.get_graph(context, opts)
    end
  end

  @doc "Returns one customer-visible lineage group after operator reauthorization."
  @spec get_operator_lineage_group(OperatorContext.t(), String.t(), keyword()) ::
          {:ok, Lineage.GroupInspector.t()} | {:error, term()}
  def get_operator_lineage_group(%OperatorContext{} = operator_context, group_id, opts)
      when is_binary(group_id) and is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Lineage.get_group(context, group_id, opts)
    end
  end

  @doc "Returns one customer-visible lineage asset after operator reauthorization."
  @spec get_operator_lineage_asset(OperatorContext.t(), String.t(), keyword()) ::
          {:ok, Lineage.AssetInspector.t()} | {:error, term()}
  def get_operator_lineage_asset(%OperatorContext{} = operator_context, asset_id, opts)
      when is_binary(asset_id) and is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Lineage.get_asset(context, asset_id, opts)
    end
  end

  @doc "Returns one customer-visible lineage edge after operator reauthorization."
  @spec get_operator_lineage_edge(OperatorContext.t(), String.t(), keyword()) ::
          {:ok, Lineage.EdgeInspector.t()} | {:error, term()}
  def get_operator_lineage_edge(%OperatorContext{} = operator_context, edge_id, opts)
      when is_binary(edge_id) and is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Lineage.get_edge(context, edge_id, opts)
    end
  end

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

  @type execution_group_counts :: %{
          active: non_neg_integer(),
          failed: non_neg_integer(),
          succeeded: non_neg_integer(),
          total: non_neg_integer()
        }
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

  @doc "Returns bounded lifecycle state for operator and release tooling."
  @spec lifecycle() :: map()
  def lifecycle, do: Lifecycle.diagnostics()

  @doc "Begins the irreversible bounded drain used before a controlled shutdown."
  @spec drain(keyword()) :: {:ok, map()}
  def drain(opts \\ []) when is_list(opts), do: Shutdown.drain(opts)

  @doc "Authenticates an operator against one explicit workspace membership."
  @spec operator_password_login(String.t(), String.t(), String.t(), keyword() | map()) ::
          {:ok, operator_session(), operator_actor()} | {:error, :invalid_credentials}
  def operator_password_login(workspace_id, username, password, opts)
      when is_binary(workspace_id) and is_binary(username) and is_binary(password) and
             (is_list(opts) or is_map(opts)) do
    with {:ok, context} <-
           WorkspaceContext.new(workspace_id, "auth:login", [:customer_reader]),
         {:ok, session, actor} <- Auth.password_login(context, username, password, opts) do
      {:ok, session, actor}
    else
      {:error, _reason} -> {:error, :invalid_credentials}
    end
  end

  @doc """
  Creates a Favn session for a pre-linked identity authenticated by the
  configured external platform adapter.

  This does not accept provider roles, groups, email addresses, or tokens.
  """
  @spec operator_external_login(String.t(), map()) ::
          {:ok, operator_session(), operator_actor()} | {:error, :invalid_credentials}
  def operator_external_login(
        workspace_id,
        %{
          provider: "azure_container_apps_entra",
          tenant_id: tenant_id,
          subject_id: subject_id
        } = identity
      )
      when is_binary(workspace_id) and is_binary(tenant_id) and is_binary(subject_id) do
    with {:ok, context} <-
           WorkspaceContext.new(workspace_id, "auth:external-login", [:customer_reader]),
         {:ok, session, actor} <- Auth.external_login(context, identity) do
      {:ok, session, actor}
    else
      {:error, _reason} -> {:error, :invalid_credentials}
    end
  end

  def operator_external_login(_workspace_id, _identity), do: {:error, :invalid_credentials}

  @doc false
  @spec trusted_local_development_login(String.t(), String.t(), String.t()) ::
          {:ok, operator_session()} | {:error, :trusted_local_development_unavailable}
  def trusted_local_development_login(workspace_id, username, capability)
      when is_binary(workspace_id) and is_binary(username) and is_binary(capability) do
    Auth.trusted_local_development_login(workspace_id, username, capability)
  end

  @doc "Resolves an operator session within one explicit workspace membership."
  @spec introspect_operator_session(String.t(), String.t()) ::
          {:ok, operator_session(), operator_actor()} | {:error, :invalid_session}
  def introspect_operator_session(workspace_id, session_token)
      when is_binary(workspace_id) and is_binary(session_token) do
    with {:ok, context} <-
           WorkspaceContext.new(workspace_id, "auth:session", [:customer_reader]),
         {:ok, session, actor} <- Auth.introspect_session(context, session_token) do
      {:ok, session, actor}
    else
      {:error, _reason} -> {:error, :invalid_session}
    end
  end

  @doc "Revalidates and revokes the current operator session."
  @spec revoke_operator_session(OperatorContext.t()) :: :ok | {:error, term()}
  def revoke_operator_session(%OperatorContext{} = operator_context) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Auth.revoke_session(context, operator_context.session_id)
    end
  end

  @doc "Returns the current actor's workspace choices after persisted reauthorization."
  @spec list_operator_workspaces(OperatorContext.t()) :: {:ok, [map()]} | {:error, term()}
  def list_operator_workspaces(%OperatorContext{} = operator_context) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :viewer) do
      Auth.list_actor_workspaces(context, actor.id)
    end
  end

  @doc "Atomically rotates the current session into another active workspace."
  @spec switch_operator_workspace(OperatorContext.t(), String.t()) ::
          {:ok, operator_session()} | {:error, term()}
  def switch_operator_workspace(%OperatorContext{} = operator_context, target_workspace_id)
      when is_binary(target_workspace_id) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :viewer) do
      Auth.switch_workspace(
        context,
        %{id: operator_context.session_id, actor_id: actor.id},
        target_workspace_id
      )
    end
  end

  @doc "Subscribes the current process to identity invalidation topics."
  @spec subscribe_operator_identity(OperatorContext.t()) :: :ok | {:error, term()}
  def subscribe_operator_identity(%OperatorContext{} = context) do
    Events.subscribe_identity(context.workspace_id, context.actor_id, context.session_id)
  end

  @doc "Pages current-workspace actors after administrator reauthorization."
  @spec page_operator_actors(OperatorContext.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def page_operator_actors(%OperatorContext{} = operator_context, opts \\ []) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :admin) do
      Auth.page_actors(context, opts)
    end
  end

  @doc "Creates an actor in the current workspace after administrator reauthorization."
  @spec create_operator_actor(
          OperatorContext.t(),
          String.t(),
          String.t(),
          String.t(),
          [atom() | String.t()]
        ) :: {:ok, operator_actor()} | {:error, term()}
  def create_operator_actor(
        %OperatorContext{} = operator_context,
        username,
        password,
        display_name,
        roles
      ) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :admin) do
      Auth.create_actor(context, username, password, display_name, roles)
    end
  end

  @doc "Attaches one exact existing username to the current workspace."
  @spec attach_operator_actor(
          OperatorContext.t(),
          String.t(),
          [atom() | String.t()]
        ) :: {:ok, operator_actor()} | {:error, term()}
  def attach_operator_actor(%OperatorContext{} = operator_context, username, roles) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :admin) do
      Auth.attach_actor_membership(context, username, roles)
    end
  end

  @doc "Changes current-workspace membership after administrator reauthorization."
  @spec update_operator_actor_membership(
          OperatorContext.t(),
          String.t(),
          [atom() | String.t()],
          :active | :suspended | :revoked
        ) :: {:ok, operator_actor()} | {:error, term()}
  def update_operator_actor_membership(
        %OperatorContext{} = operator_context,
        actor_id,
        roles,
        status
      ) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :admin) do
      Auth.update_actor_membership(context, actor_id, roles, status)
    end
  end

  @doc "Pages sessions issued for the current workspace after admin reauthorization."
  @spec page_operator_sessions(OperatorContext.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def page_operator_sessions(%OperatorContext{} = operator_context, opts \\ []) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :admin) do
      Auth.page_sessions(context, opts)
    end
  end

  @doc "Revokes one current-workspace session after admin reauthorization."
  @spec revoke_operator_managed_session(OperatorContext.t(), String.t()) ::
          :ok | {:error, term()}
  def revoke_operator_managed_session(%OperatorContext{} = operator_context, session_id) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :admin) do
      Auth.revoke_session(context, session_id)
    end
  end

  @doc "Changes the current actor's global password and revokes every actor session."
  @spec change_operator_password(OperatorContext.t(), String.t(), String.t()) ::
          :ok | {:error, term()}
  def change_operator_password(
        %OperatorContext{} = operator_context,
        current_password,
        new_password
      )
      when is_binary(current_password) and is_binary(new_password) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :viewer) do
      Auth.set_actor_password(context, actor.id, current_password, new_password)
    end
  end

  @doc "Pages redacted current-workspace authorization audit after admin reauthorization."
  @spec page_operator_audit(OperatorContext.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def page_operator_audit(%OperatorContext{} = operator_context, opts \\ []) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :admin) do
      Auth.page_audit(context, opts)
    end
  end

  @doc """
  Returns whether an operator actor has at least the required role.
  """
  @spec operator_has_role?(operator_actor(), :viewer | :operator | :admin) :: boolean()
  def operator_has_role?(actor, role) when role in [:viewer, :operator, :admin] do
    Auth.has_role?(actor, role)
  end

  @doc "Returns one manifest release through an explicit workspace authority."
  @spec get_manifest(WorkspaceContext.t(), String.t()) ::
          {:ok, Version.t()} | {:error, term()}
  def get_manifest(%WorkspaceContext{} = context, manifest_version_id)
      when is_binary(manifest_version_id) do
    ManifestStore.get_manifest(context, manifest_version_id)
  end

  @doc "Returns the active deployment's non-secret workspace configuration."
  @spec active_workspace_configuration(OperatorContext.t()) ::
          {:ok, WorkspaceConfiguration.t()} | {:error, term()}
  def active_workspace_configuration(%OperatorContext{} = operator_context) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      WorkspaceConfiguration.active(context)
    end
  end

  @doc "Returns customer-visible asset catalogue entries for an operator workspace."
  @spec active_asset_catalogue(OperatorContext.t()) ::
          {:ok, [asset_catalogue_entry()]} | {:error, term()}
  def active_asset_catalogue(%OperatorContext{} = operator_context) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Catalogue.active_asset_catalogue(context)
    end
  end

  @doc "Returns customer-visible pipeline catalogue entries for an operator workspace."
  @spec active_pipeline_catalogue(OperatorContext.t()) ::
          {:ok, [pipeline_catalogue_entry()]} | {:error, term()}
  def active_pipeline_catalogue(%OperatorContext{} = operator_context) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Catalogue.active_pipeline_catalogue(context)
    end
  end

  @doc "Returns one customer-visible pipeline detail for an operator workspace."
  @spec active_pipeline_detail(OperatorContext.t(), String.t()) ::
          {:ok, pipeline_detail()} | {:error, term()}
  def active_pipeline_detail(%OperatorContext{} = operator_context, target_id)
      when is_binary(target_id) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Catalogue.active_pipeline_detail(context, target_id)
    end
  end

  @doc """
  Returns one customer-visible asset detail for an operator workspace.

  Pass `run_context_id: id` to select one manifest-pinned pipeline context for
  run-anchor and policy-sensitive freshness projections. A unique context is
  automatic; multiple contexts are returned as an explicit ambiguous state.
  """
  @spec active_asset_detail(OperatorContext.t(), String.t(), keyword()) ::
          {:ok, asset_detail()} | {:error, term()}
  def active_asset_detail(%OperatorContext{} = operator_context, target_id, opts)
      when is_binary(target_id) and is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Catalogue.active_asset_detail(context, target_id, opts)
    end
  end

  @doc """
  Returns what one asset is and how it is written, for an operator workspace.

  Answers "what does this thing do" — the author's own documentation, its tags and
  owner, where it lands, and then whichever source it has: the query text for a SQL
  asset, the module and function for an Elixir one.
  """
  @spec active_asset_documentation(OperatorContext.t(), String.t()) ::
          {:ok, asset_documentation()} | {:error, term()}
  def active_asset_documentation(%OperatorContext{} = operator_context, target_id)
      when is_binary(target_id) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Catalogue.active_asset_documentation(context, target_id)
    end
  end

  @doc """
  Returns one asset's view of one of its runs for an operator workspace.

  Answers "what did this run actually produce for this asset" — the manifest-pinned
  data contract and checks beside the results that run recorded for them, the
  asset's own step outcome, the run metadata, and the runtime inputs the run pinned.
  A run belonging to another asset reads as `:not_found`.
  """
  @spec active_asset_run_detail(OperatorContext.t(), String.t(), run_id()) ::
          {:ok, asset_run_detail()} | {:error, term()}
  def active_asset_run_detail(%OperatorContext{} = operator_context, target_id, run_id)
      when is_binary(target_id) and is_binary(run_id) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Catalogue.active_asset_run_detail(context, target_id, run_id)
    end
  end

  @doc "Returns generation-aware coverage for one active asset after reauthorization."
  @spec get_asset_coverage(OperatorContext.t(), String.t()) ::
          {:ok, coverage_summary()} | {:error, term()}
  def get_asset_coverage(%OperatorContext{} = operator_context, target_id)
      when is_binary(target_id) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Coverage.summary(context, target_id)
    end
  end

  @doc """
  Returns every expected coverage window in one addressed range after reauthorization.

  Pass `:from` and `:until` as local dates in the asset's own coverage timezone,
  `:until` exclusive, to name the range. Unlike `page_asset_missing_coverage/3` this
  reports covered windows too, which is what a calendar of the range needs.
  """
  @spec active_asset_coverage_windows(OperatorContext.t(), String.t(), keyword()) ::
          {:ok, coverage_window_states()} | {:error, term()}
  def active_asset_coverage_windows(%OperatorContext{} = operator_context, target_id, opts \\ [])
      when is_binary(target_id) and is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Coverage.window_states(context, target_id, opts)
    end
  end

  @doc "Returns one cursor-paged set of missing coverage windows after reauthorization."
  @spec page_asset_missing_coverage(OperatorContext.t(), String.t(), keyword()) ::
          {:ok, missing_coverage_page()} | {:error, term()}
  def page_asset_missing_coverage(%OperatorContext{} = operator_context, target_id, opts \\ [])
      when is_binary(target_id) and is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Coverage.missing_windows(context, target_id, opts)
    end
  end

  @doc """
  Plans an exact backfill of missing windows.

  Pass `:window_keys` to plan named windows, `:cursor`/`:limit` to plan one page,
  or neither to plan every missing window. `:combine_windows` defaults to
  `false`. When true, the selected windows must be contiguous and the existing
  combined-append restrictions apply. The execution mode is frozen into the
  returned plan.
  """
  @spec plan_missing_coverage_backfill(OperatorContext.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def plan_missing_coverage_backfill(%OperatorContext{} = operator_context, target_id, opts \\ [])
      when is_binary(target_id) and is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :operator) do
      Coverage.plan_missing_backfill(context, target_id, opts)
    end
  end

  @doc """
  Submits an exact, revalidated missing-window backfill plan.

  `:idempotency_key` is required. The returned run id is derived here from it, so a
  caller cannot name the run. Resubmitting the *same* request under the same key
  resolves to the same run and the same backfill; a changed request under that key is
  refused, and so is a different key while the first submission is still unresolved.
  Combined or separate execution is taken from the immutable plan rather than
  accepted as a submission-time override.
  """
  @spec submit_missing_coverage_backfill(
          OperatorContext.t(),
          String.t(),
          map(),
          keyword()
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_missing_coverage_backfill(
        %OperatorContext{} = operator_context,
        target_id,
        plan,
        opts \\ []
      )
      when is_binary(target_id) and is_map(plan) and is_list(opts) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :operator),
         metadata <- %{
           actor_id: actor.id,
           session_id: operator_context.session_id,
           requested_by: :operator
         },
         {:ok, domain_opts} <-
           opts
           |> Keyword.delete(:idempotency_key)
           |> merge_coverage_metadata(metadata),
         {:ok, intent} <-
           begin_operator_command(
             context,
             operator_context,
             actor,
             "coverage.backfill.submit",
             "asset",
             target_id,
             %{
               target_id: target_id,
               plan:
                 Map.take(plan, [
                   :plan_id,
                   :plan_hash,
                   :manifest_version_id,
                   :deployment_id,
                   :window_count,
                   :selection,
                   :combine_windows
                 ])
             },
             opts
           ),
         opts <-
           domain_opts
           |> Keyword.put(:idempotency, intent.idempotency)
           |> Keyword.put(
             :root_run_id,
             OperatorAudit.deterministic_id(intent, "run", [target_id])
           ),
         result <- Coverage.submit_missing_backfill(context, target_id, plan, opts) do
      finish_operator_result(
        context,
        operator_context,
        actor,
        intent,
        "backfill",
        target_id,
        result,
        fn backfill ->
          {backfill.root_run_id,
           %{backfill_id: backfill.backfill_id, root_run_id: backfill.root_run_id},
           {:ok, backfill.root_run_id}}
        end
      )
    end
  end

  defp merge_coverage_metadata(opts, required) do
    case Keyword.get(opts, :metadata, %{}) do
      metadata when is_map(metadata) ->
        {:ok, Keyword.put(opts, :metadata, Map.merge(metadata, required))}

      _invalid ->
        {:error, :invalid_coverage_backfill_options}
    end
  end

  @doc "Creates an immutable manual rebuild plan after operator reauthorization."
  @spec plan_operator_rebuild(OperatorContext.t(), String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def plan_operator_rebuild(%OperatorContext{} = operator_context, target_id, reason, opts)
      when is_binary(target_id) and is_binary(reason) and is_list(opts) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :operator),
         {:ok, intent} <-
           begin_operator_command(
             context,
             operator_context,
             actor,
             "rebuild.plan",
             "target",
             target_id,
             %{target_id: target_id, reason: reason},
             opts
           ),
         opts <-
           opts
           |> put_rebuild_plan_idempotency(intent)
           |> Keyword.put_new(
             :operation_id,
             OperatorAudit.deterministic_id(intent, "rebuild", [target_id])
           ),
         result <- Rebuilds.plan(context, target_id, reason, opts) do
      finish_operator_result(
        context,
        operator_context,
        actor,
        intent,
        "rebuild",
        target_id,
        result,
        fn plan ->
          {plan.plan_id, %{plan_id: plan.plan_id, plan_hash: plan.plan_hash},
           {:ok, OperatorRebuilds.plan(plan, admin?(context))}}
        end
      )
    end
  end

  @doc "Starts one exact immutable rebuild plan after administrator reauthorization."
  @spec start_operator_rebuild(OperatorContext.t(), String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def start_operator_rebuild(
        %OperatorContext{} = operator_context,
        plan_id,
        plan_hash,
        opts
      )
      when is_binary(plan_id) and is_binary(plan_hash) and is_list(opts) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :admin),
         {:ok, intent} <-
           begin_operator_command(
             context,
             operator_context,
             actor,
             "rebuild.start",
             "rebuild",
             plan_id,
             %{plan_id: plan_id, plan_hash: plan_hash},
             opts
           ),
         result <-
           Rebuilds.start(
             context,
             plan_id,
             plan_hash,
             put_rebuild_command_idempotency(opts, intent)
           ) do
      finish_operator_result(
        context,
        operator_context,
        actor,
        intent,
        "rebuild",
        plan_id,
        result,
        fn operation ->
          {operation.operation_id, %{operation_id: operation.operation_id},
           {:ok, OperatorRebuilds.operation(operation, true)}}
        end
      )
    end
  end

  @doc "Returns one bounded rebuild detail after viewer reauthorization."
  @spec get_operator_rebuild(OperatorContext.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def get_operator_rebuild(%OperatorContext{} = operator_context, operation_id)
      when is_binary(operation_id) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer),
         {:ok, operation} <- Rebuilds.get(context, operation_id) do
      {:ok, OperatorRebuilds.operation(operation, admin?(context))}
    end
  end

  @doc "Pages bounded rebuild summaries after viewer reauthorization."
  @spec page_operator_rebuilds(OperatorContext.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def page_operator_rebuilds(%OperatorContext{} = operator_context, opts \\ [])
      when is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer),
         {:ok, page} <- Rebuilds.page(context, opts) do
      {:ok,
       OperatorRebuilds.page(page, &OperatorRebuilds.operation(&1, admin?(context), :summary))}
    end
  end

  @doc "Pages bounded logical rebuild items after viewer reauthorization."
  @spec page_operator_rebuild_items(OperatorContext.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def page_operator_rebuild_items(%OperatorContext{} = operator_context, operation_id, opts \\ [])
      when is_binary(operation_id) and is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer),
         {:ok, page} <- Rebuilds.page_items(context, operation_id, opts) do
      {:ok, OperatorRebuilds.page(page, &OperatorRebuilds.item/1)}
    end
  end

  @doc "Requests cancellation of one rebuild after administrator reauthorization."
  @spec cancel_operator_rebuild(OperatorContext.t(), String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def cancel_operator_rebuild(
        %OperatorContext{} = operator_context,
        operation_id,
        reason,
        opts
      )
      when is_binary(operation_id) and is_binary(reason) and is_list(opts) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :admin),
         {:ok, intent} <-
           begin_operator_command(
             context,
             operator_context,
             actor,
             "rebuild.cancel",
             "rebuild",
             operation_id,
             %{operation_id: operation_id, reason: reason},
             opts
           ),
         result <-
           Rebuilds.cancel(
             context,
             operation_id,
             reason,
             put_rebuild_command_idempotency(opts, intent)
           ) do
      finish_operator_result(
        context,
        operator_context,
        actor,
        intent,
        "rebuild",
        operation_id,
        result,
        fn operation ->
          {operation.operation_id, %{operation_id: operation.operation_id},
           {:ok, OperatorRebuilds.operation(operation, true)}}
        end
      )
    end
  end

  @doc "Retries safe failed rebuild work after administrator reauthorization."
  @spec retry_operator_rebuild(OperatorContext.t(), String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def retry_operator_rebuild(
        %OperatorContext{} = operator_context,
        operation_id,
        plan_hash,
        opts
      )
      when is_binary(operation_id) and is_binary(plan_hash) and is_list(opts) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :admin),
         {:ok, intent} <-
           begin_operator_command(
             context,
             operator_context,
             actor,
             "rebuild.retry",
             "rebuild",
             operation_id,
             %{operation_id: operation_id, plan_hash: plan_hash},
             opts
           ),
         result <-
           Rebuilds.retry(
             context,
             operation_id,
             plan_hash,
             put_rebuild_command_idempotency(opts, intent)
           ) do
      finish_operator_result(
        context,
        operator_context,
        actor,
        intent,
        "rebuild",
        operation_id,
        result,
        fn operation ->
          {operation.operation_id, %{operation_id: operation.operation_id},
           {:ok, OperatorRebuilds.operation(operation, true)}}
        end
      )
    end
  end

  @doc "Requests reconciliation of an unknown rebuild outcome after administrator reauthorization."
  @spec reconcile_operator_rebuild(OperatorContext.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def reconcile_operator_rebuild(%OperatorContext{} = operator_context, operation_id, opts)
      when is_binary(operation_id) and is_list(opts) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :admin),
         {:ok, intent} <-
           begin_operator_command(
             context,
             operator_context,
             actor,
             "rebuild.reconcile",
             "rebuild",
             operation_id,
             %{operation_id: operation_id},
             opts
           ),
         result <-
           Rebuilds.reconcile(
             context,
             operation_id,
             put_rebuild_command_idempotency(opts, intent)
           ) do
      finish_operator_result(
        context,
        operator_context,
        actor,
        intent,
        "rebuild",
        operation_id,
        result,
        fn operation ->
          {operation.operation_id, %{operation_id: operation.operation_id},
           {:ok, OperatorRebuilds.operation(operation, true)}}
        end
      )
    end
  end

  @doc "Creates an immutable evidence-backed target-recovery plan after reauthorization."
  @spec plan_operator_target_recovery(
          OperatorContext.t(),
          String.t(),
          String.t(),
          keyword()
        ) :: {:ok, map()} | {:error, term()}
  def plan_operator_target_recovery(
        %OperatorContext{} = operator_context,
        target_id,
        reason,
        opts
      )
      when is_binary(target_id) and is_binary(reason) and is_list(opts) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :operator),
         {:ok, intent} <-
           begin_operator_command(
             context,
             operator_context,
             actor,
             "target_recovery.plan",
             "target",
             target_id,
             %{target_id: target_id, reason: reason},
             opts
           ),
         opts <-
           opts
           |> Keyword.put(:idempotency_key, intent.key_hash)
           |> Keyword.put_new(:session_id, operator_context.session_id)
           |> Keyword.put_new(
             :operation_id,
             OperatorAudit.deterministic_id(intent, "target_recovery", [target_id])
           ),
         result <- TargetRecovery.plan(context, target_id, reason, opts) do
      finish_operator_result(
        context,
        operator_context,
        actor,
        intent,
        "target_recovery",
        target_id,
        result,
        fn plan ->
          {plan.plan_id, %{plan_id: plan.plan_id, plan_hash: plan.plan_hash},
           {:ok, OperatorTargetRecovery.plan(plan, admin?(context))}}
        end
      )
    end
  end

  @doc "Starts one exact target-recovery plan after administrator reauthorization."
  @spec start_operator_target_recovery(
          OperatorContext.t(),
          String.t(),
          String.t(),
          keyword()
        ) :: {:ok, map()} | {:error, term()}
  def start_operator_target_recovery(
        %OperatorContext{} = operator_context,
        operation_id,
        plan_hash,
        opts
      )
      when is_binary(operation_id) and is_binary(plan_hash) and is_list(opts) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :admin),
         {:ok, intent} <-
           begin_operator_command(
             context,
             operator_context,
             actor,
             "target_recovery.start",
             "target_recovery",
             operation_id,
             %{operation_id: operation_id, plan_hash: plan_hash},
             opts
           ),
         result <-
           TargetRecovery.start(
             context,
             operation_id,
             plan_hash,
             Keyword.drop(opts, [:idempotency_key])
           ) do
      finish_operator_result(
        context,
        operator_context,
        actor,
        intent,
        "target_recovery",
        operation_id,
        result,
        fn operation ->
          {operation.operation_id, %{operation_id: operation.operation_id},
           {:ok, OperatorTargetRecovery.operation(operation, true)}}
        end
      )
    end
  end

  @doc "Returns one bounded target-recovery operation after viewer reauthorization."
  @spec get_operator_target_recovery(OperatorContext.t(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def get_operator_target_recovery(%OperatorContext{} = operator_context, operation_id)
      when is_binary(operation_id) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer),
         {:ok, operation} <- TargetRecovery.get(context, operation_id) do
      {:ok, OperatorTargetRecovery.operation(operation, admin?(context))}
    end
  end

  @doc "Reconciles an inconclusive target recovery without retrying its marker write."
  @spec reconcile_operator_target_recovery(OperatorContext.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def reconcile_operator_target_recovery(
        %OperatorContext{} = operator_context,
        operation_id,
        opts
      )
      when is_binary(operation_id) and is_list(opts) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :admin),
         {:ok, intent} <-
           begin_operator_command(
             context,
             operator_context,
             actor,
             "target_recovery.reconcile",
             "target_recovery",
             operation_id,
             %{operation_id: operation_id},
             opts
           ),
         result <-
           TargetRecovery.reconcile(
             context,
             operation_id,
             Keyword.drop(opts, [:idempotency_key])
           ) do
      finish_operator_result(
        context,
        operator_context,
        actor,
        intent,
        "target_recovery",
        operation_id,
        result,
        fn operation ->
          {operation.operation_id, %{operation_id: operation.operation_id},
           {:ok, OperatorTargetRecovery.operation(operation, true)}}
        end
      )
    end
  end

  @doc "Lists workspace-isolated logs after reauthorizing an operator context."
  @spec list_logs(OperatorContext.t(), term(), keyword()) :: {:ok, map()} | {:error, term()}
  def list_logs(%OperatorContext{} = operator_context, filter, opts) when is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Logs.page(context, filter, opts)
    end
  end

  @doc "Replays workspace-isolated logs after reauthorizing an operator context."
  @spec replay_logs(OperatorContext.t(), term(), term(), keyword()) ::
          {:ok, [Favn.Log.Entry.t()]} | {:error, term()}
  def replay_logs(%OperatorContext{} = operator_context, cursor, filter, opts)
      when is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Logs.replay(context, cursor, filter, opts)
    end
  end

  @doc "Subscribes to workspace-isolated log wakeups after reauthorization."
  @spec subscribe_logs(OperatorContext.t(), term()) :: {:ok, term()} | {:error, term()}
  def subscribe_logs(%OperatorContext{} = operator_context, filter) do
    with {:ok, grant} <- authorize_logs_subscription(operator_context, filter) do
      activate_logs_subscription(grant)
    end
  end

  @doc "Authorizes and normalizes a caller-owned log subscription without subscribing the caller."
  @spec authorize_logs_subscription(OperatorContext.t(), term()) ::
          {:ok, map()} | {:error, term()}
  def authorize_logs_subscription(%OperatorContext{} = operator_context, filter) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Logs.prepare_subscription(context, filter)
    end
  end

  @doc "Activates a previously authorized log subscription in the calling process."
  @spec activate_logs_subscription(map()) :: {:ok, term()} | {:error, term()}
  def activate_logs_subscription(grant), do: Logs.subscribe_prepared(grant)

  @doc """
  Unsubscribes the caller from a prior backend log subscription.

  Callers must pass the subscription returned by `subscribe_logs/1`; equivalent
  filters cannot stop the owned forwarding process.
  """
  @spec unsubscribe_logs(term()) :: :ok | {:error, :invalid_log_subscription}
  def unsubscribe_logs(subscription), do: Logs.unsubscribe_logs(subscription)

  @doc """
  Submits one manifest target run for an authenticated operator actor context.

  This is the shared command boundary for browser, API, and CLI callers. The
  target decides whether the request is normalized as an asset run or a pipeline
  run; callers should not dispatch to asset/pipeline-specific submit functions.
  """
  @spec submit_operator_run(
          OperatorContext.t(),
          String.t(),
          map(),
          AssetRunRequest.t() | PipelineRunRequest.t() | map() | keyword() | nil,
          keyword()
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_operator_run(
        %OperatorContext{} = operator_context,
        manifest_version_id,
        target,
        command_input,
        opts
      )
      when is_list(opts) do
    resource_id =
      Map.get(target, :target_id) || Map.get(target, "target_id") || Map.get(target, :id) ||
        Map.get(target, "id") || manifest_version_id

    resource_type =
      target
      |> then(&(Map.get(&1, :type) || Map.get(&1, "type") || :target))
      |> to_string()

    with {:ok, context, actor} <- authorize_operator_context(operator_context, :operator),
         {:ok, intent} <-
           begin_operator_command(
             context,
             operator_context,
             actor,
             "run.submit",
             resource_type,
             resource_id,
             %{
               manifest_version_id: manifest_version_id,
               target: target,
               command_input: command_input
             },
             opts
           ),
         result <-
           OperatorCommands.submit_run(
             context,
             manifest_version_id,
             target,
             command_input,
             run_command_opts(intent)
           ) do
      finish_operator_result(
        context,
        operator_context,
        actor,
        intent,
        "run",
        resource_id,
        result,
        fn run_id -> {run_id, %{run_id: run_id}, {:ok, run_id}} end
      )
    end
  end

  @doc false
  @spec submit_operator_run(
          WorkspaceContext.t(),
          String.t(),
          map(),
          AssetRunRequest.t() | PipelineRunRequest.t() | map() | keyword() | nil,
          keyword()
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_operator_run(
        %WorkspaceContext{} = context,
        manifest_version_id,
        target,
        command_input,
        opts
      ) do
    OperatorCommands.submit_run(context, manifest_version_id, target, command_input, opts)
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
          String.t(),
          AssetBackfillRequest.t() | map() | keyword(),
          keyword()
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_operator_asset_backfill(
        %OperatorContext{} = operator_context,
        manifest_version_id,
        target_id,
        command_input,
        command_opts
      )
      when is_list(command_opts) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :operator),
         {:ok, request} <- AssetBackfillRequest.from_input(command_input),
         {:ok, version} <- ManifestStore.get_manifest(context, manifest_version_id),
         {:ok, asset} <- ManifestTarget.resolve_asset(version, target_id),
         {:ok, refresh} <-
           AssetOptions.operator_refresh(
             request.refresh_mode,
             asset.ref,
             request.dependency_mode
           ),
         {:ok, intent} <-
           begin_operator_command(
             context,
             operator_context,
             actor,
             "asset.backfill.submit",
             "asset",
             target_id,
             %{
               manifest_version_id: manifest_version_id,
               target_id: target_id,
               request: request
             },
             command_opts
           ),
         opts <-
           request
           |> operator_backfill_opts(actor, operator_context)
           |> Keyword.put(:dependencies, request.dependency_mode)
           |> Keyword.put(:refresh, refresh)
           |> Keyword.put(:idempotency, intent.idempotency)
           |> Keyword.put(
             :root_run_id,
             OperatorAudit.deterministic_id(intent, "run", [target_id])
           ),
         result <-
           FavnOrchestrator.Backfills.submit_asset(
             context,
             manifest_version_id,
             target_id,
             request.range,
             opts
           ) do
      finish_operator_result(
        context,
        operator_context,
        actor,
        intent,
        "backfill",
        target_id,
        result,
        fn backfill ->
          {backfill.root_run_id,
           %{backfill_id: backfill.backfill_id, root_run_id: backfill.root_run_id},
           {:ok, backfill.root_run_id}}
        end
      )
    end
  end

  @doc "Returns the active deployment's manifest ID for one workspace."
  @spec active_manifest(WorkspaceContext.t()) :: {:ok, String.t()} | {:error, term()}
  def active_manifest(%WorkspaceContext{} = context) do
    with {:ok, runtime} <- ManifestStore.get_runtime_state(context) do
      {:ok, runtime.manifest_version_id}
    end
  end

  @doc "Inspects an asset from the workspace's active manifest deployment."
  @spec inspect_manifest_asset(WorkspaceContext.t(), String.t(), String.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def inspect_manifest_asset(%WorkspaceContext{} = context, manifest_version_id, target_id, opts)
      when is_binary(manifest_version_id) and is_binary(target_id) and is_list(opts) do
    with {:ok, version} <-
           Manifests.get_active_target_release(
             context,
             manifest_version_id,
             :asset,
             target_id
           ),
         {:ok, result} <- inspect_manifest_asset_version(context, version, target_id, opts) do
      {:ok, result}
    end
  end

  defp inspect_manifest_asset_version(
         %WorkspaceContext{} = context,
         %Version{} = version,
         target_id,
         opts
       ) do
    with manifest_version_id <- version.manifest_version_id,
         {:ok, asset_ref} <- ManifestTarget.resolve_asset_ref(version, target_id),
         {:ok, binding} <- OperationRunnerTasks.binding(version, asset_ref) do
      request = %RelationInspectionRequest{
        manifest_version_id: manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: binding.required_runner_release_id,
        asset_ref: asset_ref,
        sample_limit: Keyword.get(opts, :sample_limit, 20)
      }

      identity =
        {:operator_inspection, target_id,
         context.request_id || Keyword.get(opts, :request_id) || unique_runner_task_identity()}

      case OperationRunnerTasks.ensure_and_await(
             context,
             version,
             asset_ref,
             :relation_inspection,
             request,
             identity,
             timeout: Keyword.get(opts, :timeout, 300_000)
           ) do
        {:ok, %RelationInspectionResult{} = result} ->
          with :ok <-
                 RunnerIdentityVerifier.verify_inspection_result(
                   binding.required_runner_release_id,
                   result
                 ) do
            {:ok, result}
          end

        {:ok, _invalid_result} ->
          {:error, :invalid_runner_inspection_result}

        {:error, _reason} = error ->
          error
      end
    end
  end

  @doc """
  Submits one pipeline backfill command for an authenticated operator actor context.

  This is the public facade boundary for browser, API, and CLI operator actions.
  Callers pass operator intent for the range and refresh mode. The orchestrator
  validates and translates that intent before submitting the runtime backfill.

  Missing or incomplete actor/session context returns `{:error,
  :unauthenticated}`; authenticated actors without the operator role return
  `{:error, :forbidden}`.

  TODO: add a narrow audit event for accepted LiveView operator commands once the
  browser command audit shape is finalized.
  """
  @spec submit_operator_pipeline_backfill(
          operator_actor_context(),
          String.t(),
          String.t(),
          PipelineBackfillRequest.t() | map() | keyword(),
          keyword()
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_operator_pipeline_backfill(
        %OperatorContext{} = operator_context,
        manifest_version_id,
        target_id,
        command_input,
        command_opts
      )
      when is_list(command_opts) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :operator),
         {:ok, request} <- PipelineBackfillRequest.from_input(command_input),
         true <- is_nil(request.coverage_baseline_id),
         {:ok, intent} <-
           begin_operator_command(
             context,
             operator_context,
             actor,
             "pipeline.backfill.submit",
             "pipeline",
             target_id,
             %{
               manifest_version_id: manifest_version_id,
               target_id: target_id,
               request: request
             },
             command_opts
           ),
         opts <-
           request
           |> operator_backfill_opts(actor, operator_context)
           |> Keyword.put(:idempotency, intent.idempotency)
           |> Keyword.put(
             :root_run_id,
             OperatorAudit.deterministic_id(intent, "run", [target_id])
           ),
         result <-
           FavnOrchestrator.Backfills.submit_pipeline(
             context,
             manifest_version_id,
             target_id,
             request.range,
             opts
           ) do
      finish_operator_result(
        context,
        operator_context,
        actor,
        intent,
        "backfill",
        target_id,
        result,
        fn backfill ->
          {backfill.root_run_id,
           %{backfill_id: backfill.backfill_id, root_run_id: backfill.root_run_id},
           {:ok, backfill.root_run_id}}
        end
      )
    else
      false -> {:error, {:unsupported_backfill_option, :coverage_baseline_id}}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Plans one pipeline backfill without writing control-plane state.

  The browser shows the plan — window count, range, and keys — before the
  operator confirms the submit, so a mistyped range is caught while it is
  still only a preview.
  """
  @spec plan_operator_pipeline_backfill(
          operator_actor_context(),
          String.t(),
          String.t(),
          PipelineBackfillRequest.t() | map() | keyword()
        ) :: {:ok, FavnOrchestrator.Backfills.plan()} | {:error, term()}
  def plan_operator_pipeline_backfill(
        %OperatorContext{} = operator_context,
        manifest_version_id,
        target_id,
        command_input \\ %{}
      ) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :operator),
         {:ok, request} <- PipelineBackfillRequest.from_input(command_input) do
      FavnOrchestrator.Backfills.plan_pipeline(
        context,
        manifest_version_id,
        target_id,
        request.range,
        maybe_put_opt([], :combine_windows, request.combine_windows)
      )
    end
  end

  @doc """
  Fetches one backfill for an authenticated operator actor context.

  Expected, succeeded, and failed window counts live on the backfill, not on
  its root run, so this is how the browser shows how a backfill is going.
  """
  @spec get_operator_backfill(operator_actor_context(), String.t()) ::
          {:ok, PersistedBackfill.t()} | {:error, term()}
  def get_operator_backfill(%OperatorContext{} = operator_context, backfill_id)
      when is_binary(backfill_id) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      FavnOrchestrator.Backfills.get(context, backfill_id)
    end
  end

  @doc """
  Returns one bounded keyset page of a backfill's windows for an operator.

  Options are `:limit`, `:status` (a window status atom to narrow to, such as
  `:failed`), and `:after` (the cursor from the previous page). A failed
  window carries its error, which is what an operator drills in for.
  """
  @spec page_operator_backfill_windows(operator_actor_context(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def page_operator_backfill_windows(
        %OperatorContext{} = operator_context,
        backfill_id,
        opts \\ []
      )
      when is_binary(backfill_id) and is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      FavnOrchestrator.Backfills.page_windows(context, backfill_id, opts)
    end
  end

  @doc """
  Lists the manifest releases visible to an operator.

  Today that is the active release alone — the same set the API's manifest
  list returns — so the browser and the CLI cannot disagree about what is
  deployed.
  """
  @spec list_operator_manifests(operator_actor_context()) ::
          {:ok, [manifest_summary()]} | {:error, term()}
  def list_operator_manifests(%OperatorContext{} = operator_context) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer),
         {:ok, %{manifest: manifest}} <- Manifests.active(context) do
      {:ok, [manifest]}
    end
  end

  @doc """
  Fetches one manifest release's details for an operator.
  """
  @spec get_operator_manifest(operator_actor_context(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def get_operator_manifest(%OperatorContext{} = operator_context, manifest_version_id)
      when is_binary(manifest_version_id) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Manifests.get_active_release(context, manifest_version_id)
    end
  end

  @doc "Requests cancellation within one explicit workspace."
  @spec cancel_run(WorkspaceContext.t(), run_id(), map()) :: :ok | {:error, term()}
  def cancel_run(%WorkspaceContext{} = context, run_id, reason)
      when is_binary(run_id) and is_map(reason) do
    cancel_run(context, run_id, reason, [])
  end

  @doc false
  @spec cancel_run(WorkspaceContext.t(), run_id(), map(), keyword()) ::
          :ok | {:error, term()}
  def cancel_run(%WorkspaceContext{} = context, run_id, reason, opts)
      when is_binary(run_id) and is_map(reason) and is_list(opts) do
    case RunSubmissions.cancel_pending(context, run_id, reason, opts) do
      :ok ->
        :ok

      {:error, %PersistenceError{kind: :not_found}} ->
        RunManager.cancel_run(context, run_id, reason, opts)

      {:error, :run_already_submitted} ->
        RunManager.cancel_run(context, run_id, reason, opts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Requests cancellation of the selected run's full submitted operation.

  The authenticated operator action includes all backfill windows and verified
  automatic recovery runs. Separately requested reruns stay independent. Scope
  is resolved again under workspace authority when the action is submitted.
  `:ok` acknowledges durable intent; background cleanup determines the outcome.
  `cancel_run/4` retains exact-run targeting for HTTP and CLI callers.
  """
  @spec cancel_operator_run(operator_actor_context(), run_id(), keyword()) ::
          :ok | {:error, term()}
  def cancel_operator_run(%OperatorContext{} = operator_context, run_id, opts)
      when is_binary(run_id) and is_list(opts) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :operator),
         {:ok, intent} <-
           begin_operator_command(
             context,
             operator_context,
             actor,
             "run.cancel",
             "run",
             run_id,
             %{run_id: run_id},
             opts
           ),
         result <-
           FavnOrchestrator.OperationCancellation.request(
             context,
             run_id,
             %{actor_id: actor.id, requested_by: :operator},
             idempotency: intent.idempotency
           ) do
      finish_operator_result(
        context,
        operator_context,
        actor,
        intent,
        "run",
        run_id,
        result,
        fn :ok -> {run_id, %{run_id: run_id}, :ok} end
      )
    end
  end

  def cancel_operator_run(_actor_context, _run_id, _opts), do: {:error, :unauthenticated}

  @doc false
  @spec retry_remaining(WorkspaceContext.t(), run_id(), keyword()) ::
          {:ok, map()} | {:partial, map()} | {:error, term()}
  def retry_remaining(%WorkspaceContext{} = context, run_id, opts)
      when is_binary(run_id) and is_list(opts) do
    with :ok <- validate_remaining_retry_opts(opts),
         {:ok, plan} <- RunRetryPlanner.remaining(context, run_id) do
      submit_remaining_retry_plan(context, plan, opts)
    end
  end

  @doc """
  Submits remaining retry work on behalf of an authenticated operator.
  """
  @spec retry_operator_run_remaining(operator_actor_context(), run_id(), keyword()) ::
          {:ok, map()} | {:partial, map()} | {:error, term()}
  def retry_operator_run_remaining(%OperatorContext{} = operator_context, run_id, opts)
      when is_binary(run_id) and is_list(opts) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :operator),
         {:ok, plan} <- RunRetryPlanner.remaining(context, run_id),
         {:ok, intent} <-
           begin_operator_command(
             context,
             operator_context,
             actor,
             "run.retry_remaining",
             "run",
             run_id,
             %{run_id: run_id, retry_plan_fingerprint: remaining_retry_plan_fingerprint(plan)},
             opts
           ),
         result <-
           submit_remaining_retry_plan(context, plan,
             metadata: %{operator_retry: true, actor_id: actor.id},
             operator_intent: intent
           ) do
      finish_operator_result(
        context,
        operator_context,
        actor,
        intent,
        "run_retry",
        run_id,
        result,
        fn
          {:partial, value} ->
            {run_id, %{source_run_id: run_id, run_ids: value.run_ids}, {:partial, value}}

          value ->
            {run_id, %{source_run_id: run_id, run_ids: value.run_ids}, {:ok, value}}
        end
      )
    end
  end

  def retry_operator_run_remaining(_actor_context, _run_id, _opts),
    do: {:error, :unauthenticated}

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
  Returns whether a failed browser command must retain its exact idempotency key.

  `true` means the durable outcome is unknown, so issuing a new key could repeat
  a write that already happened. Proven rejections return `false`.
  """
  @spec operator_command_retryable?(term()) :: boolean()
  def operator_command_retryable?(reason), do: operator_error_outcome(reason) == "unknown"

  @doc "Submits a rerun within one explicit workspace."
  @spec rerun(WorkspaceContext.t(), run_id(), keyword()) ::
          {:ok, run_id()} | {:error, term()}
  def rerun(%WorkspaceContext{} = context, source_run_id, opts)
      when is_binary(source_run_id) and is_list(opts) do
    RunSubmissions.enqueue_rerun(context, source_run_id, opts)
  end

  @doc "Loads the durable submission lifecycle for one reserved run identity."
  @spec get_run_submission(WorkspaceContext.t(), run_id()) :: {:ok, map()} | {:error, term()}
  def get_run_submission(%WorkspaceContext{} = context, run_id) when is_binary(run_id),
    do: RunSubmissions.get(context, run_id)

  @doc "Returns one bounded page of durable run submissions."
  @spec page_run_submissions(WorkspaceContext.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def page_run_submissions(%WorkspaceContext{} = context, opts) when is_list(opts),
    do: RunSubmissions.page(context, opts)

  @doc "Returns aggregate durable run-submission queue diagnostics."
  @spec run_submission_stats(WorkspaceContext.t()) :: {:ok, map()} | {:error, term()}
  def run_submission_stats(%WorkspaceContext{} = context),
    do: RunSubmissions.stats(context)

  defp submit_remaining_retry_plan(
         %WorkspaceContext{} = context,
         %{children: children, asset_count: asset_count} = plan,
         opts
       ) do
    children
    |> Enum.reduce_while({:ok, []}, fn child, {:ok, acc} ->
      {run_id, child_idempotency} = remaining_retry_identity(plan, child, opts)

      submit_opts =
        plan
        |> remaining_retry_opts(child, opts)
        |> Keyword.put(:run_id, run_id)
        |> Keyword.put(:submission_source, :child_run)
        |> maybe_put_opt(:_idempotency, child_idempotency)

      case RunSubmissions.enqueue_rerun(context, child.source_run_id, submit_opts) do
        {:ok, ^run_id} ->
          {:cont, {:ok, [run_id | acc]}}

        {:error, reason} when acc == [] ->
          {:halt, {:error, reason}}

        {:error, reason} ->
          {:halt,
           {:partial,
            %{
              source_run_id: plan.source_run_id,
              run_ids: Enum.reverse(acc),
              failed_run_id: run_id,
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

  defp new_run_id do
    suffix = 16 |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower)
    "run_#{suffix}"
  end

  defp unique_runner_task_identity do
    16 |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)
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
    |> maybe_put_opt(:window_selection, Map.get(child, :window_selection))
    |> maybe_put_opt(:anchor_window, Map.get(child, :anchor_window))
    |> maybe_put_opt(:parent_run_id, Map.get(child, :backfill_run_id))
    |> maybe_put_opt(:root_run_id, Map.get(child, :backfill_run_id))
  end

  defp validate_remaining_retry_opts(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, :invalid_retry_remaining_options}

      Keyword.keys(opts) -- [:metadata, :operator_intent] != [] ->
        {:error,
         {:invalid_retry_remaining_options, Keyword.keys(opts) -- [:metadata, :operator_intent]}}

      not is_map(Keyword.get(opts, :metadata, %{})) ->
        {:error, :invalid_retry_remaining_metadata}

      not is_nil(Keyword.get(opts, :operator_intent)) and
          not match?(%OperatorAudit{}, Keyword.get(opts, :operator_intent)) ->
        {:error, :invalid_retry_remaining_intent}

      true ->
        :ok
    end
  end

  defp remaining_retry_identity(plan, child, opts) do
    case Keyword.get(opts, :operator_intent) do
      %OperatorAudit{} = intent ->
        %CommandIdempotency{} = base_idempotency = intent.idempotency

        parts = [
          plan.source_run_id,
          child.source_run_id,
          Map.get(child, :backfill_run_id),
          Map.get(child, :window_key),
          child.node_keys
        ]

        run_id = OperatorAudit.deterministic_id(intent, "run", parts)
        fingerprint = Idempotency.request_fingerprint(parts)
        key_hash = Idempotency.request_fingerprint([intent.key_hash, parts])

        child_idempotency =
          %CommandIdempotency{
            base_idempotency
            | operation: "run.retry_remaining.child",
              key_hash: key_hash,
              request_fingerprint: fingerprint
          }

        {run_id, child_idempotency}

      nil ->
        {new_run_id(), nil}
    end
  end

  defp remaining_retry_plan_fingerprint(plan) do
    plan
    |> Map.take([:source_run_id, :asset_count, :children])
    |> Idempotency.request_fingerprint()
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

  @doc "Returns one persisted run only within the authorized workspace."
  @spec get_run(WorkspaceContext.t(), run_id()) :: {:ok, Favn.Run.t()} | {:error, term()}
  def get_run(%WorkspaceContext{} = context, run_id) when is_binary(run_id) do
    with {:ok, run_state} <- Runs.get(context, run_id),
         {:ok, pins} <- Runs.get_runtime_inputs(context, run_id) do
      run = Projector.project_run(run_state)
      metadata = maybe_put_runtime_input_lineage(run.metadata, pins)
      {:ok, %{run | metadata: metadata}}
    else
      {:error, %PersistenceError{kind: :not_found}} -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  end

  @doc "Returns one bounded keyset page of runs in the authorized workspace."
  @spec list_runs(WorkspaceContext.t(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def list_runs(%WorkspaceContext{} = context, opts) when is_list(opts) do
    case Runs.page_summaries(context, opts) do
      {:ok, page} -> {:ok, Enum.map(page.items, &Projector.project_run_summary/1)}
      {:error, _reason} = error -> error
    end
  end

  @doc "Returns one run detail after reauthorizing a browser-safe operator context."
  @spec get_run_detail(OperatorContext.t(), run_id()) :: {:ok, run_detail()} | {:error, term()}
  def get_run_detail(%OperatorContext{} = operator_context, run_id) when is_binary(run_id) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      RunReadModel.get_run_detail(context, run_id)
    end
  end

  @doc "Returns one exact run's bounded Flow data after operator reauthorization."
  @spec get_operator_run_flow(OperatorContext.t(), run_id()) ::
          {:ok,
           %{kind: :run, detail: OperatorRunView.Flow.t()}
           | %{kind: :submission, submission: map()}}
          | {:error, term()}
  def get_operator_run_flow(%OperatorContext{} = operator_context, run_id)
      when is_binary(run_id) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      case OperatorRunView.flow(context, run_id) do
        {:ok, flow} -> {:ok, %{kind: :run, detail: flow}}
        {:error, :not_found} -> get_operator_submission(context, run_id)
        {:error, _reason} = error -> error
      end
    end
  end

  @doc "Returns one exact run's lean header and bounded event rows."
  @spec get_operator_run_events(OperatorContext.t(), run_id()) ::
          {:ok,
           %{kind: :run, header: OperatorRunView.Header.t(), events: [map()]}
           | %{kind: :submission, submission: map()}}
          | {:error, term()}
  def get_operator_run_events(%OperatorContext{} = operator_context, run_id)
      when is_binary(run_id) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      with {:ok, header} <- OperatorRunView.header(context, run_id),
           {:ok, events} <- OperatorRunView.events(context, run_id) do
        {:ok, %{kind: :run, header: header, events: events}}
      else
        {:error, :not_found} -> get_operator_submission(context, run_id)
        {:error, _reason} = error -> error
      end
    end
  end

  @doc "Lists lean sibling window runs only when the operator asks for them."
  @spec list_operator_run_windows(OperatorContext.t(), run_id()) ::
          {:ok, FavnOrchestrator.Persistence.Results.RunWindowChoices.t()} | {:error, term()}
  def list_operator_run_windows(%OperatorContext{} = operator_context, run_id)
      when is_binary(run_id) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      OperatorRunView.windows(context, run_id)
    end
  end

  @doc "Returns one exact observed asset attempt after operator reauthorization."
  @spec get_operator_run_asset_attempt(OperatorContext.t(), run_id(), String.t()) ::
          {:ok, FavnOrchestrator.Persistence.Results.RunAssetAttempt.t()} | {:error, term()}
  def get_operator_run_asset_attempt(%OperatorContext{} = operator_context, run_id, asset_step_id)
      when is_binary(run_id) and is_binary(asset_step_id) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      OperatorRunView.asset_attempt(context, run_id, asset_step_id)
    end
  end

  defp get_operator_submission(context, run_id) do
    case RunSubmissions.get(context, run_id) do
      {:ok, submission} ->
        with {:ok, scope} <- FavnOrchestrator.OperationCancellation.scope(context, run_id) do
          detail =
            submission |> OperatorRunView.project_submission() |> Map.put(:cancellation, scope)

          {:ok, %{kind: :submission, submission: detail}}
        end

      {:error, %PersistenceError{kind: :not_found}} ->
        {:error, :not_found}

      {:error, _reason} = error ->
        error
    end
  end

  @doc """
  Returns runner health after operator reauthorization: live presence,
  capacity per pool and release, workspace task stats, durable session
  history, and busy/idle window totals.

  Runner presence, capacity, and session metadata are platform-global;
  task-level detail stays scoped to the authorized workspace. See
  `FavnOrchestrator.RunnerOverview.get/2` for the supported options.
  """
  @spec get_operator_runner_overview(OperatorContext.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def get_operator_runner_overview(%OperatorContext{} = operator_context, opts \\ [])
      when is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      RunnerOverview.get(context, opts)
    end
  end

  @doc """
  Pages one runner session's attributed tasks in the authorized workspace.

  Requires `:runner_instance_id`, `:session_generation`, and `:registered_at`
  from a session returned by `get_operator_runner_overview/2`; accepts
  `:ended_at`, `:statuses`, and `:limit`.
  """
  @spec get_operator_runner_session_tasks(OperatorContext.t(), keyword()) ::
          {:ok, [map()]} | {:error, term()}
  def get_operator_runner_session_tasks(%OperatorContext{} = operator_context, opts)
      when is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      RunnerOverview.session_tasks(context, opts)
    end
  end

  @doc """
  Returns a bounded execution-group page for one authorized operator workspace.

  Supported filters:

    * `:status` — one status, or a list to ask for any of several (`[:pending,
      :running]` is "in flight"). Run vocabulary is accepted and folded onto the
      four projected group statuses. A status that cannot be folded is an error
      rather than an ignored filter.
    * `:only_failed`, `:only_running` — booleans, and they override `:status`
    * `:search` — matches the group's root run id and its runs' target modules and
      names
    * `:trigger_type` — one trigger, for example `:schedule`
    * `:started_after`, `:started_before` — `DateTime` bounds on when the group's
      root run started
    * `:order` — `:started_desc` (default) or `:started_asc`
    * `:limit` — page size, capped at 500
    * `:after` — the keyset cursor from a previous page's `next_cursor`. A caller
      that never sees a workspace id may pass `%{started_at: _, root_run_id: _}`
      and the authorized workspace completes it.

  Every filter is applied by the store, so `page.has_more?`, `page.next_cursor`,
  and the returned items describe the same filtered set. Paging by the cursor
  costs the same at any depth; growing `:limit` instead re-reads from the start.
  """
  @spec page_execution_groups(OperatorContext.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def page_execution_groups(%OperatorContext{} = operator_context, filters)
      when is_list(filters) do
    cursor = group_cursor(Keyword.get(filters, :after), operator_context.workspace_id)

    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer),
         {:ok, page} <-
           Persistence.stores().operator_reads.page_execution_groups(%PageExecutionGroups{
             scope: context,
             status: execution_group_status(filters),
             search: Keyword.get(filters, :search),
             trigger_type: Keyword.get(filters, :trigger_type),
             started_after: Keyword.get(filters, :started_after),
             started_before: Keyword.get(filters, :started_before),
             order: Keyword.get(filters, :order, :started_desc),
             after: cursor,
             limit: min(Keyword.get(filters, :limit, 100), 500)
           }) do
      {:ok, %{page | items: Enum.map(page.items, &execution_group_summary/1)}}
    end
  end

  # A browser holds a cursor without a workspace in it, because a workspace id is
  # not the browser's to carry. Completing it from the session's own hint grants no
  # authority — the store scopes the page to the authorized workspace either way —
  # and it overwrites rather than defers to a supplied id: the cursor's workspace is
  # a live comparand in the keyset tie-break, so a foreign one would not read
  # another workspace but would make rows sharing an instant repeat or vanish.
  defp group_cursor(cursor, workspace_id) when is_map(cursor),
    do: Map.put(cursor, :workspace_id, workspace_id)

  defp group_cursor(cursor, _workspace_id), do: cursor

  @doc """
  Returns execution-group counts per status for one authorized operator workspace.

  Accepts `:search`, `:trigger_type`, `:started_after`, and `:started_before`,
  which narrow exactly as they do in `page_execution_groups/2`. Pass the same
  values and each count is the size of the set that filter plus that status would
  return, rather than the size of one loaded page.
  """
  @spec count_execution_groups(OperatorContext.t(), keyword()) ::
          {:ok, execution_group_counts()} | {:error, term()}
  def count_execution_groups(%OperatorContext{} = operator_context, opts \\ [])
      when is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer),
         {:ok, %ExecutionGroupCounts{} = counts} <-
           Persistence.stores().operator_reads.count_execution_groups(%CountExecutionGroups{
             scope: context,
             search: Keyword.get(opts, :search),
             trigger_type: Keyword.get(opts, :trigger_type),
             started_after: Keyword.get(opts, :started_after),
             started_before: Keyword.get(opts, :started_before)
           }) do
      {:ok, Map.from_struct(counts)}
    end
  end

  @doc "Returns bounded execution-group details for one authorized operator workspace."
  @spec get_execution_group_detail(OperatorContext.t(), run_id(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def get_execution_group_detail(%OperatorContext{} = operator_context, group_id, filters)
      when is_binary(group_id) and is_list(filters) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer),
         {:ok, detail} <-
           Persistence.stores().operator_reads.get_execution_group(%GetExecutionGroup{
             workspace_context: context,
             root_run_id: group_id,
             detail_limit: min(Keyword.get(filters, :limit, 100), 200)
           }) do
      {:ok,
       %{
         overview: execution_group_summary(detail.overview),
         child_runs: Enum.map(detail.runs.items, &execution_group_run/1),
         windows: Enum.map(detail.windows.items, &Map.from_struct/1),
         failures: Enum.map(detail.failures.items, &execution_group_run/1)
       }}
    end
  end

  @doc "Returns asset-step log context after reauthorizing an operator context."
  @spec get_asset_step_log_context(OperatorContext.t(), run_id(), String.t()) ::
          {:ok, RunReadModel.asset_step_log_context()} | {:error, term()}
  def get_asset_step_log_context(%OperatorContext{} = operator_context, run_id, asset_step_id)
      when is_binary(run_id) and is_binary(asset_step_id) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      RunReadModel.get_asset_step_log_context(context, run_id, asset_step_id)
    end
  end

  @doc "Returns one bounded run-event page in the authorized workspace."
  @spec list_run_events(WorkspaceContext.t(), run_id(), keyword()) ::
          {:ok, [RunEvent.t()]} | {:error, term()}
  def list_run_events(%WorkspaceContext{} = context, run_id, opts)
      when is_binary(run_id) and is_list(opts) do
    with {:ok, opts} <- RunEventQuery.normalize_opts(opts),
         {:ok, page} <- Runs.page_events(context, run_id, opts) do
      {:ok, Enum.map(page.items, &RunEvent.from_map/1)}
    end
  end

  @doc "Lists replayable per-run events after reauthorizing an operator context."
  @spec list_run_stream_events(OperatorContext.t(), run_id(), keyword()) ::
          {:ok, [RunEvent.t()]} | {:error, term()}
  def list_run_stream_events(%OperatorContext{} = operator_context, run_id, opts)
      when is_binary(run_id) and is_list(opts) do
    after_sequence = Keyword.get(opts, :after_sequence)
    limit = Keyword.get(opts, :limit, 200)

    with true <- is_integer(limit) and limit > 0 and limit <= 200,
         true <- is_nil(after_sequence) or (is_integer(after_sequence) and after_sequence >= 0),
         {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer),
         {:ok, run} <- Runs.get(context, run_id),
         true <- is_nil(after_sequence) or after_sequence <= run.event_seq,
         {:ok, events} <-
           list_run_events(context, run_id,
             after_sequence: after_sequence || 0,
             limit: limit
           ) do
      {:ok, events}
    else
      false -> {:error, :cursor_invalid}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Subscribes the current process to one run-scoped live event stream.
  """
  @spec subscribe_run(OperatorContext.t(), run_id()) :: :ok | {:error, term()}
  def subscribe_run(%OperatorContext{} = operator_context, run_id) when is_binary(run_id) do
    with {:ok, grant} <- authorize_run_subscription(operator_context, run_id) do
      activate_run_subscription(grant)
    end
  end

  def subscribe_run(_operator_context, _run_id), do: {:error, :invalid_run_subscription}

  @doc "Authorizes one run subscription without subscribing the authorization process."
  @spec authorize_run_subscription(OperatorContext.t(), run_id()) ::
          {:ok, map()} | {:error, term()}
  def authorize_run_subscription(%OperatorContext{} = operator_context, run_id)
      when is_binary(run_id) and byte_size(run_id) > 0 and byte_size(run_id) <= 255 do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      {:ok, %{kind: :run, workspace_id: context.workspace_id, run_id: run_id}}
    end
  end

  def authorize_run_subscription(_operator_context, _run_id),
    do: {:error, :invalid_run_subscription}

  @doc "Activates a previously authorized run subscription in the calling process."
  @spec activate_run_subscription(map()) :: :ok | {:error, term()}
  def activate_run_subscription(%{kind: :run, workspace_id: workspace_id, run_id: run_id}) do
    Events.subscribe_run(workspace_id, run_id)
  end

  def activate_run_subscription(_grant), do: {:error, :invalid_run_subscription}

  @doc """
  Unsubscribes the current process from one run-scoped live event stream.
  """
  @spec unsubscribe_run(OperatorContext.t(), run_id()) :: :ok | {:error, term()}
  def unsubscribe_run(%OperatorContext{} = operator_context, run_id) when is_binary(run_id) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Events.unsubscribe_run(context.workspace_id, run_id)
    end
  end

  def unsubscribe_run(_operator_context, _run_id), do: :ok

  @doc "Removes the calling process's local run subscription without a persistence lookup."
  @spec deactivate_run_subscription(OperatorContext.t(), run_id()) :: :ok
  def deactivate_run_subscription(%OperatorContext{workspace_id: workspace_id}, run_id)
      when is_binary(run_id) do
    Events.unsubscribe_run(workspace_id, run_id)
  end

  def deactivate_run_subscription(_operator_context, _run_id), do: :ok

  @doc "Unsubscribes the current process from cross-node durable run wake-ups."
  @spec unsubscribe_run_wakeups(OperatorContext.t()) :: :ok | {:error, term()}
  def unsubscribe_run_wakeups(%OperatorContext{} = operator_context) do
    with {:ok, _context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Events.unsubscribe_persistence_publications()
    end
  end

  @doc "Removes the calling process's local durable-publication wake-up subscription."
  @spec deactivate_run_wakeups() :: :ok
  def deactivate_run_wakeups, do: Events.unsubscribe_persistence_publications()

  @doc """
  Subscribes the current process to the global runs live event stream.
  """
  @spec subscribe_runs(OperatorContext.t()) :: :ok | {:error, term()}
  def subscribe_runs(%OperatorContext{} = operator_context) do
    with {:ok, grant} <- authorize_runs_subscription(operator_context) do
      activate_runs_subscription(grant)
    end
  end

  @doc "Authorizes a workspace runs subscription without subscribing the authorization process."
  @spec authorize_runs_subscription(OperatorContext.t()) :: {:ok, map()} | {:error, term()}
  def authorize_runs_subscription(%OperatorContext{} = operator_context) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      {:ok, %{kind: :runs, workspace_id: context.workspace_id}}
    end
  end

  @doc "Activates a previously authorized workspace runs subscription in the calling process."
  @spec activate_runs_subscription(map()) :: :ok | {:error, term()}
  def activate_runs_subscription(%{kind: :runs, workspace_id: workspace_id}) do
    Events.subscribe_runs(workspace_id)
  end

  def activate_runs_subscription(_grant), do: {:error, :invalid_run_subscription}

  @doc """
  Unsubscribes the current process from the global runs live event stream.
  """
  @spec unsubscribe_runs(OperatorContext.t()) :: :ok | {:error, term()}
  def unsubscribe_runs(%OperatorContext{} = operator_context) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Events.unsubscribe_runs(context.workspace_id)
    end
  end

  @doc "Removes the calling process's local workspace runs subscription without persistence."
  @spec deactivate_runs_subscription(OperatorContext.t()) :: :ok
  def deactivate_runs_subscription(%OperatorContext{workspace_id: workspace_id}) do
    Events.unsubscribe_runs(workspace_id)
  end

  @doc "Returns active-deployment schedules for one reauthorized operator workspace."
  @spec page_schedule_list_entries(OperatorContext.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def page_schedule_list_entries(%OperatorContext{} = operator_context, filters)
      when is_list(filters) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Schedules.page_entries(context, filters)
    end
  end

  @doc "Returns one active schedule for a reauthorized operator workspace."
  @spec get_schedule_entry(OperatorContext.t(), String.t()) ::
          {:ok, SchedulerEntry.t()} | {:error, term()}
  def get_schedule_entry(%OperatorContext{} = operator_context, schedule_id)
      when is_binary(schedule_id) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Schedules.get_entry(context, schedule_id)
    end
  end

  @doc "Activates one reviewed schedule definition for the operator workspace."
  @spec enable_schedule(OperatorContext.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def enable_schedule(%OperatorContext{} = operator_context, schedule_id, opts \\ [])
      when is_binary(schedule_id) and is_list(opts) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :operator),
         {:ok, intent} <-
           begin_operator_command(
             context,
             operator_context,
             actor,
             "schedule.enable",
             "schedule",
             schedule_id,
             %{schedule_id: schedule_id, reason: Keyword.get(opts, :reason, "operator request")},
             idempotency_key_opts(opts)
           ),
         result <-
           Schedules.activate(
             context,
             schedule_id,
             actor.id,
             Keyword.get(opts, :reason, "operator request"),
             Keyword.put(opts, :command_id, intent.key_hash)
           ) do
      finish_operator_result(
        context,
        operator_context,
        actor,
        intent,
        "schedule",
        schedule_id,
        result,
        fn receipt -> {schedule_id, %{schedule_id: schedule_id}, {:ok, receipt}} end
      )
    end
  end

  @doc "Deactivates one schedule for future occurrence submission."
  @spec disable_schedule(OperatorContext.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def disable_schedule(%OperatorContext{} = operator_context, schedule_id, opts \\ [])
      when is_binary(schedule_id) and is_list(opts) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :operator),
         {:ok, intent} <-
           begin_operator_command(
             context,
             operator_context,
             actor,
             "schedule.disable",
             "schedule",
             schedule_id,
             %{schedule_id: schedule_id, reason: Keyword.get(opts, :reason, "operator request")},
             idempotency_key_opts(opts)
           ),
         result <-
           Schedules.deactivate(
             context,
             schedule_id,
             actor.id,
             Keyword.get(opts, :reason, "operator request"),
             Keyword.put(opts, :command_id, intent.key_hash)
           ) do
      finish_operator_result(
        context,
        operator_context,
        actor,
        intent,
        "schedule",
        schedule_id,
        result,
        fn receipt -> {schedule_id, %{schedule_id: schedule_id}, {:ok, receipt}} end
      )
    end
  end

  @doc "Previews occurrences for one reauthorized operator workspace."
  @spec preview_schedule_occurrences(OperatorContext.t(), String.t(), keyword()) ::
          {:ok, [schedule_occurrence_preview()]} | {:error, term()}
  def preview_schedule_occurrences(%OperatorContext{} = operator_context, schedule_id, opts)
      when is_binary(schedule_id) and is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Schedules.preview_occurrences(context, schedule_id, opts)
    end
  end

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

  defp maybe_put_opt(opts, _key, nil), do: opts
  defp maybe_put_opt(opts, _key, ""), do: opts
  defp maybe_put_opt(opts, key, value), do: Keyword.put(opts, key, value)

  defp authorize_operator_context(%OperatorContext{} = operator_context, required_role) do
    with {:ok, lookup_context} <-
           WorkspaceContext.new(
             operator_context.workspace_id,
             "operator:session",
             [:customer_reader]
           ),
         {:ok, context, _session, actor} <-
           FavnOrchestrator.Identity.authorize_session(
             lookup_context,
             operator_context.actor_id,
             operator_context.session_id,
             required_role
           ) do
      {:ok, context, actor}
    end
  end

  defp admin?(%WorkspaceContext{roles: roles}), do: OperatorRebuilds.admin?(roles)

  defp operator_backfill_opts(request, actor, operator_context) do
    []
    |> maybe_put_opt(:refresh, operator_refresh(request.refresh_mode))
    |> maybe_put_opt(:retry_policy, request.retry_policy)
    |> maybe_put_opt(:timeout_ms, request.timeout_ms)
    |> maybe_put_opt(:combine_windows, request.combine_windows)
    |> Keyword.put(
      :metadata,
      Map.merge(request.metadata || %{}, %{
        operator_actor_id: actor.id,
        operator_session_id: operator_context.session_id
      })
    )
  end

  defp operator_refresh(:auto), do: nil
  defp operator_refresh(:missing), do: :missing
  defp operator_refresh(:force_all), do: :force

  defp begin_operator_command(
         context,
         operator_context,
         actor,
         operation,
         resource_type,
         resource_id,
         request,
         opts
       ) do
    case Keyword.fetch(opts, :idempotency_key) do
      {:ok, raw_key} ->
        OperatorAudit.begin_command(
          context,
          operator_context,
          actor,
          operation,
          resource_type,
          resource_id,
          request,
          raw_key
        )

      :error ->
        {:error, :idempotency_key_required}
    end
  end

  defp idempotency_key_opts(opts) do
    case Keyword.fetch(opts, :command_id) do
      {:ok, command_id} -> Keyword.put_new(opts, :idempotency_key, command_id)
      :error -> opts
    end
  end

  defp put_rebuild_plan_idempotency(opts, %OperatorAudit{} = intent) do
    opts
    |> Keyword.put(:idempotency, intent.idempotency)
    |> Keyword.put(:idempotency_key, intent.key_hash)
  end

  defp put_rebuild_command_idempotency(opts, %OperatorAudit{} = intent) do
    opts
    |> Keyword.delete(:idempotency_key)
    |> Keyword.put(:idempotency, intent.idempotency)
    |> Keyword.put_new(:command_id, intent.key_hash)
  end

  defp run_command_opts(%OperatorAudit{} = intent) do
    [
      run_id: OperatorAudit.deterministic_id(intent, "run"),
      idempotency: intent.idempotency,
      submission_source: :operator
    ]
  end

  defp finish_operator_result(
         context,
         operator_context,
         actor,
         intent,
         resource_type,
         fallback_resource_id,
         result,
         success
       ) do
    case result do
      {:ok, value} ->
        {resource_id, detail, public_result} = success.(value)

        finish_operator_audit(
          context,
          operator_context,
          actor,
          intent,
          "accepted",
          resource_type,
          resource_id,
          detail,
          public_result
        )

      :ok ->
        {resource_id, detail, public_result} = success.(:ok)

        finish_operator_audit(
          context,
          operator_context,
          actor,
          intent,
          "accepted",
          resource_type,
          resource_id,
          detail,
          public_result
        )

      {:partial, value} ->
        {resource_id, detail, public_result} = success.({:partial, value})

        finish_operator_audit(
          context,
          operator_context,
          actor,
          intent,
          "partial",
          resource_type,
          resource_id,
          detail,
          public_result
        )

      {:error, reason} = error ->
        outcome = operator_error_outcome(reason)

        finish_operator_audit(
          context,
          operator_context,
          actor,
          intent,
          outcome,
          resource_type,
          fallback_resource_id,
          %{error_code: operator_error_code(reason)},
          error
        )
    end
  end

  defp finish_operator_audit(
         context,
         operator_context,
         actor,
         intent,
         outcome,
         resource_type,
         resource_id,
         detail,
         result
       ) do
    case OperatorAudit.finish_command(
           context,
           operator_context,
           actor,
           intent,
           outcome,
           resource_type,
           resource_id,
           detail
         ) do
      :ok -> result
      {:error, reason} -> {:error, {:operator_audit_incomplete, reason}}
    end
  end

  defp operator_error_code(%PersistenceError{kind: kind}), do: Atom.to_string(kind)
  defp operator_error_code(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp operator_error_code({kind, _detail}) when is_atom(kind), do: Atom.to_string(kind)
  defp operator_error_code(_reason), do: "operator_command_failed"

  defp operator_error_outcome(%PersistenceError{retryable?: true}), do: "unknown"

  defp operator_error_outcome({:operator_audit_incomplete, _reason}), do: "unknown"

  defp operator_error_outcome(%PersistenceError{kind: kind})
       when kind in [:timeout, :unavailable, :internal],
       do: "unknown"

  defp operator_error_outcome(%PersistenceError{}), do: "rejected"

  defp operator_error_outcome(reason) when is_atom(reason) do
    value = Atom.to_string(reason)

    if reason in [:timeout, :unavailable, :unknown_outcome, :storage_unavailable] or
         String.contains?(value, "timeout") or String.contains?(value, "unavailable") or
         String.contains?(value, "unknown"),
       do: "unknown",
       else: "rejected"
  end

  defp operator_error_outcome({kind, _detail}) when is_atom(kind),
    do: operator_error_outcome(kind)

  defp operator_error_outcome(_reason), do: "unknown"

  defp execution_group_status(filters) do
    cond do
      Keyword.get(filters, :only_failed) -> :failed
      Keyword.get(filters, :only_running) -> :running
      true -> group_status(Keyword.get(filters, :status))
    end
  end

  # A group's status is one of four projected values, so a caller may ask in run
  # vocabulary and get the group equivalent. A list asks for any of several, which
  # is how "running or queued" is expressed without two round trips.
  #
  # Anything this vocabulary cannot express becomes `:unknown`, which the store
  # rejects as an invalid status. Folding it to `nil` instead would drop the `WHERE`
  # clause and answer "every group in the workspace", which is further from the
  # truth than an error. An empty list is the same case: no status is not any
  # status.
  defp group_status(nil), do: nil
  defp group_status([]), do: :unknown

  defp group_status(statuses) when is_list(statuses) do
    case statuses |> Enum.map(&group_status/1) |> Enum.uniq() do
      [single] -> single
      several -> if :unknown in several, do: :unknown, else: several
    end
  end

  defp group_status(status) when status in [:ok, :succeeded], do: :succeeded

  # A group whose run went partial is projected as succeeded, because the group
  # completed. Mapping `:partial` to `:failed` returned exactly the groups that did
  # not go partial.
  defp group_status(:partial), do: :succeeded
  defp group_status(status) when status in [:error, :failed], do: :failed
  defp group_status(status) when status in [:pending, :queued], do: :pending
  defp group_status(:running), do: :running
  defp group_status(_status), do: :unknown

  defp execution_group_summary(group), do: RunReadModel.from_execution_group_overview(group)

  defp execution_group_run(run) do
    %{
      id: run.run_id,
      status: run.status,
      submit_kind: run.submit_kind,
      target_refs: [],
      asset_ref: nil,
      started_at: run.inserted_at,
      finished_at: if(run.status in [:pending, :running], do: nil, else: run.updated_at),
      duration_ms:
        if(run.status in [:pending, :running],
          do: nil,
          else: max(DateTime.diff(run.updated_at, run.inserted_at, :millisecond), 0)
        )
    }
  end

  defp maybe_put_runtime_input_lineage(metadata, []), do: metadata

  defp maybe_put_runtime_input_lineage(metadata, pins),
    do: Map.put(metadata, :runtime_input_lineage, Enum.map(pins, &Pin.lineage/1))
end
