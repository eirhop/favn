defmodule FavnOrchestrator do
  @moduledoc """
  Runtime orchestrator control-plane facade for manifest-pinned operations.

  `FavnOrchestrator` is the boundary used by runtime apps, operator tooling, and
  the thin `Favn` runtime helpers. Same-BEAM operator UI code uses the operator
  wrappers here, including run submission, backfill submission, and
  `cancel_operator_run/2`, so authz stays in the control plane. It is not the
  stable authoring-time API that most application code should build against.
  """

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.RunnerClient
  alias Favn.Manifest.Version
  alias Favn.RuntimeInput.Pin
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Diagnostics
  alias FavnOrchestrator.Events
  alias FavnOrchestrator.Logs
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.Manifests
  alias FavnOrchestrator.Operator.Catalogue
  alias FavnOrchestrator.Operator.Lineage
  alias FavnOrchestrator.Operator.Commands, as: OperatorCommands
  alias FavnOrchestrator.OperatorContext
  alias FavnOrchestrator.Operator.Schedules
  alias FavnOrchestrator.OperatorCommands.AssetBackfillRequest
  alias FavnOrchestrator.OperatorCommands.AssetRunRequest
  alias FavnOrchestrator.OperatorCommands.PipelineBackfillRequest
  alias FavnOrchestrator.OperatorCommands.PipelineRunRequest
  alias FavnOrchestrator.OperatorErrorDTO
  alias FavnOrchestrator.Persistence.Error, as: PersistenceError
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries.GetExecutionGroup
  alias FavnOrchestrator.Persistence.Queries.PageExecutionGroups
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunEvents.Query, as: RunEventQuery
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunnerManifestRegistration
  alias FavnOrchestrator.RunnerReleaseCompatibility
  alias FavnOrchestrator.RunReadModel
  alias FavnOrchestrator.RunRetryPlanner
  alias FavnOrchestrator.RunSubmission.AssetOptions
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.ScheduleListEntry
  alias FavnOrchestrator.ScheduleOccurrencePreview
  alias FavnOrchestrator.SchedulerEntry

  @type run_id :: String.t()
  @type operator_actor :: Auth.actor()
  @type operator_session :: Auth.session()
  @type operator_actor_context :: OperatorContext.t()

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
  @type asset_timeline_window :: Catalogue.asset_timeline_window()
  @type asset_detail :: Catalogue.asset_detail()
  @type asset_freshness_reason :: Catalogue.asset_freshness_reason()
  @type asset_freshness_detail :: Catalogue.asset_freshness_detail()

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

  @doc "Registers the workspace's active manifest release with the runner."
  @spec register_manifest_with_runner(WorkspaceContext.t(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def register_manifest_with_runner(%WorkspaceContext{} = context, manifest_version_id)
      when is_binary(manifest_version_id) do
    with {:ok, version} <- ManifestStore.get_active_manifest(context),
         true <- version.manifest_version_id == manifest_version_id do
      register_version_with_runner(version)
    else
      false -> {:error, :manifest_not_active_in_workspace}
      {:error, _reason} = error -> error
    end
  end

  defp register_version_with_runner(%Version{} = version) do
    runner_client = configured_runner_client()
    runner_opts = configured_runner_opts()
    manifest_version_id = version.manifest_version_id

    with :ok <- validate_runner_client(runner_client),
         :ok <- RunnerReleaseCompatibility.verify_runner(runner_client, version, runner_opts) do
      content_hash = version.content_hash

      case RunnerManifestRegistration.ensure(runner_client, version, runner_opts) do
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
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Logs.subscribe_logs(context, filter)
    end
  end

  @doc """
  Unsubscribes the caller from a prior backend log subscription.

  Callers must pass the subscription returned by `subscribe_logs/1`; equivalent
  filters cannot stop the owned forwarding process.
  """
  @spec unsubscribe_logs(term()) :: :ok | {:error, :invalid_log_subscription}
  def unsubscribe_logs(subscription), do: Logs.unsubscribe_logs(subscription)

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
        %OperatorContext{} = operator_context,
        manifest_version_id,
        target_id,
        command_input \\ %{}
      ) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :operator) do
      OperatorCommands.submit_asset_run(context, manifest_version_id, target_id, command_input)
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
  def submit_operator_run(actor_context, manifest_version_id, target, command_input \\ %{})

  def submit_operator_run(
        %OperatorContext{} = operator_context,
        manifest_version_id,
        target,
        command_input
      ) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :operator) do
      OperatorCommands.submit_run(context, manifest_version_id, target, command_input, [])
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
      )

  def submit_operator_asset_backfill(
        %OperatorContext{} = operator_context,
        manifest_version_id,
        target_id,
        command_input
      ) do
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
         opts <-
           request
           |> operator_backfill_opts(actor, operator_context)
           |> Keyword.put(:dependencies, request.dependency_mode)
           |> Keyword.put(:refresh, refresh),
         {:ok, backfill} <-
           FavnOrchestrator.Backfills.submit_asset(
             context,
             manifest_version_id,
             target_id,
             request.range,
             opts
           ) do
      {:ok, backfill.root_run_id}
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
        %OperatorContext{} = operator_context,
        manifest_version_id,
        target_id,
        command_input \\ %{}
      ) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :operator) do
      OperatorCommands.submit_pipeline_run(context, manifest_version_id, target_id, command_input)
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
         {:ok, result} <- inspect_manifest_asset_version(version, target_id, opts) do
      {:ok, result}
    end
  end

  defp inspect_manifest_asset_version(%Version{} = version, target_id, opts) do
    with manifest_version_id <- version.manifest_version_id,
         {:ok, asset_ref} <- ManifestTarget.resolve_asset_ref(version, target_id),
         :ok <- validate_runner_client(configured_runner_client()),
         :ok <-
           RunnerReleaseCompatibility.verify_runner(
             configured_runner_client(),
             version,
             configured_runner_opts()
           ),
         :ok <-
           RunnerManifestRegistration.ensure(
             configured_runner_client(),
             version,
             configured_runner_opts()
           ) do
      request = %RelationInspectionRequest{
        manifest_version_id: manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: version.required_runner_release_id,
        asset_ref: asset_ref,
        sample_limit: Keyword.get(opts, :sample_limit, 20)
      }

      case configured_runner_client().inspect_relation(request, configured_runner_opts()) do
        {:ok, %RelationInspectionResult{} = result} ->
          with :ok <-
                 RunnerReleaseCompatibility.verify_inspection_result(
                   version.required_runner_release_id,
                   result
                 ) do
            {:ok, result}
          end

        {:ok, _invalid_result} ->
          {:error, :invalid_runner_inspection_result}

        {:error, _reason} = error ->
          error

        _invalid_response ->
          {:error, :invalid_runner_inspection_result}
      end
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
      )

  def submit_operator_pipeline_backfill(
        %OperatorContext{} = operator_context,
        manifest_version_id,
        target_id,
        command_input
      ) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :operator),
         {:ok, request} <- PipelineBackfillRequest.from_input(command_input),
         true <- is_nil(request.coverage_baseline_id),
         {:ok, backfill} <-
           FavnOrchestrator.Backfills.submit_pipeline(
             context,
             manifest_version_id,
             target_id,
             request.range,
             operator_backfill_opts(request, actor, operator_context)
           ) do
      {:ok, backfill.root_run_id}
    else
      false -> {:error, {:unsupported_backfill_option, :coverage_baseline_id}}
      {:error, _reason} = error -> error
    end
  end

  @doc "Requests cancellation within one explicit workspace."
  @spec cancel_run(WorkspaceContext.t(), run_id(), map()) :: :ok | {:error, term()}
  def cancel_run(%WorkspaceContext{} = context, run_id, reason)
      when is_binary(run_id) and is_map(reason) do
    RunManager.cancel_run(context, run_id, reason)
  end

  @doc false
  @spec cancel_run(WorkspaceContext.t(), run_id(), map(), keyword()) ::
          :ok | {:error, term()}
  def cancel_run(%WorkspaceContext{} = context, run_id, reason, opts)
      when is_binary(run_id) and is_map(reason) and is_list(opts) do
    RunManager.cancel_run(context, run_id, reason, opts)
  end

  @doc """
  Requests cancellation for one run on behalf of an authenticated operator.

  This is the same-BEAM boundary for browser operator actions. The orchestrator
  validates the actor/session context before forwarding cancellation to the
  run-manager lifecycle contract.
  """
  @spec cancel_operator_run(operator_actor_context(), run_id()) :: :ok | {:error, term()}
  def cancel_operator_run(%OperatorContext{} = operator_context, run_id)
      when is_binary(run_id) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :operator) do
      cancel_run(context, run_id, %{actor_id: actor.id, requested_by: :operator})
    end
  end

  def cancel_operator_run(_actor_context, _run_id), do: {:error, :unauthenticated}

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
  @spec retry_operator_run_remaining(operator_actor_context(), run_id()) ::
          {:ok, map()} | {:partial, map()} | {:error, term()}
  def retry_operator_run_remaining(%OperatorContext{} = operator_context, run_id)
      when is_binary(run_id) do
    with {:ok, context, actor} <- authorize_operator_context(operator_context, :operator) do
      retry_remaining(context, run_id, metadata: %{operator_retry: true, actor_id: actor.id})
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

  @doc "Submits a rerun within one explicit workspace."
  @spec rerun(WorkspaceContext.t(), run_id(), keyword()) ::
          {:ok, run_id()} | {:error, term()}
  def rerun(%WorkspaceContext{} = context, source_run_id, opts)
      when is_binary(source_run_id) and is_list(opts) do
    RunManager.rerun(context, source_run_id, opts)
  end

  defp submit_remaining_retry_plan(
         %WorkspaceContext{} = context,
         %{children: children, asset_count: asset_count} = plan,
         opts
       ) do
    with {:ok, submissions} <-
           prepare_remaining_retry_submissions(context, plan, children, opts) do
      admit_remaining_retry_submissions(plan, submissions, asset_count)
    end
  end

  defp prepare_remaining_retry_submissions(context, plan, children, opts) do
    children
    |> Enum.reduce_while({:ok, []}, fn child, {:ok, acc} ->
      submit_opts = remaining_retry_opts(plan, child, opts)

      case RunManager.prepare_rerun(context, child.source_run_id, submit_opts) do
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

  @doc "Returns bounded run detail after reauthorizing a browser-safe operator context."
  @spec get_operator_run_detail(OperatorContext.t(), run_id(), keyword()) ::
          {:ok, operator_run_detail()} | {:error, term()}
  def get_operator_run_detail(%OperatorContext{} = operator_context, run_id, opts)
      when is_binary(run_id) and is_list(opts) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      RunReadModel.get_operator_run_detail(context, run_id, opts)
    end
  end

  @doc "Returns a bounded execution-group page for one authorized operator workspace."
  @spec page_execution_groups(OperatorContext.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def page_execution_groups(%OperatorContext{} = operator_context, filters)
      when is_list(filters) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer),
         {:ok, page} <-
           Persistence.stores().operator_reads.page_execution_groups(%PageExecutionGroups{
             scope: context,
             status: execution_group_status(filters),
             limit: min(Keyword.get(filters, :limit, 100), 500)
           }) do
      {:ok, %{page | items: Enum.map(page.items, &execution_group_summary/1)}}
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
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer),
         {:ok, _run} <- Runs.get(context, run_id) do
      Events.subscribe_run(context.workspace_id, run_id)
    end
  end

  def subscribe_run(_operator_context, _run_id), do: {:error, :invalid_run_subscription}

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

  @doc "Unsubscribes the current process from cross-node durable run wake-ups."
  @spec unsubscribe_run_wakeups(OperatorContext.t()) :: :ok | {:error, term()}
  def unsubscribe_run_wakeups(%OperatorContext{} = operator_context) do
    with {:ok, _context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Events.unsubscribe_persistence_publications()
    end
  end

  @doc """
  Subscribes the current process to the global runs live event stream.
  """
  @spec subscribe_runs(OperatorContext.t()) :: :ok | {:error, term()}
  def subscribe_runs(%OperatorContext{} = operator_context) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Events.subscribe_runs(context.workspace_id)
    end
  end

  @doc """
  Unsubscribes the current process from the global runs live event stream.
  """
  @spec unsubscribe_runs(OperatorContext.t()) :: :ok | {:error, term()}
  def unsubscribe_runs(%OperatorContext{} = operator_context) do
    with {:ok, context, _actor} <- authorize_operator_context(operator_context, :viewer) do
      Events.unsubscribe_runs(context.workspace_id)
    end
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

  @doc "Rejects ad-hoc activation; schedule activation is manifest/deployment controlled."
  @spec enable_schedule(OperatorContext.t(), String.t()) :: {:error, term()}
  def enable_schedule(%OperatorContext{} = operator_context, schedule_id)
      when is_binary(schedule_id) do
    with {:ok, _context, _actor} <- authorize_operator_context(operator_context, :operator) do
      {:error, :schedule_activation_manifest_controlled}
    end
  end

  @doc "Rejects ad-hoc deactivation; schedule activation is manifest/deployment controlled."
  @spec disable_schedule(OperatorContext.t(), String.t()) :: {:error, term()}
  def disable_schedule(%OperatorContext{} = operator_context, schedule_id)
      when is_binary(schedule_id) do
    with {:ok, _context, _actor} <- authorize_operator_context(operator_context, :operator) do
      {:error, :schedule_activation_manifest_controlled}
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

  defp operator_backfill_opts(request, actor, operator_context) do
    []
    |> maybe_put_opt(:refresh, operator_refresh(request.refresh_mode))
    |> maybe_put_opt(:retry_policy, request.retry_policy)
    |> maybe_put_opt(:timeout_ms, request.timeout_ms)
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

  defp execution_group_status(filters) do
    cond do
      Keyword.get(filters, :only_failed) -> :failed
      Keyword.get(filters, :only_running) -> :running
      Keyword.get(filters, :status) in [:ok, :succeeded] -> :succeeded
      Keyword.get(filters, :status) in [:error, :failed, :partial] -> :failed
      Keyword.get(filters, :status) in [:pending, :queued] -> :pending
      Keyword.get(filters, :status) == :running -> :running
      true -> nil
    end
  end

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

  defp configured_runner_client do
    RuntimeConfig.current().runner_client
  end

  defp configured_runner_opts do
    RuntimeConfig.current().runner_client_opts
  end

  defp maybe_put_runtime_input_lineage(metadata, []), do: metadata

  defp maybe_put_runtime_input_lineage(metadata, pins),
    do: Map.put(metadata, :runtime_input_lineage, Enum.map(pins, &Pin.lineage/1))

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
