defmodule FavnOrchestrator.RunManager.SubmissionBuilder do
  @moduledoc false

  alias Favn.Assets.Planner
  alias Favn.Manifest.Index
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.PipelineResolver
  alias Favn.Plan
  alias Favn.Replay.InputMode
  alias Favn.Window.Policy
  alias Favn.Window.Request, as: WindowRequest
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ManifestIndexCache
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.RetryPolicyResolver
  alias FavnOrchestrator.RunManager.Submission
  alias FavnOrchestrator.RunManager.SubmissionOptions
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Runs

  @spec asset(WorkspaceContext.t(), Favn.Ref.t(), keyword()) ::
          {:ok, Submission.t()} | {:error, term()}
  def asset(%WorkspaceContext{} = context, asset_ref, opts) when is_list(opts) do
    with {:ok, scoped_opts} <- workspace_opts(context, opts) do
      do_asset(asset_ref, scoped_opts)
    end
  end

  @spec pipeline(WorkspaceContext.t(), [Favn.Ref.t()], keyword()) ::
          {:ok, Submission.t()} | {:error, term()}
  def pipeline(%WorkspaceContext{} = context, target_refs, opts) when is_list(opts) do
    with {:ok, scoped_opts} <- workspace_opts(context, opts) do
      do_pipeline(target_refs, scoped_opts)
    end
  end

  @spec pipeline_module(WorkspaceContext.t(), module(), keyword()) ::
          {:ok, Submission.t()} | {:error, term()}
  def pipeline_module(%WorkspaceContext{} = context, pipeline_module, opts)
      when is_atom(pipeline_module) and is_list(opts) do
    with {:ok, scoped_opts} <- workspace_opts(context, opts) do
      do_pipeline_module(pipeline_module, scoped_opts)
    end
  end

  @spec pipeline_ref(WorkspaceContext.t(), {module(), atom()}, keyword()) ::
          {:ok, Submission.t()} | {:error, term()}
  def pipeline_ref(%WorkspaceContext{} = context, {module, name} = pipeline_ref, opts)
      when is_atom(module) and is_atom(name) and is_list(opts) do
    with {:ok, scoped_opts} <- workspace_opts(context, opts) do
      do_pipeline_ref(pipeline_ref, scoped_opts)
    end
  end

  @spec rerun(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, Submission.t()} | {:error, term()}
  def rerun(%WorkspaceContext{} = context, source_run_id, opts)
      when is_binary(source_run_id) and is_list(opts) do
    do_rerun(source_run_id, Keyword.put(opts, :_workspace_context, context))
  end

  defp do_asset(asset_ref, opts) when is_list(opts) do
    with :ok <- validate_opts(opts),
         {:ok, run_state, version} <- build_run_submission(asset_ref, opts) do
      {:ok,
       %Submission{
         run_state: run_state,
         workspace_context: Keyword.get(opts, :_workspace_context),
         deployment_id: Keyword.get(opts, :_deployment_id),
         idempotency: Keyword.get(opts, :_idempotency),
         manifest_version: version,
         submit_kind: :manual,
         transition_metadata: %{status: run_state.status, submit_kind: :manual},
         event_metadata: %{run_id: run_state.id, submit_kind: :manual}
       }}
    end
  end

  defp do_pipeline(target_refs, opts) when is_list(target_refs) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_target_refs(target_refs),
         {:ok, run_state, version} <- build_pipeline_submission(target_refs, opts) do
      {:ok,
       %Submission{
         run_state: run_state,
         workspace_context: Keyword.get(opts, :_workspace_context),
         deployment_id: Keyword.get(opts, :_deployment_id),
         idempotency: Keyword.get(opts, :_idempotency),
         manifest_version: version,
         submit_kind: :pipeline,
         pipeline_refs: pipeline_refs_from_opts(opts),
         transition_metadata: %{
           status: run_state.status,
           submit_kind: :pipeline,
           pipeline_target_refs: target_refs
         },
         event_metadata: %{run_id: run_state.id, submit_kind: :pipeline}
       }}
    end
  end

  defp do_pipeline_module(pipeline_module, opts)
       when is_atom(pipeline_module) and is_list(opts) do
    with :ok <- validate_opts(opts),
         {:ok, run_state, version} <- build_pipeline_module_submission(pipeline_module, opts) do
      {:ok,
       %Submission{
         run_state: run_state,
         workspace_context: Keyword.get(opts, :_workspace_context),
         deployment_id: Keyword.get(opts, :_deployment_id),
         idempotency: Keyword.get(opts, :_idempotency),
         manifest_version: version,
         submit_kind: :pipeline,
         pipeline_refs: pipeline_refs_from_run(run_state),
         transition_metadata: %{
           status: run_state.status,
           submit_kind: :pipeline,
           pipeline_target_refs: run_state.target_refs,
           pipeline_module: pipeline_module
         },
         event_metadata: %{run_id: run_state.id, submit_kind: :pipeline}
       }}
    end
  end

  defp do_pipeline_ref({module, name} = pipeline_ref, opts)
       when is_atom(module) and is_atom(name) and is_list(opts) do
    with :ok <- validate_opts(opts),
         {:ok, run_state, version} <- build_pipeline_ref_submission(pipeline_ref, opts) do
      {:ok,
       %Submission{
         run_state: run_state,
         workspace_context: Keyword.get(opts, :_workspace_context),
         deployment_id: Keyword.get(opts, :_deployment_id),
         idempotency: Keyword.get(opts, :_idempotency),
         manifest_version: version,
         submit_kind: :pipeline,
         pipeline_refs: [pipeline_ref],
         transition_metadata: %{
           status: run_state.status,
           submit_kind: :pipeline,
           pipeline_target_refs: run_state.target_refs,
           pipeline_ref: pipeline_ref
         },
         event_metadata: %{run_id: run_state.id, submit_kind: :pipeline}
       }}
    end
  end

  defp do_rerun(source_run_id, opts) when is_binary(source_run_id) and is_list(opts) do
    with :ok <- validate_opts(opts),
         {:ok, source_run} <- get_run(opts, source_run_id),
         opts <-
           opts
           |> Keyword.put(:_workspace_id, source_run.workspace_id)
           |> Keyword.put(:_deployment_id, source_run.deployment_id),
         :ok <- reject_backfill_parent_rerun(source_run),
         :ok <- require_terminal_rerun_source(source_run),
         {:ok, run_state, version} <- build_rerun_submission(source_run, opts) do
      {:ok,
       %Submission{
         run_state: run_state,
         workspace_context: Keyword.get(opts, :_workspace_context),
         deployment_id: run_state.deployment_id,
         idempotency: Keyword.get(opts, :_idempotency),
         manifest_version: version,
         submit_kind: :rerun,
         pipeline_refs: pipeline_refs_from_run(run_state),
         transition_metadata: %{
           status: run_state.status,
           submit_kind: :rerun,
           rerun_of_run_id: run_state.rerun_of_run_id,
           parent_run_id: run_state.parent_run_id
         },
         event_metadata: %{run_id: run_state.id, submit_kind: :rerun}
       }}
    end
  end

  defp build_run_submission(asset_ref, opts) do
    with {:ok, input} <- SubmissionOptions.new(opts, trigger: %{kind: :manual}),
         {:ok, input_mode} <- runtime_input_mode(opts, :manual),
         {:ok, refresh_policy} <- refresh_policy_metadata(opts, input.dependencies),
         metadata_with_selection <-
           input.metadata
           |> Map.put(:asset_dependencies, input.dependencies)
           |> Map.put(:refresh_policy, refresh_policy)
           |> Map.put(:runtime_input_mode, input_mode),
         {:ok, manifest_version_id} <- resolve_manifest_version_id(opts),
         {:ok, version} <- get_manifest(opts, manifest_version_id),
         {:ok, index} <- ManifestIndexCache.fetch(version),
         {:ok, _asset} <- Index.fetch_asset(index, asset_ref),
         {:ok, plan} <-
           Planner.plan(asset_ref,
             planning_index: index.planning_index,
             dependencies: input.dependencies,
             anchor_window: input.anchor_window,
             exact_windows: input.exact_windows
           ),
         plan <- RetryPolicyResolver.annotate(plan, index, nil, input.retry_policy_override) do
      input = %{input | metadata: metadata_with_selection}
      run_state = new_run_state(input, version, plan, asset_ref, :manual)

      {:ok, run_state, version}
    end
  end

  defp build_pipeline_submission(target_refs, opts) when is_list(target_refs) and is_list(opts) do
    with {:ok, input} <- SubmissionOptions.new(opts, trigger: %{kind: :pipeline}),
         {:ok, input_mode} <- runtime_input_mode(opts, :manual),
         {:ok, refresh_policy} <- refresh_policy_metadata(opts, input.dependencies),
         metadata <-
           input.metadata
           |> Map.merge(%{
             submit_kind: :pipeline,
             pipeline_target_refs: target_refs,
             pipeline_context: Keyword.get(opts, :_pipeline_context),
             pipeline_dependencies: input.dependencies,
             pipeline_submit_ref: Keyword.get(opts, :_submit_ref),
             runtime_input_mode: input_mode,
             refresh_policy: refresh_policy
           }),
         {:ok, manifest_version_id} <- resolve_manifest_version_id(opts),
         {:ok, version} <- get_manifest(opts, manifest_version_id),
         {:ok, index} <- ManifestIndexCache.fetch(version),
         input <- %{input | metadata: metadata},
         {:ok, run_state} <-
           build_pipeline_run_state(target_refs, input, version, index, nil) do
      {:ok, run_state, version}
    end
  end

  defp build_pipeline_module_submission(pipeline_module, opts)
       when is_atom(pipeline_module) and is_list(opts) do
    window_request = Keyword.get(opts, :window_request)

    with {:ok, input} <- SubmissionOptions.new(opts, trigger: %{kind: :pipeline}),
         {:ok, input_mode} <- runtime_input_mode(opts, :manual),
         {:ok, request} <-
           normalize_pipeline_window_request(input.anchor_window, window_request),
         {:ok, manifest_version_id} <- resolve_manifest_version_id(opts),
         {:ok, version} <- get_manifest(opts, manifest_version_id),
         {:ok, index} <- ManifestIndexCache.fetch(version),
         {:ok, pipeline} <- fetch_pipeline_by_module(index, pipeline_module),
         {:ok, resolved_anchor_window} <-
           resolve_pipeline_anchor_window(pipeline, input.anchor_window, request),
         {:ok, resolution} <-
           PipelineResolver.resolve(index, pipeline,
             trigger: input.trigger,
             params: input.params,
             anchor_window: resolved_anchor_window
           ),
         {:ok, refresh_policy} <- refresh_policy_metadata(opts, resolution.dependencies),
         metadata <-
           Map.merge(input.metadata, %{
             submit_kind: :pipeline,
             pipeline_target_refs: resolution.target_refs,
             pipeline_context: resolution.pipeline_ctx,
             pipeline_dependencies: resolution.dependencies,
             pipeline_submit_ref: pipeline.module,
             pipeline_identity_ref: {pipeline.module, pipeline.name},
             pipeline_execution_policy: pipeline_execution_policy(resolution.pipeline),
             runtime_input_mode: input_mode,
             refresh_policy: refresh_policy
           }),
         input <-
           %{
             input
             | metadata: metadata,
               dependencies: resolution.dependencies,
               anchor_window: resolved_anchor_window
           },
         {:ok, run_state} <-
           build_pipeline_run_state(
             resolution.target_refs,
             input,
             version,
             index,
             resolution.pipeline.retry_policy
           ) do
      {:ok, run_state, version}
    end
  end

  defp build_pipeline_ref_submission({module, name} = pipeline_ref, opts)
       when is_atom(module) and is_atom(name) and is_list(opts) do
    window_request = Keyword.get(opts, :window_request)

    with {:ok, input} <- SubmissionOptions.new(opts, trigger: %{kind: :pipeline}),
         {:ok, input_mode} <- runtime_input_mode(opts, :manual),
         {:ok, request} <- normalize_pipeline_window_request(input.anchor_window, window_request),
         {:ok, manifest_version_id} <- resolve_manifest_version_id(opts),
         {:ok, version} <- get_manifest(opts, manifest_version_id),
         {:ok, index} <- ManifestIndexCache.fetch(version),
         {:ok, %Pipeline{} = pipeline} <- Index.fetch_pipeline(index, pipeline_ref),
         {:ok, resolved_anchor_window} <-
           resolve_pipeline_anchor_window(pipeline, input.anchor_window, request),
         {:ok, resolution} <-
           PipelineResolver.resolve(index, pipeline,
             trigger: input.trigger,
             params: input.params,
             anchor_window: resolved_anchor_window
           ),
         {:ok, refresh_policy} <- refresh_policy_metadata(opts, resolution.dependencies),
         metadata <-
           Map.merge(input.metadata, %{
             submit_kind: :pipeline,
             pipeline_target_refs: resolution.target_refs,
             pipeline_context: resolution.pipeline_ctx,
             pipeline_dependencies: resolution.dependencies,
             pipeline_submit_ref: pipeline.module,
             pipeline_identity_ref: pipeline_ref,
             pipeline_execution_policy: pipeline_execution_policy(resolution.pipeline),
             runtime_input_mode: input_mode,
             refresh_policy: refresh_policy
           }),
         input <-
           %{
             input
             | metadata: metadata,
               dependencies: resolution.dependencies,
               anchor_window: resolved_anchor_window
           },
         {:ok, run_state} <-
           build_pipeline_run_state(
             resolution.target_refs,
             input,
             version,
             index,
             resolution.pipeline.retry_policy
           ) do
      {:ok, run_state, version}
    end
  end

  defp build_pipeline_run_state(
         target_refs,
         %SubmissionOptions{} = input,
         version,
         index,
         pipeline_retry_policy
       ) do
    with :ok <- ensure_assets_exist(index, target_refs),
         {:ok, plan} <-
           Planner.plan(target_refs,
             planning_index: index.planning_index,
             dependencies: input.dependencies,
             anchor_window: input.anchor_window
           ),
         plan <-
           RetryPolicyResolver.annotate(
             plan,
             index,
             pipeline_retry_policy,
             input.retry_policy_override
           ) do
      {:ok, new_run_state(input, version, plan, List.first(plan.target_refs), :pipeline)}
    end
  end

  defp new_run_state(
         %SubmissionOptions{} = input,
         version,
         plan,
         asset_ref,
         submit_kind,
         extra \\ []
       ) do
    base = [
      id: input.run_id,
      workspace_id: input.workspace_id,
      deployment_id: input.deployment_id,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: asset_ref,
      target_refs: plan.target_refs,
      plan: plan,
      params: input.params,
      trigger: input.trigger,
      metadata: input.metadata,
      submit_kind: submit_kind,
      parent_run_id: input.parent_run_id,
      root_run_id: input.root_run_id,
      lineage_depth: input.lineage_depth,
      max_attempts: plan_max_attempts(plan),
      retry_backoff_ms: 0,
      timeout_ms: input.timeout_ms
    ]

    base
    |> Keyword.merge(extra)
    |> RunState.new()
  end

  defp build_rerun_submission(%RunState{} = source_run, opts) when is_list(opts) do
    {rerun_asset_ref, rerun_targets, rerun_dependencies} = replay_selection(source_run, opts)

    with {:ok, metadata} <- rerun_metadata(source_run, opts),
         {:ok, replay_mode} <- replay_mode(opts),
         {:ok, input_mode} <- runtime_input_mode(opts, replay_mode),
         rerun_opts <-
           opts
           |> Keyword.put(:metadata, metadata)
           |> Keyword.put(:dependencies, rerun_dependencies)
           |> Keyword.put(:lineage_depth, source_run.lineage_depth + 1),
         {:ok, input} <-
           SubmissionOptions.new(rerun_opts,
             params: source_run.params,
             trigger: %{kind: :rerun, source_run_id: source_run.id},
             timeout_ms: source_run.timeout_ms,
             parent_run_id: source_run.id,
             root_run_id: source_run.root_run_id || source_run.id
           ),
         {:ok, refresh_policy} <- refresh_policy_metadata(opts, input.dependencies),
         :ok <- validate_rerun_manifest_pin(opts, source_run),
         {:ok, version} <-
           get_manifest(opts, source_run.manifest_version_id, source_run.deployment_id),
         {:ok, index} <- ManifestIndexCache.fetch(version),
         {:ok, _asset} <- Index.fetch_asset(index, rerun_asset_ref),
         :ok <- ensure_assets_exist(index, rerun_targets),
         {:ok, plan} <-
           Planner.plan(rerun_targets,
             planning_index: index.planning_index,
             dependencies: input.dependencies,
             anchor_window: input.anchor_window
           ),
         {:ok, plan} <- maybe_filter_replay_plan(plan, Keyword.get(opts, :replay_node_keys)),
         pipeline_retry_policy <- replay_pipeline_retry_policy(source_run, index),
         plan <-
           RetryPolicyResolver.annotate(
             plan,
             index,
             pipeline_retry_policy,
             input.retry_policy_override
           ) do
      rerun_of_run_id = source_run.rerun_of_run_id || source_run.id

      metadata_with_source =
        input.metadata
        |> Map.put(:source_run_id, source_run.id)
        |> Map.put(:runtime_input_mode, input_mode)
        |> Map.put(:refresh_policy, refresh_policy)

      metadata_with_replay =
        if pipeline_origin?(source_run) do
          Map.merge(metadata_with_source, %{
            replay_submit_kind: :pipeline,
            replay_mode: replay_mode,
            pipeline_target_refs: rerun_targets,
            pipeline_dependencies: input.dependencies
          })
        else
          Map.merge(metadata_with_source, %{
            replay_mode: replay_mode,
            asset_dependencies: input.dependencies
          })
        end

      input = %{input | metadata: metadata_with_replay}

      run_state =
        new_run_state(input, version, plan, rerun_asset_ref, :rerun,
          rerun_of_run_id: rerun_of_run_id
        )

      {:ok, run_state, version}
    end
  end

  defp replay_mode(opts) do
    replay_mode = Keyword.get(opts, :replay_mode)

    with {:ok, input_mode} <- optional_input_mode(Keyword.get(opts, :input_mode)) do
      reconcile_replay_mode(replay_mode, input_mode)
    end
  end

  defp optional_input_mode(nil), do: {:ok, nil}
  defp optional_input_mode(value), do: InputMode.normalize(value)

  defp reconcile_replay_mode(nil, nil), do: {:ok, :exact_replay}
  defp reconcile_replay_mode(nil, :pinned), do: {:ok, :exact_replay}
  defp reconcile_replay_mode(nil, :inherit), do: {:ok, :resume_from_failure}
  defp reconcile_replay_mode(nil, :fresh), do: {:ok, :fresh_rerun}

  defp reconcile_replay_mode(mode, nil)
       when mode in [:exact_replay, :resume_from_failure, :fresh_rerun],
       do: {:ok, mode}

  defp reconcile_replay_mode(mode, input_mode)
       when mode in [:exact_replay, :resume_from_failure, :fresh_rerun] do
    if InputMode.default_for(mode) == input_mode,
      do: {:ok, mode},
      else: {:error, {:incompatible_replay_input_mode, mode, input_mode}}
  end

  defp reconcile_replay_mode(_mode, _input_mode), do: {:error, :invalid_replay_mode}

  defp runtime_input_mode(opts, operation) do
    InputMode.normalize(Keyword.get(opts, :input_mode, InputMode.default_for(operation)))
  end

  defp plan_max_attempts(%Plan{nodes: nodes}) do
    nodes
    |> Map.values()
    |> Enum.map(& &1.retry_policy.max_attempts)
    |> Enum.max(fn -> 1 end)
  end

  defp resolve_pipeline_anchor_window(%Pipeline{} = pipeline, nil, request),
    do: Policy.resolve_manual(pipeline.window, request)

  defp resolve_pipeline_anchor_window(_pipeline, anchor_window, _request),
    do: {:ok, anchor_window}

  defp pipeline_execution_policy(pipeline) do
    %{
      max_concurrency: Map.get(pipeline, :max_concurrency),
      execution_pool: Map.get(pipeline, :execution_pool),
      resource_recovery: Map.get(pipeline, :resource_recovery)
    }
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp rerun_base_metadata(%RunState{metadata: metadata}) when is_map(metadata) do
    Map.drop(metadata, [
      :terminal_event_type,
      "terminal_event_type",
      :cancelled,
      "cancelled",
      :cancel_requested,
      "cancel_requested",
      :cancel_reason,
      "cancel_reason",
      :cancel_requested_at,
      "cancel_requested_at",
      :cancel_forward_error,
      "cancel_forward_error",
      :in_flight_execution_ids,
      "in_flight_execution_ids"
    ])
  end

  defp rerun_base_metadata(%RunState{}), do: %{}

  defp rerun_metadata(%RunState{} = source_run, opts) do
    metadata = Keyword.get(opts, :metadata, %{})

    with :ok <- validate_metadata(metadata) do
      {:ok, Map.merge(rerun_base_metadata(source_run), metadata)}
    end
  end

  defp normalize_pipeline_window_request(nil, window_request),
    do: WindowRequest.from_value(window_request)

  defp normalize_pipeline_window_request(_anchor_window, _window_request), do: {:ok, nil}

  defp validate_target_refs(refs) when is_list(refs) do
    if refs == [] do
      {:error, :empty_pipeline_selection}
    else
      if Enum.all?(refs, &valid_ref?/1), do: :ok, else: {:error, :invalid_target_ref}
    end
  end

  defp valid_ref?({module, name}) when is_atom(module) and is_atom(name), do: true
  defp valid_ref?(_other), do: false

  defp fetch_pipeline_by_module(%Index{} = index, pipeline_module)
       when is_atom(pipeline_module) do
    pipelines = Enum.filter(Index.list_pipelines(index), &(&1.module == pipeline_module))

    case pipelines do
      [%Pipeline{} = pipeline] -> {:ok, pipeline}
      [] -> {:error, :pipeline_not_found}
      _many -> {:error, :ambiguous_pipeline_module}
    end
  end

  defp ensure_assets_exist(%Index{} = index, refs) when is_list(refs) do
    refs
    |> Enum.reduce_while(:ok, fn ref, :ok ->
      case Index.fetch_asset(index, ref) do
        {:ok, _asset} -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp replay_selection(%RunState{} = source_run, opts) do
    if pipeline_origin?(source_run) do
      pipeline_replay_selection(source_run, opts)
    else
      asset_replay_selection(source_run, opts)
    end
  end

  defp pipeline_replay_selection(%RunState{} = source_run, opts) do
    fallback_targets =
      source_run.metadata
      |> metadata_value(:pipeline_target_refs)
      |> non_empty_targets_or(source_run.target_refs)

    targets = target_override(opts, fallback_targets)

    dependencies =
      dependency_override(
        opts,
        normalize_dependencies(metadata_value(source_run.metadata, :pipeline_dependencies))
      )

    {List.first(targets) || source_run.asset_ref, targets, dependencies}
  end

  defp asset_replay_selection(%RunState{} = source_run, opts) do
    targets = target_override(opts, [source_run.asset_ref])
    dependencies = dependency_override(opts, normalize_asset_dependencies(source_run))
    {List.first(targets) || source_run.asset_ref, targets, dependencies}
  end

  defp target_override(opts, fallback) do
    opts
    |> Keyword.get(:target_refs)
    |> non_empty_targets_or(fallback)
  end

  defp non_empty_targets_or(targets, _fallback) when is_list(targets) and targets != [],
    do: targets

  defp non_empty_targets_or(_targets, fallback), do: fallback

  defp dependency_override(opts, fallback) do
    case Keyword.fetch(opts, :dependencies) do
      {:ok, value} -> value
      :error -> fallback
    end
  end

  defp maybe_filter_replay_plan(%Plan{} = plan, nil), do: {:ok, plan}

  defp maybe_filter_replay_plan(%Plan{} = plan, node_keys) when is_list(node_keys) do
    wanted = MapSet.new(node_keys)

    nodes =
      plan.nodes
      |> Enum.filter(fn {node_key, _node} -> MapSet.member?(wanted, node_key) end)
      |> Map.new()

    if map_size(nodes) == 0 do
      {:error, :empty_replay_plan}
    else
      node_stages =
        plan.node_stages
        |> Enum.map(fn stage -> Enum.filter(stage, &Map.has_key?(nodes, &1)) end)
        |> Enum.reject(&(&1 == []))

      stage_by_node_key =
        node_stages
        |> Enum.with_index()
        |> Enum.flat_map(fn {stage, index} -> Enum.map(stage, &{&1, index}) end)
        |> Map.new()

      nodes =
        Map.new(nodes, fn {node_key, node} ->
          {node_key,
           %{
             node
             | upstream: Enum.filter(Map.get(node, :upstream, []), &Map.has_key?(nodes, &1)),
               downstream: Enum.filter(Map.get(node, :downstream, []), &Map.has_key?(nodes, &1)),
               stage: Map.fetch!(stage_by_node_key, node_key)
           }}
        end)

      target_node_keys = Enum.filter(plan.target_node_keys, &Map.has_key?(nodes, &1))

      target_node_keys =
        if target_node_keys == [], do: List.flatten(node_stages), else: target_node_keys

      {:ok,
       %{
         plan
         | target_node_keys: target_node_keys,
           target_refs: target_refs_for_node_keys(nodes, target_node_keys),
           nodes: nodes,
           topo_order: target_refs_for_node_keys(nodes, List.flatten(node_stages)),
           stages: Enum.map(node_stages, &target_refs_for_node_keys(nodes, &1)),
           node_stages: node_stages
       }}
    end
  end

  defp maybe_filter_replay_plan(%Plan{}, _node_keys), do: {:error, :invalid_replay_node_keys}

  defp target_refs_for_node_keys(nodes, node_keys) do
    node_keys
    |> Enum.map(&Map.fetch!(nodes, &1).ref)
    |> Enum.uniq()
  end

  defp normalize_asset_dependencies(%RunState{} = source_run) do
    case metadata_value(source_run.metadata, :asset_dependencies) do
      nil -> normalize_dependencies(plan_dependencies(source_run.plan))
      value -> normalize_dependencies(value)
    end
  end

  defp plan_dependencies(%Favn.Plan{dependencies: dependencies}), do: dependencies
  defp plan_dependencies(_plan), do: :all

  defp metadata_value(metadata, key) when is_map(metadata) and is_atom(key) do
    Map.get(metadata, key) || Map.get(metadata, Atom.to_string(key))
  end

  defp pipeline_origin?(%RunState{submit_kind: :pipeline}), do: true

  defp pipeline_origin?(%RunState{metadata: metadata}) when is_map(metadata) do
    target_refs = metadata_value(metadata, :pipeline_target_refs)

    metadata_value(metadata, :replay_submit_kind) in [:pipeline, "pipeline"] or
      is_map(metadata_value(metadata, :pipeline_context)) or
      present_atom?(metadata_value(metadata, :pipeline_submit_ref)) or
      valid_ref?(metadata_value(metadata, :pipeline_submit_ref)) or
      (is_list(target_refs) and target_refs != [])
  end

  defp present_atom?(value) when is_atom(value) and not is_nil(value), do: true
  defp present_atom?(_value), do: false

  defp replay_pipeline_retry_policy(%RunState{} = source_run, %Index{} = index) do
    case metadata_value(source_run.metadata, :pipeline_identity_ref) ||
           metadata_value(source_run.metadata, :pipeline_submit_ref) do
      {module, name} = pipeline_ref when is_atom(module) and is_atom(name) ->
        case Index.fetch_pipeline(index, pipeline_ref) do
          {:ok, pipeline} -> pipeline.retry_policy
          {:error, _reason} -> nil
        end

      pipeline_module when is_atom(pipeline_module) and not is_nil(pipeline_module) ->
        case fetch_pipeline_by_module(index, pipeline_module) do
          {:ok, pipeline} -> pipeline.retry_policy
          {:error, _reason} -> nil
        end

      _other ->
        nil
    end
  end

  defp normalize_dependencies(:none), do: :none
  defp normalize_dependencies("none"), do: :none
  defp normalize_dependencies(_value), do: :all

  defp refresh_policy_metadata(opts, dependencies) when is_list(opts) do
    with {:ok, policy} <- RefreshPolicy.from_opts(opts),
         :ok <- validate_refresh_policy_dependencies(policy, dependencies) do
      {:ok,
       %{
         mode: policy.mode,
         refs: policy.refs,
         include_upstream?: policy.include_upstream?
       }}
    end
  end

  defp validate_refresh_policy_dependencies(%RefreshPolicy{include_upstream?: true}, :none),
    do: {:error, {:refresh_include_upstream_requires_dependencies, :all}}

  defp validate_refresh_policy_dependencies(%RefreshPolicy{}, _dependencies), do: :ok

  defp resolve_manifest_version_id(opts) when is_list(opts) do
    requested = Keyword.get(opts, :manifest_version_id)
    active = Keyword.get(opts, :_active_manifest_version_id)

    cond do
      is_binary(active) and is_nil(requested) ->
        {:ok, active}

      is_binary(active) and requested == active ->
        {:ok, active}

      is_binary(active) and is_binary(requested) ->
        {:error, {:manifest_not_active_in_workspace, requested}}

      is_binary(active) ->
        {:error, {:invalid_manifest_version_id, requested}}

      is_nil(requested) ->
        {:error, :workspace_context_required}

      is_binary(requested) and requested != "" ->
        {:ok, requested}

      true ->
        {:error, {:invalid_manifest_version_id, requested}}
    end
  end

  defp validate_rerun_manifest_pin(opts, %RunState{} = source_run) when is_list(opts) do
    case Keyword.get(opts, :manifest_version_id) do
      nil ->
        :ok

      value when is_binary(value) and value == source_run.manifest_version_id ->
        :ok

      value when is_binary(value) ->
        {:error, {:rerun_manifest_mismatch, source_run.manifest_version_id, value}}

      invalid ->
        {:error, {:invalid_manifest_version_id, invalid}}
    end
  end

  defp validate_opts(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, :invalid_options}

      key = Enum.find([:max_attempts, :retry_backoff_ms], &Keyword.has_key?(opts, &1)) ->
        {:error, {:unsupported_retry_option, key, :use_retry_policy}}

      true ->
        :ok
    end
  end

  defp validate_metadata(value) when is_map(value), do: :ok
  defp validate_metadata(_value), do: {:error, :invalid_run_metadata}

  defp reject_backfill_parent_rerun(%RunState{submit_kind: submit_kind})
       when submit_kind in [:backfill_pipeline, :backfill_asset],
       do: {:error, :backfill_parent_rerun_not_supported}

  defp reject_backfill_parent_rerun(_run), do: :ok

  defp require_terminal_rerun_source(%RunState{status: status}) do
    if RunState.terminal_status?(status), do: :ok, else: {:error, {:run_not_terminal, status}}
  end

  defp workspace_opts(%WorkspaceContext{} = context, opts) do
    with {:ok, runtime} <- ManifestStore.get_runtime_state(context) do
      {:ok,
       opts
       |> Keyword.put(:_workspace_context, context)
       |> Keyword.put(:_workspace_id, context.workspace_id)
       |> Keyword.put(:_deployment_id, runtime.deployment_id)
       |> Keyword.put(:_active_manifest_version_id, runtime.manifest_version_id)}
    end
  end

  defp get_manifest(opts, manifest_version_id, deployment_id \\ nil) do
    case Keyword.get(opts, :_workspace_context) do
      %WorkspaceContext{} = context ->
        ManifestStore.get_deployment_manifest(
          context,
          deployment_id || Keyword.get(opts, :_deployment_id),
          manifest_version_id
        )

      nil ->
        {:error, :workspace_context_required}
    end
  end

  defp get_run(opts, run_id) do
    case Keyword.get(opts, :_workspace_context) do
      %WorkspaceContext{} = context -> Runs.get(context, run_id)
      nil -> {:error, :workspace_context_required}
    end
  end

  defp pipeline_refs_from_opts(opts) do
    case Keyword.get(opts, :_submit_ref) do
      {module, name} = ref when is_atom(module) and is_atom(name) -> [ref]
      _other -> []
    end
  end

  defp pipeline_refs_from_run(%RunState{metadata: metadata}) do
    case metadata_value(metadata, :pipeline_identity_ref) ||
           metadata_value(metadata, :pipeline_submit_ref) do
      {module, name} = ref when is_atom(module) and is_atom(name) -> [ref]
      _other -> []
    end
  end
end
