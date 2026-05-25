defmodule FavnOrchestrator.RunManager.SubmissionBuilder do
  @moduledoc false

  alias Favn.Assets.Planner
  alias Favn.Manifest.Index
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.PipelineResolver
  alias Favn.Window.Anchor
  alias Favn.Window.Policy
  alias Favn.Window.Request, as: WindowRequest
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.RunManager.Submission
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  @spec asset(Favn.Ref.t(), keyword()) :: {:ok, Submission.t()} | {:error, term()}
  def asset(asset_ref, opts) when is_list(opts) do
    with {:ok, run_state, version} <- build_run_submission(asset_ref, opts) do
      {:ok,
       %Submission{
         run_state: run_state,
         manifest_version: version,
         submit_kind: :manual,
         transition_metadata: %{status: run_state.status, submit_kind: :manual},
         event_metadata: %{run_id: run_state.id, submit_kind: :manual}
       }}
    end
  end

  @spec pipeline([Favn.Ref.t()], keyword()) :: {:ok, Submission.t()} | {:error, term()}
  def pipeline(target_refs, opts) when is_list(target_refs) and is_list(opts) do
    with :ok <- validate_target_refs(target_refs),
         {:ok, metadata} <- build_pipeline_metadata(target_refs, opts),
         submit_opts <-
           opts |> Keyword.put(:metadata, metadata) |> Keyword.put(:_submit_kind, :pipeline),
         {:ok, run_state, version} <- build_pipeline_submission(target_refs, submit_opts) do
      {:ok,
       %Submission{
         run_state: run_state,
         manifest_version: version,
         submit_kind: :pipeline,
         transition_metadata: %{
           status: run_state.status,
           submit_kind: :pipeline,
           pipeline_target_refs: target_refs
         },
         event_metadata: %{run_id: run_state.id, submit_kind: :pipeline}
       }}
    end
  end

  @spec pipeline_module(module(), keyword()) :: {:ok, Submission.t()} | {:error, term()}
  def pipeline_module(pipeline_module, opts) when is_atom(pipeline_module) and is_list(opts) do
    with {:ok, run_state, version} <- build_pipeline_module_submission(pipeline_module, opts) do
      {:ok,
       %Submission{
         run_state: run_state,
         manifest_version: version,
         submit_kind: :pipeline,
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

  @spec rerun(String.t(), keyword()) :: {:ok, Submission.t()} | {:error, term()}
  def rerun(source_run_id, opts) when is_binary(source_run_id) and is_list(opts) do
    with {:ok, source_run} <- Storage.get_run(source_run_id),
         :ok <- reject_backfill_parent_rerun(source_run),
         {:ok, run_state, version} <- build_rerun_submission(source_run, opts) do
      {:ok,
       %Submission{
         run_state: run_state,
         manifest_version: version,
         submit_kind: :rerun,
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
    run_id = Keyword.get(opts, :run_id, new_run_id())
    params = Keyword.get(opts, :params, %{})
    trigger = Keyword.get(opts, :trigger, %{kind: :manual})
    metadata = Keyword.get(opts, :metadata, %{})
    max_attempts = Keyword.get(opts, :max_attempts, 1)
    retry_backoff_ms = Keyword.get(opts, :retry_backoff_ms, 0)
    timeout_ms = Keyword.get(opts, :timeout_ms, RunState.default_timeout_ms())
    dependencies = Keyword.get(opts, :dependencies, :all)
    anchor_window = Keyword.get(opts, :anchor_window)
    exact_windows = Keyword.get(opts, :exact_windows, %{})

    with :ok <- validate_params(params),
         :ok <- validate_trigger(trigger),
         :ok <- validate_metadata(metadata),
         :ok <- validate_dependencies(dependencies),
         {:ok, refresh_policy} <- refresh_policy_metadata(opts, dependencies),
         metadata_with_selection <-
           metadata
           |> Map.put(:asset_dependencies, dependencies)
           |> Map.put(:refresh_policy, refresh_policy),
         :ok <- validate_max_attempts(max_attempts),
         :ok <- validate_retry_backoff_ms(retry_backoff_ms),
         :ok <- validate_timeout_ms(timeout_ms),
         :ok <- validate_anchor_window(anchor_window),
         {:ok, manifest_version_id} <- resolve_manifest_version_id(opts),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         {:ok, _asset} <- Index.fetch_asset(index, asset_ref),
         {:ok, plan} <-
           Planner.plan(asset_ref,
             planning_index: index.planning_index,
             dependencies: dependencies,
             anchor_window: anchor_window,
             exact_windows: exact_windows
           ) do
      run_state =
        RunState.new(
          id: run_id,
          manifest_version_id: version.manifest_version_id,
          manifest_content_hash: version.content_hash,
          asset_ref: asset_ref,
          target_refs: plan.target_refs,
          plan: plan,
          params: params,
          trigger: trigger,
          metadata: metadata_with_selection,
          submit_kind: :manual,
          parent_run_id: Keyword.get(opts, :parent_run_id),
          root_run_id: Keyword.get(opts, :root_run_id),
          lineage_depth: Keyword.get(opts, :lineage_depth, 0),
          max_attempts: max_attempts,
          retry_backoff_ms: retry_backoff_ms,
          timeout_ms: timeout_ms
        )

      {:ok, run_state, version}
    end
  end

  defp build_pipeline_submission(target_refs, opts) when is_list(target_refs) and is_list(opts) do
    run_id = Keyword.get(opts, :run_id, new_run_id())
    params = Keyword.get(opts, :params, %{})
    trigger = Keyword.get(opts, :trigger, %{kind: :pipeline})
    metadata = Keyword.get(opts, :metadata, %{})
    max_attempts = Keyword.get(opts, :max_attempts, 1)
    retry_backoff_ms = Keyword.get(opts, :retry_backoff_ms, 0)
    timeout_ms = Keyword.get(opts, :timeout_ms, RunState.default_timeout_ms())
    dependencies = Keyword.get(opts, :dependencies, :all)
    anchor_window = Keyword.get(opts, :anchor_window)

    with :ok <- validate_params(params),
         :ok <- validate_trigger(trigger),
         :ok <- validate_metadata(metadata),
         :ok <- validate_dependencies(dependencies),
         {:ok, refresh_policy} <- refresh_policy_metadata(opts, dependencies),
         metadata <- Map.put(metadata, :refresh_policy, refresh_policy),
         metadata <- maybe_put_pipeline_execution_policy(metadata),
         :ok <- validate_anchor_window(anchor_window),
         :ok <- validate_max_attempts(max_attempts),
         :ok <- validate_retry_backoff_ms(retry_backoff_ms),
         :ok <- validate_timeout_ms(timeout_ms),
         {:ok, manifest_version_id} <- resolve_manifest_version_id(opts),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         :ok <- ensure_assets_exist(index, target_refs),
         {:ok, plan} <-
           Planner.plan(target_refs,
             planning_index: index.planning_index,
             dependencies: dependencies,
             anchor_window: anchor_window
           ) do
      primary_ref = List.first(plan.target_refs)

      run_state =
        RunState.new(
          id: run_id,
          manifest_version_id: version.manifest_version_id,
          manifest_content_hash: version.content_hash,
          asset_ref: primary_ref,
          target_refs: plan.target_refs,
          plan: plan,
          params: params,
          trigger: trigger,
          metadata: metadata,
          submit_kind: :pipeline,
          parent_run_id: Keyword.get(opts, :parent_run_id),
          root_run_id: Keyword.get(opts, :root_run_id),
          lineage_depth: Keyword.get(opts, :lineage_depth, 0),
          max_attempts: max_attempts,
          retry_backoff_ms: retry_backoff_ms,
          timeout_ms: timeout_ms
        )

      {:ok, run_state, version}
    end
  end

  defp build_pipeline_module_submission(pipeline_module, opts)
       when is_atom(pipeline_module) and is_list(opts) do
    trigger = Keyword.get(opts, :trigger, %{kind: :pipeline})
    params = Keyword.get(opts, :params, %{})
    anchor_window = Keyword.get(opts, :anchor_window)
    window_request = Keyword.get(opts, :window_request)

    with :ok <- validate_trigger(trigger),
         :ok <- validate_params(params),
         {:ok, request} <- normalize_pipeline_window_request(anchor_window, window_request),
         {:ok, manifest_version_id} <- resolve_manifest_version_id(opts),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         {:ok, pipeline} <- fetch_pipeline_by_module(index, pipeline_module),
         {:ok, resolved_anchor_window} <-
           resolve_pipeline_anchor_window(pipeline, anchor_window, request),
         {:ok, resolution} <-
           PipelineResolver.resolve(index, pipeline,
             trigger: trigger,
             params: params,
             anchor_window: resolved_anchor_window
           ) do
      metadata =
        opts
        |> Keyword.get(:metadata, %{})
        |> Map.merge(%{
          submit_kind: :pipeline,
          pipeline_target_refs: resolution.target_refs,
          pipeline_context: resolution.pipeline_ctx,
          pipeline_dependencies: resolution.dependencies,
          pipeline_submit_ref: pipeline_module,
          pipeline_execution_policy: pipeline_execution_policy(resolution.pipeline)
        })

      build_pipeline_submission(
        resolution.target_refs,
        opts
        |> Keyword.put(:manifest_version_id, version.manifest_version_id)
        |> Keyword.put(:trigger, trigger)
        |> Keyword.put(:params, params)
        |> Keyword.put(:anchor_window, resolved_anchor_window)
        |> Keyword.put(:dependencies, resolution.dependencies)
        |> Keyword.put(:metadata, metadata)
      )
    end
  end

  defp build_rerun_submission(%RunState{} = source_run, opts) when is_list(opts) do
    run_id = Keyword.get(opts, :run_id, new_run_id())
    params = Keyword.get(opts, :params, source_run.params)
    trigger = Keyword.get(opts, :trigger, %{kind: :rerun, source_run_id: source_run.id})
    metadata = Map.merge(rerun_base_metadata(source_run), Keyword.get(opts, :metadata, %{}))
    max_attempts = Keyword.get(opts, :max_attempts, source_run.max_attempts)
    retry_backoff_ms = Keyword.get(opts, :retry_backoff_ms, source_run.retry_backoff_ms)
    timeout_ms = Keyword.get(opts, :timeout_ms, source_run.timeout_ms)
    {rerun_asset_ref, rerun_targets, rerun_dependencies} = replay_selection(source_run)

    with :ok <- validate_params(params),
         :ok <- validate_trigger(trigger),
         :ok <- validate_metadata(metadata),
         :ok <- validate_max_attempts(max_attempts),
         :ok <- validate_retry_backoff_ms(retry_backoff_ms),
         :ok <- validate_timeout_ms(timeout_ms),
         :ok <- validate_dependencies(rerun_dependencies),
         {:ok, refresh_policy} <- refresh_policy_metadata(opts, rerun_dependencies),
         :ok <- validate_rerun_manifest_pin(opts, source_run),
         {:ok, version} <- ManifestStore.get_manifest(source_run.manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         {:ok, _asset} <- Index.fetch_asset(index, rerun_asset_ref),
         :ok <- ensure_assets_exist(index, rerun_targets),
         anchor_window <- Keyword.get(opts, :anchor_window),
         :ok <- validate_anchor_window(anchor_window),
         {:ok, plan} <-
           Planner.plan(rerun_targets,
             planning_index: index.planning_index,
             dependencies: rerun_dependencies,
             anchor_window: anchor_window
           ) do
      rerun_of_run_id = source_run.rerun_of_run_id || source_run.id
      parent_run_id = Keyword.get(opts, :parent_run_id, source_run.id)
      root_run_id = Keyword.get(opts, :root_run_id, source_run.root_run_id || source_run.id)

      metadata_with_source =
        metadata
        |> Map.put(:source_run_id, source_run.id)
        |> Map.put(:refresh_policy, refresh_policy)

      metadata_with_replay =
        if pipeline_origin?(source_run) do
          Map.merge(metadata_with_source, %{
            replay_submit_kind: :pipeline,
            replay_mode: :exact_replay,
            pipeline_target_refs: rerun_targets,
            pipeline_dependencies: rerun_dependencies
          })
        else
          Map.merge(metadata_with_source, %{
            replay_mode: :exact_replay,
            asset_dependencies: rerun_dependencies
          })
        end

      run_state =
        RunState.new(
          id: run_id,
          manifest_version_id: source_run.manifest_version_id,
          manifest_content_hash: source_run.manifest_content_hash,
          asset_ref: rerun_asset_ref,
          target_refs: plan.target_refs,
          plan: plan,
          params: params,
          trigger: trigger,
          metadata: metadata_with_replay,
          submit_kind: :rerun,
          rerun_of_run_id: rerun_of_run_id,
          parent_run_id: parent_run_id,
          root_run_id: root_run_id,
          lineage_depth: source_run.lineage_depth + 1,
          max_attempts: max_attempts,
          retry_backoff_ms: retry_backoff_ms,
          timeout_ms: timeout_ms
        )

      {:ok, run_state, version}
    end
  end

  defp resolve_pipeline_anchor_window(%Pipeline{} = pipeline, nil, request),
    do: Policy.resolve_manual(pipeline.window, request)

  defp resolve_pipeline_anchor_window(_pipeline, anchor_window, _request) do
    with :ok <- validate_anchor_window(anchor_window), do: {:ok, anchor_window}
  end

  defp pipeline_execution_policy(pipeline) do
    %{
      max_concurrency: Map.get(pipeline, :max_concurrency),
      execution_pool: Map.get(pipeline, :execution_pool)
    }
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp maybe_put_pipeline_execution_policy(%{pipeline_execution_policy: _policy} = metadata),
    do: metadata

  defp maybe_put_pipeline_execution_policy(metadata), do: metadata

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

  defp normalize_pipeline_window_request(nil, window_request),
    do: WindowRequest.from_value(window_request)

  defp normalize_pipeline_window_request(anchor_window, _window_request) do
    with :ok <- validate_anchor_window(anchor_window), do: {:ok, nil}
  end

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

  defp build_pipeline_metadata(target_refs, opts) do
    user_metadata = Keyword.get(opts, :metadata, %{})

    {:ok,
     Map.merge(user_metadata, %{
       submit_kind: :pipeline,
       pipeline_target_refs: target_refs,
       pipeline_context: Keyword.get(opts, :_pipeline_context),
       pipeline_dependencies: Keyword.get(opts, :dependencies),
       pipeline_submit_ref: Keyword.get(opts, :_submit_ref)
     })}
  end

  defp replay_selection(%RunState{} = source_run) do
    if pipeline_origin?(source_run) do
      replay_targets =
        case Map.get(source_run.metadata, :pipeline_target_refs) do
          targets when is_list(targets) and targets != [] -> targets
          _other -> source_run.target_refs
        end

      replay_asset_ref = List.first(replay_targets) || source_run.asset_ref

      replay_dependencies =
        normalize_dependencies(metadata_value(source_run.metadata, :pipeline_dependencies))

      {replay_asset_ref, replay_targets, replay_dependencies}
    else
      replay_dependencies = normalize_asset_dependencies(source_run)

      {source_run.asset_ref, [source_run.asset_ref], replay_dependencies}
    end
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
    Map.get(metadata, :replay_submit_kind) == :pipeline or
      is_map(Map.get(metadata, :pipeline_context)) or
      present_atom?(Map.get(metadata, :pipeline_submit_ref)) or
      (is_list(Map.get(metadata, :pipeline_target_refs)) and
         Map.get(metadata, :pipeline_target_refs) != [])
  end

  defp present_atom?(value) when is_atom(value) and not is_nil(value), do: true
  defp present_atom?(_value), do: false

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
    case Keyword.get(opts, :manifest_version_id) do
      nil -> ManifestStore.get_active_manifest()
      value when is_binary(value) and value != "" -> {:ok, value}
      invalid -> {:error, {:invalid_manifest_version_id, invalid}}
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

  defp validate_params(value) when is_map(value), do: :ok
  defp validate_params(_value), do: {:error, :invalid_run_params}

  defp validate_trigger(value) when is_map(value), do: :ok
  defp validate_trigger(_value), do: {:error, :invalid_pipeline_trigger}

  defp validate_metadata(value) when is_map(value), do: :ok
  defp validate_metadata(_value), do: {:error, :invalid_run_metadata}

  defp reject_backfill_parent_rerun(%RunState{submit_kind: :backfill_pipeline}),
    do: {:error, :backfill_parent_rerun_not_supported}

  defp reject_backfill_parent_rerun(_run), do: :ok

  defp validate_dependencies(:all), do: :ok
  defp validate_dependencies(:none), do: :ok
  defp validate_dependencies(_value), do: {:error, :invalid_dependencies}

  defp validate_anchor_window(nil), do: :ok
  defp validate_anchor_window(%Anchor{} = anchor), do: Anchor.validate(anchor)
  defp validate_anchor_window(_value), do: {:error, :invalid_anchor_window}

  defp validate_max_attempts(value) when is_integer(value) and value > 0, do: :ok
  defp validate_max_attempts(_value), do: {:error, :invalid_max_attempts}

  defp validate_retry_backoff_ms(value) when is_integer(value) and value >= 0, do: :ok
  defp validate_retry_backoff_ms(_value), do: {:error, :invalid_retry_backoff_ms}

  defp validate_timeout_ms(value) when is_integer(value) and value > 0, do: :ok
  defp validate_timeout_ms(_value), do: {:error, :invalid_timeout_ms}

  defp new_run_id do
    "run_" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end
end
