defmodule FavnOrchestrator.RunManager do
  @moduledoc """
  Orchestrator run admission, rerun, cancellation, and per-run server startup.
  """

  use GenServer

  alias Favn.Assets.Planner
  alias Favn.Contracts.RunnerClient
  alias Favn.Manifest.Index
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.PipelineResolver
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RunServer
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  @type state :: %{
          run_pids: %{required(String.t()) => pid()},
          monitors: %{required(reference()) => String.t()}
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, %{}, name: name)
  end

  @spec submit_asset_run(Favn.Ref.t(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def submit_asset_run({module, name} = asset_ref, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    GenServer.call(__MODULE__, {:submit_asset_run, asset_ref, opts}, :infinity)
  end

  @spec submit_pipeline_run([Favn.Ref.t()], keyword()) :: {:ok, String.t()} | {:error, term()}
  def submit_pipeline_run(target_refs, opts \\ []) when is_list(target_refs) and is_list(opts) do
    GenServer.call(__MODULE__, {:submit_pipeline_run, target_refs, opts}, :infinity)
  end

  @spec submit_pipeline_module_run(module(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def submit_pipeline_module_run(pipeline_module, opts \\ [])
      when is_atom(pipeline_module) and is_list(opts) do
    GenServer.call(__MODULE__, {:submit_pipeline_module_run, pipeline_module, opts}, :infinity)
  end

  @spec rerun(String.t(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def rerun(source_run_id, opts \\ []) when is_binary(source_run_id) and is_list(opts) do
    GenServer.call(__MODULE__, {:rerun, source_run_id, opts}, :infinity)
  end

  @spec cancel_run(String.t(), map()) :: :ok | {:error, term()}
  def cancel_run(run_id, reason \\ %{}) when is_binary(run_id) and is_map(reason) do
    GenServer.call(__MODULE__, {:cancel_run, run_id, reason}, :infinity)
  end

  @impl true
  def init(_args), do: {:ok, %{run_pids: %{}, monitors: %{}}}

  @impl true
  def handle_call({:submit_asset_run, asset_ref, opts}, _from, state) do
    reply =
      with {:ok, run_state, version} <- build_run_submission(asset_ref, opts),
           :ok <-
             Projector.persist_snapshot_with_event(run_state, :run_created, %{
               status: run_state.status,
               submit_kind: :manual
             }),
           {:ok, pid} <- start_run_server(run_state, version) do
        ref = Process.monitor(pid)

        next_state =
          state
          |> put_in([:run_pids, run_state.id], pid)
          |> put_in([:monitors, ref], run_state.id)

        {{:ok, run_state.id}, next_state}
      end

    case reply do
      {{:ok, run_id}, next_state} -> {:reply, {:ok, run_id}, next_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:submit_pipeline_run, target_refs, opts}, _from, state) do
    reply =
      with :ok <- validate_target_refs(target_refs),
           metadata <- build_pipeline_metadata(target_refs, opts),
           submit_opts <-
             opts |> Keyword.put(:metadata, metadata) |> Keyword.put(:_submit_kind, :pipeline),
           {:ok, run_state, version} <- build_pipeline_submission(target_refs, submit_opts),
           :ok <-
             Projector.persist_snapshot_with_event(run_state, :run_created, %{
               status: run_state.status,
               submit_kind: :pipeline,
               pipeline_target_refs: target_refs
             }),
           {:ok, pid} <- start_run_server(run_state, version) do
        ref = Process.monitor(pid)

        next_state =
          state
          |> put_in([:run_pids, run_state.id], pid)
          |> put_in([:monitors, ref], run_state.id)

        {{:ok, run_state.id}, next_state}
      end

    case reply do
      {{:ok, run_id}, next_state} -> {:reply, {:ok, run_id}, next_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:submit_pipeline_module_run, pipeline_module, opts}, _from, state) do
    reply =
      with {:ok, run_state, version} <- build_pipeline_module_submission(pipeline_module, opts),
           :ok <-
             Projector.persist_snapshot_with_event(run_state, :run_created, %{
               status: run_state.status,
               submit_kind: :pipeline,
               pipeline_target_refs: run_state.target_refs,
               pipeline_module: pipeline_module
             }),
           {:ok, pid} <- start_run_server(run_state, version) do
        ref = Process.monitor(pid)

        next_state =
          state
          |> put_in([:run_pids, run_state.id], pid)
          |> put_in([:monitors, ref], run_state.id)

        {{:ok, run_state.id}, next_state}
      end

    case reply do
      {{:ok, run_id}, next_state} -> {:reply, {:ok, run_id}, next_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:rerun, source_run_id, opts}, _from, state) do
    reply =
      with {:ok, source_run} <- Storage.get_run(source_run_id),
           {:ok, run_state, version} <- build_rerun_submission(source_run, opts),
           :ok <-
             Projector.persist_snapshot_with_event(run_state, :run_created, %{
               status: run_state.status,
               submit_kind: :rerun,
               rerun_of_run_id: run_state.rerun_of_run_id,
               parent_run_id: run_state.parent_run_id
             }),
           {:ok, pid} <- start_run_server(run_state, version) do
        ref = Process.monitor(pid)

        next_state =
          state
          |> put_in([:run_pids, run_state.id], pid)
          |> put_in([:monitors, ref], run_state.id)

        {{:ok, run_state.id}, next_state}
      end

    case reply do
      {{:ok, run_id}, next_state} -> {:reply, {:ok, run_id}, next_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:cancel_run, run_id, reason}, _from, state) do
    reply =
      case Storage.get_run(run_id) do
        {:ok, run} ->
          with :ok <- validate_cancel_reason(reason),
               :ok <- forward_cancel_if_inflight(run, reason),
               {:ok, cancel_requested, cancelled} <- build_cancel_snapshots(run, reason),
               :ok <-
                 Projector.persist_snapshot_with_event(cancel_requested, :run_cancel_requested, %{
                   reason: reason
                 }) do
            Projector.persist_snapshot_with_event(cancelled, :run_cancelled, %{reason: reason})
          end

        {:error, _reason} = error ->
          error
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, monitors} ->
        {:noreply, %{state | monitors: monitors}}

      {run_id, monitors} ->
        {:noreply, %{state | monitors: monitors, run_pids: Map.delete(state.run_pids, run_id)}}
    end
  end

  defp build_run_submission(asset_ref, opts) do
    run_id = Keyword.get(opts, :run_id, new_run_id())
    params = Keyword.get(opts, :params, %{})
    trigger = Keyword.get(opts, :trigger, %{kind: :manual})
    metadata = Keyword.get(opts, :metadata, %{})
    max_attempts = Keyword.get(opts, :max_attempts, 1)
    retry_backoff_ms = Keyword.get(opts, :retry_backoff_ms, 0)
    timeout_ms = Keyword.get(opts, :timeout_ms, 5_000)

    with :ok <- validate_params(params),
         :ok <- validate_trigger(trigger),
         :ok <- validate_metadata(metadata),
         :ok <- validate_max_attempts(max_attempts),
         :ok <- validate_retry_backoff_ms(retry_backoff_ms),
         :ok <- validate_timeout_ms(timeout_ms),
         {:ok, manifest_version_id} <- resolve_manifest_version_id(opts),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         {:ok, _asset} <- Index.fetch_asset(index, asset_ref),
         {:ok, plan} <-
           Planner.plan(asset_ref, graph_index: index.graph_index, dependencies: :all) do
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
          metadata: metadata,
          submit_kind: :manual,
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
    timeout_ms = Keyword.get(opts, :timeout_ms, 5_000)
    dependencies = Keyword.get(opts, :dependencies, :all)

    with :ok <- validate_params(params),
         :ok <- validate_trigger(trigger),
         :ok <- validate_metadata(metadata),
         :ok <- validate_dependencies(dependencies),
         :ok <- validate_max_attempts(max_attempts),
         :ok <- validate_retry_backoff_ms(retry_backoff_ms),
         :ok <- validate_timeout_ms(timeout_ms),
         {:ok, manifest_version_id} <- resolve_manifest_version_id(opts),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         :ok <- ensure_assets_exist(index, target_refs),
         {:ok, plan} <-
           Planner.plan(target_refs, graph_index: index.graph_index, dependencies: dependencies) do
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

    with :ok <- validate_trigger(trigger),
         :ok <- validate_params(params),
         {:ok, manifest_version_id} <- resolve_manifest_version_id(opts),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         {:ok, pipeline} <- fetch_pipeline_by_module(index, pipeline_module),
         {:ok, resolution} <-
           PipelineResolver.resolve(index, pipeline,
             trigger: trigger,
             params: params,
             anchor_window: anchor_window
           ) do
      metadata =
        opts
        |> Keyword.get(:metadata, %{})
        |> Map.merge(%{
          submit_kind: :pipeline,
          pipeline_target_refs: resolution.target_refs,
          pipeline_context: resolution.pipeline_ctx,
          pipeline_dependencies: resolution.dependencies,
          pipeline_submit_ref: pipeline_module
        })

      build_pipeline_submission(
        resolution.target_refs,
        opts
        |> Keyword.put(:manifest_version_id, version.manifest_version_id)
        |> Keyword.put(:trigger, trigger)
        |> Keyword.put(:params, params)
        |> Keyword.put(:dependencies, resolution.dependencies)
        |> Keyword.put(:metadata, metadata)
      )
    end
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

    Map.merge(user_metadata, %{
      submit_kind: :pipeline,
      pipeline_target_refs: target_refs,
      pipeline_context: Keyword.get(opts, :_pipeline_context),
      pipeline_dependencies: Keyword.get(opts, :dependencies),
      pipeline_submit_ref: Keyword.get(opts, :_submit_ref)
    })
  end

  defp build_rerun_submission(%RunState{} = source_run, opts) when is_list(opts) do
    run_id = Keyword.get(opts, :run_id, new_run_id())
    params = Keyword.get(opts, :params, source_run.params)
    trigger = Keyword.get(opts, :trigger, %{kind: :rerun, source_run_id: source_run.id})
    metadata = Map.merge(source_run.metadata, Keyword.get(opts, :metadata, %{}))
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
         :ok <- validate_rerun_manifest_pin(opts, source_run),
         {:ok, version} <- ManifestStore.get_manifest(source_run.manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         {:ok, _asset} <- Index.fetch_asset(index, rerun_asset_ref),
         :ok <- ensure_assets_exist(index, rerun_targets),
         {:ok, plan} <-
           Planner.plan(rerun_targets,
             graph_index: index.graph_index,
             dependencies: rerun_dependencies
           ) do
      rerun_of_run_id = source_run.rerun_of_run_id || source_run.id
      root_run_id = source_run.root_run_id || source_run.id
      metadata_with_source = Map.put(metadata, :source_run_id, source_run.id)

      metadata_with_replay =
        if source_run.submit_kind == :pipeline do
          Map.merge(metadata_with_source, %{
            replay_submit_kind: :pipeline,
            pipeline_target_refs: rerun_targets,
            pipeline_dependencies: rerun_dependencies
          })
        else
          metadata_with_source
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
          parent_run_id: source_run.id,
          root_run_id: root_run_id,
          lineage_depth: source_run.lineage_depth + 1,
          max_attempts: max_attempts,
          retry_backoff_ms: retry_backoff_ms,
          timeout_ms: timeout_ms
        )

      {:ok, run_state, version}
    end
  end

  defp replay_selection(%RunState{submit_kind: :pipeline} = source_run) do
    replay_targets =
      case Map.get(source_run.metadata, :pipeline_target_refs) do
        targets when is_list(targets) and targets != [] -> targets
        _other -> source_run.target_refs
      end

    replay_asset_ref = List.first(replay_targets) || source_run.asset_ref

    replay_dependencies =
      normalize_dependencies(Map.get(source_run.metadata, :pipeline_dependencies))

    {replay_asset_ref, replay_targets, replay_dependencies}
  end

  defp replay_selection(%RunState{} = source_run) do
    {source_run.asset_ref, [source_run.asset_ref], :all}
  end

  defp normalize_dependencies(:none), do: :none
  defp normalize_dependencies(_value), do: :all

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

  defp start_run_server(%RunState{} = run_state, version) do
    child_spec = %{
      id: {RunServer, run_state.id},
      start: {RunServer, :start_link, [%{run_state: run_state, version: version}]},
      restart: :temporary,
      shutdown: 5000,
      type: :worker
    }

    DynamicSupervisor.start_child(FavnOrchestrator.RunSupervisor, child_spec)
  end

  defp validate_params(value) when is_map(value), do: :ok
  defp validate_params(_value), do: {:error, :invalid_run_params}

  defp validate_trigger(value) when is_map(value), do: :ok
  defp validate_trigger(_value), do: {:error, :invalid_pipeline_trigger}

  defp validate_metadata(value) when is_map(value), do: :ok
  defp validate_metadata(_value), do: {:error, :invalid_run_metadata}

  defp validate_cancel_reason(value) when is_map(value), do: :ok
  defp validate_cancel_reason(_value), do: {:error, :invalid_cancel_reason}

  defp validate_dependencies(:all), do: :ok
  defp validate_dependencies(:none), do: :ok
  defp validate_dependencies(_value), do: {:error, :invalid_dependencies}

  defp validate_max_attempts(value) when is_integer(value) and value > 0, do: :ok
  defp validate_max_attempts(_value), do: {:error, :invalid_max_attempts}

  defp validate_retry_backoff_ms(value) when is_integer(value) and value >= 0, do: :ok
  defp validate_retry_backoff_ms(_value), do: {:error, :invalid_retry_backoff_ms}

  defp validate_timeout_ms(value) when is_integer(value) and value > 0, do: :ok
  defp validate_timeout_ms(_value), do: {:error, :invalid_timeout_ms}

  defp build_cancel_snapshots(%RunState{status: status}, _reason)
       when status in [:ok, :error, :cancelled, :timed_out] do
    {:error, :run_already_terminal}
  end

  defp build_cancel_snapshots(%RunState{} = run, reason) do
    cancel_requested =
      RunState.transition(run,
        metadata:
          Map.merge(run.metadata, %{
            cancel_requested: true,
            cancel_reason: reason,
            cancel_requested_at: DateTime.utc_now(),
            in_flight_execution_ids: []
          })
      )

    cancelled =
      RunState.transition(cancel_requested,
        status: :cancelled,
        error: {:cancelled, reason},
        runner_execution_id: nil,
        metadata: Map.put(cancel_requested.metadata, :cancelled, true)
      )

    {:ok, cancel_requested, cancelled}
  end

  defp forward_cancel_if_inflight(%RunState{} = run, reason) do
    runner_client = Application.get_env(:favn_orchestrator, :runner_client, nil)
    runner_opts = Application.get_env(:favn_orchestrator, :runner_client_opts, [])
    execution_ids = inflight_execution_ids(run)

    if execution_ids == [] do
      :ok
    else
      with :ok <- validate_runner_client(runner_client) do
        Enum.reduce_while(execution_ids, :ok, fn execution_id, :ok ->
          case runner_client.cancel_work(
                 execution_id,
                 %{run_id: run.id, reason: reason, requested_at: DateTime.utc_now()},
                 runner_opts
               ) do
            :ok -> {:cont, :ok}
            {:error, _cancel_reason} = error -> {:halt, error}
          end
        end)
      end
    end
  end

  defp inflight_execution_ids(%RunState{} = run) do
    metadata_ids =
      case Map.get(run.metadata, :in_flight_execution_ids, []) do
        ids when is_list(ids) -> ids
        _other -> []
      end

    [run.runner_execution_id | metadata_ids]
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
  end

  defp validate_runner_client(module) when is_atom(module) do
    callbacks = RunnerClient.behaviour_info(:callbacks)

    with {:module, ^module} <- Code.ensure_loaded(module),
         true <-
           Enum.all?(callbacks, fn {name, arity} -> function_exported?(module, name, arity) end) do
      :ok
    else
      _ -> {:error, :runner_client_not_available}
    end
  end

  defp validate_runner_client(_module), do: {:error, :runner_client_not_available}

  defp new_run_id do
    "run_" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end
end
