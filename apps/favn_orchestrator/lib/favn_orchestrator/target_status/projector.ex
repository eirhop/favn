defmodule FavnOrchestrator.TargetStatus.Projector do
  @moduledoc """
  Maintains the target-status projection from orchestrator-owned truth.

  Normal write paths update this projection after run transitions and freshness
  writes. The rebuild path intentionally scans authoritative persisted state and
  replaces rows for one manifest version, keeping the projection repairable rather
  than authoritative.
  """

  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Version
  alias Favn.Manifest.Index
  alias Favn.Manifest.PipelineResolver
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.CursorPage
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.TargetStatus

  @latest_freshness_key Favn.Freshness.Key.latest()

  @doc """
  Projects a persisted run transition into current asset and pipeline target rows.
  """
  @spec project_transition(RunState.t(), atom(), map()) :: :ok | {:error, term()}
  def project_transition(%RunState{} = run_state, _event_type, _data) do
    targets = asset_targets(run_state) ++ pipeline_targets(run_state)

    Enum.reduce_while(targets, :ok, fn {kind, target_id, target_ref_text}, :ok ->
      case upsert_from_run(run_state, kind, target_id, target_ref_text) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @doc """
  Projects a persisted asset freshness state into the matching asset target row.
  """
  @spec project_freshness_state(AssetFreshnessState.t()) :: :ok | {:error, term()}
  def project_freshness_state(%AssetFreshnessState{freshness_key: freshness_key})
      when freshness_key != @latest_freshness_key,
      do: :ok

  def project_freshness_state(%AssetFreshnessState{} = freshness_state) do
    ref = {freshness_state.asset_ref_module, freshness_state.asset_ref_name}
    manifest_version_id = freshness_state.manifest_version_id

    if is_binary(manifest_version_id) do
      target_id = TargetStatus.target_id_for_asset(ref)
      target_ref_text = TargetStatus.ref_text(ref)

      existing = get_existing(manifest_version_id, :asset, target_id, target_ref_text)
      status = from_freshness(freshness_state, existing, target_id, target_ref_text)
      Storage.upsert_target_status(status)
    else
      :ok
    end
  end

  @doc """
  Rebuilds all asset and pipeline target status rows for one persisted manifest.
  """
  @spec rebuild_manifest(Version.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def rebuild_manifest(%Version{} = version) do
    manifest_version_id = version.manifest_version_id

    with {:ok, index} <- Index.build_from_version(version),
         {:ok, runs} <- Storage.list_runs(manifest_version_id: manifest_version_id),
         {:ok, freshness_states} <- scan_freshness_states(manifest_version_id) do
      statuses =
        asset_rebuild_statuses(version, runs, freshness_states) ++
          pipeline_rebuild_statuses(version, index, runs)

      with :ok <-
             Storage.replace_target_statuses({:manifest_version, manifest_version_id}, statuses) do
        {:ok, length(statuses)}
      end
    end
  end

  @doc false
  @spec status_from_run(RunState.t(), TargetStatus.target_kind(), String.t(), String.t()) ::
          TargetStatus.t()
  def status_from_run(%RunState{} = run, kind, target_id, target_ref_text)
      when kind in [:asset, :pipeline] do
    existing =
      TargetStatus.unknown(run.manifest_version_id, kind, target_id, target_ref_text)

    from_run(run, existing, kind, target_id, target_ref_text)
  end

  defp upsert_from_run(%RunState{} = run, kind, target_id, target_ref_text) do
    existing = get_existing(run.manifest_version_id, kind, target_id, target_ref_text)

    run
    |> from_run(existing, kind, target_id, target_ref_text)
    |> Storage.upsert_target_status()
  end

  defp get_existing(manifest_version_id, kind, target_id, target_ref_text) do
    case Storage.get_target_status(manifest_version_id, kind, target_id) do
      {:ok, %TargetStatus{} = status} ->
        status

      {:error, :not_found} ->
        TargetStatus.unknown(manifest_version_id, kind, target_id, target_ref_text)

      {:error, _reason} ->
        TargetStatus.unknown(manifest_version_id, kind, target_id, target_ref_text)
    end
  end

  defp asset_rebuild_statuses(%Version{} = version, runs, freshness_states) do
    Enum.map(List.wrap(version.manifest.assets), fn asset ->
      target_id = TargetStatus.target_id_for_asset(asset.ref)
      target_ref_text = TargetStatus.ref_text(asset.ref)

      base = TargetStatus.unknown(version.manifest_version_id, :asset, target_id, target_ref_text)

      run_status =
        runs
        |> Enum.filter(&run_references_asset?(&1, asset.ref))
        |> Enum.sort_by(&DateTime.to_unix(run_time(&1), :microsecond))
        |> Enum.reduce(base, &from_run(&1, &2, :asset, target_id, target_ref_text))

      case latest_freshness_for_ref(freshness_states, asset.ref) do
        nil -> run_status
        freshness -> from_freshness(freshness, run_status, target_id, target_ref_text)
      end
    end)
  end

  defp pipeline_rebuild_statuses(%Version{} = version, %Index{} = index, runs) do
    Enum.map(List.wrap(version.manifest.pipelines), fn pipeline ->
      target_id = TargetStatus.target_id_for_pipeline(pipeline.module)
      target_ref_text = TargetStatus.ref_text(pipeline.module)
      selected_assets = pipeline_selected_assets(index, pipeline)

      base =
        TargetStatus.unknown(version.manifest_version_id, :pipeline, target_id, target_ref_text)

      runs
      |> Enum.filter(&pipeline_matches_run?(&1, pipeline, selected_assets))
      |> Enum.sort_by(&DateTime.to_unix(run_time(&1), :microsecond))
      |> Enum.reduce(base, &from_run(&1, &2, :pipeline, target_id, target_ref_text))
    end)
  end

  defp from_run(%RunState{} = run, %TargetStatus{} = existing, kind, target_id, target_ref_text) do
    run_at = run_time(run)
    latest? = latest_evidence?(run_at, run.event_seq, existing)
    run_status = Map.get(run, :status)

    attrs = %{
      manifest_version_id: run.manifest_version_id,
      target_kind: kind,
      target_id: target_id,
      target_ref_text: target_ref_text,
      status: if(latest?, do: TargetStatus.status_from_run(run_status), else: existing.status),
      latest_run_id: if(latest?, do: run.id, else: existing.latest_run_id),
      latest_run_status: if(latest?, do: run_status, else: existing.latest_run_status),
      latest_run_at: if(latest?, do: run_at, else: existing.latest_run_at),
      latest_run_duration_ms:
        if(latest?, do: run_duration_ms(run), else: existing.latest_run_duration_ms),
      latest_success_run_id: success_run_id(run, existing),
      latest_success_at: success_run_at(run, existing, run_at),
      latest_failure_run_id: failure_run_id(run, existing),
      latest_failure_at: failure_run_at(run, existing, run_at),
      in_flight_run_id: in_flight_run_id(run, existing),
      freshness_status: existing.freshness_status,
      freshness_key: existing.freshness_key,
      updated_at: max_datetime(run.updated_at || run_at, existing.updated_at),
      updated_seq: max(run.event_seq || 0, existing.updated_seq || 0),
      payload: existing.payload || %{}
    }

    {:ok, status} = TargetStatus.new(attrs)
    status
  end

  defp from_freshness(
         %AssetFreshnessState{} = freshness,
         %TargetStatus{} = existing,
         target_id,
         target_ref_text
       ) do
    freshness_status = freshness.latest_attempt_status || freshness.status
    latest_run_id = freshness.latest_attempt_run_id || freshness.latest_success_run_id

    latest_run_at =
      freshness.latest_attempt_at || freshness.latest_success_at || freshness.updated_at

    attrs = %{
      existing
      | target_id: target_id,
        target_ref_text: target_ref_text,
        status: TargetStatus.status_from_freshness(freshness_status),
        latest_run_id: latest_run_id || existing.latest_run_id,
        latest_run_status: freshness_status || existing.latest_run_status,
        latest_run_at: latest_run_at || existing.latest_run_at,
        latest_success_run_id: freshness.latest_success_run_id || existing.latest_success_run_id,
        latest_success_at: freshness.latest_success_at || existing.latest_success_at,
        latest_failure_run_id: freshness_failure_run_id(freshness, existing),
        latest_failure_at: freshness_failure_at(freshness, existing),
        in_flight_run_id: freshness_in_flight_run_id(freshness, existing),
        freshness_status: freshness.status,
        freshness_key: freshness.freshness_key,
        updated_at: max_datetime(freshness.updated_at, existing.updated_at),
        updated_seq: existing.updated_seq || 0
    }

    {:ok, status} = TargetStatus.new(Map.from_struct(attrs))
    status
  end

  defp latest_evidence?(%DateTime{} = at, seq, %TargetStatus{} = existing) do
    case existing.latest_run_at do
      nil ->
        true

      %DateTime{} = existing_at ->
        case DateTime.compare(at, existing_at) do
          :gt -> true
          :eq -> (seq || 0) >= (existing.updated_seq || 0)
          :lt -> false
        end
    end
  end

  defp success_run_id(%RunState{status: :ok, id: id}, _existing), do: id
  defp success_run_id(_run, existing), do: existing.latest_success_run_id

  defp success_run_at(%RunState{status: :ok}, _existing, run_at), do: run_at
  defp success_run_at(_run, existing, _run_at), do: existing.latest_success_at

  defp failure_run_id(%RunState{status: status, id: id}, _existing)
       when status in [:partial, :error, :cancelled, :timed_out],
       do: id

  defp failure_run_id(_run, existing), do: existing.latest_failure_run_id

  defp failure_run_at(%RunState{status: status}, _existing, run_at)
       when status in [:partial, :error, :cancelled, :timed_out],
       do: run_at

  defp failure_run_at(_run, existing, _run_at), do: existing.latest_failure_at

  defp in_flight_run_id(%RunState{status: status, id: id}, _existing)
       when status in [:pending, :running],
       do: id

  defp in_flight_run_id(%RunState{id: id}, %TargetStatus{in_flight_run_id: id}), do: nil
  defp in_flight_run_id(_run, existing), do: existing.in_flight_run_id

  defp freshness_failure_run_id(%AssetFreshnessState{} = freshness, _existing)
       when freshness.status in [:error, :cancelled, :timed_out, :blocked],
       do: freshness.latest_attempt_run_id

  defp freshness_failure_run_id(_freshness, existing), do: existing.latest_failure_run_id

  defp freshness_failure_at(%AssetFreshnessState{} = freshness, _existing)
       when freshness.status in [:error, :cancelled, :timed_out, :blocked],
       do: freshness.latest_attempt_at || freshness.updated_at

  defp freshness_failure_at(_freshness, existing), do: existing.latest_failure_at

  defp freshness_in_flight_run_id(%AssetFreshnessState{status: :running} = freshness, _existing),
    do: freshness.latest_attempt_run_id

  defp freshness_in_flight_run_id(%AssetFreshnessState{} = freshness, %TargetStatus{} = existing) do
    if existing.in_flight_run_id == freshness.latest_attempt_run_id,
      do: nil,
      else: existing.in_flight_run_id
  end

  defp asset_targets(%RunState{} = run) do
    run_refs(run)
    |> Enum.map(fn ref ->
      {:asset, TargetStatus.target_id_for_asset(ref), TargetStatus.ref_text(ref)}
    end)
  end

  defp pipeline_targets(%RunState{} = run) do
    case pipeline_submit_ref(run) do
      nil ->
        []

      module when is_atom(module) ->
        [{:pipeline, TargetStatus.target_id_for_pipeline(module), TargetStatus.ref_text(module)}]
    end
  end

  defp run_refs(%RunState{} = run) do
    refs = [run.asset_ref | List.wrap(run.target_refs)] ++ result_refs(run)

    refs
    |> Enum.filter(&match?({_module, _name}, &1))
    |> Enum.uniq()
  end

  defp result_refs(%RunState{} = run) do
    run
    |> Map.get(:result, %{})
    |> result_field(:asset_results, [])
    |> List.wrap()
    |> Enum.map(&result_field(&1, :ref, nil))
  end

  defp run_references_asset?(%RunState{} = run, ref), do: ref in run_refs(run)

  defp latest_freshness_for_ref(freshness_states, {module, name}) do
    Enum.find(freshness_states, fn %AssetFreshnessState{} = state ->
      state.asset_ref_module == module and state.asset_ref_name == name and
        state.freshness_key == @latest_freshness_key
    end)
  end

  defp pipeline_selected_assets(%Index{} = index, %Pipeline{} = pipeline) do
    case PipelineResolver.resolve(index, pipeline, trigger: %{kind: :target_status_rebuild}) do
      {:ok, resolution} -> resolution.target_refs
      {:error, _reason} -> raw_pipeline_selector_refs(index, pipeline)
    end
  end

  defp raw_pipeline_selector_refs(%Index{} = index, %Pipeline{} = pipeline) do
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

  defp raw_pipeline_selector_ref(_selector), do: nil

  defp pipeline_matches_run?(%RunState{} = run, %Pipeline{} = pipeline, _selected_assets) do
    case pipeline_submit_ref(run) do
      nil -> false
      submit_ref -> same_pipeline_ref?(submit_ref, pipeline.module)
    end
  end

  defp pipeline_submit_ref(%RunState{} = run) do
    case metadata_value(run, :pipeline_submit_ref) do
      nil -> nil
      value when is_atom(value) -> value
      value when is_binary(value) -> existing_atom(value)
      _other -> nil
    end
  end

  defp same_pipeline_ref?(module, module) when is_atom(module), do: true

  defp same_pipeline_ref?(value, module) when is_atom(module),
    do: to_string(value) == Atom.to_string(module)

  defp metadata_value(%RunState{} = run, key) do
    metadata = run.metadata || %{}
    Map.get(metadata, key, Map.get(metadata, Atom.to_string(key)))
  end

  defp result_field(map, key, default) when is_map(map),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp result_field(_value, _key, default), do: default

  defp existing_atom(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> nil
  end

  defp run_time(%RunState{} = run) do
    run.updated_at || run.inserted_at || DateTime.from_unix!(0)
  end

  defp run_duration_ms(%RunState{} = run) do
    inserted_at = run.inserted_at
    updated_at = run.updated_at

    cond do
      run.status in [:ok, :partial, :error, :cancelled, :timed_out] and
        match?(%DateTime{}, inserted_at) and match?(%DateTime{}, updated_at) ->
        max(DateTime.diff(updated_at, inserted_at, :millisecond), 0)

      true ->
        nil
    end
  end

  defp max_datetime(nil, %DateTime{} = right), do: right
  defp max_datetime(%DateTime{} = left, nil), do: left
  defp max_datetime(nil, nil), do: DateTime.utc_now()

  defp max_datetime(%DateTime{} = left, %DateTime{} = right) do
    if DateTime.compare(left, right) == :lt, do: right, else: left
  end

  defp scan_freshness_states(manifest_version_id) do
    scan_freshness_states(manifest_version_id, nil, [])
  end

  defp scan_freshness_states(manifest_version_id, cursor, acc) do
    scan_opts = [limit: Page.max_limit()]
    scan_opts = if is_nil(cursor), do: scan_opts, else: Keyword.put(scan_opts, :after, cursor)

    case Storage.scan_asset_freshness_states(
           [manifest_version_id: manifest_version_id],
           scan_opts
         ) do
      {:ok, %CursorPage{items: items, next_cursor: nil}} ->
        {:ok, Enum.reverse(items, acc)}

      {:ok, %CursorPage{items: items, next_cursor: next_cursor}} ->
        scan_freshness_states(manifest_version_id, next_cursor, Enum.reverse(items, acc))

      {:error, :asset_freshness_state_not_supported} ->
        {:ok, []}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
