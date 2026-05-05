defmodule FavnOrchestrator.API.DTO do
  @moduledoc false

  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Run.AssetResult
  alias Favn.Window.Policy
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.Storage.JsonSafe

  @spec normalize(term()) :: term()
  def normalize(value), do: JsonSafe.data(value)

  @spec actor(map()) :: map()
  def actor(actor) when is_map(actor) do
    %{
      id: actor.id,
      username: actor.username,
      display_name: actor.display_name,
      roles: Enum.map(actor.roles, &Atom.to_string/1),
      status: Atom.to_string(actor.status),
      inserted_at: datetime(Map.get(actor, :inserted_at)),
      updated_at: datetime(Map.get(actor, :updated_at))
    }
  end

  @spec session(map()) :: map()
  def session(session) when is_map(session) do
    %{
      id: session.id,
      actor_id: session.actor_id,
      provider: session.provider,
      issued_at: datetime(session.issued_at),
      expires_at: datetime(session.expires_at),
      revoked_at: datetime(session.revoked_at)
    }
  end

  @spec schedule(map()) :: map()
  def schedule(entry) when is_map(entry) do
    %{
      id: FavnOrchestrator.schedule_entry_id(entry),
      pipeline_module: module_name(entry.pipeline_module),
      schedule_id: atom_name(entry.schedule_id),
      cron: entry.cron,
      timezone: entry.timezone,
      overlap: atom_name(entry.overlap),
      missed: atom_name(entry.missed),
      active: entry.active,
      window: window_policy(entry.window),
      schedule_fingerprint: entry.schedule_fingerprint,
      manifest_version_id: entry.manifest_version_id,
      manifest_content_hash: entry.manifest_content_hash,
      last_evaluated_at: datetime(entry.last_evaluated_at),
      last_due_at: datetime(entry.last_due_at),
      last_submitted_due_at: datetime(entry.last_submitted_due_at),
      in_flight_run_id: entry.in_flight_run_id,
      queued_due_at: datetime(entry.queued_due_at),
      updated_at: datetime(entry.updated_at)
    }
  end

  @spec manifest_targets(map()) :: map()
  def manifest_targets(targets) when is_map(targets) do
    %{
      manifest_version_id: field(targets, :manifest_version_id),
      assets: Enum.map(List.wrap(field(targets, :assets, [])), &manifest_asset_target/1),
      pipelines: Enum.map(List.wrap(field(targets, :pipelines, [])), &manifest_pipeline_target/1)
    }
  end

  @spec manifest_asset_target(map()) :: map()
  def manifest_asset_target(target) when is_map(target) do
    asset_ref = ref_to_string(field(target, :asset_ref))
    target_id = field(target, :target_id)

    %{
      target_id: target_id,
      label: asset_ref || target_id,
      asset_ref: asset_ref,
      type: atom_name(field(target, :type)),
      relation: normalize(field(target, :relation)),
      metadata: normalize(field(target, :metadata, %{})),
      runtime_config: normalize(field(target, :runtime_config, %{})),
      depends_on: Enum.map(List.wrap(field(target, :depends_on, [])), &ref_to_string/1),
      materialization: normalize(field(target, :materialization)),
      window: normalize(field(target, :window))
    }
  end

  @spec manifest_pipeline_target(map()) :: map()
  def manifest_pipeline_target(target) when is_map(target) do
    target_id = field(target, :target_id)

    %{
      target_id: target_id,
      label: target_id,
      window: normalize(field(target, :window))
    }
  end

  @spec run_summary(map()) :: map()
  def run_summary(run) when is_map(run) do
    %{
      id: run.id,
      status: atom_name(run.status),
      submit_kind: atom_name(run.submit_kind),
      manifest_version_id: run.manifest_version_id,
      event_seq: run.event_seq,
      started_at: datetime(run.started_at),
      finished_at: datetime(run.finished_at),
      target_refs: Enum.map(List.wrap(run.target_refs), &ref_to_string/1),
      asset_results: asset_results(run.asset_results),
      error: error_payload(run.error)
    }
  end

  @spec run_detail(map()) :: map()
  def run_detail(run) when is_map(run) do
    %{
      id: run.id,
      status: atom_name(run.status),
      submit_kind: atom_name(run.submit_kind),
      manifest_version_id: run.manifest_version_id,
      manifest_content_hash: run.manifest_content_hash,
      event_seq: run.event_seq,
      started_at: datetime(run.started_at),
      finished_at: datetime(run.finished_at),
      timeout_ms: run.timeout_ms,
      retry_backoff_ms: run.retry_backoff_ms,
      rerun_of_run_id: run.rerun_of_run_id,
      parent_run_id: run.parent_run_id,
      root_run_id: run.root_run_id,
      target_refs: Enum.map(List.wrap(run.target_refs), &ref_to_string/1),
      params: normalize(run.params),
      trigger: normalize(run.trigger),
      metadata: normalize(run.metadata),
      result: normalize(run.result),
      pipeline: normalize(run.pipeline),
      pipeline_context: normalize(run.pipeline_context),
      asset_results: asset_results(run.asset_results),
      node_results: node_results(run.node_results),
      error: error_payload(run.error)
    }
  end

  @spec asset_results(map() | term()) :: [map()]
  def asset_results(results) when is_map(results) do
    results
    |> Map.values()
    |> Enum.map(&asset_result/1)
    |> Enum.sort_by(&asset_result_sort_key/1)
  end

  def asset_results(_results), do: []

  @spec asset_result(term()) :: map()
  def asset_result(%AssetResult{} = result) do
    %{
      asset_ref: ref_to_string(result.ref),
      stage: result.stage,
      status: atom_name(result.status),
      started_at: datetime(result.started_at),
      finished_at: datetime(result.finished_at),
      duration_ms: result.duration_ms,
      meta: normalize(result.meta),
      error: error_payload(result.error),
      attempt_count: result.attempt_count,
      max_attempts: result.max_attempts,
      attempts: normalize(result.attempts),
      next_retry_at: datetime(result.next_retry_at)
    }
  end

  def asset_result(result) when is_map(result) do
    result
    |> normalize()
    |> Map.put_new("asset_ref", ref_to_string(Map.get(result, :ref) || Map.get(result, "ref")))
  end

  def asset_result(result), do: %{asset_ref: nil, error: error_payload(result)}

  defp asset_result_sort_key(result) when is_map(result) do
    {
      Map.get(result, :stage) || Map.get(result, "stage") || 0,
      Map.get(result, :asset_ref) || Map.get(result, "asset_ref") || ""
    }
  end

  @spec node_results(map() | term()) :: [map()]
  def node_results(results) when is_map(results) do
    Enum.map(results, fn {node_key, result} ->
      %{
        node_key: normalize(node_key),
        result: asset_result(result)
      }
    end)
  end

  def node_results(_results), do: []

  @spec run_event(RunEvent.t()) :: map()
  def run_event(%RunEvent{} = event) do
    %{
      schema_version: event.schema_version,
      run_id: event.run_id,
      sequence: event.sequence,
      event_type: event_name(event.event_type),
      entity: Atom.to_string(event.entity),
      occurred_at: datetime(event.occurred_at),
      status: event_status(event.status),
      manifest_version_id: event.manifest_version_id,
      manifest_content_hash: event.manifest_content_hash,
      asset_ref: ref_to_string(event.asset_ref),
      stage: event.stage,
      data: normalize(event.data)
    }
  end

  @spec inspection_result(RelationInspectionResult.t() | term()) ::
          map() | list() | String.t() | number() | boolean() | nil
  def inspection_result(%RelationInspectionResult{} = result) do
    %{
      asset_ref: ref_to_string(result.asset_ref),
      relation_ref: relation_ref(result.relation_ref),
      relation: sql_relation(result.relation),
      columns: Enum.map(List.wrap(result.columns), &sql_column/1),
      row_count: result.row_count,
      sample: normalize(result.sample),
      table_metadata: normalize(result.table_metadata),
      adapter: module_name(result.adapter),
      inspected_at: datetime(result.inspected_at),
      warnings: normalize(result.warnings),
      error: error_payload(result.error)
    }
  end

  def inspection_result(result), do: normalize(result)

  @spec backfill_window(BackfillWindow.t()) :: map()
  def backfill_window(%BackfillWindow{} = window) do
    %{
      backfill_run_id: window.backfill_run_id,
      child_run_id: window.child_run_id,
      pipeline_module: module_name(window.pipeline_module),
      manifest_version_id: window.manifest_version_id,
      coverage_baseline_id: window.coverage_baseline_id,
      window_kind: atom_name(window.window_kind),
      window_start_at: datetime(window.window_start_at),
      window_end_at: datetime(window.window_end_at),
      timezone: window.timezone,
      window_key: window.window_key,
      status: atom_name(window.status),
      attempt_count: window.attempt_count,
      latest_attempt_run_id: window.latest_attempt_run_id,
      last_success_run_id: window.last_success_run_id,
      last_error: error_payload(window.last_error),
      errors: Enum.map(window.errors, &error_payload/1),
      metadata: normalize(window.metadata),
      started_at: datetime(window.started_at),
      finished_at: datetime(window.finished_at),
      created_at: datetime(window.created_at),
      updated_at: datetime(window.updated_at)
    }
  end

  @spec coverage_baseline(CoverageBaseline.t()) :: map()
  def coverage_baseline(%CoverageBaseline{} = baseline) do
    %{
      baseline_id: baseline.baseline_id,
      pipeline_module: module_name(baseline.pipeline_module),
      source_key: baseline.source_key,
      segment_key_hash: baseline.segment_key_hash,
      segment_key_redacted: baseline.segment_key_redacted,
      window_kind: atom_name(baseline.window_kind),
      timezone: baseline.timezone,
      coverage_start_at: datetime(baseline.coverage_start_at),
      coverage_until: datetime(baseline.coverage_until),
      created_by_run_id: baseline.created_by_run_id,
      manifest_version_id: baseline.manifest_version_id,
      status: atom_name(baseline.status),
      errors: Enum.map(baseline.errors, &error_payload/1),
      metadata: normalize(baseline.metadata),
      created_at: datetime(baseline.created_at),
      updated_at: datetime(baseline.updated_at)
    }
  end

  @spec asset_window_state(AssetWindowState.t()) :: map()
  def asset_window_state(%AssetWindowState{} = state) do
    %{
      asset_ref_module: module_name(state.asset_ref_module),
      asset_ref_name: atom_name(state.asset_ref_name),
      pipeline_module: module_name(state.pipeline_module),
      manifest_version_id: state.manifest_version_id,
      window_kind: atom_name(state.window_kind),
      window_start_at: datetime(state.window_start_at),
      window_end_at: datetime(state.window_end_at),
      timezone: state.timezone,
      window_key: state.window_key,
      status: atom_name(state.status),
      latest_run_id: state.latest_run_id,
      latest_parent_run_id: state.latest_parent_run_id,
      latest_success_run_id: state.latest_success_run_id,
      latest_error: error_payload(state.latest_error),
      errors: Enum.map(state.errors, &error_payload/1),
      rows_written: state.rows_written,
      metadata: normalize(state.metadata),
      updated_at: datetime(state.updated_at)
    }
  end

  @spec audit(map()) :: map()
  def audit(entry) when is_map(entry) do
    entry
    |> normalize()
    |> Map.update("occurred_at", nil, &datetime/1)
  end

  @spec audit_entry(map()) :: map()
  def audit_entry(entry) when is_map(entry), do: audit(entry)

  @spec page(Page.t(term()), (term() -> term())) :: map()
  def page(%Page{} = page, mapper) when is_function(mapper, 1) do
    %{
      items: Enum.map(page.items, mapper),
      pagination: %{
        limit: page.limit,
        offset: page.offset,
        has_more: page.has_more?,
        next_offset: page.next_offset
      }
    }
  end

  defp relation_ref(nil), do: nil

  defp relation_ref(%Favn.RelationRef{} = ref) do
    %{
      connection: atom_name(ref.connection),
      catalog: ref.catalog,
      schema: ref.schema,
      name: ref.name
    }
  end

  defp sql_relation(nil), do: nil

  defp sql_relation(%{__struct__: _struct} = relation) do
    %{
      catalog: Map.get(relation, :catalog),
      schema: Map.get(relation, :schema),
      name: Map.get(relation, :name),
      type: atom_name(Map.get(relation, :type)),
      metadata: normalize(Map.get(relation, :metadata, %{}))
    }
  end

  defp sql_relation(relation), do: normalize(relation)

  defp sql_column(%{__struct__: _struct} = column) do
    %{
      name: Map.get(column, :name),
      position: Map.get(column, :position),
      data_type: Map.get(column, :data_type),
      nullable: Map.get(column, :nullable?),
      default: normalize(Map.get(column, :default)),
      comment: Map.get(column, :comment),
      metadata: normalize(Map.get(column, :metadata, %{}))
    }
  end

  defp sql_column(column), do: normalize(column)

  defp event_name(value) when is_atom(value), do: Atom.to_string(value)
  defp event_name(value) when is_binary(value), do: value
  defp event_name(_value), do: nil

  defp event_status(nil), do: nil
  defp event_status(value) when is_atom(value), do: Atom.to_string(value)
  defp event_status(value) when is_binary(value), do: value
  defp event_status(_value), do: nil

  defp ref_to_string(nil), do: nil

  defp ref_to_string({module, name}) when is_atom(module) and is_atom(name) do
    Atom.to_string(module) <> ":" <> Atom.to_string(name)
  end

  defp ref_to_string(value) when is_binary(value), do: value
  defp ref_to_string(_value), do: nil

  defp datetime(nil), do: nil
  defp datetime(%DateTime{} = dt), do: DateTime.to_iso8601(dt)
  defp datetime(value) when is_binary(value), do: value
  defp datetime(_value), do: nil

  defp atom_name(nil), do: nil
  defp atom_name(value) when is_atom(value), do: Atom.to_string(value)
  defp atom_name(value) when is_binary(value), do: value
  defp atom_name(_value), do: nil

  defp window_policy(nil), do: nil

  defp window_policy(%Policy{} = policy) do
    %{
      kind: atom_name(policy.kind),
      anchor: atom_name(policy.anchor),
      timezone: policy.timezone,
      allow_full_load: policy.allow_full_load
    }
  end

  defp module_name(nil), do: nil
  defp module_name(value) when is_atom(value), do: Atom.to_string(value)
  defp module_name(value) when is_binary(value), do: value
  defp module_name(_value), do: nil

  defp error_payload(nil), do: nil
  defp error_payload(value), do: JsonSafe.error(value)

  defp field(map, key, default \\ nil) when is_map(map) and is_atom(key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end
end
