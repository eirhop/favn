defmodule FavnOrchestrator.Operator.Catalogue do
  @moduledoc """
  Builds manifest-pinned operator catalogue and target detail read models.

  This module owns catalogue composition below the public `FavnOrchestrator`
  facade. It reads orchestrator state directly and returns backend DTOs; UI code
  must continue to call the facade rather than this implementation module.
  """

  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias Favn.Window.Key, as: WindowKey
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Operator.Catalogue.AssetFreshness
  alias FavnOrchestrator.Operator.Catalogue.RunHistory
  alias FavnOrchestrator.Operator.Catalogue.Status
  alias FavnOrchestrator.Operator.Catalogue.Targets
  alias FavnOrchestrator.Operator.Catalogue.Timeline
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries.GetAssetDetailState
  alias FavnOrchestrator.Persistence.Queries.GetTargetStatuses
  alias FavnOrchestrator.Persistence.Queries.PageTargetRuns
  alias FavnOrchestrator.Persistence.Results.TargetStatus, as: PersistenceTargetStatus
  alias FavnOrchestrator.Persistence.WorkspaceContext

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
          required(:assurance) => map() | nil,
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

  @doc "Returns customer-visible asset catalogue entries in one workspace deployment."
  @spec active_asset_catalogue(WorkspaceContext.t()) ::
          {:ok, [asset_catalogue_entry()]} | {:error, term()}
  def active_asset_catalogue(%WorkspaceContext{} = context) do
    with {:ok, {runtime, grants}} <-
           ManifestStore.get_active_deployment(context, customer_visible_only: true),
         {:ok, version} <- ManifestStore.get_manifest(context, runtime.manifest_version_id),
         granted <- granted_ids(grants, :asset),
         targets <-
           version.manifest.assets
           |> List.wrap()
           |> Enum.map(&Targets.asset/1)
           |> Enum.filter(&MapSet.member?(granted, &1.target_id)),
         {:ok, statuses} <- target_statuses(context, runtime, :asset, targets) do
      {:ok, catalogue_entries(targets, statuses)}
    end
  end

  @doc "Returns customer-visible pipeline catalogue entries in one workspace deployment."
  @spec active_pipeline_catalogue(WorkspaceContext.t()) ::
          {:ok, [pipeline_catalogue_entry()]} | {:error, term()}
  def active_pipeline_catalogue(%WorkspaceContext{} = context) do
    with {:ok, {runtime, grants}} <-
           ManifestStore.get_active_deployment(context, customer_visible_only: true),
         {:ok, version} <- ManifestStore.get_manifest(context, runtime.manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         granted <- granted_ids(grants, :pipeline),
         targets <-
           version.manifest.pipelines
           |> List.wrap()
           |> Enum.map(&Targets.pipeline(index, &1))
           |> Enum.filter(&MapSet.member?(granted, &1.target_id)),
         {:ok, statuses} <- target_statuses(context, runtime, :pipeline, targets) do
      {:ok, catalogue_entries(targets, statuses)}
    end
  end

  @doc "Returns one customer-visible pipeline detail in a workspace deployment."
  @spec active_pipeline_detail(WorkspaceContext.t(), String.t()) ::
          {:ok, pipeline_detail()} | {:error, term()}
  def active_pipeline_detail(%WorkspaceContext{} = context, target_id)
      when is_binary(target_id) do
    with {:ok, {runtime, grants}} <-
           ManifestStore.get_active_deployment(context, customer_visible_only: true),
         true <- MapSet.member?(granted_ids(grants, :pipeline), target_id),
         {:ok, version} <- ManifestStore.get_manifest(context, runtime.manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         {:ok, pipeline} <- pipeline_for_target(version, target_id),
         {:ok, status} <- target_status(context, runtime, :pipeline, target_id),
         {:ok, page} <- target_runs(context, runtime, :pipeline, target_id),
         target <- Targets.pipeline(index, pipeline),
         detail <-
           target
           |> Map.put(:manifest_version_id, version.manifest_version_id)
           |> Status.put(status || unknown_status(context, runtime, :pipeline, target_id))
           |> Map.put(:runs, Enum.map(page.items, &RunHistory.entry(&1.run))) do
      {:ok, detail}
    else
      false -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  end

  @doc "Returns one customer-visible asset detail in a workspace deployment."
  @spec active_asset_detail(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, asset_detail()} | {:error, term()}
  def active_asset_detail(%WorkspaceContext{} = context, target_id, opts)
      when is_binary(target_id) and is_list(opts) do
    with {:ok, opts} <- normalize_asset_detail_opts(opts),
         {:ok, {runtime, grants}} <-
           ManifestStore.get_active_deployment(context, customer_visible_only: true),
         true <- MapSet.member?(granted_ids(grants, :asset), target_id),
         {:ok, version} <- ManifestStore.get_manifest(context, runtime.manifest_version_id),
         {:ok, asset} <- asset_for_target(version, target_id),
         {:ok, status} <- target_status(context, runtime, :asset, target_id),
         {:ok, page} <- target_runs(context, runtime, :asset, target_id),
         {:ok, projection_state} <- asset_projection_state(context, runtime, target_id),
         {:ok, freshness_states} <-
           catalogue_freshness_states(projection_state.freshness_states, asset),
         {:ok, window_states} <-
           catalogue_window_states(projection_state.window_states, asset, version) do
      runs = Enum.map(page.items, & &1.run)

      {:ok,
       asset_detail_entry(
         version,
         asset,
         target_id,
         status || unknown_status(context, runtime, :asset, target_id),
         freshness_states,
         window_states,
         runs,
         opts
       )}
    else
      false -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_asset_detail_opts(opts) do
    with true <- Keyword.keyword?(opts),
         [] <- Keyword.keys(opts) -- [:now, :today],
         :ok <- validate_optional_datetime(Keyword.get(opts, :now), :now),
         :ok <- validate_optional_date(Keyword.get(opts, :today), :today) do
      {:ok, opts}
    else
      false ->
        {:error, :invalid_asset_detail_options}

      unsupported when is_list(unsupported) ->
        {:error, {:unsupported_asset_detail_options, Enum.uniq(unsupported)}}

      {:error, _reason} = error ->
        error
    end
  end

  defp validate_optional_datetime(nil, _field), do: :ok
  defp validate_optional_datetime(%DateTime{}, _field), do: :ok

  defp validate_optional_datetime(value, field),
    do: {:error, {:invalid_asset_detail_option, field, value}}

  defp validate_optional_date(nil, _field), do: :ok
  defp validate_optional_date(%Date{}, _field), do: :ok

  defp validate_optional_date(value, field),
    do: {:error, {:invalid_asset_detail_option, field, value}}

  defp target_statuses(context, runtime, target_kind, targets) do
    target_ids = Enum.map(targets, & &1.target_id)

    with {:ok, statuses} <-
           Persistence.stores().operator_reads.get_target_statuses(%GetTargetStatuses{
             workspace_context: context,
             manifest_version_id: runtime.manifest_version_id,
             target_kind: target_kind,
             target_ids: target_ids
           }) do
      indexed = Map.new(statuses, &{&1.target_id, &1})

      {:ok,
       Map.new(targets, fn target ->
         {target.target_id,
          Map.get(indexed, target.target_id) ||
            unknown_status(context, runtime, target_kind, target.target_id)}
       end)}
    end
  end

  defp target_status(context, runtime, target_kind, target_id) do
    with {:ok, statuses} <-
           Persistence.stores().operator_reads.get_target_statuses(%GetTargetStatuses{
             workspace_context: context,
             manifest_version_id: runtime.manifest_version_id,
             target_kind: target_kind,
             target_ids: [target_id]
           }) do
      {:ok, List.first(statuses)}
    end
  end

  defp unknown_status(context, runtime, target_kind, target_id) do
    %PersistenceTargetStatus{
      workspace_id: context.workspace_id,
      deployment_id: runtime.deployment_id,
      target_kind: target_kind,
      target_id: target_id,
      status: :unknown,
      run_id: nil,
      event_id: nil,
      source_publication_id: 0,
      updated_at: runtime.activated_at || DateTime.utc_now()
    }
  end

  defp target_runs(context, runtime, target_kind, target_id) do
    Persistence.stores().operator_reads.page_target_runs(%PageTargetRuns{
      workspace_context: context,
      deployment_id: runtime.deployment_id,
      target_kind: target_kind,
      target_id: target_id,
      limit: 50
    })
  end

  defp asset_projection_state(context, runtime, target_id) do
    Persistence.stores().operator_reads.get_asset_detail_state(%GetAssetDetailState{
      workspace_context: context,
      deployment_id: runtime.deployment_id,
      manifest_version_id: runtime.manifest_version_id,
      target_id: target_id,
      limit: 200
    })
  end

  defp catalogue_freshness_states(states, asset) do
    map_validated(states, fn state ->
      payload = state.payload || %{}
      {module, name} = asset.ref

      AssetFreshnessState.new(%{
        asset_ref_module: module,
        asset_ref_name: name,
        freshness_key: state.freshness_key,
        status: catalogue_freshness_status(state.status),
        freshness_version: field(payload, :freshness_version),
        latest_success_run_id: field(payload, :run_id),
        latest_success_node_key: nil,
        latest_success_at: state.updated_at,
        latest_attempt_run_id: field(payload, :run_id),
        latest_attempt_status: catalogue_freshness_status(state.status),
        latest_attempt_at: state.updated_at,
        manifest_version_id: field(payload, :manifest_version_id),
        manifest_content_hash: field(payload, :manifest_content_hash),
        input_versions: [],
        metadata: %{"input_fingerprint" => field(payload, :input_fingerprint)},
        updated_at: state.updated_at
      })
    end)
  end

  defp catalogue_window_states(states, asset, version) do
    map_validated(states, fn state ->
      with "window:" <> encoded_key <- state.window_key,
           {:ok, key} <- WindowKey.decode(encoded_key) do
        {module, name} = asset.ref

        AssetWindowState.new(%{
          asset_ref_module: module,
          asset_ref_name: name,
          pipeline_module: nil,
          manifest_version_id: version.manifest_version_id,
          window_kind: key.kind,
          window_start_at: state.window_start,
          window_end_at: state.window_end,
          timezone: key.timezone,
          window_key: state.window_key,
          status: catalogue_window_status(state.status),
          latest_run_id: state.run_id,
          latest_parent_run_id: nil,
          latest_success_run_id: if(state.status == :succeeded, do: state.run_id),
          latest_error: nil,
          errors: [],
          rows_written: field(state.payload, :rows_written),
          metadata: state.payload || %{},
          updated_at: state.updated_at
        })
      else
        _other -> {:error, {:invalid_window_projection_key, state.window_key}}
      end
    end)
  end

  defp map_validated(values, mapper) do
    values
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
      case mapper.(value) do
        {:ok, mapped} -> {:cont, {:ok, [mapped | acc]}}
        {:error, reason} -> {:halt, {:error, {:invalid_persisted_projection, reason}}}
      end
    end)
    |> then(fn
      {:ok, mapped} -> {:ok, Enum.reverse(mapped)}
      error -> error
    end)
  end

  defp catalogue_freshness_status(:fresh), do: :ok
  defp catalogue_freshness_status(:stale), do: :error
  defp catalogue_freshness_status(:failed), do: :error
  defp catalogue_freshness_status(status), do: status

  defp catalogue_window_status(:succeeded), do: :ok
  defp catalogue_window_status(:failed), do: :error
  defp catalogue_window_status(status), do: status

  defp granted_ids(grants, target_kind) do
    grants
    |> Enum.filter(&(&1.target_kind == target_kind and &1.customer_visible))
    |> MapSet.new(& &1.target_id)
  end

  defp pipeline_for_target(%Version{} = version, target_id) do
    version.manifest.pipelines
    |> List.wrap()
    |> Enum.find(&(Targets.pipeline(&1).target_id == target_id))
    |> case do
      nil -> {:error, :not_found}
      pipeline -> {:ok, pipeline}
    end
  end

  defp asset_for_target(%Version{} = version, target_id) do
    version.manifest.assets
    |> List.wrap()
    |> Enum.find(&(Targets.asset(&1).target_id == target_id))
    |> case do
      nil -> {:error, :not_found}
      asset -> {:ok, asset}
    end
  end

  defp catalogue_entries(targets, statuses) do
    targets
    |> Enum.map(fn target ->
      status = Map.fetch!(statuses, target.target_id)
      Status.put(target, status)
    end)
    |> Enum.sort_by(& &1.label)
  end

  defp asset_detail_entry(
         %Version{} = version,
         asset,
         _target_id,
         status,
         freshness_states,
         asset_window_states,
         runs,
         opts
       ) do
    target = Targets.asset(asset)
    ref_string = Targets.ref_string(asset.ref)
    latest_freshness = AssetFreshness.latest_for_ref(freshness_states, ref_string)
    latest_run = latest_run_for_ref(runs, ref_string)
    runs_by_id = Map.new(runs, &{&1.id, &1})

    timeline =
      Timeline.build(
        version,
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
    |> Status.put(status)
    |> Map.put(:freshness, AssetFreshness.detail(asset, version, freshness_states, opts))
    |> Map.put(:assurance, assurance_detail(asset, latest_run))
    |> Map.merge(timeline)
    |> Map.put(:can_run_asset?, true)
  end

  defp assurance_detail(%{sql_execution: nil}, _latest_run), do: nil

  defp assurance_detail(%{sql_execution: execution, ref: asset_ref}, latest_run) do
    contract = Map.get(execution, :contract)
    checks = List.wrap(Map.get(execution, :checks))

    if is_nil(contract) and checks == [] do
      nil
    else
      meta = latest_asset_meta(latest_run, asset_ref)
      results = meta |> field(:check_results, []) |> List.wrap()
      results_by_name = Map.new(results, &{to_string(field(&1, :name)), &1})

      %{
        contract: contract_detail(contract),
        checks:
          Enum.map(checks, &check_detail(&1, Map.get(results_by_name, Atom.to_string(&1.name)))),
        quality_status:
          normalize_enum(field(meta, :quality_status), [:passed, :warning, :failed]),
        write_outcome:
          normalize_enum(field(meta, :write_outcome), [
            :written,
            :no_op,
            :rolled_back,
            :not_started,
            :unknown
          ]),
        contract_validation: contract_validation_detail(field(meta, :contract_validation)),
        latest_run_id: latest_run && Map.get(latest_run, :id)
      }
    end
  end

  defp contract_detail(nil), do: nil

  defp contract_detail(contract) do
    %{
      grain:
        case contract.grain do
          nil -> nil
          grain -> %{by: grain.by, description: grain.description}
        end,
      columns: Enum.map(contract.columns, &contract_column_detail/1),
      unique_keys: Enum.map(contract.unique_keys, & &1.columns),
      row_count:
        case contract.row_count do
          nil ->
            nil

          row_count ->
            %{
              min: row_count.min,
              when: row_count.when,
              on_violation: row_count.on_violation
            }
        end
    }
  end

  defp contract_column_detail(column) do
    %{
      name: column.name,
      type: column.type,
      nullable?: column.nullable?,
      description: column.description,
      tags: column.tags,
      renamed_from: column.renamed_from,
      via: column.via,
      sources: Enum.map(column.sources, &lineage_detail/1)
    }
  end

  defp lineage_detail(%{kind: :asset} = lineage) do
    %{kind: :asset, asset_ref: lineage.asset_ref, column: lineage.column}
  end

  defp lineage_detail(%{kind: :external} = lineage) do
    %{kind: :external, dataset: lineage.dataset, column: lineage.column}
  end

  defp check_detail(check, latest_result) do
    %{
      name: check.name,
      origin: check.origin,
      claim_id: check.claim_id,
      phase: check.at,
      when: check.when,
      on_violation: check.on_violation,
      message: check.message,
      latest_result: check_result_detail(latest_result)
    }
  end

  defp check_result_detail(nil), do: nil

  defp check_result_detail(result) do
    %{
      outcome:
        normalize_enum(field(result, :outcome), [
          :passed,
          :warned,
          :failed,
          :materialization_skipped,
          :condition_skipped,
          :not_run,
          :errored
        ]),
      metrics: field(result, :metrics, %{}),
      duration_ms: field(result, :duration_ms),
      reason: field(result, :reason),
      message: field(result, :message)
    }
  end

  defp contract_validation_detail(nil), do: nil

  defp contract_validation_detail(validation) do
    %{
      status: normalize_enum(field(validation, :status), [:passed, :failed]),
      expected_columns: field(validation, :expected_columns, []),
      observed_columns: field(validation, :observed_columns, []),
      differences: field(validation, :differences, []),
      observed_column_count: field(validation, :observed_column_count),
      observed_truncated?: field(validation, :observed_truncated?, false)
    }
  end

  defp latest_asset_meta(nil, _asset_ref), do: %{}

  defp latest_asset_meta(latest_run, asset_ref) do
    result =
      case Map.get(latest_run, :asset_results) do
        results when is_map(results) ->
          Map.get(results, asset_ref)

        _other ->
          latest_run
          |> field(:result, %{})
          |> field(:asset_results, [])
          |> find_asset_result(asset_ref)
      end

    field(result, :meta, %{})
  end

  defp find_asset_result(results, asset_ref) when is_map(results), do: Map.get(results, asset_ref)

  defp find_asset_result(results, asset_ref) when is_list(results),
    do: Enum.find(results, &(field(&1, :ref) == asset_ref))

  defp find_asset_result(_results, _asset_ref), do: nil

  defp field(value, key, default \\ nil)
  defp field(nil, _key, default), do: default

  defp field(value, key, default) when is_map(value) do
    Map.get(value, key, Map.get(value, Atom.to_string(key), default))
  end

  defp field(_value, _key, default), do: default

  defp normalize_enum(value, allowed) when is_atom(value),
    do: if(value in allowed, do: value, else: value)

  defp normalize_enum(value, allowed) when is_binary(value) do
    Enum.find(allowed, value, &(Atom.to_string(&1) == value))
  end

  defp normalize_enum(value, _allowed), do: value

  defp latest_run_for_ref(runs, ref_string) do
    runs
    |> Enum.flat_map(&RunHistory.ref_entries/1)
    |> Enum.filter(fn {run_ref_string, _run} -> run_ref_string == ref_string end)
    |> Enum.map(fn {_run_ref_string, run} -> run end)
    |> RunHistory.latest()
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
end
