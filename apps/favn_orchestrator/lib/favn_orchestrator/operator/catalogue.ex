defmodule FavnOrchestrator.Operator.Catalogue do
  @moduledoc """
  Builds manifest-pinned operator catalogue and target detail read models.

  This module owns catalogue composition below the public `FavnOrchestrator`
  facade. It reads orchestrator state directly and returns backend DTOs; UI code
  must continue to call the facade rather than this implementation module.
  """

  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias Favn.SQL.Contract.Param
  alias FavnOrchestrator.Freshness.Query, as: FreshnessQuery
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Operator.Catalogue.AssetFreshness
  alias FavnOrchestrator.Operator.Catalogue.PageReader
  alias FavnOrchestrator.Operator.Catalogue.RunHistory
  alias FavnOrchestrator.Operator.Catalogue.Status
  alias FavnOrchestrator.Operator.Catalogue.Targets
  alias FavnOrchestrator.Operator.Catalogue.Timeline
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.TargetStatus
  alias FavnOrchestrator.TargetStatus.Projector, as: TargetStatusProjector

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

  @doc "Lists stable operator-facing manifest summaries."
  @spec list_manifest_summaries() :: {:ok, [manifest_summary()]} | {:error, term()}
  def list_manifest_summaries do
    with {:ok, versions} <- ManifestStore.list_manifests() do
      {:ok,
       versions
       |> Enum.map(&manifest_summary/1)
       |> Enum.sort_by(& &1.manifest_version_id)}
    end
  end

  @doc "Returns one stable operator-facing manifest summary."
  @spec get_manifest_summary(String.t()) :: {:ok, manifest_summary()} | {:error, term()}
  def get_manifest_summary(manifest_version_id) when is_binary(manifest_version_id) do
    with {:ok, version} <- ManifestStore.get_manifest(manifest_version_id) do
      {:ok, manifest_summary(version)}
    end
  end

  @doc "Returns manifest-scoped operator submit targets."
  @spec manifest_targets(String.t()) :: {:ok, manifest_targets()} | {:error, term()}
  def manifest_targets(manifest_version_id) when is_binary(manifest_version_id) do
    with {:ok, version} <- ManifestStore.get_manifest(manifest_version_id) do
      {:ok,
       %{
         manifest_version_id: manifest_version_id,
         assets: Targets.assets(version),
         pipelines: Targets.pipelines(version)
       }}
    end
  end

  @doc "Returns operator submit targets for the active manifest."
  @spec active_manifest_targets() :: {:ok, manifest_targets()} | {:error, term()}
  def active_manifest_targets do
    with {:ok, manifest_version_id} <- ManifestStore.get_active_manifest() do
      manifest_targets(manifest_version_id)
    end
  end

  @doc "Rebuilds current target-status projections for a manifest version."
  @spec rebuild_target_statuses(String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def rebuild_target_statuses(manifest_version_id) when is_binary(manifest_version_id) do
    with {:ok, version} <- ManifestStore.get_manifest(manifest_version_id) do
      TargetStatusProjector.rebuild_manifest(version)
    end
  end

  @doc "Returns asset catalogue entries for the active manifest."
  @spec active_asset_catalogue() :: {:ok, [asset_catalogue_entry()]} | {:error, term()}
  def active_asset_catalogue do
    with {:ok, manifest_version_id} <- ManifestStore.get_active_manifest(),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         targets <- Enum.map(List.wrap(version.manifest.assets), &Targets.asset/1),
         {:ok, statuses} <- target_statuses(manifest_version_id, :asset, targets) do
      {:ok, catalogue_entries(targets, statuses)}
    end
  end

  @doc "Returns pipeline catalogue entries for the active manifest."
  @spec active_pipeline_catalogue() :: {:ok, [pipeline_catalogue_entry()]} | {:error, term()}
  def active_pipeline_catalogue do
    with {:ok, manifest_version_id} <- ManifestStore.get_active_manifest(),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         targets <-
           Enum.map(List.wrap(version.manifest.pipelines), &Targets.pipeline(index, &1)),
         {:ok, statuses} <- target_statuses(manifest_version_id, :pipeline, targets) do
      {:ok, catalogue_entries(targets, statuses)}
    end
  end

  @doc "Returns one active pipeline's operator detail read model."
  @spec active_pipeline_detail(String.t()) :: {:ok, pipeline_detail()} | {:error, term()}
  def active_pipeline_detail(target_id) when is_binary(target_id) do
    with {:ok, manifest_version_id} <- ManifestStore.get_active_manifest(),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         {:ok, pipeline} <- pipeline_for_target(version, target_id),
         {:ok, status} <- target_status(manifest_version_id, :pipeline, target_id),
         {:ok, recent_runs} <-
           Storage.list_target_runs(manifest_version_id, :pipeline, pipeline.module, limit: 50) do
      {:ok, pipeline_detail_entry(version, index, pipeline, target_id, status, recent_runs)}
    end
  end

  @doc "Returns one active asset's operator detail read model."
  @spec active_asset_detail(String.t(), keyword()) :: {:ok, asset_detail()} | {:error, term()}
  def active_asset_detail(target_id, opts \\ [])
      when is_binary(target_id) and is_list(opts) do
    with {:ok, opts} <- normalize_asset_detail_opts(opts),
         {:ok, manifest_version_id} <- ManifestStore.get_active_manifest(),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         {:ok, asset} <- asset_for_target(version, target_id),
         {:ok, freshness_states} <- detail_freshness_states(manifest_version_id),
         {:ok, asset_window_states} <-
           detail_asset_window_states(manifest_version_id, asset.ref),
         {:ok, recent_runs} <-
           Storage.list_target_runs(manifest_version_id, :asset, asset.ref, limit: 50),
         {:ok, status} <- target_status(manifest_version_id, :asset, target_id) do
      {:ok,
       asset_detail_entry(
         version,
         asset,
         target_id,
         status,
         freshness_states,
         asset_window_states,
         recent_runs,
         opts
       )}
    end
  end

  defp manifest_summary(%Version{} = version) do
    manifest = version.manifest

    %{
      manifest_version_id: version.manifest_version_id,
      content_hash: version.content_hash,
      asset_count: length(List.wrap(manifest.assets)),
      pipeline_count: length(List.wrap(manifest.pipelines)),
      schedule_count: length(List.wrap(manifest.schedules))
    }
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

  defp detail_freshness_states(manifest_version_id) do
    case PageReader.all(fn offset ->
           FreshnessQuery.list_asset_freshness(
             manifest_version_id: manifest_version_id,
             limit: Page.max_limit(),
             offset: offset
           )
         end) do
      {:error, :asset_freshness_state_not_supported} ->
        {:ok, []}

      result ->
        result
    end
  end

  defp detail_asset_window_states(manifest_version_id, {module, name}) do
    PageReader.all(fn offset ->
      Storage.list_asset_window_states(
        manifest_version_id: manifest_version_id,
        asset_ref_module: module,
        asset_ref_name: name,
        limit: Page.max_limit(),
        offset: offset
      )
    end)
  end

  defp catalogue_entries(targets, statuses) do
    targets
    |> Enum.map(fn target ->
      status = Map.fetch!(statuses, target.target_id)
      Status.put(target, status)
    end)
    |> Enum.sort_by(& &1.label)
  end

  defp pipeline_detail_entry(
         %Version{} = version,
         %Index{} = index,
         pipeline,
         target_id,
         status,
         runs
       ) do
    target = Targets.pipeline(index, pipeline)
    pipeline_runs = RunHistory.for_pipeline(pipeline, target, runs)

    status =
      status ||
        TargetStatus.unknown(version.manifest_version_id, :pipeline, target_id, target.label)

    target
    |> Map.put(:manifest_version_id, version.manifest_version_id)
    |> Status.put(status)
    |> Map.put(:runs, Enum.map(pipeline_runs, &RunHistory.entry/1))
  end

  defp asset_detail_entry(
         %Version{} = version,
         asset,
         target_id,
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

    status =
      status || TargetStatus.unknown(version.manifest_version_id, :asset, target_id, ref_string)

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

  defp assurance_detail(%{assurance: nil}, _latest_run), do: nil

  defp assurance_detail(%{assurance: assurance, ref: asset_ref}, latest_run) do
    contract = Map.get(assurance, :contract)
    checks = List.wrap(Map.get(assurance, :checks))

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
      columns:
        contract.columns
        |> Enum.with_index()
        |> Enum.map(fn {column, index} ->
          contract_column_detail(column, composition_origin(contract.compositions, index))
        end),
      compositions:
        Enum.map(contract.compositions, fn composition ->
          %{
            module: composition.module,
            start_index: composition.start_index,
            columns: composition.columns
          }
        end),
      unique_keys: Enum.map(contract.unique_keys, & &1.columns),
      row_count:
        case contract.row_count do
          nil ->
            nil

          row_count ->
            %{
              equals: row_count_equals_detail(row_count.equals),
              min: row_count.min,
              max: row_count.max,
              when: row_count.when,
              on_violation: row_count.on_violation
            }
        end
    }
  end

  defp contract_column_detail(column, origin) do
    %{
      name: column.name,
      type: column.type,
      nullable?: column.nullable?,
      description: column.description,
      tags: column.tags,
      renamed_from: column.renamed_from,
      via: column.via,
      sources: Enum.map(column.sources, &lineage_detail/1),
      origin: origin
    }
  end

  defp composition_origin(compositions, index) do
    case Enum.find(compositions, fn composition ->
           index >= composition.start_index and
             index < composition.start_index + length(composition.columns)
         end) do
      nil -> %{kind: :local}
      composition -> %{kind: :fragment, module: composition.module}
    end
  end

  defp row_count_equals_detail(nil), do: nil
  defp row_count_equals_detail(%Param{name: name}), do: %{source: :param, name: name}
  defp row_count_equals_detail(value), do: %{source: :literal, value: value}

  defp lineage_detail(%{kind: :asset} = lineage) do
    %{kind: :asset, asset_ref: lineage.asset_ref, column: lineage.column}
  end

  defp lineage_detail(%{kind: :external} = lineage) do
    %{kind: :external, dataset: lineage.dataset, column: lineage.column}
  end

  defp check_detail(check, latest_result) do
    %{
      name: field(check, :name),
      origin: field(check, :origin),
      claim_id: field(check, :claim_id),
      phase: field(check, :at),
      when: field(check, :when),
      on_violation: field(check, :on_violation),
      message: field(check, :message),
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
