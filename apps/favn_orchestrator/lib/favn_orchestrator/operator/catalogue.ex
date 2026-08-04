defmodule FavnOrchestrator.Operator.Catalogue do
  @moduledoc """
  Builds manifest-pinned operator catalogue and target detail read models.

  This module owns catalogue composition below the public `FavnOrchestrator`
  facade. It reads orchestrator state directly and returns backend DTOs; UI code
  must continue to call the facade rather than this implementation module.
  """

  alias Favn.Manifest.Version
  alias Favn.RuntimeInput.Pin
  alias Favn.Window.Key, as: WindowKey
  alias FavnOrchestrator.AssetRunContext
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Coverage
  alias FavnOrchestrator.Freshness.StateLoader
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Operator.Catalogue.Assurance
  alias FavnOrchestrator.Operator.Catalogue.AssetFreshness
  alias FavnOrchestrator.Operator.Catalogue.RunHistory
  alias FavnOrchestrator.Operator.Catalogue.Status
  alias FavnOrchestrator.Operator.Catalogue.Targets
  alias FavnOrchestrator.Operator.Catalogue.Timeline
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries.GetAssetWindowStates
  alias FavnOrchestrator.Persistence.Queries.GetTargetBindings
  alias FavnOrchestrator.Persistence.Queries.GetTargetStatuses
  alias FavnOrchestrator.Persistence.Error, as: PersistenceError
  alias FavnOrchestrator.Persistence.Queries.PageTargetRuns
  alias FavnOrchestrator.Persistence.Results.TargetStatus, as: PersistenceTargetStatus
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.TargetGenerations

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
          optional(:name) => String.t(),
          optional(:module_path) => [String.t()],
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
          optional(:name) => String.t(),
          optional(:module_path) => [String.t()],
          optional(:type) => String.t(),
          optional(:relation) => map() | nil,
          optional(:metadata) => map(),
          required(:status) => :healthy | :running | :failed | :unknown,
          required(:latest_run_id) => String.t() | nil,
          required(:latest_run_status) => atom() | nil,
          required(:latest_run_at) => DateTime.t() | nil,
          required(:coverage) => Favn.Coverage.Summary.t(),
          required(:compatibility) => map()
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
          required(:source) => :refresh_timeline | :freshness_timeline | :data_coverage_timeline,
          required(:kind) => :hour | :day | :month | :year,
          required(:value) => String.t(),
          required(:timezone) => String.t(),
          required(:label) => String.t(),
          required(:date) => Date.t(),
          required(:range) => String.t(),
          required(:status) =>
            :fresh | :covered | :running | :failed | :missing | :stale | :unknown,
          required(:latest_run_id) => String.t() | nil,
          required(:latest_run_status) => atom() | nil,
          required(:latest_run_at) => DateTime.t() | nil,
          required(:run_enabled?) => boolean(),
          required(:run_disabled_reason) => atom() | nil,
          required(:run_label) => String.t() | nil
        }

  @type asset_dependency :: %{
          required(:name) => String.t() | nil,
          required(:asset_ref) => String.t(),
          required(:target_id) => String.t() | nil
        }

  @type asset_run_window :: %{
          required(:kind) => :hour | :day | :month | :year,
          required(:value) => String.t(),
          required(:label) => String.t(),
          required(:range) => String.t()
        }

  @type asset_run_history_entry :: %{
          required(:id) => String.t(),
          required(:status) => atom(),
          required(:submit_kind) => atom() | nil,
          required(:started_at) => DateTime.t() | nil,
          required(:finished_at) => DateTime.t() | nil,
          required(:duration_ms) => non_neg_integer() | nil,
          required(:window) => asset_run_window() | nil
        }

  @type asset_detail :: %{
          required(:target_id) => String.t(),
          required(:manifest_version_id) => String.t(),
          required(:label) => String.t(),
          required(:name) => String.t(),
          required(:asset_ref) => String.t() | nil,
          required(:canonical_asset_ref) => Favn.Ref.t(),
          required(:module_path) => [String.t()],
          required(:relation) => map() | nil,
          required(:type) => String.t() | nil,
          required(:description) => String.t() | nil,
          required(:metadata) => map(),
          required(:upstream) => [asset_dependency()],
          required(:downstream) => [asset_dependency()],
          required(:status) => :healthy | :running | :failed | :unknown,
          required(:latest_run_id) => String.t() | nil,
          required(:latest_run_status) => atom() | nil,
          required(:latest_run_at) => DateTime.t() | nil,
          required(:window) => map() | nil,
          required(:refresh_timeline) => [asset_timeline_window()],
          required(:freshness_timeline) => [asset_timeline_window()] | nil,
          required(:data_coverage_timeline) => [asset_timeline_window()] | nil,
          required(:has_freshness_timeline?) => boolean(),
          required(:has_data_windows?) => boolean(),
          required(:can_run_asset?) => boolean(),
          required(:run_contexts) => [map()],
          required(:selected_run_context) => map() | nil,
          required(:run_context_status) => AssetRunContext.status(),
          required(:freshness) => asset_freshness_detail(),
          required(:coverage) => Favn.Coverage.Summary.t(),
          required(:coverage_policy) => map() | nil,
          required(:coverage_gaps) => [map()],
          required(:coverage_pagination) => map(),
          required(:compatibility) => map(),
          required(:assurance) => map() | nil,
          required(:runs) => [asset_run_history_entry()],
          required(:timeline) => [asset_timeline_window()]
        }

  @type asset_run_result :: %{
          required(:status) => atom() | nil,
          required(:stage) => non_neg_integer() | nil,
          required(:started_at) => DateTime.t() | nil,
          required(:finished_at) => DateTime.t() | nil,
          required(:duration_ms) => non_neg_integer() | nil,
          required(:attempt_count) => non_neg_integer() | nil,
          required(:max_attempts) => non_neg_integer() | nil,
          required(:error) => term(),
          required(:meta) => map()
        }

  @type asset_run_detail :: %{
          required(:run_id) => String.t(),
          required(:target_id) => String.t(),
          required(:status) => atom(),
          required(:submit_kind) => atom() | nil,
          required(:trigger) => map(),
          required(:started_at) => DateTime.t() | nil,
          required(:finished_at) => DateTime.t() | nil,
          required(:duration_ms) => non_neg_integer() | nil,
          required(:window) => map() | nil,
          required(:error) => term(),
          required(:assurance) => map() | nil,
          required(:asset_result) => asset_run_result() | nil,
          required(:runtime_inputs) => [map()]
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
         {:ok, targets} <- deployment_target_descriptors(grants, :asset),
         {:ok, statuses} <- target_statuses(context, runtime, :asset, targets),
         {:ok, compatibilities} <- target_compatibilities(context, targets),
         {:ok, coverages} <-
           Coverage.summaries(context, Enum.map(targets, & &1.target_id)),
         :ok <- coverage_snapshot(coverages, runtime.manifest_version_id) do
      {:ok, catalogue_entries(targets, statuses, coverages, compatibilities)}
    end
  end

  @doc "Returns customer-visible pipeline catalogue entries in one workspace deployment."
  @spec active_pipeline_catalogue(WorkspaceContext.t()) ::
          {:ok, [pipeline_catalogue_entry()]} | {:error, term()}
  def active_pipeline_catalogue(%WorkspaceContext{} = context) do
    with {:ok, {runtime, grants}} <-
           ManifestStore.get_active_deployment(context, customer_visible_only: true),
         {:ok, targets} <- deployment_target_descriptors(grants, :pipeline),
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
         {:ok, target} <- deployment_target_descriptor(grants, :pipeline, target_id),
         {:ok, status} <- target_status(context, runtime, :pipeline, target_id),
         {:ok, page} <- target_runs(context, runtime, :pipeline, target_id),
         detail <-
           target
           |> Map.put(:manifest_version_id, runtime.manifest_version_id)
           |> Status.put(status || unknown_status(context, runtime, :pipeline, target_id))
           |> Map.put(
             :runs,
             Enum.map(page.items, &(Projector.project_run_summary(&1) |> RunHistory.entry()))
           ) do
      {:ok, detail}
    else
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
         {:ok, run_context_selection} <-
           AssetRunContext.select(version, asset, Keyword.get(opts, :run_context_id)),
         now <- Keyword.get(opts, :now) || DateTime.utc_now(),
         {:ok, coverage_page} <-
           Coverage.missing_windows(context, target_id, evaluated_at: now, limit: 100),
         :ok <- coverage_snapshot(coverage_page.summary, runtime.manifest_version_id),
         {:ok, compatibilities} <-
           target_compatibilities(context, [
             %{target_id: target_id, persisted?: not is_nil(asset.target_descriptor)}
           ]),
         freshness_opts <- freshness_opts(opts, run_context_selection),
         {:ok, freshness_plan} <- AssetFreshness.plan(asset, version, now, freshness_opts),
         {:ok, loaded_freshness} <-
           StateLoader.load(
             context,
             runtime.deployment_id,
             freshness_plan,
             assets_by_ref(version),
             now: now
           ),
         {:ok, status} <- target_status(context, runtime, :asset, target_id),
         {:ok, page} <- target_runs(context, runtime, :asset, target_id),
         {:ok, run_history} <- asset_run_history(context, page),
         {:ok, projected_window_states} <- asset_window_states(context, asset, target_id),
         {:ok, window_states} <-
           catalogue_window_states(projected_window_states, asset, version) do
      detail_opts =
        opts
        |> Keyword.put(:now, now)
        |> Keyword.put(:freshness_plan, freshness_plan)

      detail =
        asset_detail_entry(
          version,
          asset,
          target_id,
          status || unknown_status(context, runtime, :asset, target_id),
          loaded_freshness.states,
          window_states,
          run_history,
          detail_opts,
          run_context_selection
        )
        |> Map.put(:coverage, coverage_page.summary)
        |> Map.put(:coverage_policy, coverage_policy(asset.coverage))
        |> Map.put(:coverage_gaps, coverage_page.items)
        |> Map.put(:coverage_pagination, coverage_page.pagination)
        |> Map.put(:compatibility, Map.fetch!(compatibilities, target_id))

      {:ok, detail}
    else
      false -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Returns one asset's view of one of its own runs in a workspace deployment.

  Deliberately narrow. Selecting a run must not re-resolve the freshness plan, the
  coverage page, and the window timeline that `active_asset_detail/3` builds, so
  this reads the manifest asset, the run snapshot, and the run's pinned inputs and
  nothing else.
  """
  @spec active_asset_run_detail(WorkspaceContext.t(), String.t(), String.t()) ::
          {:ok, asset_run_detail()} | {:error, term()}
  def active_asset_run_detail(%WorkspaceContext{} = context, target_id, run_id)
      when is_binary(target_id) and is_binary(run_id) do
    with {:ok, {runtime, grants}} <-
           ManifestStore.get_active_deployment(context, customer_visible_only: true),
         true <- MapSet.member?(granted_ids(grants, :asset), target_id),
         {:ok, version} <- ManifestStore.get_manifest(context, runtime.manifest_version_id),
         {:ok, asset} <- asset_for_target(version, target_id),
         {:ok, run_state} <- Runs.get(context, run_id),
         run <- Projector.project_run(run_state),
         true <- run_covers_asset?(run, asset.ref),
         {:ok, pins} <- Runs.get_runtime_inputs(context, run_id) do
      {:ok, asset_run_detail_entry(asset, target_id, run, pins)}
    else
      false -> {:error, :not_found}
      {:error, %PersistenceError{kind: :not_found}} -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  end

  # A run id is a URL parameter, so a run belonging to another asset must read as
  # missing rather than render an unrelated result under this asset's name.
  defp run_covers_asset?(run, asset_ref) do
    Map.get(run, :asset_ref) == asset_ref or
      asset_ref in List.wrap(Map.get(run, :target_refs)) or
      Map.has_key?(Map.get(run, :asset_results) || %{}, asset_ref)
  end

  defp asset_run_detail_entry(asset, target_id, run, pins) do
    asset_result = Map.get(run.asset_results || %{}, asset.ref)

    %{
      run_id: run.id,
      target_id: target_id,
      status: run.status,
      submit_kind: run.submit_kind,
      trigger: run.trigger || %{},
      started_at: run.started_at,
      finished_at: run.finished_at,
      duration_ms: RunHistory.duration_ms(run),
      window: run_window(run),
      error: run.error,
      assurance: Assurance.detail(asset, run),
      asset_result: asset_result_detail(asset_result),
      runtime_inputs: runtime_inputs(pins, asset.ref)
    }
  end

  defp asset_result_detail(nil), do: nil

  defp asset_result_detail(result) do
    %{
      status: Map.get(result, :status),
      stage: Map.get(result, :stage),
      started_at: Map.get(result, :started_at),
      finished_at: Map.get(result, :finished_at),
      duration_ms: Map.get(result, :duration_ms),
      attempt_count: Map.get(result, :attempt_count),
      max_attempts: Map.get(result, :max_attempts),
      error: Map.get(result, :error),
      # The whole bag, not a chosen field: the operator UI already owns the priority
      # ordering and the disclosure for asset output metadata, and a key this release
      # does not know about is still evidence.
      meta: Map.get(result, :meta) || %{}
    }
  end

  defp run_window(run) do
    field(run.params, :window) || field(run.metadata, :selected_window) ||
      field(run.metadata, :window)
  end

  # `Pin.lineage/1` reports which payload a resolver selected, not the payload: a
  # pin's own params may be declared sensitive, so the identity and fingerprint are
  # the whole safe projection.
  defp runtime_inputs(pins, asset_ref) do
    pins
    |> Enum.filter(&match?({^asset_ref, _window}, &1.node_key))
    |> Enum.map(&Pin.lineage/1)
  end

  defp coverage_snapshot(%Favn.Coverage.Summary{} = summary, manifest_version_id) do
    if summary.manifest_version_id == manifest_version_id,
      do: :ok,
      else: {:error, :catalogue_snapshot_changed}
  end

  defp coverage_snapshot(coverages, manifest_version_id) when is_map(coverages) do
    if Enum.all?(coverages, fn {_target_id, summary} ->
         summary.manifest_version_id == manifest_version_id
       end),
       do: :ok,
       else: {:error, :catalogue_snapshot_changed}
  end

  defp normalize_asset_detail_opts(opts) do
    with true <- Keyword.keyword?(opts),
         [] <- Keyword.keys(opts) -- [:now, :today, :run_context_id],
         :ok <- validate_optional_datetime(Keyword.get(opts, :now), :now),
         :ok <- validate_optional_date(Keyword.get(opts, :today), :today),
         :ok <- validate_optional_run_context_id(Keyword.get(opts, :run_context_id)) do
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

  defp validate_optional_run_context_id(nil), do: :ok
  defp validate_optional_run_context_id(value) when is_binary(value) and value != "", do: :ok

  defp validate_optional_run_context_id(value),
    do: {:error, {:invalid_asset_detail_option, :run_context_id, value}}

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

  defp asset_window_states(context, asset, target_id) do
    with {:ok, generations} <- TargetGenerations.for_reads(context, %{asset.ref => asset}) do
      case Map.get(generations, asset.ref) do
        %{evidence_generation_id: generation_id} ->
          Persistence.stores().operator_reads.get_asset_window_states(%GetAssetWindowStates{
            workspace_context: context,
            evidence_generation_id: generation_id,
            target_id: target_id,
            limit: 200
          })

        nil ->
          {:ok, []}
      end
    end
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

  defp catalogue_window_status(:succeeded), do: :ok
  defp catalogue_window_status(:failed), do: :error
  defp catalogue_window_status(status), do: status

  defp granted_ids(grants, target_kind) do
    grants
    |> Enum.filter(&(&1.target_kind == target_kind and &1.customer_visible))
    |> MapSet.new(& &1.target_id)
  end

  defp deployment_target_descriptors(grants, target_kind) do
    grants
    |> Enum.filter(&(&1.target_kind == target_kind and &1.customer_visible))
    |> Enum.reduce_while({:ok, []}, fn grant, {:ok, acc} ->
      case restore_deployment_target(grant) do
        {:ok, target} -> {:cont, {:ok, [target | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> then(fn
      {:ok, targets} -> {:ok, Enum.reverse(targets)}
      error -> error
    end)
  end

  defp deployment_target_descriptor(grants, target_kind, target_id) do
    grants
    |> Enum.find(
      &(&1.target_kind == target_kind and &1.target_id == target_id and &1.customer_visible)
    )
    |> case do
      nil -> {:error, :not_found}
      grant -> restore_deployment_target(grant)
    end
  end

  defp restore_deployment_target(grant) do
    target =
      grant.descriptor
      |> Targets.restore_descriptor()
      |> Map.put(:target_id, grant.target_id)

    if is_binary(target[:label]) do
      {:ok, target}
    else
      {:error, :deployment_target_descriptor_missing}
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

  defp catalogue_entries(targets, statuses, coverages \\ %{}, compatibilities \\ %{}) do
    targets
    |> Enum.map(fn target ->
      status = Map.fetch!(statuses, target.target_id)

      target
      |> Status.put(status)
      |> maybe_put_coverage(coverages)
      |> maybe_put_compatibility(compatibilities)
    end)
    |> Enum.sort_by(& &1.label)
  end

  defp maybe_put_compatibility(target, compatibilities) do
    Map.put(
      target,
      :compatibility,
      Map.get(compatibilities, target.target_id, non_persisted_compatibility())
    )
  end

  defp target_compatibilities(context, targets) do
    target_ids = targets |> Enum.map(& &1.target_id) |> Enum.uniq()

    target_ids
    |> Enum.chunk_every(500)
    |> Enum.reduce_while({:ok, %{}}, fn chunk, {:ok, acc} ->
      case Persistence.stores().target_generations.get_bindings(%GetTargetBindings{
             workspace_context: context,
             target_ids: chunk
           }) do
        {:ok, bindings} ->
          next = Map.new(bindings, &{&1.target_id, compatibility_view(&1)})
          {:cont, {:ok, Map.merge(acc, next)}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> then(fn
      {:ok, compatibilities} ->
        {:ok,
         Map.new(targets, fn target ->
           fallback =
             if Map.get(target, :persisted?, false),
               do: missing_binding_compatibility(),
               else: non_persisted_compatibility()

           {target.target_id, Map.get(compatibilities, target.target_id, fallback)}
         end)}

      error ->
        error
    end)
  end

  defp compatibility_view(binding) do
    %{
      status: binding.compatibility_status,
      reason_code: binding.reason_code,
      diff: binding.compatibility_diff,
      active_generation_id: binding.active_generation_id,
      desired_descriptor_hash: binding.desired_descriptor_hash,
      physical_fingerprint: binding.active_physical_fingerprint,
      persisted?: true,
      blocks_writes?:
        binding.compatibility_status in [
          :rebuild_required,
          :unexpected_drift,
          :operator_decision
        ]
    }
  end

  defp non_persisted_compatibility do
    %{
      status: :ready,
      reason_code: "not_persisted",
      diff: %{},
      active_generation_id: nil,
      desired_descriptor_hash: nil,
      physical_fingerprint: nil,
      persisted?: false,
      blocks_writes?: false
    }
  end

  defp missing_binding_compatibility do
    %{
      status: :operator_decision,
      reason_code: "target_binding_missing",
      diff: %{},
      active_generation_id: nil,
      desired_descriptor_hash: nil,
      physical_fingerprint: nil,
      persisted?: true,
      blocks_writes?: true
    }
  end

  defp maybe_put_coverage(target, coverages) do
    case Map.fetch(coverages, target.target_id) do
      {:ok, coverage} -> Map.put(target, :coverage, coverage)
      :error -> target
    end
  end

  defp coverage_policy(nil), do: nil

  defp coverage_policy(coverage) do
    %{
      timezone: coverage.timezone,
      timezone_source: coverage.timezone_source,
      scope_source: coverage.scope_source,
      declared_from: coverage.declared_from.start_at,
      effective_from: coverage.effective_from.start_at,
      through: coverage_through(coverage.through),
      availability_delay_seconds: coverage.availability_delay_seconds
    }
  end

  defp coverage_through(value) when value in [:current, :latest_closed], do: value
  defp coverage_through(value), do: value.start_at

  defp asset_detail_entry(
         %Version{} = version,
         asset,
         _target_id,
         status,
         freshness_states,
         asset_window_states,
         run_history,
         opts,
         run_context_selection
       ) do
    target = Targets.asset(asset)
    ref_string = Targets.ref_string(asset.ref)
    latest_freshness = AssetFreshness.latest_for_ref(freshness_states, ref_string)
    latest_run = run_history.latest
    runs_by_id = Map.new(run_history.items, &{&1.id, &1})

    context_opts =
      opts
      |> Keyword.put(:asset_run_context, run_context_selection.selected)
      |> Keyword.put(:run_context_status, run_context_selection.status)

    timeline =
      Timeline.build(
        version,
        asset,
        latest_freshness,
        latest_run,
        freshness_states,
        asset_window_states,
        runs_by_id,
        context_opts
      )

    context_descriptors = Enum.map(run_context_selection.contexts, &AssetRunContext.descriptor/1)

    selected_context_descriptor =
      run_context_selection.selected && AssetRunContext.descriptor(run_context_selection.selected)

    target
    |> Map.take([
      :target_id,
      :label,
      :asset_ref,
      :name,
      :module_path,
      :relation,
      :type,
      :window,
      :metadata
    ])
    |> Map.put(:manifest_version_id, version.manifest_version_id)
    |> Map.put(:description, asset.description)
    |> Map.merge(asset_dependencies(version, asset))
    |> Map.put(:canonical_asset_ref, asset.ref)
    |> Status.put(status)
    |> Map.put(:freshness, AssetFreshness.detail(asset, version, freshness_states, context_opts))
    |> Map.put(:assurance, Assurance.detail(asset, run_history.latest_snapshot))
    |> Map.merge(timeline)
    |> Map.put(:runs, asset_run_entries(run_history.items, timeline))
    |> Map.put(:run_contexts, context_descriptors)
    |> Map.put(:selected_run_context, selected_context_descriptor)
    |> Map.put(:run_context_status, run_context_selection.status)
    |> Map.put(:can_run_asset?, run_context_selection.status != :ambiguous)
  end

  # Both directions come out of the manifest this call already loaded, so neither
  # costs a query. Upstream is the asset's own `depends_on`. Downstream has to be
  # found by asking every other asset who it depends on, because nothing records the
  # reverse edge.
  defp asset_dependencies(%Version{} = version, asset) do
    assets = List.wrap(version.manifest.assets)
    ref_string = Targets.ref_string(asset.ref)
    by_ref = Map.new(assets, &{Targets.ref_string(&1.ref), &1})

    upstream =
      asset.depends_on
      |> List.wrap()
      |> Enum.map(&Targets.ref_string/1)
      |> Enum.uniq()
      |> Enum.map(&dependency_entry(&1, Map.get(by_ref, &1)))
      |> Enum.sort_by(& &1.name)

    downstream =
      assets
      |> Enum.filter(&depends_on_ref?(&1, ref_string))
      |> Enum.map(&Targets.asset_reference/1)
      |> Enum.sort_by(& &1.name)

    %{upstream: upstream, downstream: downstream}
  end

  defp depends_on_ref?(candidate, ref_string) do
    candidate.depends_on
    |> List.wrap()
    |> Enum.any?(&(Targets.ref_string(&1) == ref_string))
  end

  # A declared dependency can name an asset this deployment does not carry — one
  # dropped from the manifest, or one another team owns. It is reported by its ref
  # with nowhere to link rather than silently left out of the list.
  defp dependency_entry(ref_string, nil) do
    %{name: Targets.asset_name(ref_string, nil), asset_ref: ref_string, target_id: nil}
  end

  defp dependency_entry(_ref_string, asset), do: Targets.asset_reference(asset)

  defp freshness_opts(opts, run_context_selection) do
    opts
    |> Keyword.put(:asset_run_context, run_context_selection.selected)
    |> Keyword.put(:run_context_status, run_context_selection.status)
  end

  defp assets_by_ref(%Version{manifest: %{assets: assets}}) when is_list(assets),
    do: Map.new(assets, &{&1.ref, &1})

  defp assets_by_ref(%Version{}), do: %{}

  defp field(value, key, default \\ nil)
  defp field(nil, _key, default), do: default

  defp field(value, key, default) when is_map(value) do
    Map.get(value, key, Map.get(value, Atom.to_string(key), default))
  end

  defp field(_value, _key, default), do: default

  # `page_target_runs` filters on `target_id`, so every row already belongs to this
  # asset and the newest row is its latest run. The ref filter this replaced could
  # never match: a compact history row from that query carries no `asset_ref` and an
  # empty `target_refs`, so it selected nothing and the asset detail reported no
  # latest run however many times the asset had run.
  defp asset_run_history(context, page) do
    runs = Enum.map(page.items, &Projector.project_run_summary/1)
    latest = RunHistory.latest(runs)

    with {:ok, snapshot} <- run_snapshot(context, latest) do
      {:ok, %{items: runs, latest: latest, latest_snapshot: snapshot}}
    end
  end

  # `Assurance` reads check evidence out of a run's `asset_results`, which only a
  # snapshot carries, so the latest run is loaded in full. Passing the compact row
  # left every check result, quality status, write outcome, and contract validation
  # nil no matter what the run recorded.
  defp run_snapshot(_context, nil), do: {:ok, nil}

  defp run_snapshot(context, %{id: run_id}) do
    case Runs.get(context, run_id) do
      {:ok, run_state} ->
        {:ok, Projector.project_run(run_state)}

      # Retention can prune a snapshot while its history row survives. That is a run
      # with no evidence, not a failed read.
      {:error, %PersistenceError{kind: :not_found}} ->
        {:ok, nil}

      {:error, _reason} = error ->
        error
    end
  end

  defp asset_run_entries(runs, timeline) do
    windows_by_run = windows_by_run_id(timeline)

    Enum.map(runs, &RunHistory.asset_entry(&1, Map.get(windows_by_run, &1.id)))
  end

  # Data windows are read first so a windowed asset labels a run with the window it
  # wrote rather than the refresh period it happened to land in.
  defp windows_by_run_id(timeline) do
    [:data_coverage_timeline, :refresh_timeline, :freshness_timeline]
    |> Enum.flat_map(&List.wrap(Map.get(timeline, &1)))
    |> Enum.reduce(%{}, fn window, acc ->
      case window.latest_run_id do
        nil -> acc
        run_id -> Map.put_new(acc, run_id, Map.take(window, [:kind, :value, :label, :range]))
      end
    end)
  end
end
