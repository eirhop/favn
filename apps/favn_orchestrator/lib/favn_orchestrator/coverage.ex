defmodule FavnOrchestrator.Coverage do
  @moduledoc """
  Generation-aware asset coverage summaries and cursor-paged missing windows.

  Coverage reads pin the active manifest and evidence generation, evaluate
  expected windows at one explicit instant, and query only bounded success
  evidence from persistence.
  """

  alias Favn.Coverage.Expected
  alias Favn.Coverage.Summary
  alias FavnOrchestrator.Backfills
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.TargetDescriptor
  alias FavnOrchestrator.Coverage.Cursor
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries.CountSuccessfulAssetWindows
  alias FavnOrchestrator.Persistence.Queries.GetSuccessfulAssetWindowKeys
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.TargetGenerations
  alias FavnOrchestrator.Telemetry
  alias Favn.Freshness.Key, as: FreshnessKey
  alias Favn.Window.Key, as: WindowKey

  @default_page 100
  @max_page 500
  @max_backfill_windows 10_000

  @type missing_page :: %{
          required(:summary) => Summary.t(),
          required(:items) => [map()],
          required(:pagination) => %{
            required(:limit) => pos_integer(),
            required(:has_more) => boolean(),
            required(:next_cursor) => String.t() | nil
          }
        }

  @doc "Returns a bounded coverage summary for one active asset target."
  @spec summary(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, Summary.t()} | {:error, term()}
  def summary(%WorkspaceContext{} = context, target_id, opts \\ [])
      when is_binary(target_id) and is_list(opts) do
    timed_query(context, target_id, :summary, fn ->
      with :ok <- validate_options(opts, [:evaluated_at]),
           evaluated_at <- Keyword.get(opts, :evaluated_at, DateTime.utc_now()),
           :ok <- validate_datetime(evaluated_at),
           {:ok, snapshot} <- active_asset(context, target_id) do
        summarize(context, snapshot, evaluated_at)
      end
    end)
  end

  @doc "Returns bounded coverage summaries for visible active asset targets."
  @spec summaries(WorkspaceContext.t(), [String.t()], keyword()) ::
          {:ok, %{optional(String.t()) => Summary.t()}} | {:error, term()}
  def summaries(%WorkspaceContext{} = context, target_ids, opts \\ [])
      when is_list(target_ids) and is_list(opts) do
    timed_query(context, nil, :summaries, fn ->
      with :ok <- validate_options(opts, [:evaluated_at]),
           evaluated_at <- Keyword.get(opts, :evaluated_at, DateTime.utc_now()),
           :ok <- validate_datetime(evaluated_at),
           true <- Enum.all?(target_ids, &is_binary/1),
           {:ok, {runtime, grants}} <-
             ManifestStore.get_active_deployment(context, customer_visible_only: true),
           granted <- granted_asset_ids(grants),
           true <- Enum.all?(target_ids, &MapSet.member?(granted, &1)),
           {:ok, version} <- ManifestStore.get_manifest(context, runtime.manifest_version_id),
           {:ok, snapshots} <- asset_snapshots(version, runtime, target_ids),
           identities <- batch_identities(context, snapshots),
           {:ok, summaries} <-
             summarize_snapshots(context, snapshots, identities, evaluated_at) do
        {:ok, summaries}
      else
        false -> {:error, :not_found}
        {:error, _reason} = error -> error
      end
    end)
  end

  @doc "Returns one cursor-paged set of exact missing expected windows."
  @spec missing_windows(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, missing_page()} | {:error, term()}
  def missing_windows(%WorkspaceContext{} = context, target_id, opts \\ [])
      when is_binary(target_id) and is_list(opts) do
    timed_query(context, target_id, :missing_windows, fn ->
      with :ok <- validate_options(opts, [:evaluated_at, :limit, :cursor]),
           {:ok, limit} <- page_limit(Keyword.get(opts, :limit, @default_page)),
           {:ok, snapshot} <- active_asset(context, target_id),
           {:ok, cursor} <- decode_cursor(Keyword.get(opts, :cursor)),
           {:ok, evaluated_at, after_key} <- evaluation_position(snapshot, cursor, opts),
           {:ok, result} <- missing_page(context, snapshot, evaluated_at, after_key, limit),
           :ok <- validate_cursor_result(cursor, result.summary) do
        {:ok, result}
      end
    end)
  end

  @doc "Builds an immutable exact backfill plan from all or one page of missing windows."
  @spec plan_missing_backfill(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def plan_missing_backfill(%WorkspaceContext{} = context, target_id, opts \\ [])
      when is_binary(target_id) and is_list(opts) do
    with :ok <- validate_options(opts, [:evaluated_at, :cursor, :limit]),
         evaluated_at <- Keyword.get(opts, :evaluated_at, DateTime.utc_now()),
         :ok <- validate_datetime(evaluated_at),
         {:ok, selection} <- backfill_selection(opts),
         {:ok, summary, items} <-
           missing_selection(context, target_id, evaluated_at, selection),
         {:ok, snapshot} <- active_asset(context, target_id),
         true <- snapshot.version.manifest_version_id == summary.manifest_version_id do
      build_backfill_plan(summary, items, snapshot.runtime.deployment_id, selection)
    else
      false -> {:error, :coverage_selection_stale}
      {:error, _reason} = error -> error
    end
  end

  defp missing_selection(context, target_id, evaluated_at, %{mode: :all}) do
    with {:ok, first_page} <-
           missing_windows(context, target_id,
             evaluated_at: evaluated_at,
             limit: @max_page
           ),
         :ok <- plannable_summary(first_page.summary),
         :ok <- backfill_limit(first_page.summary.missing_count),
         {:ok, items} <- collect_missing(context, target_id, first_page, first_page.items) do
      {:ok, first_page.summary, items}
    end
  end

  defp missing_selection(context, target_id, evaluated_at, selection) do
    page_opts =
      [evaluated_at: evaluated_at, limit: selection.limit]
      |> maybe_put_cursor(selection.cursor)

    with {:ok, page} <- missing_windows(context, target_id, page_opts),
         :ok <- plannable_summary(page.summary),
         :ok <- nonempty_page(page.items) do
      {:ok, page.summary, page.items}
    end
  end

  defp backfill_selection(opts) do
    if Keyword.has_key?(opts, :cursor) or Keyword.has_key?(opts, :limit) do
      with {:ok, limit} <- page_limit(Keyword.get(opts, :limit, @max_page)),
           {:ok, cursor} <- optional_cursor(Keyword.get(opts, :cursor)) do
        {:ok, %{mode: :page, cursor: cursor, limit: limit}}
      end
    else
      {:ok, %{mode: :all}}
    end
  end

  defp optional_cursor(nil), do: {:ok, nil}

  defp optional_cursor(value) when is_binary(value) and byte_size(value) <= 4096,
    do: {:ok, value}

  defp optional_cursor(_value), do: {:error, :invalid_coverage_cursor}

  defp maybe_put_cursor(opts, nil), do: opts
  defp maybe_put_cursor(opts, cursor), do: Keyword.put(opts, :cursor, cursor)

  defp nonempty_page([]), do: {:error, :coverage_page_complete}
  defp nonempty_page(_items), do: :ok

  @doc "Revalidates and submits one exact missing-window backfill plan."
  @spec submit_missing_backfill(WorkspaceContext.t(), String.t(), map(), keyword()) ::
          {:ok, FavnOrchestrator.Persistence.Results.Backfill.t()} | {:error, term()}
  def submit_missing_backfill(%WorkspaceContext{} = context, target_id, plan, opts \\ [])
      when is_binary(target_id) and is_map(plan) and is_list(opts) do
    with {:ok, selected} <- normalize_backfill_plan(plan),
         true <- selected.target_id == target_id,
         {:ok, current} <-
           plan_missing_backfill(
             context,
             target_id,
             selected.selection
             |> selection_options()
             |> Keyword.put(:evaluated_at, selected.evaluated_at)
           ),
         true <- current.plan_id == selected.plan_id and current.plan_hash == selected.plan_hash,
         {:ok, snapshot} <- active_asset(context, target_id),
         true <- snapshot.version.manifest_version_id == selected.manifest_version_id,
         true <- snapshot.runtime.deployment_id == selected.deployment_id,
         {:ok, submit_opts} <- put_coverage_metadata(opts, selected),
         {:ok, backfill} <-
           Backfills.submit_asset_windows(
             context,
             selected.manifest_version_id,
             target_id,
             selected.anchors,
             submit_opts
           ) do
      {:ok, backfill}
    else
      false ->
        {:error, :coverage_selection_stale}

      {:error, reason}
      when reason in [
             :coverage_complete,
             :coverage_page_complete,
             :coverage_cursor_stale,
             :target_generation_uninitialized,
             :not_found
           ] ->
        {:error, :coverage_selection_stale}

      {:error, {:coverage_unknown, _reason}} ->
        {:error, :coverage_selection_stale}

      {:error, {:too_many_backfill_windows, _count, _limit}} ->
        {:error, :coverage_selection_stale}

      {:error, _reason} = error ->
        error
    end
  end

  defp summarize(context, snapshot, evaluated_at) do
    summarize_identity(context, snapshot, coverage_identity(context, snapshot), evaluated_at)
  end

  defp summarize_identity(context, snapshot, identity, evaluated_at) do
    case identity do
      {:unknown, reason} ->
        unknown_summary(snapshot, evaluated_at, reason)

      {:error, _reason} ->
        unknown_summary(snapshot, evaluated_at, :authoritative_state_unavailable)

      {:ok, identity} ->
        known_summary(context, snapshot, identity, evaluated_at)
    end
  end

  defp granted_asset_ids(grants) do
    grants
    |> Enum.filter(&(&1.target_kind == :asset and &1.customer_visible))
    |> MapSet.new(& &1.target_id)
  end

  defp asset_snapshots(version, runtime, target_ids) do
    Enum.reduce_while(target_ids, {:ok, []}, fn target_id, {:ok, acc} ->
      case ManifestTarget.resolve_asset(version, target_id) do
        {:ok, asset} ->
          snapshot = %{runtime: runtime, version: version, asset: asset, target_id: target_id}
          {:cont, {:ok, [snapshot | acc]}}

        {:error, _reason} ->
          {:halt, {:error, :not_found}}
      end
    end)
    |> then(fn
      {:ok, snapshots} -> {:ok, Enum.reverse(snapshots)}
      error -> error
    end)
  end

  defp batch_identities(context, snapshots) do
    assets_by_ref = Map.new(snapshots, &{&1.asset.ref, &1.asset})

    case TargetGenerations.for_reads(context, assets_by_ref) do
      {:ok, identities} -> identities
      {:error, _reason} -> :unavailable
    end
  end

  defp summarize_snapshots(context, snapshots, identities, evaluated_at) do
    Enum.reduce_while(snapshots, {:ok, %{}}, fn snapshot, {:ok, acc} ->
      identity = identity_from_batch(snapshot.asset, identities)

      case summarize_identity(context, snapshot, identity, evaluated_at) do
        {:ok, summary} -> {:cont, {:ok, Map.put(acc, snapshot.target_id, summary)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp identity_from_batch(_asset, :unavailable),
    do: {:unknown, :authoritative_state_unavailable}

  defp identity_from_batch(%Asset{window: nil}, _identities),
    do: {:unknown, :non_windowed_asset}

  defp identity_from_batch(%Asset{coverage: nil}, _identities),
    do: {:unknown, :coverage_not_declared}

  defp identity_from_batch(%Asset{} = asset, identities) do
    case Map.get(identities, asset.ref) do
      nil when is_struct(asset.target_descriptor, TargetDescriptor) ->
        {:unknown, :target_generation_uninitialized}

      nil ->
        {:unknown, :authoritative_state_unavailable}

      identity ->
        {:ok, identity}
    end
  end

  defp known_summary(context, snapshot, identity, evaluated_at) do
    with {:ok, evaluation} <- Expected.evaluate(snapshot.asset.coverage, evaluated_at),
         checksum <- selection_checksum(snapshot, identity, evaluation),
         {:ok, covered_count} <- covered_count(context, snapshot, identity, evaluation) do
      missing_count = evaluation.expected_count - covered_count

      Summary.new(%{
        status: if(missing_count == 0, do: :complete, else: :incomplete),
        evaluated_at: evaluated_at,
        manifest_version_id: snapshot.version.manifest_version_id,
        target_id: snapshot.target_id,
        first_window: evaluation.first_window,
        last_expected_window: evaluation.last_expected_window,
        expected_count: evaluation.expected_count,
        covered_count: covered_count,
        missing_count: missing_count,
        evidence_generation_id: identity.evidence_generation_id,
        active_target_generation_id: identity.target_generation_id,
        evaluation_checksum: checksum
      })
    else
      {:error, :coverage_window_limit_exceeded} = error ->
        error

      {:error, _reason} ->
        unknown_summary(snapshot, evaluated_at, :authoritative_state_unavailable)
    end
  end

  defp missing_page(context, snapshot, evaluated_at, after_key, limit) do
    with {:ok, summary} <- summarize(context, snapshot, evaluated_at) do
      if summary.status == :unknown do
        {:ok, %{summary: summary, items: [], pagination: page_metadata(limit, false, nil)}}
      else
        identity = %{
          evidence_generation_id: summary.evidence_generation_id,
          target_generation_id: summary.active_target_generation_id
        }

        with {:ok, evaluation} <- Expected.evaluate(snapshot.asset.coverage, evaluated_at),
             {:ok, expected_page} <- Expected.page(evaluation, after_key, limit) do
          case successful_keys(context, snapshot, identity, expected_page) do
            {:ok, successful_keys} ->
              build_missing_page(snapshot, summary, expected_page, successful_keys, limit)

            {:error, _reason} ->
              unknown_page(snapshot, evaluated_at, limit)
          end
        end
      end
    end
  end

  defp build_missing_page(snapshot, summary, expected_page, successful_keys, limit) do
    successful = MapSet.new(successful_keys)

    items =
      expected_page.items
      |> Enum.reject(&MapSet.member?(successful, storage_window_key(&1)))
      |> Enum.map(&missing_window/1)

    next_cursor =
      if expected_page.has_more? do
        Cursor.encode(%{
          target_id: snapshot.target_id,
          manifest_version_id: snapshot.version.manifest_version_id,
          evidence_generation_id: summary.evidence_generation_id,
          active_target_generation_id: summary.active_target_generation_id,
          evaluated_at: summary.evaluated_at,
          evaluation_checksum: summary.evaluation_checksum,
          after_window_key: expected_page.next_after
        })
      end

    {:ok,
     %{
       summary: summary,
       items: items,
       pagination: page_metadata(limit, expected_page.has_more?, next_cursor)
     }}
  end

  defp unknown_page(snapshot, evaluated_at, limit) do
    with {:ok, summary} <-
           unknown_summary(snapshot, evaluated_at, :authoritative_state_unavailable) do
      {:ok, %{summary: summary, items: [], pagination: page_metadata(limit, false, nil)}}
    end
  end

  defp active_asset(context, target_id) do
    with {:ok, {runtime, grants}} <-
           ManifestStore.get_active_deployment(context, customer_visible_only: true),
         true <-
           Enum.any?(grants, &(&1.target_kind == :asset and &1.target_id == target_id)),
         {:ok, version} <- ManifestStore.get_manifest(context, runtime.manifest_version_id),
         {:ok, asset} <- ManifestTarget.resolve_asset(version, target_id) do
      {:ok, %{runtime: runtime, version: version, asset: asset, target_id: target_id}}
    else
      false -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  end

  defp coverage_identity(_context, %{asset: %Asset{window: nil}}),
    do: {:unknown, :non_windowed_asset}

  defp coverage_identity(_context, %{asset: %Asset{coverage: nil}}),
    do: {:unknown, :coverage_not_declared}

  defp coverage_identity(context, %{asset: %Asset{} = asset}) do
    with {:ok, identities} <- TargetGenerations.for_reads(context, %{asset.ref => asset}) do
      case Map.get(identities, asset.ref) do
        nil when is_struct(asset.target_descriptor, TargetDescriptor) ->
          {:unknown, :target_generation_uninitialized}

        nil ->
          {:error, :evidence_generation_unavailable}

        identity ->
          {:ok, identity}
      end
    end
  end

  defp covered_count(_context, _snapshot, _identity, %{expected_count: 0}), do: {:ok, 0}

  defp covered_count(context, snapshot, identity, evaluation) do
    Persistence.stores().operator_reads.count_successful_asset_windows(
      %CountSuccessfulAssetWindows{
        workspace_context: context,
        evidence_generation_id: identity.evidence_generation_id,
        target_id: snapshot.target_id,
        first_window_start: evaluation.first_window.start_at,
        last_window_start: evaluation.last_expected_window.start_at
      }
    )
  end

  defp successful_keys(_context, _snapshot, _identity, %{items: []}), do: {:ok, []}

  defp successful_keys(context, snapshot, identity, expected_page) do
    Persistence.stores().operator_reads.get_successful_asset_window_keys(
      %GetSuccessfulAssetWindowKeys{
        workspace_context: context,
        evidence_generation_id: identity.evidence_generation_id,
        target_id: snapshot.target_id,
        window_keys: Enum.map(expected_page.items, &storage_window_key/1)
      }
    )
  end

  defp unknown_summary(snapshot, evaluated_at, reason) do
    Summary.new(%{
      status: :unknown,
      unknown_reason: reason,
      evaluated_at: evaluated_at,
      manifest_version_id: snapshot.version.manifest_version_id,
      target_id: snapshot.target_id
    })
  end

  defp missing_window(anchor) do
    %{
      window_key: WindowKey.encode(anchor.key),
      kind: anchor.kind,
      timezone: anchor.timezone,
      start_at: anchor.start_at,
      end_at: anchor.end_at
    }
  end

  defp storage_window_key(anchor), do: FreshnessKey.window!(anchor.key)

  defp selection_checksum(snapshot, identity, evaluation) do
    payload = %{
      manifest_version_id: snapshot.version.manifest_version_id,
      target_id: snapshot.target_id,
      evidence_generation_id: identity.evidence_generation_id,
      active_target_generation_id: identity.target_generation_id,
      coverage_checksum: evaluation.checksum
    }

    :crypto.hash(:sha256, Serializer.encode_canonical!(payload))
    |> Base.encode16(case: :lower)
  end

  defp decode_cursor(nil), do: {:ok, nil}
  defp decode_cursor(cursor), do: Cursor.decode(cursor)

  defp evaluation_position(_snapshot, nil, opts) do
    evaluated_at = Keyword.get(opts, :evaluated_at, DateTime.utc_now())

    case validate_datetime(evaluated_at) do
      :ok -> {:ok, evaluated_at, nil}
      {:error, _reason} = error -> error
    end
  end

  defp evaluation_position(snapshot, cursor, _opts) do
    if cursor.target_id == snapshot.target_id and
         cursor.manifest_version_id == snapshot.version.manifest_version_id do
      {:ok, cursor.evaluated_at, cursor.after_window_key}
    else
      {:error, :coverage_cursor_stale}
    end
  end

  defp validate_cursor_result(nil, _summary), do: :ok

  defp validate_cursor_result(
         _cursor,
         %Summary{status: :unknown, unknown_reason: :authoritative_state_unavailable}
       ),
       do: :ok

  defp validate_cursor_result(cursor, summary) do
    if summary.status != :unknown and
         cursor.evidence_generation_id == summary.evidence_generation_id and
         cursor.active_target_generation_id == summary.active_target_generation_id and
         cursor.evaluation_checksum == summary.evaluation_checksum,
       do: :ok,
       else: {:error, :coverage_cursor_stale}
  end

  defp page_metadata(limit, has_more?, next_cursor),
    do: %{limit: limit, has_more: has_more?, next_cursor: next_cursor}

  defp page_limit(value) when is_integer(value) and value in 1..@max_page, do: {:ok, value}
  defp page_limit(_value), do: {:error, :invalid_coverage_page_limit}

  defp validate_options(opts, allowed) do
    if Keyword.keyword?(opts) and Keyword.keys(opts) -- allowed == [],
      do: :ok,
      else: {:error, :invalid_coverage_options}
  end

  defp validate_datetime(%DateTime{}), do: :ok
  defp validate_datetime(_value), do: {:error, :invalid_coverage_evaluated_at}

  defp plannable_summary(%Summary{status: :unknown, unknown_reason: reason}),
    do: {:error, {:coverage_unknown, reason}}

  defp plannable_summary(%Summary{missing_count: 0}), do: {:error, :coverage_complete}
  defp plannable_summary(%Summary{}), do: :ok

  defp backfill_limit(count) when count <= @max_backfill_windows, do: :ok

  defp backfill_limit(count),
    do: {:error, {:too_many_backfill_windows, count, @max_backfill_windows}}

  defp collect_missing(_context, _target_id, %{pagination: %{has_more: false}}, acc),
    do: {:ok, acc}

  defp collect_missing(context, target_id, page, acc) do
    with {:ok, next} <-
           missing_windows(context, target_id,
             cursor: page.pagination.next_cursor,
             limit: @max_page
           ) do
      collect_missing(context, target_id, next, acc ++ next.items)
    end
  end

  defp build_backfill_plan(summary, items, deployment_id, selection) do
    plan = %{
      target_id: summary.target_id,
      manifest_version_id: summary.manifest_version_id,
      deployment_id: deployment_id,
      evidence_generation_id: summary.evidence_generation_id,
      active_target_generation_id: summary.active_target_generation_id,
      evaluated_at: summary.evaluated_at,
      evaluation_checksum: summary.evaluation_checksum,
      selection: selection,
      window_count: length(items),
      windows: items
    }

    hash = backfill_plan_hash(plan)

    {:ok,
     plan
     |> Map.put(:plan_hash, hash)
     |> Map.put(:plan_id, "coverage_plan_" <> String.slice(hash, 0, 32))}
  end

  defp normalize_backfill_plan(plan) do
    with target_id when is_binary(target_id) <- field(plan, :target_id),
         manifest_version_id when is_binary(manifest_version_id) <-
           field(plan, :manifest_version_id),
         deployment_id when is_binary(deployment_id) <- field(plan, :deployment_id),
         evidence_generation_id when is_binary(evidence_generation_id) <-
           field(plan, :evidence_generation_id),
         active_target_generation_id <- field(plan, :active_target_generation_id),
         {:ok, evaluated_at} <- parse_datetime(field(plan, :evaluated_at)),
         evaluation_checksum when is_binary(evaluation_checksum) <-
           field(plan, :evaluation_checksum),
         {:ok, selection} <- normalize_selection(field(plan, :selection)),
         plan_id when is_binary(plan_id) <- field(plan, :plan_id),
         plan_hash when is_binary(plan_hash) <- field(plan, :plan_hash),
         windows when is_list(windows) and windows != [] <- field(plan, :windows),
         {:ok, anchors} <- plan_anchors(windows),
         normalized <- %{
           target_id: target_id,
           manifest_version_id: manifest_version_id,
           deployment_id: deployment_id,
           evidence_generation_id: evidence_generation_id,
           active_target_generation_id: active_target_generation_id,
           evaluated_at: evaluated_at,
           evaluation_checksum: evaluation_checksum,
           selection: selection,
           window_count: length(windows),
           windows: Enum.map(anchors, &missing_window/1),
           anchors: anchors,
           plan_id: plan_id,
           plan_hash: plan_hash
         },
         true <-
           plan_hash == backfill_plan_hash(Map.drop(normalized, [:anchors, :plan_id, :plan_hash])),
         true <- plan_id == "coverage_plan_" <> String.slice(plan_hash, 0, 32) do
      {:ok, normalized}
    else
      _invalid -> {:error, :invalid_coverage_backfill_plan}
    end
  end

  defp plan_anchors(windows) do
    Enum.reduce_while(windows, {:ok, []}, fn window, {:ok, acc} ->
      with encoded_key when is_binary(encoded_key) <- field(window, :window_key),
           {:ok, key} <- WindowKey.decode(encoded_key),
           {:ok, parsed_start} <- parse_datetime(field(window, :start_at)),
           {:ok, parsed_end} <- parse_datetime(field(window, :end_at)),
           {:ok, start_at} <-
             DateTime.shift_zone(parsed_start, key.timezone, Favn.Timezone.database!()),
           {:ok, end_at} <-
             DateTime.shift_zone(parsed_end, key.timezone, Favn.Timezone.database!()),
           {:ok, anchor} <-
             Favn.Window.Anchor.new(key.kind, start_at, end_at, timezone: key.timezone),
           true <- anchor.key == key do
        {:cont, {:ok, [anchor | acc]}}
      else
        _invalid -> {:halt, {:error, :invalid_coverage_backfill_plan}}
      end
    end)
    |> then(fn
      {:ok, anchors} -> {:ok, Enum.reverse(anchors)}
      error -> error
    end)
  end

  defp backfill_plan_hash(plan) do
    payload = %{
      target_id: plan.target_id,
      manifest_version_id: plan.manifest_version_id,
      deployment_id: plan.deployment_id,
      evidence_generation_id: plan.evidence_generation_id,
      active_target_generation_id: plan.active_target_generation_id,
      evaluated_at: DateTime.to_iso8601(plan.evaluated_at),
      evaluation_checksum: plan.evaluation_checksum,
      selection: plan.selection,
      window_count: plan.window_count,
      windows:
        Enum.map(plan.windows, fn window ->
          %{
            window_key: field(window, :window_key),
            kind: field(window, :kind),
            timezone: field(window, :timezone),
            start_at: window |> field(:start_at) |> datetime_string(),
            end_at: window |> field(:end_at) |> datetime_string()
          }
        end)
    }

    :crypto.hash(:sha256, Serializer.encode_canonical!(payload))
    |> Base.encode16(case: :lower)
  end

  defp coverage_plan_metadata(plan) do
    %{
      coverage_plan_id: plan.plan_id,
      coverage_plan_hash: plan.plan_hash,
      coverage_evaluation_checksum: plan.evaluation_checksum,
      coverage_evidence_generation_id: plan.evidence_generation_id
    }
  end

  defp normalize_selection(%{mode: :all}), do: {:ok, %{mode: :all}}
  defp normalize_selection(%{"mode" => "all"}), do: {:ok, %{mode: :all}}

  defp normalize_selection(selection) when is_map(selection) do
    with mode when mode in [:page, "page"] <- field(selection, :mode),
         {:ok, limit} <- page_limit(field(selection, :limit)),
         {:ok, cursor} <- optional_cursor(field(selection, :cursor)) do
      {:ok, %{mode: :page, cursor: cursor, limit: limit}}
    else
      _invalid -> {:error, :invalid_coverage_backfill_plan}
    end
  end

  defp normalize_selection(_selection), do: {:error, :invalid_coverage_backfill_plan}

  defp selection_options(%{mode: :all}), do: []
  defp selection_options(%{mode: :page, cursor: nil, limit: limit}), do: [limit: limit]

  defp selection_options(%{mode: :page, cursor: cursor, limit: limit}),
    do: [cursor: cursor, limit: limit]

  defp put_coverage_metadata(opts, plan) do
    metadata = Keyword.get(opts, :metadata, %{})

    if Keyword.keyword?(opts) and is_map(metadata) do
      required_generation = %{
        target_id: plan.target_id,
        evidence_generation_id: plan.evidence_generation_id,
        target_generation_id: plan.active_target_generation_id
      }

      {:ok,
       opts
       |> Keyword.put(:metadata, Map.merge(metadata, coverage_plan_metadata(plan)))
       |> Keyword.put(:required_generation, required_generation)}
    else
      {:error, :invalid_coverage_backfill_options}
    end
  end

  defp timed_query(context, target_id, operation, fun) do
    started_at = System.monotonic_time()
    result = fun.()

    Telemetry.emit(
      :coverage_query,
      %{
        duration: System.monotonic_time() - started_at,
        result_count: coverage_result_count(result)
      },
      %{
        workspace_id: context.workspace_id,
        target_id: target_id,
        operation: operation,
        status: coverage_result_status(result)
      }
    )

    result
  end

  defp coverage_result_count({:ok, %Summary{}}), do: 1
  defp coverage_result_count({:ok, %{summary: %Summary{}, items: items}}), do: length(items)
  defp coverage_result_count({:ok, summaries}) when is_map(summaries), do: map_size(summaries)
  defp coverage_result_count(_result), do: 0

  defp coverage_result_status({:ok, %Summary{status: status}}), do: status

  defp coverage_result_status({:ok, %{summary: %Summary{status: status}}}),
    do: status

  defp coverage_result_status({:ok, _result}), do: :ok
  defp coverage_result_status({:error, reason}) when is_atom(reason), do: reason
  defp coverage_result_status({:error, reason}) when is_tuple(reason), do: elem(reason, 0)
  defp coverage_result_status({:error, _reason}), do: :error

  defp parse_datetime(%DateTime{} = value), do: {:ok, value}

  defp parse_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, _reason} -> {:error, :invalid_coverage_backfill_plan}
    end
  end

  defp parse_datetime(_value), do: {:error, :invalid_coverage_backfill_plan}

  defp datetime_string(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp datetime_string(value) when is_binary(value), do: value

  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end
