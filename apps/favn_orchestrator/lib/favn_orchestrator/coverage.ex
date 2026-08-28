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

  # One calendar unit is at most 31 days, 25 hours, or 12 months. Year grain has no
  # unit above it, so its screen is every year in coverage — this bounds that, and
  # anything else is a caller asking for a range no screen was going to draw.
  @window_states_cap 500
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

  @typedoc """
  One expected window and whether the pinned generation has evidence for it.
  """
  @type window_state :: %{
          required(:window_key) => String.t(),
          required(:kind) => atom(),
          required(:timezone) => String.t(),
          required(:start_at) => DateTime.t(),
          required(:end_at) => DateTime.t(),
          required(:covered?) => boolean()
        }

  @typedoc """
  Every expected window in one addressed range, plus the bounds of the whole range.

  `missing_windows/3` answers "what is missing", which is what a backfill needs. A
  calendar needs the complement as well — a period with no evidence and a period
  nobody looked at must not draw the same — and it cannot work the difference out
  itself: an hour-grained day has 23, 24, or 25 hours depending on daylight saving,
  and only the evaluator knows which.

  `first_expected_at` and `last_expected_at` bound navigation, so a caller can offer
  to step back exactly as far as coverage goes and no further.
  """
  @type window_states :: %{
          required(:summary) => Summary.t(),
          required(:kind) => atom() | nil,
          required(:timezone) => String.t() | nil,
          required(:windows) => [window_state()],
          required(:first_expected_at) => DateTime.t() | nil,
          required(:last_expected_at) => DateTime.t() | nil
        }

  @doc "Returns a bounded coverage summary for one active asset target."
  @spec summary(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, Summary.t()} | {:error, term()}
  def summary(%WorkspaceContext{} = context, target_id, opts \\ [])
      when is_binary(target_id) and is_list(opts) do
    timed_query(context, target_id, :summary, fn ->
      with :ok <- validate_options(opts, [:evaluated_at]),
           evaluated_at <- Keyword.get(opts, :evaluated_at, DateTime.utc_now()),
           :ok <- validate_datetime(evaluated_at) do
        with_active_asset(context, target_id, &summarize(context, &1, evaluated_at))
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
           true <- Enum.all?(target_ids, &MapSet.member?(granted, &1)) do
        ManifestStore.with_manifest(context, runtime.manifest_version_id, fn version ->
          with {:ok, snapshots} <- asset_snapshots(version, runtime, target_ids),
               identities <- batch_identities(context, snapshots),
               {:ok, summaries} <-
                 summarize_snapshots(context, snapshots, identities, evaluated_at) do
            {:ok, summaries}
          end
        end)
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
           {:ok, limit} <- page_limit(Keyword.get(opts, :limit, @default_page)) do
        with_active_asset(context, target_id, fn snapshot ->
          with {:ok, cursor} <- decode_cursor(Keyword.get(opts, :cursor)),
               {:ok, evaluated_at, after_key} <- evaluation_position(snapshot, cursor, opts),
               {:ok, result} <- missing_page(context, snapshot, evaluated_at, after_key, limit),
               :ok <- validate_cursor_result(cursor, result.summary) do
            {:ok, result}
          end
        end)
      end
    end)
  end

  @doc """
  Returns every expected window in one addressed range, covered or not.

  `:from` and `:until` name the range as local dates in the asset's own coverage
  timezone, `:until` exclusive. Dates rather than instants because midnight is not
  guaranteed to exist — a clock change can skip it — and resolving that is this
  layer's job, not a caller's. `:from` is floored to its period and clamped into
  coverage, so a caller may ask for a month without checking that the month is inside
  the range; omitting both reads from the start of coverage to the safety cap.

  A range rather than a count because a count cannot name a calendar unit: a month
  holds 28 to 31 days and a day holds 23, 24, or 25 hours. A caller drawing one
  unit passes that unit's bounds and gets exactly the periods inside it.
  """
  @spec window_states(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, window_states()} | {:error, term()}
  def window_states(%WorkspaceContext{} = context, target_id, opts \\ [])
      when is_binary(target_id) and is_list(opts) do
    timed_query(context, target_id, :window_states, fn ->
      with :ok <- validate_options(opts, [:evaluated_at, :from, :until]),
           :ok <- validate_optional_date(Keyword.get(opts, :from)),
           :ok <- validate_optional_date(Keyword.get(opts, :until)),
           evaluated_at <- Keyword.get(opts, :evaluated_at, DateTime.utc_now()),
           :ok <- validate_datetime(evaluated_at) do
        with_active_asset(context, target_id, fn snapshot ->
          with {:ok, summary} <- summarize(context, snapshot, evaluated_at) do
            addressed_states(context, snapshot, summary, opts)
          end
        end)
      end
    end)
  end

  defp validate_optional_date(nil), do: :ok
  defp validate_optional_date(%Date{}), do: :ok
  defp validate_optional_date(_other), do: {:error, :invalid_coverage_options}

  # No range asked for reads from the start of coverage.
  defp range_start(nil, _timezone, evaluation), do: {:ok, evaluation.first_window.start_at}
  defp range_start(%Date{} = date, timezone, _evaluation), do: local_start(date, timezone)

  defp range_end(nil, _timezone), do: {:ok, nil}
  defp range_end(%Date{} = date, timezone), do: local_start(date, timezone)

  # A date names a period, not an instant, and in some zones its midnight does not
  # exist — 1970-01-01 in Asia/Ho_Chi_Minh, a spring-forward day in a zone that
  # shifts at midnight. `:gap` gives the first instant that does exist, which is the
  # start of that period; `:ambiguous` takes the earlier of the two.
  defp local_start(%Date{} = date, timezone) do
    case DateTime.new(date, ~T[00:00:00], timezone, Favn.Timezone.database!()) do
      {:ok, instant} -> {:ok, instant}
      {:ambiguous, earlier, _later} -> {:ok, earlier}
      {:gap, _before, first_after} -> {:ok, first_after}
      {:error, _reason} -> {:error, :invalid_coverage_options}
    end
  end

  # Coverage that cannot be evaluated has no windows to draw, and inventing a range
  # for it would put an empty calendar on screen where an explanation belongs.
  defp addressed_states(_context, _snapshot, %Summary{status: :unknown} = summary, _opts),
    do: {:ok, unknown_window_states(summary)}

  defp addressed_states(context, snapshot, summary, opts) do
    identity = %{
      evidence_generation_id: summary.evidence_generation_id,
      target_generation_id: summary.active_target_generation_id
    }

    with {:ok, evaluation} <- Expected.evaluate(snapshot.asset.coverage, summary.evaluated_at),
         timezone <- evaluation.coverage.timezone,
         {:ok, from} <- range_start(Keyword.get(opts, :from), timezone, evaluation),
         {:ok, until} <- range_end(Keyword.get(opts, :until), timezone),
         {:ok, page} <- Expected.page_between(evaluation, from, until, @window_states_cap) do
      case successful_keys(context, snapshot, identity, page) do
        {:ok, keys} ->
          {:ok, addressed_window_states(summary, evaluation, page.items, keys)}

        # The counts came back but the exact keys did not, so which periods are
        # covered is unknown. Reporting them all as missing would draw a calendar of
        # gaps that may not exist, and keeping the counted summary beside no windows
        # would say two contradictory things at once.
        {:error, _reason} ->
          with {:ok, unknown} <-
                 unknown_summary(snapshot, summary.evaluated_at, :authoritative_state_unavailable) do
            {:ok, unknown_window_states(unknown)}
          end
      end
    end
  end

  defp addressed_window_states(summary, evaluation, anchors, successful_keys) do
    covered = MapSet.new(successful_keys)

    %{
      summary: summary,
      kind: evaluation.coverage.kind,
      timezone: evaluation.coverage.timezone,
      first_expected_at: evaluation.first_window.start_at,
      last_expected_at:
        evaluation.last_expected_window && evaluation.last_expected_window.start_at,
      windows: Enum.map(anchors, &window_state(&1, covered))
    }
  end

  defp window_state(anchor, covered) do
    anchor
    |> missing_window()
    |> Map.put(:covered?, MapSet.member?(covered, storage_window_key(anchor)))
  end

  defp unknown_window_states(summary) do
    %{
      summary: summary,
      kind: nil,
      timezone: nil,
      windows: [],
      first_expected_at: nil,
      last_expected_at: nil
    }
  end

  @doc """
  Builds an immutable exact backfill plan from missing windows.

  Three selections, in order of precedence: `:window_keys` plans exactly the named
  windows, `:cursor`/`:limit` plans one page, and neither plans every missing
  window. The selection is part of the plan hash, so a plan reviewed as three days
  cannot be submitted as a whole year.
  """
  @spec plan_missing_backfill(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def plan_missing_backfill(%WorkspaceContext{} = context, target_id, opts \\ [])
      when is_binary(target_id) and is_list(opts) do
    with :ok <-
           validate_options(opts, [
             :evaluated_at,
             :cursor,
             :limit,
             :window_keys,
             :combine_windows
           ]),
         combine_windows <- Keyword.get(opts, :combine_windows, false),
         :ok <- validate_combine_windows(combine_windows),
         evaluated_at <- Keyword.get(opts, :evaluated_at, DateTime.utc_now()),
         :ok <- validate_datetime(evaluated_at),
         {:ok, selection} <- backfill_selection(opts),
         {:ok, summary, items} <-
           missing_selection(context, target_id, evaluated_at, selection) do
      with_active_asset(context, target_id, fn snapshot ->
        with true <- snapshot.version.manifest_version_id == summary.manifest_version_id,
             :ok <- validate_combined_backfill(context, snapshot, items, combine_windows) do
          build_backfill_plan(
            summary,
            items,
            snapshot.runtime.deployment_id,
            selection,
            combine_windows
          )
        else
          false -> {:error, :coverage_selection_stale}
          {:error, _reason} = error -> error
        end
      end)
    else
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

  defp missing_selection(context, target_id, evaluated_at, %{mode: :explicit} = selection) do
    with {:ok, summary, missing} <-
           missing_selection(context, target_id, evaluated_at, %{mode: :all}) do
      requested = MapSet.new(selection.window_keys)
      selected = Enum.filter(missing, &MapSet.member?(requested, &1.window_key))

      # Every named window has to still be missing. One of them being covered since
      # the operator selected it means the plan they are about to review is not the
      # one they built, and silently planning the remainder would run something
      # other than what was asked for.
      if length(selected) == MapSet.size(requested),
        do: {:ok, summary, selected},
        else: {:error, :coverage_selection_stale}
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
    cond do
      Keyword.has_key?(opts, :window_keys) and
          (Keyword.has_key?(opts, :cursor) or Keyword.has_key?(opts, :limit)) ->
        {:error, :invalid_coverage_options}

      Keyword.has_key?(opts, :window_keys) ->
        explicit_selection(Keyword.fetch!(opts, :window_keys))

      Keyword.has_key?(opts, :cursor) or Keyword.has_key?(opts, :limit) ->
        with {:ok, limit} <- page_limit(Keyword.get(opts, :limit, @max_page)),
             {:ok, cursor} <- optional_cursor(Keyword.get(opts, :cursor)) do
          {:ok, %{mode: :page, cursor: cursor, limit: limit}}
        end

      true ->
        {:ok, %{mode: :all}}
    end
  end

  # Sorted, so the plan hash depends on which windows were chosen and not on the
  # order they were clicked in.
  defp explicit_selection(keys) when is_list(keys) and keys != [] do
    unique = Enum.uniq(keys)

    if length(unique) == length(keys) and length(keys) <= @max_backfill_windows and
         Enum.all?(keys, &(is_binary(&1) and byte_size(&1) in 1..255)) do
      {:ok, %{mode: :explicit, window_keys: Enum.sort(keys)}}
    else
      {:error, :invalid_coverage_window_selection}
    end
  end

  defp explicit_selection(_keys), do: {:error, :invalid_coverage_window_selection}

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
             |> Keyword.put(:combine_windows, selected.combine_windows)
             |> Keyword.put(:evaluated_at, selected.evaluated_at)
           ),
         true <- current.plan_id == selected.plan_id and current.plan_hash == selected.plan_hash,
         {:ok, submit_opts} <- put_coverage_metadata(opts, selected) do
      with_active_asset(context, target_id, fn snapshot ->
        with true <- snapshot.version.manifest_version_id == selected.manifest_version_id,
             true <- snapshot.runtime.deployment_id == selected.deployment_id,
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
          false -> {:error, :coverage_selection_stale}
          {:error, reason} -> {:error, reason}
        end
      end)
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

  defp with_active_asset(context, target_id, fun) do
    with {:ok, {runtime, grants}} <-
           ManifestStore.get_active_deployment(context, customer_visible_only: true),
         true <-
           Enum.any?(grants, &(&1.target_kind == :asset and &1.target_id == target_id)) do
      ManifestStore.with_manifest(context, runtime.manifest_version_id, fn version ->
        with {:ok, %Asset{ref: {module, name}} = asset}
             when is_atom(module) and is_atom(name) <-
               ManifestTarget.resolve_asset(version, target_id) do
          fun.(%{runtime: runtime, version: version, asset: asset, target_id: target_id})
        end
      end)
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

  defp validate_combine_windows(value) when is_boolean(value), do: :ok
  defp validate_combine_windows(_value), do: {:error, :invalid_coverage_options}

  defp validate_combined_backfill(_context, _snapshot, _items, false), do: :ok

  defp validate_combined_backfill(_context, snapshot, items, true) do
    with {:ok, anchors} <- plan_anchors(items),
         :ok <-
           Backfills.validate_combined_asset_windows(
             snapshot.version,
             snapshot.asset,
             anchors,
             []
           ) do
      :ok
    end
  end

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

  defp build_backfill_plan(summary, items, deployment_id, selection, combine_windows) do
    plan = %{
      target_id: summary.target_id,
      manifest_version_id: summary.manifest_version_id,
      deployment_id: deployment_id,
      evidence_generation_id: summary.evidence_generation_id,
      active_target_generation_id: summary.active_target_generation_id,
      evaluated_at: summary.evaluated_at,
      evaluation_checksum: summary.evaluation_checksum,
      selection: selection,
      combine_windows: combine_windows,
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
         combine_windows when is_boolean(combine_windows) <- field(plan, :combine_windows),
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
           combine_windows: combine_windows,
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
      combine_windows: plan.combine_windows,
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

  defp normalize_selection(%{mode: mode} = selection) when mode in [:explicit, "explicit"],
    do: explicit_selection(field(selection, :window_keys))

  defp normalize_selection(%{"mode" => "explicit"} = selection),
    do: explicit_selection(field(selection, :window_keys))

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
  defp selection_options(%{mode: :explicit, window_keys: keys}), do: [window_keys: keys]
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
       |> Keyword.put(:combine_windows, plan.combine_windows)
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
