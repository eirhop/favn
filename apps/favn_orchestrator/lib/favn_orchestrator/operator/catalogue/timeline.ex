defmodule FavnOrchestrator.Operator.Catalogue.Timeline do
  @moduledoc """
  Resolves an asset's periods, and answers the two questions a detail read has
  about them.

  Nothing renders a strip of periods any more, so none is published. What is left
  needs the same walk: the period an asset is currently due for, which prefills its
  run dialog, and the period each of its recent runs wrote, which labels that run.

  Two walks answer both — the asset's own data windows and the run anchors it is
  refreshed on — plus a direct read of calendar-period freshness evidence, whose grain
  is its own and which neither walk enumerates.

  Each walked period therefore carries only what answers those two questions: its grain
  and value, the label and range a run entry shows, and the run that wrote it. A run
  configuration is built for one period — the refresh anchor — because that is the only
  one a dialog can open on. No period carries a status, because no caller can reach one:
  whether a period is covered or fresh is
  `FavnOrchestrator.Operator.Catalogue.AssetFreshness`'s answer, and computing a second
  one here cost a plan per period to produce something nothing rendered.

  Values are derived from validated manifest policy and backend state. An invalid
  persisted kind or timezone falls back to the explicit daily UTC policy rather
  than crashing an operator read.
  """

  alias Favn.Freshness.Key, as: FreshnessKey
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.TimePeriod
  alias Favn.Timezone
  alias Favn.Window.Policy
  alias Favn.Window.Spec, as: WindowSpec
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.AssetRunContext
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Operator.Catalogue.AssetFreshness
  alias FavnOrchestrator.Operator.Catalogue.Status
  alias FavnOrchestrator.Operator.Catalogue.Targets
  alias FavnOrchestrator.Operator.WindowSelection

  @period_count 30
  @default_kind :day
  @default_timezone "Etc/UTC"

  @typedoc """
  What a detail read needs from an asset's periods.

  `run_windows` is keyed by run id and is not part of the DTO; the caller uses it to
  label each run entry with the period that run wrote.
  """
  @type resolved :: %{
          required(:has_data_windows?) => boolean(),
          required(:default_run_config) => map() | nil,
          required(:run_windows) => %{optional(String.t()) => map()}
        }

  @doc "Resolves the periods behind an operator asset-detail read."
  @spec build(
          Version.t(),
          Asset.t(),
          AssetFreshnessState.t() | nil,
          map() | nil,
          [AssetFreshnessState.t()],
          [AssetWindowState.t()],
          %{optional(String.t()) => map()},
          keyword()
        ) :: resolved()
  def build(
        %Version{} = version,
        %Asset{} = asset,
        latest_freshness,
        latest_run,
        freshness_states,
        asset_window_states,
        runs_by_id,
        opts
      )
      when is_list(freshness_states) and is_list(asset_window_states) and is_map(runs_by_id) and
             is_list(opts) do
    opts = normalize_run_context_opts(version, asset, opts)

    refresh_timeline =
      if opts[:run_context_status] == :ambiguous do
        []
      else
        refresh_timeline(
          asset,
          latest_freshness,
          latest_run,
          freshness_states,
          runs_by_id,
          opts
        )
      end

    data_coverage_timeline =
      data_coverage_timeline(
        asset,
        latest_freshness,
        latest_run,
        freshness_states,
        asset_window_states,
        opts
      )

    %{
      has_data_windows?: not is_nil(data_coverage_timeline),
      default_run_config: due_run_config(refresh_timeline),
      run_windows:
        run_windows([
          data_coverage_timeline,
          refresh_timeline,
          calendar_freshness_windows(asset, freshness_states)
        ])
    }
  end

  # Evidence a calendar-period freshness policy records under its own grain, which
  # neither walk above enumerates: an asset with a monthly window and `freshness
  # :daily` persists `calendar:day:<tz>:<date>`, so a walk of months matches none of
  # it. Without this, only that asset's most recent run carried a period — the walks
  # reach the latest one through `maybe_put_latest_run/5` and no earlier one at all.
  #
  # The label is the period the asset *writes*, not the day its freshness happened to
  # be evaluated on: a monthly asset's run wrote July, so it reads "Jul 2026" like
  # every other run of that asset, rather than "Jul 16" beside them.
  #
  # Read straight from the states rather than by walking periods: only the run each one
  # points at is wanted.
  defp calendar_freshness_windows(asset, freshness_states) do
    asset_ref_string = Targets.ref_string(asset.ref)
    {written_kind, _timezone} = coverage_policy(asset)

    Enum.flat_map(freshness_states, fn
      %AssetFreshnessState{} = state ->
        with ^asset_ref_string <- AssetFreshness.ref_string(state),
             {:ok, {:calendar, kind, _timezone, value}} <-
               FreshnessKey.parse(state.freshness_key),
             run_id when is_binary(run_id) <- Status.latest_run_id(state, nil),
             {:ok, written} <- written_value(written_kind, kind, value) do
          [
            %{
              latest_run_id: run_id,
              kind: written_kind,
              value: written,
              label: window_label(written_kind, written),
              range: window_range(written_kind, written)
            }
          ]
        else
          _other -> []
        end

      _state ->
        []
    end)
  end

  defp written_value(written_kind, key_kind, value) do
    {:ok, value_from_date(written_kind, value_date(key_kind, value))}
  rescue
    # A key whose value does not parse as its own kind is corrupt, and an operator read
    # drops the label rather than failing the whole screen over one run.
    _error -> :error
  end

  # The last period of the refresh walk is offset zero: the anchor the asset is due
  # for right now. That is what a run dialog should open on.
  defp due_run_config(refresh_timeline) do
    case List.last(refresh_timeline) do
      %{} = window -> Map.get(window, :latest_run_config) || Map.get(window, :default_run_config)
      nil -> nil
    end
  end

  # Data windows are read first so a windowed asset labels a run with the window it
  # wrote rather than the refresh period it happened to land in.
  defp run_windows(timelines) do
    timelines
    |> Enum.flat_map(&List.wrap/1)
    |> Enum.reduce(%{}, fn window, acc ->
      case window.latest_run_id do
        nil -> acc
        run_id -> Map.put_new(acc, run_id, Map.take(window, [:kind, :value, :label, :range]))
      end
    end)
  end

  # No run config here: a coverage period only ever labels a run. The period a dialog
  # opens on is the refresh anchor, which `due_run_config/1` reads.
  defp data_coverage_timeline(
         %{window: nil},
         _latest_freshness,
         _latest_run,
         _freshness_states,
         _asset_window_states,
         _opts
       ),
       do: nil

  defp data_coverage_timeline(
         asset,
         latest_freshness,
         latest_run,
         freshness_states,
         asset_window_states,
         opts
       ) do
    {kind, timezone} = coverage_policy(asset)
    selected_value = selected_value(kind, timezone, latest_freshness, latest_run, opts)
    window_states = window_states_by_value(asset, asset_window_states, kind, timezone)
    freshness_states = freshness_by_value(asset, freshness_states, kind, timezone)

    latest_run_value =
      latest_freshness
      |> Status.latest_run_at(latest_run)
      |> value_from_datetime(kind, timezone)

    for offset <- 0..(@period_count - 1) do
      value = shift_value(kind, timezone, selected_value, offset - (@period_count - 1))
      window_state = Map.get(window_states, value)
      window_freshness = Map.get(freshness_states, value)

      %{
        kind: kind,
        value: value,
        label: window_label(kind, value),
        range: window_range(kind, value),
        latest_run_id:
          window_latest_run_id(window_state) || Status.latest_run_id(window_freshness, nil)
      }
      |> maybe_put_latest_run(latest_freshness, latest_run, value, latest_run_value)
    end
  end

  defp refresh_timeline(
         asset,
         latest_freshness,
         latest_run,
         freshness_states,
         runs_by_id,
         opts
       ) do
    {kind, timezone} = refresh_policy(opts)

    selected_value =
      selected_refresh_value(kind, timezone, latest_freshness, latest_run, opts)

    freshness_by_value = freshness_by_value(asset, freshness_states, kind, timezone)

    latest_run_value =
      latest_freshness
      |> Status.latest_run_at(latest_run)
      |> value_from_datetime(kind, timezone)

    for offset <- 0..(@period_count - 1) do
      value = shift_value(kind, timezone, selected_value, offset - (@period_count - 1))
      freshness = Map.get(freshness_by_value, value)

      %{
        kind: kind,
        value: value,
        label: window_label(kind, value),
        range: window_range(kind, value),
        latest_run_id: Status.latest_run_id(freshness, nil)
      }
      |> maybe_put_latest_run(latest_freshness, latest_run, value, latest_run_value)
      |> maybe_put_run_config(offset, kind, timezone, runs_by_id)
    end
  end

  defp refresh_policy(opts) do
    case Keyword.get(opts, :asset_run_context) do
      %AssetRunContext{policy: %Policy{kind: kind}, timezone: timezone} ->
        normalize_policy(kind, timezone)

      %AssetRunContext{policy: nil, timezone: timezone} ->
        normalize_policy(@default_kind, timezone)

      _context ->
        default_policy()
    end
  end

  defp normalize_run_context_opts(version, asset, opts) do
    if Keyword.has_key?(opts, :run_context_status) do
      opts
    else
      case AssetRunContext.select(version, asset) do
        {:ok, selection} ->
          opts
          |> Keyword.put(:asset_run_context, selection.selected)
          |> Keyword.put(:run_context_status, selection.status)

        {:error, _reason} ->
          opts
          |> Keyword.put(:asset_run_context, nil)
          |> Keyword.put(:run_context_status, :unavailable)
      end
    end
  end

  defp coverage_policy(%{window: %WindowSpec{kind: kind, timezone: timezone}}),
    do: normalize_policy(kind, timezone)

  defp coverage_policy(%{window: window}) when is_atom(window) do
    case WindowSelection.normalize_kind(window) do
      {:ok, kind} -> normalize_policy(kind, @default_timezone)
      {:error, _reason} -> default_policy()
    end
  end

  defp coverage_policy(%{window: %{} = window}) do
    kind = field(window, :kind)
    timezone = field(window, :timezone) || @default_timezone

    case WindowSelection.normalize_kind(kind) do
      {:ok, kind} -> normalize_policy(kind, timezone)
      {:error, _reason} -> default_policy()
    end
  end

  defp coverage_policy(_asset), do: default_policy()

  defp normalize_policy(kind, timezone) when kind in [:hour, :day, :month, :year] do
    timezone = timezone || @default_timezone

    if Timezone.valid_identifier?(timezone), do: {kind, timezone}, else: default_policy()
  end

  defp normalize_policy(_kind, _timezone), do: default_policy()
  defp default_policy, do: {@default_kind, @default_timezone}

  # Only the last period of the refresh walk is offset zero, and only that one's config
  # is ever read. Building one for the other 29 cost a `runs_by_id` lookup and a merge
  # each to produce a map `due_run_config/1` never looks at.
  defp maybe_put_run_config(window, offset, kind, timezone, runs_by_id)
       when offset == @period_count - 1 do
    window
    |> Map.put(
      :default_run_config,
      default_run_config(:refresh_timeline, kind, window.value, timezone)
    )
    |> put_latest_run_config(runs_by_id)
  end

  defp maybe_put_run_config(window, _offset, _kind, _timezone, _runs_by_id), do: window

  defp default_run_config(source, kind, value, timezone) do
    %{
      source: source,
      kind: kind,
      value: value,
      timezone: timezone,
      dependencies: :all,
      refresh: :auto
    }
  end

  defp put_latest_run_config(%{latest_run_id: run_id} = window, runs_by_id)
       when is_binary(run_id) do
    case Map.get(runs_by_id, run_id) do
      nil -> window
      run -> Map.put(window, :latest_run_config, run_config(run, window.default_run_config))
    end
  end

  defp put_latest_run_config(window, _runs_by_id), do: window

  defp run_config(run, default_config) do
    metadata = normalize_map(Map.get(run, :metadata))

    default_config
    |> Map.put(:dependencies, field(metadata, :asset_dependencies) || default_config.dependencies)
    |> Map.put(
      :refresh,
      refresh_config(field(metadata, :refresh_policy), default_config.refresh)
    )
  end

  defp refresh_config(%{mode: :auto}, _default), do: :auto
  defp refresh_config(%{mode: :missing}, _default), do: :missing
  defp refresh_config(%{mode: :force}, _default), do: :force

  defp refresh_config(%{mode: :force_assets, include_upstream?: true}, _default),
    do: :force_selected_upstream

  defp refresh_config(%{mode: :force_assets}, _default), do: :force_selected
  defp refresh_config(_refresh_policy, default), do: default

  defp selected_value(kind, timezone, latest_freshness, latest_run, opts) do
    case {opts[:now], opts[:today], Status.latest_run_at(latest_freshness, latest_run)} do
      {%DateTime{} = now, _today, _latest_run_at} -> value_from_datetime(now, kind, timezone)
      {_now, %Date{} = date, _latest_run_at} -> value_from_date(kind, date)
      {_now, _today, %DateTime{} = datetime} -> value_from_datetime(datetime, kind, timezone)
      _other -> value_from_date(kind, Date.utc_today())
    end
  end

  defp selected_refresh_value(kind, timezone, latest_freshness, latest_run, opts) do
    reference_at =
      Keyword.get(opts, :now) || Status.latest_run_at(latest_freshness, latest_run) ||
        DateTime.utc_now()

    case Keyword.get(opts, :asset_run_context) do
      %AssetRunContext{} = run_context ->
        case AssetRunContext.anchor(run_context, reference_at) do
          {:ok, anchor} -> value_from_datetime(anchor.start_at, kind, timezone)
          {:error, _reason} -> selected_value(kind, timezone, latest_freshness, latest_run, opts)
        end

      _context ->
        selected_value(kind, timezone, latest_freshness, latest_run, opts)
    end
  end

  defp freshness_by_value(asset, freshness_states, timeline_kind, timeline_timezone) do
    asset_ref_string = Targets.ref_string(asset.ref)

    Enum.reduce(freshness_states, %{}, fn
      %AssetFreshnessState{} = state, acc ->
        with ^asset_ref_string <- AssetFreshness.ref_string(state),
             {:ok, value} <-
               timeline_value(state.freshness_key, timeline_kind, timeline_timezone) do
          Map.update(acc, value, state, &newer_state(&1, state))
        else
          _other -> acc
        end

      _state, acc ->
        acc
    end)
  end

  defp timeline_value(freshness_key, timeline_kind, timeline_timezone) do
    case FreshnessKey.parse(freshness_key) do
      {:ok, {:window, %{kind: ^timeline_kind} = window_key}} ->
        {:ok, window_key_value(window_key, timeline_kind, timeline_timezone)}

      {:ok, {:window_refresh, %{kind: ^timeline_kind} = window_key, _, _, _}} ->
        {:ok, window_key_value(window_key, timeline_kind, timeline_timezone)}

      {:ok, {:calendar, ^timeline_kind, ^timeline_timezone, value}} ->
        {:ok, value}

      {:ok, {:window_refresh, _, ^timeline_kind, ^timeline_timezone, value}} ->
        {:ok, value}

      _other ->
        :error
    end
  end

  defp window_key_value(%{start_at_us: start_at_us}, kind, timezone) do
    start_at_us
    |> DateTime.from_unix!(:microsecond)
    |> value_from_datetime(kind, timezone)
  end

  defp newer_state(%AssetFreshnessState{} = left, %AssetFreshnessState{} = right) do
    if DateTime.compare(left.updated_at, right.updated_at) == :lt, do: right, else: left
  end

  defp window_states_by_value(asset, asset_window_states, timeline_kind, timezone) do
    {asset_ref_module, asset_ref_name} = asset.ref

    asset_window_states
    |> Enum.filter(fn
      %AssetWindowState{
        asset_ref_module: ^asset_ref_module,
        asset_ref_name: ^asset_ref_name,
        window_kind: ^timeline_kind
      } ->
        true

      _state ->
        false
    end)
    |> Map.new(fn %AssetWindowState{} = state ->
      {value_from_datetime(state.window_start_at, timeline_kind, timezone), state}
    end)
  end

  defp window_latest_run_id(%AssetWindowState{latest_run_id: run_id}) when is_binary(run_id),
    do: run_id

  defp window_latest_run_id(_state), do: nil

  defp value_from_datetime(nil, _kind, _timezone), do: nil

  defp value_from_datetime(%DateTime{} = datetime, kind, timezone) do
    shifted = DateTime.shift_zone!(datetime, timezone, Timezone.database!())

    case kind do
      :hour -> "#{Date.to_iso8601(DateTime.to_date(shifted))}T#{pad2(shifted.hour)}"
      :day -> shifted |> DateTime.to_date() |> Date.to_iso8601()
      :month -> format_month(shifted.year, shifted.month)
      :year -> Integer.to_string(shifted.year)
    end
  end

  defp value_from_date(:hour, %Date{} = date), do: "#{Date.to_iso8601(date)}T00"
  defp value_from_date(:day, %Date{} = date), do: Date.to_iso8601(date)
  defp value_from_date(:month, %Date{} = date), do: format_month(date.year, date.month)
  defp value_from_date(:year, %Date{} = date), do: Integer.to_string(date.year)

  defp shift_value(kind, timezone, value, 0), do: normalize_value(kind, timezone, value)

  defp shift_value(kind, timezone, value, count) do
    {:ok, period} = TimePeriod.bounds(kind, value, timezone)
    {:ok, shifted} = TimePeriod.shift(period.start_at, kind, count)
    value_from_datetime(shifted, kind, timezone)
  end

  defp normalize_value(kind, timezone, value) do
    {:ok, period} = TimePeriod.bounds(kind, value, timezone)
    value_from_datetime(period.start_at, kind, timezone)
  end

  defp value_date(:hour, <<date::binary-size(10), "T", _hour::binary-size(2), _rest::binary>>),
    do: Date.from_iso8601!(date)

  defp value_date(:day, value), do: Date.from_iso8601!(value)

  defp value_date(:month, <<year::binary-size(4), "-", month::binary-size(2)>>),
    do: Date.new!(String.to_integer(year), String.to_integer(month), 1)

  defp value_date(:year, value), do: Date.new!(String.to_integer(value), 1, 1)

  defp window_label(:hour, <<date::binary-size(10), "T", hour::binary-size(2), rest::binary>>) do
    date
    |> Date.from_iso8601!()
    |> Calendar.strftime("%b %-d")
    |> then(&"#{&1} #{hour}:00#{hour_offset_label(rest)}")
  end

  defp window_label(:day, value),
    do: value |> Date.from_iso8601!() |> Calendar.strftime("%b %-d")

  defp window_label(:month, value), do: :month |> value_date(value) |> Calendar.strftime("%b %Y")
  defp window_label(:year, value), do: value

  defp window_range(:hour, <<date::binary-size(10), "T", hour::binary-size(2), rest::binary>>) do
    date
    |> Date.from_iso8601!()
    |> Calendar.strftime("%b %-d, %Y")
    |> then(&"#{&1} #{hour}:00#{hour_offset_label(rest)}")
  end

  defp window_range(:day, value),
    do: value |> Date.from_iso8601!() |> Calendar.strftime("%b %-d, %Y")

  defp window_range(:month, value),
    do: :month |> value_date(value) |> Calendar.strftime("%B %Y")

  defp window_range(:year, value), do: value

  defp hour_offset_label(""), do: ""
  defp hour_offset_label(offset), do: " #{offset}"

  # The period the latest run landed in has no per-period evidence of its own until that
  # evidence is written, so without this the newest run carries no label at all.
  defp maybe_put_latest_run(
         %{latest_run_id: nil} = window,
         latest_freshness,
         latest_run,
         value,
         value
       ),
       do: Map.put(window, :latest_run_id, Status.latest_run_id(latest_freshness, latest_run))

  defp maybe_put_latest_run(window, _latest_freshness, _latest_run, _value, _latest_run_value),
    do: window

  defp field(value, key) when is_map(value) do
    case Map.fetch(value, key) do
      {:ok, field_value} -> field_value
      :error -> Map.get(value, Atom.to_string(key))
    end
  end

  defp normalize_map(value) when is_map(value), do: value
  defp normalize_map(_value), do: %{}

  defp format_month(year, month), do: "#{year}-#{pad2(month)}"
  defp pad2(value), do: value |> Integer.to_string() |> String.pad_leading(2, "0")
end
