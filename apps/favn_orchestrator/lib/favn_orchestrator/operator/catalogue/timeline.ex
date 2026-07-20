defmodule FavnOrchestrator.Operator.Catalogue.Timeline do
  @moduledoc """
  Builds the bounded refresh and data-coverage timelines for an asset detail.

  Timeline values are derived from validated manifest policy and backend state.
  Invalid persisted kinds or timezones fall back to the explicit daily UTC
  policy instead of crashing an operator read.
  """

  alias Favn.Assets.Planner
  alias Favn.Freshness.Key, as: FreshnessKey
  alias Favn.Freshness.Policy, as: FreshnessPolicy
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.TimePeriod
  alias Favn.Timezone
  alias Favn.Window.Key, as: WindowKey
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

  @doc "Builds timeline fields merged into an operator asset-detail DTO."
  @spec build(
          Version.t(),
          Asset.t(),
          AssetFreshnessState.t() | nil,
          map() | nil,
          [AssetFreshnessState.t()],
          [AssetWindowState.t()],
          %{optional(String.t()) => map()},
          keyword()
        ) :: map()
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
    {refresh_kind, refresh_timezone} = refresh_policy(opts)
    {coverage_kind, _coverage_timezone} = coverage_policy(asset)
    freshness_policy = timeline_freshness_policy(asset)

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
        runs_by_id,
        opts
      )

    freshness_timeline =
      freshness_timeline(
        version,
        asset,
        latest_freshness,
        latest_run,
        freshness_states,
        freshness_policy,
        opts
      )

    {freshness_timeline_label, freshness_cadence_label} =
      freshness_labels(freshness_policy)

    {refresh_timeline_label, refresh_cadence_label} =
      if opts[:run_context_status] == :ambiguous do
        {"Run context required", "Select a pipeline context"}
      else
        {kind_label(refresh_kind, "run anchors"),
         "#{kind_label(refresh_kind, "run anchors")} #{refresh_timezone}"}
      end

    %{
      refresh_timeline_label: refresh_timeline_label,
      refresh_cadence_label: refresh_cadence_label,
      freshness_timeline_label: freshness_timeline_label,
      freshness_cadence_label: freshness_cadence_label,
      data_coverage_timeline_label: kind_label(coverage_kind, "data windows"),
      refresh_timeline: refresh_timeline,
      freshness_timeline: freshness_timeline,
      data_coverage_timeline: data_coverage_timeline,
      has_freshness_timeline?: not is_nil(freshness_timeline),
      has_data_windows?: not is_nil(data_coverage_timeline),
      timeline: data_coverage_timeline || refresh_timeline
    }
  end

  defp data_coverage_timeline(
         %{window: nil},
         _latest_freshness,
         _latest_run,
         _freshness_states,
         _asset_window_states,
         _runs_by_id,
         _opts
       ),
       do: nil

  defp data_coverage_timeline(
         asset,
         latest_freshness,
         latest_run,
         freshness_states,
         asset_window_states,
         runs_by_id,
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
      date = value_date(kind, value)
      window_state = Map.get(window_states, value)
      window_freshness = Map.get(freshness_states, value)

      %{
        id: window_id(kind, value),
        kind: kind,
        value: value,
        timezone: timezone,
        label: window_label(kind, value),
        date: date,
        range: window_range(kind, value),
        status:
          coverage_status(
            window_state,
            window_freshness,
            latest_freshness,
            latest_run,
            value,
            latest_run_value
          ),
        latest_run_id:
          window_latest_run_id(window_state) || Status.latest_run_id(window_freshness, nil),
        latest_run_status:
          window_latest_run_status(window_state) ||
            Status.latest_run_status(window_freshness, nil),
        latest_run_at:
          window_latest_run_at(window_state) || Status.latest_run_at(window_freshness, nil),
        run_label: "Run this window"
      }
      |> put_window_run_state(asset)
      |> maybe_put_latest_run(latest_freshness, latest_run, value, latest_run_value)
      |> Map.put(:source, :data_coverage_timeline)
      |> Map.put(
        :default_run_config,
        default_run_config(:data_coverage_timeline, kind, value, timezone)
      )
      |> put_latest_run_config(runs_by_id)
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
        id: "refresh:#{kind}:#{value}",
        source: :refresh_timeline,
        kind: kind,
        value: value,
        timezone: timezone,
        label: window_label(kind, value),
        date: value_date(kind, value),
        range: window_range(kind, value),
        status: refresh_status(freshness, latest_freshness, latest_run, value, latest_run_value),
        latest_run_id: Status.latest_run_id(freshness, nil),
        latest_run_status: Status.latest_run_status(freshness, nil),
        latest_run_at: Status.latest_run_at(freshness, nil),
        run_enabled?: true,
        run_disabled_reason: nil,
        run_label: "Run asset",
        default_run_config: default_run_config(:refresh_timeline, kind, value, timezone)
      }
      |> maybe_put_latest_run(latest_freshness, latest_run, value, latest_run_value)
      |> put_latest_run_config(runs_by_id)
    end
  end

  defp freshness_timeline(
         _version,
         _asset,
         _latest_freshness,
         _latest_run,
         _freshness_states,
         nil,
         _opts
       ),
       do: nil

  defp freshness_timeline(
         version,
         asset,
         latest_freshness,
         latest_run,
         freshness_states,
         %{kind: kind, timezone: timezone} = freshness_policy,
         opts
       ) do
    states_by_value =
      freshness_states_by_calendar_value(asset, freshness_states, kind, timezone)

    for value <- freshness_period_values(kind, timezone, latest_freshness, latest_run, opts) do
      states_by_identity = Map.get(states_by_value, value, %{})

      {status, states} =
        freshness_period_evidence(
          version,
          asset,
          freshness_policy,
          value,
          states_by_identity,
          opts
        )

      latest_state =
        Enum.max_by(
          states,
          &DateTime.to_unix(&1.updated_at, :microsecond),
          &>=/2,
          fn -> nil end
        )

      %{
        id: "freshness:#{kind}:#{value}",
        source: :freshness_timeline,
        kind: kind,
        value: value,
        timezone: timezone,
        label: window_label(kind, value),
        date: value_date(kind, value),
        range: window_range(kind, value),
        status: status,
        latest_run_id: Status.latest_run_id(latest_state, nil),
        latest_run_status: Status.latest_run_status(latest_state, nil),
        latest_run_at: Status.latest_run_at(latest_state, nil),
        run_enabled?: false,
        run_disabled_reason: :freshness_period_not_runnable,
        run_label: nil
      }
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

  defp timeline_freshness_policy(%Asset{
         freshness: %FreshnessPolicy{mode: :window_success},
         window: %WindowSpec{
           refresh_from: kind,
           timezone: timezone
         }
       })
       when not is_nil(kind),
       do: %{kind: kind, timezone: timezone, identity: :window_refresh}

  defp timeline_freshness_policy(%Asset{
         freshness: %FreshnessPolicy{mode: :calendar_period, kind: kind, timezone: timezone}
       }),
       do: %{kind: kind, timezone: timezone, identity: :calendar}

  defp timeline_freshness_policy(_asset), do: nil

  defp freshness_labels(nil), do: {nil, nil}

  defp freshness_labels(%{kind: kind, timezone: timezone}) do
    {
      kind_label(kind, "freshness periods"),
      "#{kind_label(kind, "freshness")} #{timezone}"
    }
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

  defp kind_label(:hour, suffix), do: "Hourly #{suffix}"
  defp kind_label(:day, suffix), do: "Daily #{suffix}"
  defp kind_label(:month, suffix), do: "Monthly #{suffix}"
  defp kind_label(:year, suffix), do: "Yearly #{suffix}"

  defp refresh_status(
         %AssetFreshnessState{status: :ok},
         _latest_freshness,
         _latest_run,
         _value,
         _latest_run_value
       ),
       do: :fresh

  defp refresh_status(
         %AssetFreshnessState{status: :error},
         _latest_freshness,
         _latest_run,
         _value,
         _latest_run_value
       ),
       do: :failed

  defp refresh_status(
         %AssetFreshnessState{status: status},
         _latest_freshness,
         _latest_run,
         _value,
         _latest_run_value
       )
       when status in [:running, :pending],
       do: :running

  defp refresh_status(nil, latest_freshness, latest_run, value, value),
    do: refresh_status_from_latest(latest_freshness, latest_run)

  defp refresh_status(nil, _latest_freshness, _latest_run, _value, _latest_run_value),
    do: :missing

  defp refresh_status_from_latest(%AssetFreshnessState{status: :ok}, _run), do: :fresh
  defp refresh_status_from_latest(%AssetFreshnessState{status: :error}, _run), do: :failed

  defp refresh_status_from_latest(_freshness, %{status: status})
       when status in [:running, :pending],
       do: :running

  defp refresh_status_from_latest(_freshness, %{status: :ok}), do: :fresh

  defp refresh_status_from_latest(_freshness, %{status: status})
       when status in [:partial, :error, :cancelled, :timed_out],
       do: :failed

  defp refresh_status_from_latest(_freshness, _run), do: :unknown

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

  defp put_window_run_state(%{id: window_id} = window, asset) do
    with {:ok, window_request} <- WindowSelection.data_coverage_request(window_id),
         {:ok, _anchor_window} <- WindowSelection.resolve(asset, window_request) do
      window
      |> Map.put(:run_enabled?, true)
      |> Map.put(:run_disabled_reason, nil)
    else
      {:error, reason} ->
        window
        |> Map.put(:run_enabled?, false)
        |> Map.put(:run_disabled_reason, run_disabled_reason(reason))
    end
  end

  defp run_disabled_reason({:window_request_without_policy, _kind}),
    do: :asset_has_no_window_policy

  defp run_disabled_reason(_reason), do: :invalid_window

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

  defp freshness_states_by_calendar_value(
         asset,
         freshness_states,
         timeline_kind,
         timeline_timezone
       ) do
    asset_ref_string = Targets.ref_string(asset.ref)

    freshness_states
    |> Enum.reduce(%{}, fn
      %AssetFreshnessState{} = state, acc ->
        with ^asset_ref_string <- AssetFreshness.ref_string(state),
             {:ok, {value, identity}} <-
               calendar_value(
                 state.freshness_key,
                 timeline_kind,
                 timeline_timezone
               ) do
          Map.update(acc, value, %{identity => state}, fn states ->
            Map.update(states, identity, state, &newer_state(&1, state))
          end)
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

  defp calendar_value(freshness_key, timeline_kind, timeline_timezone) do
    case FreshnessKey.parse(freshness_key) do
      {:ok, {:calendar, ^timeline_kind, ^timeline_timezone, value}} ->
        {:ok, {value, :calendar}}

      {:ok, {:window_refresh, window_key, ^timeline_kind, ^timeline_timezone, value}} ->
        {:ok, {value, WindowKey.encode(window_key)}}

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

  defp freshness_period_evidence(
         version,
         asset,
         freshness_policy,
         value,
         states_by_identity,
         opts
       ) do
    cond do
      freshness_policy.identity == :window_refresh and
          opts[:run_context_status] == :ambiguous ->
        {:unknown, []}

      map_size(states_by_identity) == 0 ->
        {:missing, []}

      true ->
        case expected_freshness_identities(
               version,
               asset,
               freshness_policy,
               value,
               opts[:asset_run_context]
             ) do
          {:ok, expected_identities} ->
            expected_count = MapSet.size(expected_identities)

            if expected_count > 0 do
              states =
                expected_identities
                |> Enum.map(&Map.get(states_by_identity, &1))
                |> Enum.reject(&is_nil/1)

              {freshness_period_status(states, expected_count), states}
            else
              {:unknown, []}
            end

          _error ->
            {:unknown, []}
        end
    end
  end

  defp expected_freshness_identities(
         _version,
         _asset,
         %{identity: :calendar},
         _value,
         _run_context
       ),
       do: {:ok, MapSet.new([:calendar])}

  defp expected_freshness_identities(
         _version,
         asset,
         %{identity: :window_refresh, kind: kind, timezone: timezone},
         value,
         %AssetRunContext{} = run_context
       ) do
    with {:ok, period} <- calendar_period(kind, value, timezone),
         {:ok, anchor_window} <- AssetRunContext.anchor(run_context, period.start_at),
         {:ok, plan} <-
           Planner.plan(asset.ref,
             dependencies: :none,
             planning_index: run_context.index.planning_index,
             anchor_window: anchor_window
           ) do
      identities =
        Enum.reduce(plan.target_node_keys, MapSet.new(), fn
          {ref, window_key}, acc when ref == asset.ref and is_map(window_key) ->
            case WindowKey.validate(window_key) do
              :ok -> MapSet.put(acc, WindowKey.encode(window_key))
              {:error, _reason} -> acc
            end

          _node_key, acc ->
            acc
        end)

      if MapSet.size(identities) > 0,
        do: {:ok, identities},
        else: {:error, :freshness_target_windows_not_found}
    else
      _error -> {:error, :freshness_target_windows_not_found}
    end
  end

  defp expected_freshness_identities(
         _version,
         _asset,
         %{identity: :window_refresh},
         _value,
         _run_context
       ),
       do: {:error, :asset_run_context_required}

  defp freshness_period_values(kind, timezone, latest_freshness, latest_run, opts) do
    {:ok, selected_period} = selected_period(kind, timezone, latest_freshness, latest_run, opts)

    for offset <- -(@period_count - 1)..0 do
      {:ok, start_at} = TimePeriod.shift(selected_period.start_at, kind, offset)
      calendar_period_value(kind, timezone, start_at)
    end
  end

  defp selected_period(kind, timezone, latest_freshness, latest_run, opts) do
    case {opts[:now], opts[:today], Status.latest_run_at(latest_freshness, latest_run)} do
      {%DateTime{} = now, _today, _latest_run_at} ->
        TimePeriod.current(kind, now, timezone)

      {_now, %Date{} = date, _latest_run_at} ->
        TimePeriod.bounds(kind, value_from_date(kind, date), timezone)

      {_now, _today, %DateTime{} = datetime} ->
        TimePeriod.current(kind, datetime, timezone)

      _other ->
        TimePeriod.bounds(kind, value_from_date(kind, Date.utc_today()), timezone)
    end
  end

  defp calendar_period_value(kind, timezone, %DateTime{} = datetime) do
    {:ok, {:calendar, ^kind, ^timezone, value}} =
      kind
      |> FreshnessKey.calendar!(timezone, datetime)
      |> FreshnessKey.parse()

    value
  end

  defp calendar_period(:hour, <<local_hour::binary-size(13), offset::binary>>, timezone)
       when byte_size(offset) > 0 do
    with {:ok, datetime, _utc_offset} <-
           DateTime.from_iso8601(local_hour <> ":00:00" <> offset) do
      TimePeriod.current(:hour, datetime, timezone)
    end
  end

  defp calendar_period(kind, value, timezone), do: TimePeriod.bounds(kind, value, timezone)

  defp freshness_period_status(states, expected_window_count) do
    statuses = Enum.map(states, &Status.catalogue(&1, nil))

    cond do
      :failed in statuses -> :failed
      :running in statuses -> :running
      length(states) < expected_window_count -> :missing
      Enum.all?(statuses, &(&1 == :healthy)) -> :fresh
      true -> :unknown
    end
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

  defp coverage_status(
         %AssetWindowState{status: :ok},
         _window_freshness,
         _latest_freshness,
         _latest_run,
         _value,
         _latest_run_value
       ),
       do: :covered

  defp coverage_status(
         %AssetWindowState{status: status},
         _window_freshness,
         _latest_freshness,
         _latest_run,
         _value,
         _latest_run_value
       )
       when status in [:running, :pending],
       do: :running

  defp coverage_status(
         %AssetWindowState{status: status},
         _window_freshness,
         _latest_freshness,
         _latest_run,
         _value,
         _latest_run_value
       )
       when status in [:partial, :error, :cancelled, :timed_out],
       do: :failed

  defp coverage_status(
         _window_state,
         window_freshness,
         latest_freshness,
         latest_run,
         value,
         latest_run_value
       ),
       do:
         timeline_status(window_freshness, latest_freshness, latest_run, value, latest_run_value)

  defp window_latest_run_id(%AssetWindowState{latest_run_id: run_id}) when is_binary(run_id),
    do: run_id

  defp window_latest_run_id(_state), do: nil

  defp window_latest_run_status(%AssetWindowState{status: status}) when not is_nil(status),
    do: status

  defp window_latest_run_status(_state), do: nil

  defp window_latest_run_at(%AssetWindowState{updated_at: %DateTime{} = updated_at}),
    do: updated_at

  defp window_latest_run_at(_state), do: nil

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

  defp window_id(kind, value), do: "window:#{kind}:#{value}"

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

  defp timeline_status(%AssetFreshnessState{} = freshness, _latest, _run, _value, _latest_value),
    do: coverage_status_from_catalogue(Status.catalogue(freshness, nil))

  defp timeline_status(nil, latest_freshness, latest_run, value, value),
    do: coverage_status_from_catalogue(Status.catalogue(latest_freshness, latest_run))

  defp timeline_status(nil, _latest_freshness, _latest_run, _value, _latest_run_value),
    do: :missing

  defp coverage_status_from_catalogue(:healthy), do: :covered
  defp coverage_status_from_catalogue(:failed), do: :failed
  defp coverage_status_from_catalogue(:running), do: :running
  defp coverage_status_from_catalogue(_status), do: :unknown

  defp maybe_put_latest_run(window, latest_freshness, latest_run, value, value) do
    window
    |> put_if_missing(:latest_run_id, Status.latest_run_id(latest_freshness, latest_run))
    |> put_if_missing(:latest_run_status, Status.latest_run_status(latest_freshness, latest_run))
    |> put_if_missing(:latest_run_at, Status.latest_run_at(latest_freshness, latest_run))
  end

  defp maybe_put_latest_run(window, _latest_freshness, _latest_run, _value, _latest_run_value),
    do: window

  defp put_if_missing(map, key, value) do
    if is_nil(Map.get(map, key)), do: Map.put(map, key, value), else: map
  end

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
