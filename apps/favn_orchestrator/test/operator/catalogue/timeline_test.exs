defmodule FavnOrchestrator.Operator.Catalogue.TimelineTest do
  use ExUnit.Case, async: true

  alias Favn.Freshness.Key, as: FreshnessKey
  alias Favn.Freshness.Policy, as: FreshnessPolicy
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.Version
  alias Favn.Window.Key, as: WindowKey
  alias Favn.Window.Policy, as: WindowPolicy
  alias Favn.Window.Spec, as: WindowSpec
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Operator.Catalogue.AssetFreshness
  alias FavnOrchestrator.Operator.Catalogue.Timeline

  @asset_ref {__MODULE__.Orders, :asset}
  @now ~U[2026-07-17 10:00:00Z]

  test "separates run anchors, exact data windows, and composite calendar freshness" do
    asset = asset_fixture()
    states = [freshness_state(:june, :ok), freshness_state(:july, :ok)]

    detail =
      Timeline.build(
        version_fixture(asset),
        asset,
        List.last(states),
        nil,
        states,
        [],
        %{},
        now: @now
      )

    run_anchor = List.last(detail.refresh_timeline)
    june = Enum.find(detail.data_coverage_timeline, &(&1.value == "2026-06"))
    july = Enum.find(detail.data_coverage_timeline, &(&1.value == "2026-07"))
    freshness = List.last(detail.freshness_timeline)

    assert detail.refresh_timeline_label == "Monthly run anchors"
    assert detail.refresh_cadence_label == "Monthly run anchors Europe/Oslo"
    assert run_anchor.kind == :month
    assert run_anchor.timezone == "Europe/Oslo"
    assert run_anchor.default_run_config.source == :refresh_timeline
    assert run_anchor.default_run_config.kind == :month

    assert %{status: :covered} = june
    assert %{status: :covered} = july

    assert detail.freshness_timeline_label == "Daily freshness periods"
    assert detail.freshness_cadence_label == "Daily freshness Europe/Oslo"
    assert freshness.value == "2026-07-17"
    assert freshness.timezone == "Europe/Oslo"
    assert freshness.status == :fresh
    refute freshness.run_enabled?
  end

  test "does not mark a partial or failed composite calendar period fresh" do
    asset = asset_fixture()
    version = version_fixture(asset)

    partial =
      Timeline.build(
        version,
        asset,
        freshness_state(:june, :ok),
        nil,
        [freshness_state(:june, :ok)],
        [],
        %{},
        now: @now
      )

    assert List.last(partial.freshness_timeline).status == :missing

    header = AssetFreshness.detail(asset, version, [freshness_state(:june, :ok)], now: @now)
    refute header.state == :fresh

    wrong_window_states = [freshness_state(:may, :ok), freshness_state(:june, :ok)]

    wrong_windows =
      Timeline.build(
        version,
        asset,
        List.last(wrong_window_states),
        nil,
        wrong_window_states,
        [],
        %{},
        now: @now
      )

    assert List.last(wrong_windows.freshness_timeline).status == :missing

    failed_states = [freshness_state(:june, :ok), freshness_state(:july, :error)]

    failed =
      Timeline.build(
        version,
        asset,
        List.last(failed_states),
        nil,
        failed_states,
        [],
        %{},
        now: @now
      )

    assert List.last(failed.freshness_timeline).status == :failed
  end

  test "requires every fine-grained asset window expanded from a coarse run anchor" do
    asset = daily_asset_fixture()
    version = version_fixture(asset)

    start_at =
      DateTime.new!(
        ~D[2026-07-17],
        ~T[00:00:00],
        "Europe/Oslo",
        Favn.Timezone.database!()
      )

    state = freshness_state_for(:day, start_at, ~D[2026-07-17], :ok, "one_day")

    detail =
      Timeline.build(
        version,
        asset,
        state,
        nil,
        [state],
        [],
        %{},
        now: @now
      )

    assert List.last(detail.freshness_timeline).status == :missing

    header = AssetFreshness.detail(asset, version, [state], now: @now)
    refute header.state == :fresh
  end

  defp asset_fixture do
    %Asset{
      ref: @asset_ref,
      module: elem(@asset_ref, 0),
      name: elem(@asset_ref, 1),
      window:
        WindowSpec.new!(:month,
          lookback: 1,
          refresh_from: :day,
          required: true,
          timezone: "Europe/Oslo"
        ),
      freshness: FreshnessPolicy.from_value!(window_success: true)
    }
  end

  defp daily_asset_fixture do
    %Asset{
      ref: @asset_ref,
      module: elem(@asset_ref, 0),
      name: elem(@asset_ref, 1),
      window: WindowSpec.new!(:day, refresh_from: :day, timezone: "Europe/Oslo"),
      freshness: FreshnessPolicy.from_value!(window_success: true)
    }
  end

  defp version_fixture(
         asset,
         window_policy \\ WindowPolicy.new!(:monthly, anchor: :current_period)
       ) do
    {:ok, graph} = Graph.build([asset])

    schedule = %Schedule{
      module: __MODULE__.Schedules,
      name: :daily,
      ref: {__MODULE__.Schedules, :daily},
      cron: "0 8 * * *",
      timezone: "Europe/Oslo"
    }

    pipeline = %Pipeline{
      module: __MODULE__.Pipelines,
      name: :monthly,
      selectors: [{:asset, @asset_ref}],
      schedule: {:ref, schedule.ref},
      window: window_policy
    }

    %Version{
      manifest_version_id: "mv_timeline_composite_#{asset.window.kind}",
      content_hash: "sha256:timeline-composite-#{asset.window.kind}",
      manifest: %Manifest{
        assets: [asset],
        pipelines: [pipeline],
        schedules: [schedule],
        graph: graph
      }
    }
  end

  defp freshness_state(month, status) do
    start_at =
      month
      |> month_date()
      |> DateTime.new!(~T[00:00:00], "Europe/Oslo", Favn.Timezone.database!())

    freshness_state_for(:month, start_at, ~D[2026-07-17], status, month)
  end

  defp freshness_state_for(window_kind, start_at, calendar_period, status, id) do
    window_key = WindowKey.new!(window_kind, start_at, "Europe/Oslo")

    refresh_kind = if window_kind == :hour, do: :hour, else: :day

    freshness_key =
      FreshnessKey.window_refresh!(
        window_key,
        refresh_kind,
        "Europe/Oslo",
        calendar_period
      )

    run_id = "run_#{id}"

    {:ok, state} =
      AssetFreshnessState.new(%{
        asset_ref_module: elem(@asset_ref, 0),
        asset_ref_name: elem(@asset_ref, 1),
        freshness_key: freshness_key,
        status: status,
        freshness_version: "#{id}:v1",
        latest_success_run_id: run_id,
        latest_success_node_key: {@asset_ref, id},
        latest_success_at: @now,
        latest_attempt_run_id: run_id,
        latest_attempt_status: status,
        latest_attempt_at: @now,
        updated_at: @now
      })

    state
  end

  defp month_date(:may), do: ~D[2026-05-01]
  defp month_date(:june), do: ~D[2026-06-01]
  defp month_date(:july), do: ~D[2026-07-01]
end
