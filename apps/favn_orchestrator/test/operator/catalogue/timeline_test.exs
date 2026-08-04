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
  alias FavnOrchestrator.AssetRunContext
  alias FavnOrchestrator.Operator.Catalogue.AssetFreshness
  alias FavnOrchestrator.Operator.Catalogue.Timeline

  @asset_ref {__MODULE__.Orders, :asset}
  @now ~U[2026-07-17 10:00:00Z]

  describe "the period an asset is due for" do
    test "comes from the run anchor policy, not the asset's own window grain" do
      asset = asset_fixture()
      states = [freshness_state(:june, :ok), freshness_state(:july, :ok)]

      resolved =
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

      # The asset writes monthly windows and is refreshed on a monthly anchor, so both
      # agree here. What matters is which one the dialog opens on: the anchor, because
      # that is what a run submits against.
      assert resolved.default_run_config.source == :refresh_timeline
      assert resolved.default_run_config.kind == :month
      assert resolved.default_run_config.value == "2026-07"
      assert resolved.default_run_config.timezone == "Europe/Oslo"
      assert resolved.default_run_config.dependencies == :all
      assert resolved.default_run_config.refresh == :auto
    end

    test "falls back to the schedule timezone when the pipeline declares no window" do
      asset = asset_fixture()

      resolved =
        Timeline.build(version_fixture(asset, nil), asset, nil, nil, [], [], %{}, now: @now)

      assert resolved.default_run_config.kind == :day
      assert resolved.default_run_config.timezone == "Europe/Oslo"
    end

    test "prefers the configuration a failed run used, so a retry repeats it" do
      asset = asset_fixture()
      state = freshness_state(:july, :error)

      run = %{
        id: "run_july",
        metadata: %{
          asset_dependencies: :none,
          refresh_policy: %{mode: :force_assets, include_upstream?: true}
        }
      }

      resolved =
        Timeline.build(
          version_fixture(asset),
          asset,
          state,
          nil,
          [state],
          [],
          %{"run_july" => run},
          now: @now
        )

      assert resolved.default_run_config.dependencies == :none
      assert resolved.default_run_config.refresh == :force_selected_upstream
    end

    test "is absent for an asset no pipeline unambiguously owns" do
      asset = asset_fixture()
      states = [freshness_state(:june, :ok), freshness_state(:july, :ok)]
      version = multi_pipeline_version_fixture(asset, :declared)

      resolved =
        Timeline.build(version, asset, List.last(states), nil, states, [], %{}, now: @now)

      # Two pipelines could own it, so there is no anchor to be due for. Offering one
      # would be a guess an operator could not check.
      assert resolved.default_run_config == nil

      freshness = AssetFreshness.detail(asset, version, states, now: @now)
      assert freshness.state == :unknown
      assert [%{kind: :run_context_required}] = freshness.reasons
    end

    test "does not depend on the order the pipelines happen to be declared in" do
      asset = asset_fixture()
      states = [freshness_state(:june, :ok), freshness_state(:july, :ok)]

      declared =
        Timeline.build(
          multi_pipeline_version_fixture(asset, :declared),
          asset,
          List.last(states),
          nil,
          states,
          [],
          %{},
          now: @now
        )

      reversed =
        Timeline.build(
          multi_pipeline_version_fixture(asset, :reversed),
          asset,
          List.last(states),
          nil,
          states,
          [],
          %{},
          now: @now
        )

      assert declared == reversed
    end

    test "follows the run context it is given rather than choosing one" do
      asset = asset_fixture()
      states = [freshness_state(:june, :ok), freshness_state(:july, :ok)]
      version = multi_pipeline_version_fixture(asset, :declared)

      assert {:ok, contexts} = AssetRunContext.list(version, asset)
      scheduled = Enum.find(contexts, &(&1.pipeline.name == :scheduled_current))
      manual = Enum.find(contexts, &(&1.pipeline.name == :manual_previous))

      assert resolve(version, asset, states, scheduled).default_run_config
             |> Map.take([:value, :timezone]) == %{value: "2026-07", timezone: "Europe/Oslo"}

      assert resolve(version, asset, states, manual).default_run_config
             |> Map.take([:value, :timezone]) == %{value: "2026-06", timezone: "Etc/UTC"}
    end
  end

  describe "the period each run wrote" do
    test "labels a run from the asset's own data window, not its refresh anchor" do
      asset = asset_fixture()
      states = [freshness_state(:june, :ok), freshness_state(:july, :ok)]

      resolved =
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

      assert %{kind: :month, value: "2026-07", label: "Jul 2026", range: "July 2026"} =
               resolved.run_windows["run_july"]

      assert %{value: "2026-06"} = resolved.run_windows["run_june"]
    end

    # A `freshness :daily` policy on a monthly-windowed asset records its evidence under
    # `calendar:day:<tz>:<date>`, which a walk of months matches nowhere. The walks reach
    # only the *latest* such run, through `maybe_put_latest_run/5`, so every earlier one
    # rendered with no period at all.
    test "labels every run whose only evidence is a calendar-period freshness key" do
      asset = calendar_freshness_asset()

      june = calendar_freshness_state(~D[2026-06-11], :ok, "june")
      july = calendar_freshness_state(~D[2026-07-16], :ok, "july")

      resolved =
        Timeline.build(
          version_fixture(asset),
          asset,
          july,
          nil,
          [june, july],
          [],
          %{},
          now: @now
        )

      # Both, and both named by the period the asset writes rather than the day its
      # freshness was evaluated on — a monthly asset's run wrote July, not 16 July.
      assert %{kind: :month, value: "2026-07", label: "Jul 2026"} =
               resolved.run_windows["run_july"]

      assert %{kind: :month, value: "2026-06", label: "Jun 2026"} =
               resolved.run_windows["run_june"]
    end

    test "has no entry for a run outside the periods it walked" do
      asset = asset_fixture()
      state = freshness_state(:may, :ok)

      resolved =
        Timeline.build(version_fixture(asset), asset, state, nil, [state], [], %{}, now: @now)

      # May is inside the thirty-period walk, so it is labelled; a run id that never
      # appears is simply absent rather than labelled with something invented.
      assert Map.has_key?(resolved.run_windows, "run_may")
      refute Map.has_key?(resolved.run_windows, "run_never_happened")
    end
  end

  describe "has_data_windows?" do
    test "is true for an asset that writes one window per period" do
      asset = asset_fixture()

      resolved =
        Timeline.build(version_fixture(asset), asset, nil, nil, [], [], %{}, now: @now)

      assert resolved.has_data_windows?
    end

    test "is false for an asset that replaces its whole relation" do
      asset = %{asset_fixture() | window: nil}

      resolved =
        Timeline.build(version_fixture(asset), asset, nil, nil, [], [], %{}, now: @now)

      refute resolved.has_data_windows?
    end
  end

  # Composite calendar freshness — a calendar period is fresh only when every window
  # expanded into it succeeded — is `AssetFreshness`'s answer, not the period walk's.
  # These cover it here because the walk used to answer it too, and a change that moves
  # the rule back should have to break something.
  describe "composite calendar freshness" do
    test "a partially covered calendar period is not fresh" do
      asset = asset_fixture()
      version = version_fixture(asset)

      header = AssetFreshness.detail(asset, version, [freshness_state(:june, :ok)], now: @now)

      refute header.state == :fresh
    end

    test "a coarse anchor expanded into finer windows requires every one of them" do
      asset = daily_asset_fixture()
      version = version_fixture(asset)

      start_at =
        DateTime.new!(~D[2026-07-17], ~T[00:00:00], "Europe/Oslo", Favn.Timezone.database!())

      state = freshness_state_for(:day, start_at, ~D[2026-07-17], :ok, "one_day")
      header = AssetFreshness.detail(asset, version, [state], now: @now)

      refute header.state == :fresh
    end
  end

  defp resolve(version, asset, states, run_context) do
    Timeline.build(
      version,
      asset,
      List.last(states),
      nil,
      states,
      [],
      %{},
      now: @now,
      asset_run_context: run_context,
      run_context_status: :selected
    )
  end

  defp asset_fixture do
    %Asset{
      ref: @asset_ref,
      module: elem(@asset_ref, 0),
      name: elem(@asset_ref, 1),
      window:
        WindowSpec.new!(:month,
          refresh_from: :day,
          required: true,
          timezone: "Europe/Oslo"
        ),
      freshness: FreshnessPolicy.from_value!(window_success: true)
    }
  end

  # A monthly window with a daily calendar freshness policy: the grain the asset writes
  # and the grain its freshness is recorded under disagree, which is the whole point.
  defp calendar_freshness_asset do
    %{asset_fixture() | freshness: FreshnessPolicy.from_value!(:daily)}
  end

  defp calendar_freshness_state(%Date{} = period, status, id) do
    freshness_key = FreshnessKey.calendar!(:day, "Europe/Oslo", period)
    run_id = "run_#{id}"

    {:ok, state} =
      AssetFreshnessState.new(%{
        asset_ref_module: elem(@asset_ref, 0),
        asset_ref_name: elem(@asset_ref, 1),
        freshness_key: freshness_key,
        evidence_generation_id: "ag_test",
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
         window_policy \\ WindowPolicy.new!(:monthly,
           anchor: :current_period,
           lookback: 1,
           timezone: "Europe/Oslo"
         )
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
      manifest_version_id: "mv_timeline_composite_#{asset.window && asset.window.kind}",
      content_hash: "sha256:timeline-composite-#{asset.window && asset.window.kind}",
      manifest: %Manifest{
        assets: [asset],
        pipelines: [pipeline],
        schedules: [schedule],
        graph: graph
      }
    }
  end

  defp multi_pipeline_version_fixture(asset, order) do
    {:ok, graph} = Graph.build([asset])

    schedule = %Schedule{
      module: __MODULE__.Schedules,
      name: :daily,
      ref: {__MODULE__.Schedules, :daily},
      cron: "0 8 * * *",
      timezone: "Europe/Oslo"
    }

    manual = %Pipeline{
      module: __MODULE__.ManualPipeline,
      name: :manual_previous,
      selectors: [{:asset, @asset_ref}],
      window:
        WindowPolicy.new!(:monthly,
          anchor: :previous_complete_period,
          timezone: "Etc/UTC"
        )
    }

    scheduled = %Pipeline{
      module: __MODULE__.ScheduledPipeline,
      name: :scheduled_current,
      selectors: [{:asset, @asset_ref}],
      schedule: {:ref, schedule.ref},
      window:
        WindowPolicy.new!(:monthly,
          anchor: :current_period,
          timezone: "Europe/Oslo"
        )
    }

    pipelines = if order == :reversed, do: [scheduled, manual], else: [manual, scheduled]

    %Version{
      manifest_version_id: "mv_timeline_multi",
      content_hash: "sha256:timeline-multi",
      manifest: %Manifest{
        assets: [asset],
        pipelines: pipelines,
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
        evidence_generation_id: "ag_test",
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
