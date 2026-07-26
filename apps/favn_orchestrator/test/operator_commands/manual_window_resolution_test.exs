defmodule FavnOrchestrator.OperatorCommands.ManualWindowResolutionTest do
  use ExUnit.Case, async: true

  alias Favn.Coverage.Effective
  alias Favn.Coverage.Spec, as: CoverageSpec
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Manifest.Pipeline
  alias Favn.Window.Policy
  alias Favn.Window.Request
  alias Favn.Window.Spec, as: WindowSpec
  alias FavnOrchestrator.OperatorCommands.ManualWindowResolution

  @asset_ref {MyApp.Raw.Events, :events}
  @delayed_asset_ref {MyApp.Raw.Accounts, :accounts}
  @current_asset_ref {MyApp.Raw.Users, :users}

  test "missing input pins the latest complete window after selected asset availability" do
    evaluated_at = ~U[2026-07-01 00:30:00Z]
    pipeline = pipeline(:current_period)
    index = index_with_delay(3_600)

    assert {:ok, resolution} =
             ManualWindowResolution.resolve(
               index,
               pipeline,
               [@asset_ref],
               nil,
               evaluated_at
             )

    assert resolution.mode == :latest_complete
    assert resolution.evaluated_at == evaluated_at
    assert resolution.availability_delay_seconds == 3_600
    assert [anchor] = resolution.selection.requested_anchors
    assert anchor.start_at == ~U[2026-06-29 00:00:00Z]
    assert anchor.end_at == ~U[2026-06-30 00:00:00Z]
  end

  test "an explicit request remains exact and ignores availability delay" do
    assert {:ok, request} = Request.parse("day:2026-06-20")

    assert {:ok, resolution} =
             ManualWindowResolution.resolve(
               index_with_delay(86_400),
               pipeline(:previous_complete_period),
               [@asset_ref],
               request,
               ~U[2026-07-01 00:30:00Z]
             )

    assert resolution.mode == :explicit
    assert resolution.availability_delay_seconds == 0
    assert [anchor] = resolution.selection.requested_anchors
    assert anchor.start_at == ~U[2026-06-20 00:00:00Z]
  end

  test "uses the greatest latest-closed delay across selected assets" do
    index =
      %Index{
        assets_by_ref: %{
          @asset_ref => asset(@asset_ref, coverage(:latest_closed, 1_800)),
          @delayed_asset_ref => asset(@delayed_asset_ref, coverage(:latest_closed, 7_200)),
          @current_asset_ref => asset(@current_asset_ref, coverage(:current, 0))
        }
      }

    assert {:ok, resolution} =
             ManualWindowResolution.resolve(
               index,
               pipeline(:previous_complete_period),
               [@asset_ref, @delayed_asset_ref, @current_asset_ref],
               nil,
               ~U[2026-07-01 01:30:00Z]
             )

    assert resolution.availability_delay_seconds == 7_200
    assert [anchor] = resolution.selection.requested_anchors
    assert anchor.start_at == ~U[2026-06-29 00:00:00Z]
  end

  test "a non-windowed pipeline keeps full-load behavior" do
    pipeline = %Pipeline{module: MyApp.Pipelines.All, name: :all}

    assert {:ok, resolution} =
             ManualWindowResolution.resolve(
               %Index{},
               pipeline,
               [],
               nil,
               ~U[2026-07-01 00:30:00Z]
             )

    assert resolution.mode == :unwindowed
    assert resolution.selection == nil
    assert resolution.availability_delay_seconds == 0
  end

  defp pipeline(anchor) do
    %Pipeline{
      module: MyApp.Pipelines.Daily,
      name: :daily,
      window: Policy.new!(:daily, anchor: anchor, timezone: "Etc/UTC")
    }
  end

  defp index_with_delay(delay_seconds) do
    %Index{
      assets_by_ref: %{
        @asset_ref => asset(@asset_ref, coverage(:latest_closed, delay_seconds))
      }
    }
  end

  defp asset(ref, coverage) do
    %Asset{ref: ref, window: WindowSpec.new!(:day), coverage: coverage}
  end

  defp coverage(through, delay_seconds) do
    opts = [from: ~D[2026-01-01], through: through]

    opts =
      if through == :latest_closed,
        do: Keyword.put(opts, :availability_delay, {:seconds, delay_seconds}),
        else: opts

    Effective.resolve(
      CoverageSpec.new!(opts),
      WindowSpec.new!(:day, timezone: "Etc/UTC"),
      nil
    )
    |> elem(1)
  end
end
