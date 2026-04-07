defmodule Favn.FreshnessTest do
  use ExUnit.Case

  defmodule FreshnessAssets do
    use Favn.Assets

    @asset true
    @window Favn.Window.daily()
    def daily(_ctx), do: :ok
  end

  setup do
    state = Favn.TestSetup.capture_state()
    :ok = Favn.TestSetup.setup_asset_modules([FreshnessAssets], reload_graph?: true)
    :ok = Favn.TestSetup.clear_memory_storage_adapter()

    on_exit(fn ->
      Favn.TestSetup.restore_state(state, reload_graph?: true)
    end)

    :ok
  end

  test "check_asset_freshness/2 returns missing when no persisted success exists" do
    key =
      Favn.Window.Key.new!(
        :day,
        DateTime.from_naive!(~N[2025-01-10 00:00:00], "Etc/UTC"),
        "Etc/UTC"
      )

    assert {:ok, result} =
             Favn.check_asset_freshness({FreshnessAssets, :daily},
               window_key: key,
               max_age_seconds: 60
             )

    assert result.status == :missing
  end

  test "check_asset_freshness/2 returns fresh/stale based on max_age_seconds" do
    range = %{
      kind: :day,
      start_at: DateTime.from_naive!(~N[2025-01-10 00:00:00], "Etc/UTC"),
      end_at: DateTime.from_naive!(~N[2025-01-11 00:00:00], "Etc/UTC"),
      timezone: "Etc/UTC"
    }

    assert {:ok, run_id} = Favn.backfill_asset({FreshnessAssets, :daily}, range: range)
    assert {:ok, run} = Favn.await_run(run_id)
    [node_key] = run.plan.target_node_keys
    {_, key} = node_key

    assert {:ok, fresh} =
             Favn.check_asset_freshness({FreshnessAssets, :daily},
               window_key: key,
               now: DateTime.add(run.finished_at, 10, :second),
               max_age_seconds: 60
             )

    assert fresh.status == :fresh

    assert {:ok, stale} =
             Favn.check_asset_freshness({FreshnessAssets, :daily},
               window_key: key,
               now: DateTime.add(run.finished_at, 300, :second),
               max_age_seconds: 60
             )

    assert stale.status == :stale
  end

  test "missing_asset_windows/3 returns unmaterialized node keys in range" do
    range = %{
      kind: :day,
      start_at: DateTime.from_naive!(~N[2025-01-10 00:00:00], "Etc/UTC"),
      end_at: DateTime.from_naive!(~N[2025-01-13 00:00:00], "Etc/UTC"),
      timezone: "Etc/UTC"
    }

    assert {:ok, run_id} = Favn.backfill_asset({FreshnessAssets, :daily}, range: range)
    assert {:ok, run} = Favn.await_run(run_id)
    [first_key | _] = run.plan.target_node_keys

    latest = run |> Map.put(:node_results, %{first_key => run.node_results[first_key]})
    assert :ok = Favn.Storage.put_run(latest)

    assert {:ok, missing} = Favn.missing_asset_windows({FreshnessAssets, :daily}, range)
    assert length(missing) == 2
  end
end
