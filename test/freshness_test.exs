defmodule Favn.FreshnessTest do
  use ExUnit.Case

  alias Favn.Run
  alias Favn.Run.AssetResult
  alias Favn.Window.Key

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
      Key.new!(
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

  test "check_asset_freshness/2 scans full successful history by default" do
    key =
      Key.new!(
        :day,
        DateTime.from_naive!(~N[2025-01-10 00:00:00], "Etc/UTC"),
        "Etc/UTC"
      )

    ref = {FreshnessAssets, :daily}

    stale_at = DateTime.from_naive!(~N[2025-01-10 00:10:00], "Etc/UTC")
    fresh_at = DateTime.from_naive!(~N[2025-01-10 12:00:00], "Etc/UTC")

    stale_run = synthetic_success_run("freshness-stale", ref, key, stale_at)
    fresh_run = synthetic_success_run("freshness-fresh", ref, key, fresh_at)

    assert :ok = Favn.Storage.put_run(stale_run)

    Enum.each(1..220, fn index ->
      assert :ok =
               Favn.Storage.put_run(synthetic_success_run("noise-#{index}", ref, nil, stale_at))
    end)

    assert :ok = Favn.Storage.put_run(fresh_run)

    assert {:ok, result} =
             Favn.check_asset_freshness(ref,
               window_key: key,
               now: DateTime.add(fresh_at, 10, :second),
               max_age_seconds: 60
             )

    assert result.status == :fresh
    assert result.last_materialized_at == fresh_at
  end

  test "check_asset_freshness/2 returns fresh when max_age_seconds is nil" do
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
               max_age_seconds: nil
             )

    assert fresh.status == :fresh
    assert fresh.max_age_seconds == nil
  end

  defp synthetic_success_run(run_id, ref, window_key, finished_at) do
    node_key = {ref, window_key}

    result = %AssetResult{
      ref: ref,
      stage: 0,
      status: :ok,
      started_at: finished_at,
      finished_at: finished_at,
      duration_ms: 0,
      attempt_count: 1,
      max_attempts: 1
    }

    %Run{
      id: run_id,
      status: :ok,
      event_seq: 1,
      started_at: finished_at,
      finished_at: finished_at,
      target_refs: [ref],
      node_results: %{node_key => result},
      asset_results: %{ref => result}
    }
  end
end
