defmodule FavnOrchestrator.Storage.Freshness.AssetFreshnessStateCodecTest do
  use ExUnit.Case, async: true

  alias Favn.Freshness.Key, as: FreshnessKey
  alias Favn.Window.Key, as: WindowKey
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Storage.Freshness.AssetFreshnessStateCodec

  test "round-trips JSON-safe asset freshness state DTOs" do
    window_key = WindowKey.new!(:day, ~U[2026-05-09 00:00:00Z], "Etc/UTC")
    node_key = {{__MODULE__.Orders, :asset}, window_key}

    {:ok, state} =
      AssetFreshnessState.new(%{
        asset_ref_module: __MODULE__.Orders,
        asset_ref_name: :asset,
        freshness_key: FreshnessKey.window!(window_key),
        status: :ok,
        freshness_version: "orders:v1",
        latest_success_run_id: "run_orders",
        latest_success_node_key: node_key,
        latest_success_at: ~U[2026-05-09 12:00:00Z],
        latest_attempt_run_id: "run_orders",
        latest_attempt_status: :ok,
        latest_attempt_at: ~U[2026-05-09 12:00:00Z],
        manifest_version_id: "mv_1",
        manifest_content_hash: "hash_1",
        input_versions: [
          %{
            upstream_ref: {__MODULE__.RawOrders, :asset},
            upstream_node_key: {{__MODULE__.RawOrders, :asset}, nil},
            freshness_version: "raw:v1",
            success_run_id: "run_raw"
          }
        ],
        metadata: %{rows_written: 42},
        updated_at: ~U[2026-05-09 12:00:00Z]
      })

    expected = %{state | metadata: %{"rows_written" => 42}}

    assert {:ok, payload} = AssetFreshnessStateCodec.encode(state)
    assert {:ok, ^expected} = AssetFreshnessStateCodec.decode(payload)
  end
end
