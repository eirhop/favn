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

  test "rejects malformed input versions instead of dropping them" do
    {:ok, state} =
      AssetFreshnessState.new(%{
        asset_ref_module: __MODULE__.Orders,
        asset_ref_name: :asset,
        freshness_key: "latest",
        status: :ok,
        input_versions: [:invalid],
        updated_at: ~U[2026-05-09 12:00:00Z]
      })

    assert {:error, {:invalid_input_version, :invalid}} =
             AssetFreshnessStateCodec.encode(state)

    state = %{state | input_versions: %{bad: "version"}}

    assert {:error, {:invalid_input_version, :bad, "version"}} =
             AssetFreshnessStateCodec.encode(state)
  end

  test "rejects malformed persisted input-version fields" do
    window_key = WindowKey.new!(:day, ~U[2026-05-09 00:00:00Z], "Etc/UTC")

    {:ok, state} =
      AssetFreshnessState.new(%{
        asset_ref_module: __MODULE__.Orders,
        asset_ref_name: :asset,
        freshness_key: "latest",
        status: :ok,
        input_versions: [
          %{
            upstream_ref: {__MODULE__.RawOrders, :asset},
            upstream_node_key: {{__MODULE__.RawOrders, :asset}, window_key},
            freshness_version: "raw:v1"
          }
        ],
        updated_at: ~U[2026-05-09 12:00:00Z]
      })

    {:ok, payload} = AssetFreshnessStateCodec.encode(state)
    dto = Jason.decode!(payload)

    assert {:error, {:invalid_input_versions, %{}}} =
             dto
             |> Map.put("input_versions", %{})
             |> Jason.encode!()
             |> AssetFreshnessStateCodec.decode()

    [input_version] = dto["input_versions"]
    invalid = Map.put(input_version, "freshness_version", 123)

    assert {:error, {:invalid_input_version_field, :freshness_version, 123}} =
             dto
             |> Map.put("input_versions", [invalid])
             |> Jason.encode!()
             |> AssetFreshnessStateCodec.decode()
  end

  test "rejects unknown and malformed persisted module identities" do
    {:ok, state} =
      AssetFreshnessState.new(%{
        asset_ref_module: __MODULE__.Orders,
        asset_ref_name: :asset,
        freshness_key: "latest",
        status: :ok,
        updated_at: ~U[2026-05-09 12:00:00Z]
      })

    {:ok, payload} = AssetFreshnessStateCodec.encode(state)
    dto = Jason.decode!(payload)
    module = "Elixir.FavnOrchestrator.Test.UnloadedFreshness#{System.unique_integer([:positive])}"

    assert {:error, {:unknown_module, ^module}} =
             dto
             |> Map.put("asset_ref_module", module)
             |> Jason.encode!()
             |> AssetFreshnessStateCodec.decode()

    assert_raise ArgumentError, fn -> String.to_existing_atom(module) end

    assert {:error, {:invalid_module, "Elixir.Bad;System.halt()"}} =
             dto
             |> Map.put("asset_ref_module", "Elixir.Bad;System.halt()")
             |> Jason.encode!()
             |> AssetFreshnessStateCodec.decode()
  end

  test "rejects malformed optional persistence identities" do
    assert {:error, {:invalid_asset_freshness_field, :freshness_version, 123}} =
             AssetFreshnessState.new(%{
               asset_ref_module: __MODULE__.Orders,
               asset_ref_name: :asset,
               freshness_key: "latest",
               freshness_version: 123,
               status: :ok,
               updated_at: ~U[2026-05-09 12:00:00Z]
             })
  end
end
