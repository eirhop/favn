defmodule FavnOrchestrator.Freshness.StalenessTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Freshness.Staleness

  @now ~U[2026-05-08 12:00:00Z]
  @raw_ref {__MODULE__.Raw, :asset}
  @silver_ref {__MODULE__.Silver, :asset}
  @raw_key {@raw_ref, nil}
  @silver_key {@silver_ref, nil}

  test "builds consumed input versions from upstream states" do
    states = %{@raw_key => freshness_state(@raw_ref, @raw_key, "raw:v1", "run_raw")}

    assert Staleness.consumed_input_versions([@raw_key], states) == [
             %{
               upstream_ref: @raw_ref,
               upstream_node_key: @raw_key,
               freshness_version: "raw:v1",
               success_run_id: "run_raw"
             }
           ]
  end

  test "fresh when consumed versions match current upstream versions" do
    states = %{@raw_key => freshness_state(@raw_ref, @raw_key, "raw:v1", "run_raw")}
    downstream = %{input_versions: Staleness.consumed_input_versions([@raw_key], states)}

    assert Staleness.freshness(downstream, %{upstream: [@raw_key]}, states) == :fresh
  end

  test "stale when upstream version changed" do
    downstream = %{
      input_versions: [
        %{
          upstream_ref: @raw_ref,
          upstream_node_key: @raw_key,
          freshness_version: "raw:v1",
          success_run_id: "run_old"
        }
      ]
    }

    states = %{@raw_key => freshness_state(@raw_ref, @raw_key, "raw:v2", "run_new")}

    assert Staleness.freshness(downstream, [@raw_key], states) ==
             {:stale,
              [
                %{
                  type: :upstream_version_changed,
                  upstream_ref: @raw_ref,
                  upstream_node_key: @raw_key,
                  consumed_version: "raw:v1",
                  current_version: "raw:v2",
                  current_success_run_id: "run_new"
                }
              ]}
  end

  test "stale when current upstream state is missing" do
    downstream = %{
      input_versions: [
        %{
          upstream_ref: @raw_ref,
          upstream_node_key: @raw_key,
          freshness_version: "raw:v1",
          success_run_id: "run_raw"
        }
      ]
    }

    assert Staleness.freshness(downstream, [@raw_key], %{}) ==
             {:stale,
              [
                %{
                  type: :missing_upstream_version,
                  upstream_ref: @raw_ref,
                  upstream_node_key: @raw_key,
                  consumed_version: "raw:v1",
                  current_version: nil,
                  current_success_run_id: nil
                }
              ]}
  end

  test "accepts JSON-safe list-shaped input_versions with string keys" do
    downstream = %{
      input_versions: [
        %{
          "upstream_ref" => @raw_ref,
          "upstream_node_key" => @raw_key,
          "freshness_version" => "raw:v1",
          "success_run_id" => "run_raw"
        },
        %{
          "upstream_ref" => @silver_ref,
          "upstream_node_key" => @silver_key,
          "freshness_version" => "silver:v1",
          "success_run_id" => "run_silver"
        }
      ]
    }

    states = %{
      @raw_key => freshness_state(@raw_ref, @raw_key, "raw:v1", "run_raw"),
      @silver_key => freshness_state(@silver_ref, @silver_key, "silver:v1", "run_silver")
    }

    assert Staleness.freshness(downstream, [@raw_key, @silver_key], states) == :fresh
  end

  defp freshness_state({module, name}, node_key, version, run_id) do
    {:ok, state} =
      AssetFreshnessState.new(%{
        asset_ref_module: module,
        asset_ref_name: name,
        freshness_key: inspect(node_key),
        status: :ok,
        freshness_version: version,
        latest_success_run_id: run_id,
        latest_success_node_key: node_key,
        latest_success_at: @now,
        latest_attempt_run_id: run_id,
        latest_attempt_status: :ok,
        latest_attempt_at: @now,
        updated_at: @now
      })

    state
  end
end
