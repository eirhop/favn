defmodule FavnOrchestrator.Operator.Catalogue.AssetFreshnessTest do
  use ExUnit.Case, async: true

  alias Favn.Freshness.Key
  alias Favn.Freshness.Policy
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.Plan
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Operator.Catalogue.AssetFreshness

  @now ~U[2026-07-20 10:00:00Z]
  @raw_ref {MyApp.Raw, :orders}
  @gold_ref {MyApp.Gold, :orders}
  @raw_key {@raw_ref, nil}
  @gold_key {@gold_ref, nil}

  test "explains fresh, changed-upstream, and truly missing upstream evidence" do
    raw_v1 = state(@raw_ref, @raw_key, "raw-v1", [])

    gold =
      state(@gold_ref, @gold_key, "gold-v1", [
        %{
          upstream_ref: @raw_ref,
          upstream_node_key: @raw_key,
          freshness_version: "raw-v1",
          success_run_id: "run-raw-v1"
        }
      ])

    opts = [now: @now, freshness_plan: plan(), run_context_status: :selected]

    assert %{state: :fresh} =
             AssetFreshness.detail(gold_asset(), version(), [raw_v1, gold], opts)

    raw_v2 = state(@raw_ref, @raw_key, "raw-v2", [])

    assert %{
             state: :stale,
             reasons: [%{kind: :upstream_version_changed, previous_version: "raw-v1"}]
           } = AssetFreshness.detail(gold_asset(), version(), [raw_v2, gold], opts)

    assert %{state: :stale, reasons: [%{kind: :upstream_missing}]} =
             AssetFreshness.detail(gold_asset(), version(), [gold], opts)
  end

  defp version do
    %Version{
      manifest_version_id: "manifest-v1",
      content_hash: "sha256:test",
      manifest: %Manifest{assets: [raw_asset(), gold_asset()]}
    }
  end

  defp raw_asset do
    %Asset{ref: @raw_ref, module: elem(@raw_ref, 0), name: elem(@raw_ref, 1)}
  end

  defp gold_asset do
    %Asset{
      ref: @gold_ref,
      module: elem(@gold_ref, 0),
      name: elem(@gold_ref, 1),
      depends_on: [@raw_ref],
      freshness: Policy.from_value!(max_age: {:days, 1})
    }
  end

  defp plan do
    nodes = %{
      @raw_key => node(@raw_ref, @raw_key, [], [@gold_key], 0),
      @gold_key => node(@gold_ref, @gold_key, [@raw_key], [], 1)
    }

    %Plan{
      target_refs: [@gold_ref],
      target_node_keys: [@gold_key],
      nodes: nodes,
      topo_order: [@raw_ref, @gold_ref],
      stages: [[@raw_ref], [@gold_ref]],
      node_stages: [[@raw_key], [@gold_key]]
    }
  end

  defp node(ref, node_key, upstream, downstream, stage) do
    %{
      ref: ref,
      node_key: node_key,
      window: nil,
      upstream: upstream,
      downstream: downstream,
      stage: stage,
      action: :run
    }
  end

  defp state({module, name}, node_key, version, input_versions) do
    {:ok, state} =
      AssetFreshnessState.new(%{
        asset_ref_module: module,
        asset_ref_name: name,
        freshness_key: Key.latest(),
        status: :ok,
        freshness_version: version,
        latest_success_run_id: "run-#{version}",
        latest_success_node_key: node_key,
        latest_success_at: @now,
        latest_attempt_run_id: "run-#{version}",
        latest_attempt_status: :ok,
        latest_attempt_at: @now,
        input_versions: input_versions,
        updated_at: @now
      })

    state
  end
end
