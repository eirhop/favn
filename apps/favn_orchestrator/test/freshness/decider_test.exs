defmodule FavnOrchestrator.Freshness.DeciderTest do
  use ExUnit.Case, async: true

  alias Favn.Freshness.{Key, Policy}
  alias Favn.Plan
  alias Favn.Window.Key, as: WindowKey
  alias Favn.Window.Runtime
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Freshness.Decider

  @now ~U[2026-05-08 12:00:00Z]
  @raw_ref {__MODULE__.Raw, :asset}
  @stage_ref {__MODULE__.Stage, :asset}
  @gold_ref {__MODULE__.Gold, :asset}
  @raw_key {@raw_ref, nil}
  @stage_key {@stage_ref, nil}
  @gold_key {@gold_ref, nil}

  test "forced node runs despite fresh state" do
    freshness_key = Key.latest()
    state = freshness_state(@raw_ref, @raw_key, freshness_key, status: :ok)

    assert %{decision: :run, reason: :forced} =
             Decider.decide(plan(), @raw_key,
               assets_by_ref: %{@raw_ref => %{freshness: Policy.from_value!(max_age: {:days, 1})}},
               forced_node_keys: [@raw_key],
               prior_states: %{@raw_key => state},
               now: @now
             )
  end

  test "upstream refreshed in same run causes downstream run" do
    assert %{decision: :run, reason: :upstream_refreshed} =
             Decider.decide(plan(), @stage_key,
               assets_by_ref: %{@stage_ref => %{freshness: nil}},
               refreshed_node_keys: [@raw_key],
               now: @now
             )
  end

  test "upstream skipped fresh satisfies dependency without dirtying downstream" do
    raw_state = freshness_state(@raw_ref, @raw_key, Key.latest(), version: "raw:v1")

    stage_state =
      freshness_state(@stage_ref, @stage_key, Key.latest(),
        input_versions: [
          %{
            upstream_ref: @raw_ref,
            upstream_node_key: @raw_key,
            freshness_version: "raw:v1",
            success_run_id: "run_raw"
          }
        ]
      )

    assert %{decision: :skipped_fresh, reason: :max_age} =
             Decider.decide(plan(), @stage_key,
               assets_by_ref: %{
                 @stage_ref => %{freshness: Policy.from_value!(max_age: {:days, 1})}
               },
               upstream_statuses: %{@raw_key => :skipped_fresh},
               prior_states: %{@stage_key => stage_state, @raw_key => raw_state},
               current_states: %{@raw_key => raw_state},
               now: @now
             )
  end

  test "upstream blocking status blocks downstream" do
    assert %{
             decision: :blocked,
             reason: :upstream_blocked,
             blocking_upstream: [@raw_key]
           } =
             Decider.decide(plan(), @stage_key,
               upstream_statuses: %{@raw_key => :error},
               now: @now
             )
  end

  test "upstream version changed causes run with stale reason" do
    prior =
      freshness_state(@stage_ref, @stage_key, Key.latest(),
        input_versions: [
          %{
            upstream_ref: @raw_ref,
            upstream_node_key: @raw_key,
            freshness_version: "raw:v1",
            success_run_id: "run_raw_old"
          }
        ]
      )

    current_upstream = freshness_state(@raw_ref, @raw_key, Key.latest(), version: "raw:v2")

    assert %{
             decision: :run,
             reason: :upstream_version_changed,
             stale_reasons: [%{type: :upstream_version_changed, current_version: "raw:v2"}]
           } =
             Decider.decide(plan(), @stage_key,
               prior_states: %{@stage_key => prior},
               current_states: %{@raw_key => current_upstream},
               now: @now
             )
  end

  test "window_success skips exact window when prior success exists" do
    runtime = runtime_window()
    node_key = {@raw_ref, runtime.key}
    plan = plan(%{node_key => %{plan_node(@raw_ref) | node_key: node_key, window: runtime}})
    freshness_key = Key.window!(runtime.key)
    state = freshness_state(@raw_ref, node_key, freshness_key, status: :ok)

    assert %{decision: :skipped_fresh, reason: :window_success, freshness_key: ^freshness_key} =
             Decider.decide(plan, node_key,
               assets_by_ref: %{@raw_ref => %{freshness: elem(Policy.window_success(), 1)}},
               prior_states: %{freshness_key => state},
               now: @now
             )
  end

  test "own state lookup prefers ref and freshness key" do
    raw_state = freshness_state(@raw_ref, @raw_key, Key.latest(), status: :ok)

    stage_state =
      freshness_state(@stage_ref, @stage_key, Key.latest(),
        status: :ok,
        input_versions: [
          %{
            upstream_ref: @raw_ref,
            upstream_node_key: @raw_key,
            freshness_version: "asset:v1",
            success_run_id: "run_asset"
          }
        ]
      )

    assert %{decision: :skipped_fresh, reason: :max_age} =
             Decider.decide(plan(), @stage_key,
               assets_by_ref: %{
                 @stage_ref => %{freshness: Policy.from_value!(max_age: {:days, 1})}
               },
               prior_states: %{
                 {@raw_ref, Key.latest()} => raw_state,
                 {@stage_ref, Key.latest()} => stage_state
               },
               now: @now
             )
  end

  test "non-windowed nil policy runs under auto" do
    assert %{decision: :run, reason: :no_freshness_policy} =
             Decider.decide(plan(), @raw_key,
               assets_by_ref: %{@raw_ref => %{freshness: nil}},
               now: @now
             )
  end

  test ":always runs under auto even with success state" do
    state = freshness_state(@raw_ref, @raw_key, Key.latest(), status: :ok)

    assert %{decision: :run, reason: :always} =
             Decider.decide(plan(), @raw_key,
               assets_by_ref: %{@raw_ref => %{freshness: :always}},
               prior_states: %{@raw_key => state},
               now: @now
             )
  end

  test ":missing skips prior success including :always" do
    state = freshness_state(@raw_ref, @raw_key, Key.latest(), status: :ok)

    assert %{decision: :skipped_fresh, reason: :existing_success} =
             Decider.decide(plan(), @raw_key,
               assets_by_ref: %{@raw_ref => %{freshness: :always}},
               refresh_policy: :missing,
               prior_states: %{@raw_key => state},
               now: @now
             )
  end

  test ":missing skips prior success before stale upstream-version checks" do
    raw_state = freshness_state(@raw_ref, @raw_key, Key.latest(), version: "raw:v2")

    stage_state =
      freshness_state(@stage_ref, @stage_key, Key.latest(),
        input_versions: [
          %{
            upstream_ref: @raw_ref,
            upstream_node_key: @raw_key,
            freshness_version: "raw:v1",
            success_run_id: "run_raw_old"
          }
        ]
      )

    assert %{decision: :skipped_fresh, reason: :existing_success} =
             Decider.decide(plan(), @stage_key,
               assets_by_ref: %{@stage_ref => %{freshness: :always}},
               refresh_policy: :missing,
               prior_states: %{@stage_key => stage_state},
               current_states: %{@raw_key => raw_state},
               now: @now
             )
  end

  test "daily calendar uses calendar day key and Europe/Oslo boundary" do
    now = ~U[2026-05-08 22:30:00Z]
    freshness_key = Key.calendar!(:day, "Europe/Oslo", ~D[2026-05-09])
    state = freshness_state(@raw_ref, @raw_key, freshness_key, status: :ok)

    assert %{
             decision: :skipped_fresh,
             reason: :calendar_period,
             freshness_key: ^freshness_key
           } =
             Decider.decide(plan(), @raw_key,
               assets_by_ref: %{
                 @raw_ref => %{freshness: Policy.from_value!({:daily, timezone: "Europe/Oslo"})}
               },
               prior_states: %{freshness_key => state},
               now: now
             )
  end

  test "max_age rolling freshness" do
    fresh_state =
      freshness_state(@raw_ref, @raw_key, Key.latest(),
        latest_success_at: DateTime.add(@now, -59, :minute)
      )

    expired_state =
      freshness_state(@raw_ref, @raw_key, Key.latest(),
        latest_success_at: DateTime.add(@now, -61, :minute)
      )

    assets_by_ref = %{
      @raw_ref => %{freshness: Policy.from_value!(max_age: {:hours, 1})},
      @stage_ref => %{freshness: Policy.from_value!(max_age: {:hours, 1})}
    }

    assert %{decision: :skipped_fresh, reason: :max_age} =
             Decider.decide(plan(), @raw_key,
               assets_by_ref: assets_by_ref,
               prior_states: %{@raw_key => fresh_state},
               now: @now
             )

    assert %{decision: :run, reason: :freshness_expired} =
             Decider.decide(plan(), @raw_key,
               assets_by_ref: assets_by_ref,
               prior_states: %{@raw_key => expired_state},
               now: @now
             )
  end

  defp plan(nodes \\ nil) do
    nodes =
      nodes ||
        %{
          @raw_key => plan_node(@raw_ref, upstream: [], downstream: [@stage_key], stage: 0),
          @stage_key =>
            plan_node(@stage_ref, upstream: [@raw_key], downstream: [@gold_key], stage: 1),
          @gold_key => plan_node(@gold_ref, upstream: [@stage_key], downstream: [], stage: 2)
        }

    %Plan{
      target_refs: [@gold_ref],
      target_node_keys: [@gold_key],
      nodes: nodes,
      topo_order: [@raw_ref, @stage_ref, @gold_ref],
      stages: [[@raw_ref], [@stage_ref], [@gold_ref]],
      node_stages: [[@raw_key], [@stage_key], [@gold_key]]
    }
  end

  defp plan_node(ref, opts \\ []) do
    %{
      ref: ref,
      node_key: {ref, nil},
      window: nil,
      upstream: Keyword.get(opts, :upstream, []),
      downstream: Keyword.get(opts, :downstream, []),
      stage: Keyword.get(opts, :stage, 0),
      action: :run
    }
  end

  defp runtime_window do
    start_at = ~U[2026-05-08 00:00:00Z]
    end_at = ~U[2026-05-09 00:00:00Z]
    anchor_key = WindowKey.new!(:day, start_at, "Etc/UTC")

    Runtime.new!(:day, start_at, end_at, anchor_key)
  end

  defp freshness_state(ref, node_key, freshness_key, opts) do
    {module, name} = ref
    run_id = Keyword.get(opts, :run_id, "run_#{name}")
    status = Keyword.get(opts, :status, :ok)
    latest_success_at = Keyword.get(opts, :latest_success_at, @now)

    {:ok, state} =
      AssetFreshnessState.new(%{
        asset_ref_module: module,
        asset_ref_name: name,
        freshness_key: freshness_key,
        status: status,
        freshness_version: Keyword.get(opts, :version, "#{name}:v1"),
        latest_success_run_id: run_id,
        latest_success_node_key: node_key,
        latest_success_at: latest_success_at,
        latest_attempt_run_id: run_id,
        latest_attempt_status: status,
        latest_attempt_at: latest_success_at,
        input_versions: Keyword.get(opts, :input_versions, []),
        updated_at: latest_success_at
      })

    state
  end
end
