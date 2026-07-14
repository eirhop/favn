defmodule FavnOrchestrator.RunServer.Execution.FreshnessContextTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Version
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.RunServer.Execution.FreshnessContext
  alias FavnOrchestrator.RunState

  test "normalizes allowlisted string statuses and rejects unknown persisted statuses" do
    timed_out_key = {{__MODULE__.TimedOut, :asset}, nil}
    malformed_key = {{__MODULE__.Malformed, :asset}, nil}
    unknown_status = "status_that_must_not_become_an_atom"

    run =
      RunState.new(
        id: "run_freshness_context",
        manifest_version_id: "mv_1",
        manifest_content_hash: "hash_1",
        asset_ref: {__MODULE__.TimedOut, :asset}
      )
      |> RunState.transition(
        status: :error,
        result: %{
          node_results: [
            %{node_key: timed_out_key, status: "timed_out"},
            %{"node_key" => malformed_key, "status" => unknown_status}
          ]
        }
      )

    context = %{
      assets_by_ref: %{},
      refresh_policy: %RefreshPolicy{},
      prior_states: %{},
      current_states: %{},
      completed_node_keys: MapSet.new(),
      refreshed_node_keys: MapSet.new(),
      upstream_statuses: %{},
      now: ~U[2026-07-13 10:00:00Z]
    }

    {next_context, ^run} =
      FreshnessContext.record_completed_after_failure(
        run,
        %Version{},
        [timed_out_key, malformed_key],
        %{},
        context
      )

    assert next_context.upstream_statuses == %{
             timed_out_key => :timed_out,
             malformed_key => :error
           }

    assert_raise ArgumentError, fn -> String.to_existing_atom(unknown_status) end
  end
end
