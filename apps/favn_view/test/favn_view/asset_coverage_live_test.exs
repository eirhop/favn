defmodule FavnView.AssetCoverageLiveTest do
  use ExUnit.Case, async: false

  alias FavnView.AssetDetailLive
  alias FavnView.Auth.Scope

  setup do
    previous = Application.get_env(:favn_view, :plan_missing_coverage_backfill_fun)

    on_exit(fn ->
      if previous do
        Application.put_env(:favn_view, :plan_missing_coverage_backfill_fun, previous)
      else
        Application.delete_env(:favn_view, :plan_missing_coverage_backfill_fun)
      end
    end)

    :ok
  end

  test "coverage planning sends and keeps the selected combine-windows mode" do
    test_pid = self()

    Application.put_env(:favn_view, :plan_missing_coverage_backfill_fun, fn
      :operator_context, "asset:orders", opts ->
        send(test_pid, {:coverage_plan_requested, opts})
        {:ok, %{plan_hash: String.duplicate("a", 64), window_count: 2, windows: []}}
    end)

    assert {:noreply, combined_socket} =
             AssetDetailLive.handle_event(
               "plan_missing_coverage",
               %{"coverage_backfill" => %{"combine_windows" => "true"}},
               coverage_socket()
             )

    assert_received {:coverage_plan_requested, combined_opts}
    assert combined_opts[:combine_windows]
    assert combined_socket.assigns.coverage_combine_windows?
    assert combined_socket.assigns.coverage_plan

    assert {:noreply, separate_socket} =
             AssetDetailLive.handle_event(
               "plan_missing_coverage",
               %{"coverage_backfill" => %{"combine_windows" => "false"}},
               coverage_socket()
             )

    assert_received {:coverage_plan_requested, separate_opts}
    refute separate_opts[:combine_windows]
    refute separate_socket.assigns.coverage_combine_windows?
    assert separate_socket.assigns.coverage_plan
  end

  test "changing the mode invalidates a plan already under review" do
    socket =
      coverage_socket()
      |> Phoenix.Component.assign(
        coverage_plan: %{plan_hash: String.duplicate("a", 64)},
        coverage_action_error: "Old error"
      )

    assert {:noreply, changed_socket} =
             AssetDetailLive.handle_event(
               "change_coverage_backfill",
               %{"coverage_backfill" => %{"combine_windows" => "true"}},
               socket
             )

    assert changed_socket.assigns.coverage_combine_windows?
    assert changed_socket.assigns.coverage_plan == nil
    assert changed_socket.assigns.coverage_action_error == nil
  end

  defp coverage_socket do
    %Phoenix.LiveView.Socket{
      transport_pid: self(),
      assigns: %{
        __changed__: %{},
        current_scope: %Scope{operator_context: :operator_context},
        can_submit_runs?: true,
        asset: %{
          can_run_asset?: true,
          compatibility: %{blocks_writes?: false},
          target_id: "asset:orders",
          coverage: %{evaluated_at: ~U[2026-08-25 08:00:00Z]}
        },
        coverage_selection: MapSet.new(),
        coverage_combine_windows?: false,
        coverage_plan: nil,
        coverage_action_error: nil,
        planning_coverage?: false
      }
    }
  end
end
