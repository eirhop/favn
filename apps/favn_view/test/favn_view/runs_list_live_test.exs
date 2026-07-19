defmodule FavnView.RunsListLiveTest do
  use ExUnit.Case, async: false

  import Phoenix.LiveViewTest

  alias FavnView.RunsListLive

  setup do
    previous = Application.get_env(:favn_view, :page_execution_groups_fun)

    on_exit(fn -> restore_env(:page_execution_groups_fun, previous) end)
  end

  test "mounts after the first execution group has been persisted" do
    Application.put_env(:favn_view, :page_execution_groups_fun, fn _context, _filters ->
      {:ok, %{items: [execution_group()]}}
    end)

    socket = %Phoenix.LiveView.Socket{
      assigns: %{
        __changed__: %{},
        current_scope: %{operator_context: :operator_context}
      }
    }

    assert {:ok, socket} = RunsListLive.mount(%{}, %{}, socket)

    assert [%{id: "run-1", status: :running, health: %{status: :active}}] =
             socket.assigns.groups
  end

  test "renders an execution group without target assets" do
    Application.put_env(:favn_view, :page_execution_groups_fun, fn _context, _filters ->
      {:ok, %{items: [%{execution_group() | target_assets: []}]}}
    end)

    socket = %Phoenix.LiveView.Socket{
      assigns: %{
        __changed__: %{},
        current_scope: %{operator_context: :operator_context}
      }
    }

    assert {:ok, socket} = RunsListLive.mount(%{}, %{}, socket)
    assert render_component(&RunsListLive.render/1, socket.assigns) =~ "No target"
  end

  defp execution_group do
    counts = %{total: 1, completed: 0, failed: 0, running: 1, queued: 0}

    %{
      id: "run-1",
      root_execution_group_id: "run-1",
      status: :running,
      health: :active,
      active?: true,
      trigger_type: nil,
      target_assets: ["MyApp.Assets.Orders.asset"],
      root_status: :running,
      started_at: DateTime.utc_now(),
      finished_at: nil,
      duration_ms: nil,
      total_windows: 0,
      completed_windows: 0,
      failed_windows: 0,
      total_asset_attempts: 1,
      completed_asset_attempts: 0,
      failed_asset_attempts: 0,
      running_asset_attempts: 1,
      queued_asset_attempts: 0,
      failure_count: 0,
      progress: %{unit: :assets, label: "0 / 1 asset attempts", counts: counts},
      summary_totals: %{
        windows: %{total: 0, completed: 0, failed: 0},
        asset_attempts: counts
      },
      last_activity_at: DateTime.utc_now(),
      currently_running_asset_attempts: [],
      child_run_ids: []
    }
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_view, key)
  defp restore_env(key, value), do: Application.put_env(:favn_view, key, value)
end
