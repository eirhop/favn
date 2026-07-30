defmodule FavnView.Dev.DesignSystem.Fixtures.Status do
  @moduledoc """
  Concern groups for the status screen.

  The wording matters as much as the shape here: this screen's whole job is to
  say what is wrong in an operator's language, so the fixtures carry the same
  sentences the real page builds rather than placeholder text. If a detail line
  reads badly here, it reads badly in production.
  """

  @doc """
  Every group, in the order the page renders them: failures, then staleness,
  then work waiting on a decision.
  """
  @spec groups() :: [map()]
  def groups do
    [failing_runs(), assets(), schedules(), operations()]
  end

  @doc """
  One group with one concern, for checking that a group does not need a crowd to
  look right.
  """
  @spec single_group() :: [map()]
  def single_group do
    [%{failing_runs() | concerns: Enum.take(failing_runs().concerns, 1)}]
  end

  defp failing_runs do
    %{
      id: :failing_runs,
      title: "Failing runs",
      description: "A run reached a terminal state with failed work in it.",
      icon: "hero-exclamation-triangle",
      tone: :error,
      concerns: [
        %{
          id: "run-1",
          title: "CrmDaily",
          detail: "2 of 14 assets failed.",
          meta: "Jul 28 06:12",
          action_label: "Open run",
          action_path: "/runs/run_8f2c1d7a"
        },
        %{
          id: "run-2",
          title: "WarehouseNightly",
          detail: "1 of 31 assets failed.",
          meta: "Jul 28 02:00",
          action_label: "Open run",
          action_path: "/runs/run_5b91ee34"
        }
      ]
    }
  end

  defp assets do
    %{
      id: :assets,
      title: "Assets needing attention",
      description: "Health, coverage, or target compatibility is not clear.",
      icon: "hero-sparkles",
      tone: :warning,
      concerns: [
        %{
          id: "asset-1",
          title: "mart_daily_sales",
          detail: "The last run failed. Declared windows are missing.",
          tone: :error,
          meta: nil,
          action_label: "Open asset",
          action_path: "/assets/duckdb.sales.mart_daily_sales"
        },
        %{
          id: "asset-2",
          title: "stg_orders",
          detail: "Later than its freshness policy allows.",
          tone: :warning,
          meta: nil,
          action_label: "Open asset",
          action_path: "/assets/duckdb.sales.stg_orders"
        },
        %{
          id: "asset-3",
          title: "warehouse_mart_account_health",
          detail: "The target must be rebuilt before writes.",
          tone: :warning,
          meta: nil,
          action_label: "Open asset",
          action_path: "/assets/duckdb.sales.warehouse_mart_account_health"
        }
      ]
    }
  end

  defp schedules do
    %{
      id: :schedules,
      title: "Schedules not running",
      description: "A schedule errored, or is waiting for an operator to activate it.",
      icon: "hero-calendar-days",
      tone: :warning,
      concerns: [
        %{
          id: "schedule-1",
          title: "CrmReference",
          detail: "Never activated, so it has never run.",
          tone: :warning,
          meta: nil,
          action_label: "Open schedule",
          action_path: "/schedules/crm_reference"
        }
      ]
    }
  end

  defp operations do
    %{
      id: :operations,
      title: "Operations to reconcile",
      description: "A rebuild ended in an unknown outcome and needs reconciling.",
      icon: "hero-arrow-path-rounded-square",
      tone: :warning,
      concerns: [
        %{
          id: "operation-1",
          title: "Rebuild rb_41c9a0",
          detail: "State is unknown; the outcome is not established.",
          meta: "Jul 27 22:41",
          action_label: "Open rebuild",
          action_path: "/rebuilds/rb_41c9a0e7"
        }
      ]
    }
  end
end
