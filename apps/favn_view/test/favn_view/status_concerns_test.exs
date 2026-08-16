defmodule FavnView.StatusConcernsTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest, only: [render_component: 2]

  alias FavnView.Components.Navigation
  alias FavnView.Components.StatusPage
  alias FavnView.StatusConcerns

  doctest StatusConcerns

  defp build(sources, opts \\ []),
    do: StatusConcerns.build(sources, Keyword.put_new(opts, :timezone, "Etc/UTC"))

  defp sources(overrides \\ %{}) do
    Map.merge(
      %{
        runs: {:ok, %{items: []}},
        assets: {:ok, []},
        schedules: {:ok, %{items: []}},
        rebuilds: {:ok, %{items: []}}
      },
      overrides
    )
  end

  describe "build/2" do
    test "nothing wrong produces no groups, which is a state and not an error" do
      assert build(sources()) == %{groups: [], unavailable: []}
    end

    test "an empty group is dropped rather than rendered empty" do
      result =
        build(sources(%{rebuilds: {:ok, %{items: [%{id: "rb_1", state: :succeeded}]}}}))

      assert result.groups == []
    end

    test "orders groups by severity: failures, then assets, then schedules, then operations" do
      result =
        build(
          sources(%{
            runs: {:ok, %{items: [%{id: "run_1", failed_count: 1, total_count: 3}]}},
            assets: {:ok, [%{target_id: "duckdb.s.a", status: :stale}]},
            schedules: {:ok, %{items: [%{id: "s1", activation_state: :pending_activation}]}},
            rebuilds: {:ok, %{items: [%{id: "rb_1", state: :unknown}]}}
          })
        )

      assert Enum.map(result.groups, & &1.id) == [
               :failing_runs,
               :assets,
               :schedules,
               :operations
             ]
    end

    test "an unusable source is named, and the sources that answered still render" do
      result =
        build(
          sources(%{
            schedules: {:error, :unavailable},
            rebuilds: {:error, :timeout},
            assets: {:ok, [%{target_id: "duckdb.s.a", status: :failed}]}
          })
        )

      assert result.unavailable == ["Schedules", "Rebuilds"]
      assert Enum.map(result.groups, & &1.id) == [:assets]
    end

    test "caps each group so the page does not become a list view" do
      items = for index <- 1..20, do: %{id: "run_#{index}", failed_count: 1}
      result = build(sources(%{runs: {:ok, %{items: items}}}), limit: 3)

      assert [%{concerns: concerns}] = result.groups
      assert length(concerns) == 3
    end
  end

  describe "wording" do
    test "a run says how much of it failed" do
      result =
        build(
          sources(%{
            runs:
              {:ok,
               %{
                 items: [
                   %{id: "run_1", trigger_label: "CrmDaily", failed_count: 2, total_count: 14}
                 ]
               }}
          })
        )

      assert [%{concerns: [concern]}] = result.groups
      assert concern.title == "CrmDaily"
      assert concern.detail == "2 of 14 assets failed."
      assert concern.action_path == "/runs/run_1"
    end

    test "a run with no counts still says something true" do
      result = build(sources(%{runs: {:ok, %{items: [%{id: "run_1"}]}}}))

      assert [%{concerns: [concern]}] = result.groups
      assert concern.detail == "The run finished with failures."
      assert concern.title == "Run run_1"
    end

    test "an asset states every reason it needs attention, not just the first" do
      entry = %{
        target_id: "duckdb.sales.mart",
        relation: %{name: "mart_daily_sales"},
        status: :failed,
        coverage: %{status: :incomplete},
        compatibility: %{status: :rebuild_required}
      }

      result = build(sources(%{assets: {:ok, [entry]}}))

      assert [%{concerns: [concern]}] = result.groups
      assert concern.title == "mart_daily_sales"
      assert concern.detail =~ "The last run failed."
      assert concern.detail =~ "Declared windows are missing."
      assert concern.detail =~ "must be rebuilt"
      assert concern.tone == :error
    end

    test "a schedule error is reported verbatim rather than summarised away" do
      entry = %{
        id: "sched_1",
        pipeline_module: Elixir.Demo.CrmDaily,
        last_scheduler_error: %{message: "cron expression rejected"}
      }

      result = build(sources(%{schedules: {:ok, %{items: [entry]}}}))

      assert [%{concerns: [concern]}] = result.groups
      assert concern.title == "Demo.CrmDaily"
      assert concern.detail == "cron expression rejected"
      assert concern.tone == :error
    end

    test "only an unresolved rebuild is a concern" do
      refute StatusConcerns.rebuild_concern?(%{state: :failed})
      refute StatusConcerns.rebuild_concern?(%{state: :succeeded})
      assert StatusConcerns.rebuild_concern?(%{state: :needs_reconciliation})
    end

    test "an asset that has never run is new, not broken" do
      refute StatusConcerns.asset_concern?(%{status: :unknown})
      refute StatusConcerns.asset_concern?(%{status: :healthy})
    end

    test "a schedule that is simply disabled was disabled on purpose" do
      refute StatusConcerns.schedule_concern?(%{activation_state: :disabled})
      assert StatusConcerns.schedule_concern?(%{activation_state: :pending_activation})
    end
  end

  describe "the page" do
    defp render_page(assigns) do
      render_component(
        &StatusPage.status_page/1,
        Map.merge(%{groups: [], unavailable: [], nav_items: Navigation.items(:status)}, assigns)
      )
    end

    test "says nothing needs you rather than showing an empty list" do
      html = render_page(%{})

      assert html =~ "Nothing needs you"
      assert html =~ "status-all-clear"
    end

    test "counts the concerns in the subtitle" do
      groups = FavnView.Dev.DesignSystem.Fixtures.Status.groups()

      assert render_page(%{groups: groups}) =~ "#{StatusPage.total_concerns(groups)} things need"
    end

    test "renders one row per concern with its action" do
      html = render_page(%{groups: FavnView.Dev.DesignSystem.Fixtures.Status.single_group()})

      assert html =~ "CrmDaily"
      assert html =~ "2 of 14 assets failed."
      assert html =~ "Open run"
      assert html =~ ~s|data-testid="status-concern"|
    end

    test "names an unreachable source instead of pretending it is clear" do
      html = render_page(%{unavailable: ["Schedules"]})

      assert html =~ "Schedules could not be reached"
    end

    test "loading and error are distinct states" do
      assert render_page(%{loading: true}) =~ "Checking for anything"
      assert render_page(%{error: "load_failed"}) =~ "Could not load status"
    end
  end
end
