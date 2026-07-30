defmodule FavnView.Dev.DesignSystem.Examples.Pages do
  @moduledoc """
  Curated examples for whole screens.

  A page component is a pure function of one view model, so an example is just
  that view model — no slots, no interaction, no setup. Every screen is covered
  in the four states a LiveView can actually be in (loaded, empty, loading,
  failed) plus the states specific to that screen, because those four are where
  layout most often falls apart and where a screenshot is most worth taking.

  The view models come from the `sample_*` functions the page components already
  expose and from `FavnView.Dev.DesignSystem.Fixtures`, so a page rendered here
  and a page asserted in a `favn_view` test are the same page.
  """

  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.LineagePage
  alias FavnView.Components.Navigation
  alias FavnView.Components.PipelineDetailPage
  alias FavnView.Components.PipelinesPage
  alias FavnView.Components.ScheduleDetailPage
  alias FavnView.Components.SchedulesPage
  alias FavnView.Dev.DesignSystem.Example
  alias FavnView.Dev.DesignSystem.Fixtures
  alias FavnView.Dev.DesignSystem.Fixtures.AssetDetail
  alias FavnView.Dev.DesignSystem.Fixtures.Runs
  alias FavnView.Dev.DesignSystem.Fixtures.RunsList
  alias FavnView.Dev.DesignSystem.Fixtures.Schedules
  alias FavnView.Dev.DesignSystem.Fixtures.Timeline

  @doc """
  Every curated page example, keyed by catalogue entry id.
  """
  @spec all() :: %{String.t() => [Example.t()]}
  def all do
    %{}
    |> Map.merge(status())
    |> Map.merge(asset_catalogue())
    |> Map.merge(asset_detail())
    |> Map.merge(timelines())
    |> Map.merge(lineage())
    |> Map.merge(log_pages())
    |> Map.merge(pipelines())
    |> Map.merge(runs())
    |> Map.merge(schedules())
    |> Map.merge(rebuilds())
    |> Map.merge(recovery())
    |> Map.merge(errors())
  end

  defp status do
    nav_items = Navigation.items(:status)

    %{
      "status_page/status_page" => [
        Example.attrs(
          :needs_attention,
          %{groups: Fixtures.Status.groups(), unavailable: [], nav_items: nav_items},
          "The page an operator actually arrives at: failures first, then staleness, then pending work."
        ),
        Example.attrs(
          :one_concern,
          %{groups: Fixtures.Status.single_group(), unavailable: [], nav_items: nav_items},
          "A group renders the same whether it holds one row or eight."
        ),
        Example.attrs(
          :all_clear,
          %{groups: [], unavailable: [], nav_items: nav_items},
          "Nothing wrong is a real state, not an empty one."
        ),
        Example.attrs(
          :degraded,
          %{
            groups: Fixtures.Status.single_group(),
            unavailable: ["Schedules", "Rebuilds"],
            nav_items: nav_items
          },
          "Two sources unreachable. The sources that answered still render."
        ),
        Example.attrs(:loading, %{groups: [], loading: true, nav_items: nav_items}),
        Example.attrs(:error, %{groups: [], error: "load_failed", nav_items: nav_items})
      ]
    }
  end

  defp asset_catalogue do
    filters = %{search: "", connection: "all", catalogue: "all"}

    base = %{
      assets: AssetCataloguePage.sample_assets(),
      filters: filters,
      active_mode: :list,
      loading: false,
      error: nil,
      nav_items: AssetCataloguePage.nav_items(:assets),
      connection_options: AssetCataloguePage.connection_options(),
      catalogue_options: AssetCataloguePage.catalogue_options()
    }

    %{
      "asset_catalogue_page/asset_catalogue_page" => [
        Example.attrs(:catalogue, base),
        Example.attrs(
          :lineage_mode,
          Map.merge(base, %{
            active_mode: :lineage,
            lineage_graph: LineagePage.sample_graph(),
            lineage_inspector: LineagePage.sample_group_inspector(),
            lineage_loading: false,
            lineage_error: nil,
            lineage_search: "",
            lineage_zoom: 62,
            lineage_inspector_open?: true,
            lineage_canvas_hook?: false
          }),
          "The graph shares the catalogue's shell and filters."
        ),
        Example.attrs(
          :empty,
          Map.merge(base, %{
            assets: [],
            filters: %{search: "orders", connection: "duckdb", catalogue: "marketing"}
          }),
          "Filtered to nothing. The filters must stay visible so the operator can undo them."
        ),
        Example.attrs(:loading, Map.merge(base, %{assets: [], loading: true})),
        Example.attrs(:error, Map.merge(base, %{assets: [], error: "load_failed"}))
      ]
    }
  end

  defp asset_detail do
    %{
      "asset_detail_page/asset_detail_page" => [
        Example.attrs(
          :refresh_timeline,
          AssetDetail.base_attrs(),
          "The default screen: refresh timeline, nothing selected."
        ),
        Example.attrs(
          :full_refresh_asset,
          AssetDetail.attrs(%{
            title: "stg_payments",
            status: "Unknown",
            status_tone: :neutral,
            has_freshness_timeline?: false,
            has_data_windows?: false,
            freshness_timeline: nil,
            data_coverage_timeline: nil,
            data_coverage_window_range: "No windows",
            freshness: FavnView.Components.AssetDetailPage.sample_freshness(:unknown)
          }),
          "An asset with no windows at all. No timeline toggle, no window selection."
        ),
        Example.attrs(
          :active_data_coverage,
          AssetDetail.attrs(%{active_timeline: :data_coverage})
        ),
        Example.attrs(
          :active_freshness,
          AssetDetail.attrs(%{active_timeline: :freshness})
        ),
        Example.attrs(
          :incomplete_coverage,
          AssetDetail.attrs(%{
            coverage: AssetDetail.coverage(:incomplete),
            coverage_gaps: AssetDetail.coverage_gaps(),
            coverage_pagination: AssetDetail.coverage_pagination(true)
          }),
          "Missing windows, with more pages of gaps behind a cursor."
        ),
        Example.attrs(
          :later_coverage_page,
          AssetDetail.attrs(%{
            coverage: AssetDetail.coverage(:incomplete),
            coverage_gaps: AssetDetail.coverage_gaps(),
            coverage_pagination: AssetDetail.coverage_pagination(true),
            coverage_page_cursor: "opaque-page-2-cursor"
          })
        ),
        Example.attrs(
          :unknown_coverage,
          AssetDetail.attrs(%{
            coverage: AssetDetail.coverage(:unknown),
            coverage_policy: nil,
            coverage_gaps: []
          }),
          "Coverage was never declared. This must not read as complete coverage."
        ),
        Example.attrs(
          :coverage_plan_review,
          AssetDetail.attrs(%{
            coverage: AssetDetail.coverage(:incomplete),
            coverage_gaps: AssetDetail.coverage_gaps(),
            coverage_plan: AssetDetail.coverage_plan()
          })
        ),
        Example.attrs(
          :rebuild_available,
          AssetDetail.attrs(%{compatibility: AssetDetail.compatibility(:rebuild_available)}),
          "A rebuild is possible and writes still work."
        ),
        Example.attrs(
          :rebuild_required,
          AssetDetail.attrs(%{
            can_run_asset?: false,
            compatibility: AssetDetail.compatibility(:rebuild_required)
          }),
          "Writes are blocked until the target is rebuilt."
        ),
        Example.attrs(
          :unexpected_drift,
          AssetDetail.attrs(%{
            can_run_asset?: false,
            compatibility: AssetDetail.compatibility(:unexpected_drift)
          }),
          "The physical target changed underneath Favn. Must not look like a normal rebuild."
        ),
        Example.attrs(
          :operator_decision,
          AssetDetail.attrs(%{
            can_run_asset?: false,
            compatibility: AssetDetail.compatibility(:operator_decision)
          }),
          "An unmanaged target: Favn refuses to adopt it without a decision."
        ),
        Example.attrs(
          :ambiguous_run_context,
          AssetDetail.attrs(%{
            can_run_asset?: false,
            run_contexts: AssetDetail.run_contexts(),
            selected_run_context: nil,
            run_context_status: :ambiguous,
            refresh_timeline: [],
            refresh_window_range: "No windows",
            refresh_timeline_label: "Run context required",
            refresh_cadence_label: "Select a pipeline context"
          }),
          "Two pipelines could own this asset, so there is no timeline until one is chosen."
        ),
        Example.attrs(
          :selected_refresh_period,
          AssetDetail.attrs(%{selected_window: List.last(Timeline.refresh_timeline())})
        ),
        Example.attrs(
          :selected_data_window,
          AssetDetail.attrs(%{
            active_timeline: :data_coverage,
            selected_window: List.last(Timeline.data_coverage_timeline())
          })
        ),
        Example.attrs(
          :run_config_open,
          AssetDetail.attrs(%{run_config_open?: true, run_config: Timeline.default_run_config()})
        ),
        Example.attrs(
          :prefilled_failed_run_config,
          AssetDetail.attrs(%{
            selected_window: Enum.at(Timeline.refresh_timeline(), 1),
            run_config_open?: true,
            run_config:
              Timeline.run_config(:refresh_timeline, :day, "2026-06-10", "none", "force_all")
          }),
          "Retrying a failed window prefills the config that failed."
        ),
        Example.attrs(
          :submit_success,
          AssetDetail.attrs(%{
            selected_window: List.last(Timeline.refresh_timeline()),
            submitted_run_id: "run_01HZ"
          })
        ),
        Example.attrs(
          :submit_error,
          AssetDetail.attrs(%{
            selected_window: List.last(Timeline.refresh_timeline()),
            selected_window_error: "Could not submit run."
          })
        ),
        Example.attrs(
          :non_runnable_window,
          AssetDetail.attrs(%{
            active_timeline: :data_coverage,
            selected_window:
              Timeline.data_window("2026-06-12", "Jun 12", :muted)
              |> Map.merge(%{run_enabled?: false, run_disabled_reason: :invalid_window})
          })
        ),
        Example.attrs(:freshness_fresh, AssetDetail.freshness_attrs(:fresh)),
        Example.attrs(
          :freshness_stale,
          AssetDetail.freshness_attrs(:stale),
          "Stale with an upstream reason: the cause belongs on the screen."
        ),
        Example.attrs(:freshness_unknown, AssetDetail.freshness_attrs(:unknown)),
        Example.attrs(:freshness_always_run, AssetDetail.freshness_attrs(:always_run)),
        Example.attrs(
          :contract_and_row_counts,
          AssetDetail.assurance_attrs(),
          "A contract with a composed fragment and a parameterised row-count claim."
        )
      ]
    }
  end

  defp timelines do
    panel = AssetDetail.window_timeline_panel_attrs()

    %{
      "asset_detail_page/window_timeline_panel" => [
        Example.attrs(:refresh_and_data, panel),
        Example.attrs(
          :refresh_only,
          Map.merge(panel, %{
            has_data_windows?: false,
            data_coverage_timeline: nil,
            has_freshness_timeline?: false,
            freshness_timeline: nil
          })
        ),
        Example.attrs(:active_data_coverage, Map.put(panel, :active_timeline, :data_coverage)),
        Example.attrs(:active_freshness, Map.put(panel, :active_timeline, :freshness)),
        Example.attrs(
          :selected_refresh_period,
          Map.put(panel, :selected_window, List.last(Timeline.refresh_timeline()))
        ),
        Example.attrs(
          :run_config_open,
          Map.merge(panel, %{run_config_open?: true, run_config: Timeline.default_run_config()})
        )
      ],
      "asset_detail_page/timeline_window" => [
        Example.attrs(:fresh, %{window: Timeline.refresh_window("2026-06-12", "Jun 12", :success)}),
        Example.attrs(:running, %{
          window: Timeline.refresh_window("2026-06-13", "Jun 13", :warning)
        }),
        Example.attrs(:failed, %{window: Timeline.data_window("2026-06-10", "Jun 10", :error)}),
        Example.attrs(
          :missing,
          %{window: Timeline.data_window("2026-06-11", "Jun 11", :muted)},
          "A window with no data at all."
        ),
        Example.attrs(:selected, %{
          selected: true,
          window: Timeline.data_window("2026-06-12", "Jun 12", :success)
        }),
        Example.attrs(
          :selected_running,
          %{selected: true, window: Timeline.refresh_window("2026-06-13", "Jun 13", :warning)},
          "Selection is judged per status: the boundary rule only applies to a window an operator has picked."
        ),
        Example.attrs(:selected_failed, %{
          selected: true,
          window: Timeline.data_window("2026-06-10", "Jun 10", :error)
        }),
        Example.attrs(:selected_missing, %{
          selected: true,
          window: Timeline.data_window("2026-06-11", "Jun 11", :muted)
        })
      ]
    }
  end

  defp lineage do
    base = %{
      graph: nil,
      inspector: nil,
      view_mode: :all,
      search: "",
      loading: false,
      error: nil,
      zoom: 62,
      canvas_hook?: false
    }

    %{
      "lineage_page/lineage_page" => [
        Example.attrs(
          :full_page,
          Map.merge(base, %{
            graph: LineagePage.sample_graph(),
            inspector: LineagePage.sample_group_inspector()
          })
        ),
        Example.attrs(:empty, base),
        Example.attrs(:loading, Map.put(base, :loading, true)),
        Example.attrs(
          :error,
          Map.put(base, :error, %{message: "No active manifest is available."})
        )
      ]
    }
  end

  defp log_pages do
    %{
      "log_pages/global_logs_page" => [
        Example.attrs(:global, Fixtures.Logs.global_page_attrs())
      ],
      "log_pages/run_logs_page" => [
        Example.attrs(:run, Fixtures.Logs.run_page_attrs())
      ],
      "log_pages/asset_run_logs_page" => [
        Example.attrs(:running, Fixtures.Logs.asset_page_attrs()),
        Example.attrs(
          :with_output_metadata,
          Fixtures.Logs.asset_page_attrs(%{
            status: "Succeeded",
            status_tone: :success,
            live?: false,
            output_status: :ok,
            output_metadata: Fixtures.Logs.output_metadata()
          }),
          "A finished step shows what it wrote alongside its logs."
        )
      ]
    }
  end

  defp pipelines do
    base = %{
      pipelines: PipelinesPage.sample_pipelines(),
      filters: %{search: "", status: "all"},
      loading: false,
      error: nil,
      nav_items: PipelinesPage.nav_items(:pipelines),
      status_options: PipelinesPage.status_options()
    }

    backfill = %{
      from: "2024-01",
      to: "2026-12",
      kind: "month",
      timezone: "Etc/UTC",
      refresh: "missing"
    }

    %{
      "pipelines_page/pipelines_page" => [
        Example.attrs(:pipelines, base),
        Example.attrs(
          :empty,
          Map.merge(base, %{pipelines: [], filters: %{search: "not_real", status: "failed"}})
        ),
        Example.attrs(:loading, Map.merge(base, %{pipelines: [], loading: true})),
        Example.attrs(:error, Map.merge(base, %{pipelines: [], error: "load_failed"}))
      ],
      "pipeline_detail_page/pipeline_detail_page" => [
        Example.attrs(:with_history, %{
          pipeline: PipelineDetailPage.sample_pipeline(),
          nav_items: PipelinesPage.nav_items(:pipelines),
          backfill_config: backfill
        }),
        Example.attrs(
          :no_history,
          %{
            pipeline: %{
              PipelineDetailPage.sample_pipeline()
              | runs: [],
                status: :unknown,
                status_label: "Unknown"
            },
            nav_items: PipelinesPage.nav_items(:pipelines),
            backfill_config: backfill
          },
          "Declared but never run."
        )
      ]
    }
  end

  defp runs do
    %{
      "runs_list_page/runs_list_page" => [
        Example.attrs(
          :today,
          RunsList.today(),
          "The default view: one day, so no day headers, and each status says how much it holds before a click."
        ),
        Example.attrs(
          :failed_today,
          RunsList.failed_today(),
          "Failures only, reached from their own count. The counts do not move, because status is the axis they leave open."
        ),
        Example.attrs(
          :month_with_gaps,
          RunsList.month_with_gaps(),
          "A month of one pipeline. The days with no row are the answer to \"did it run every day\"."
        ),
        Example.attrs(
          :custom_range,
          RunsList.custom_range(),
          "An explicit pair of dates, so the range control shows its inputs."
        ),
        Example.attrs(
          :truncated,
          RunsList.truncated(),
          "More runs than one page holds, so the days stop where the page did rather than claiming they were empty."
        ),
        Example.attrs(
          :later_page,
          RunsList.later_page(),
          "A page reached by stepping older. One page is one read of the index, at any depth."
        ),
        Example.attrs(
          :filters_open,
          RunsList.filters_open(),
          "The narrow-screen disclosure, opened. On a wide screen these controls are always out."
        ),
        Example.attrs(:empty, RunsList.empty()),
        Example.attrs(:no_matches, RunsList.no_matches()),
        Example.attrs(:error, RunsList.unavailable())
      ],
      "run_detail_page/run_detail_page" => [
        Example.attrs(
          :running_backfill,
          %{
            run: Runs.backfill(:running),
            run_id: "run_backfill_8f2c9d1",
            nav_items: Runs.nav_items()
          },
          "The flow is the primary visual: stage order is dependency order."
        ),
        Example.attrs(
          :failed_backfill,
          %{
            run: Runs.backfill(:partial),
            run_id: "run_backfill_8f2c9d1",
            nav_items: Runs.nav_items()
          },
          "A stage-2 failure renders in its own lane, under the stage that fed it."
        ),
        Example.attrs(:attempt_selected, %{
          run: Runs.backfill(:partial),
          run_id: "run_backfill_8f2c9d1",
          nav_items: Runs.nav_items(),
          selected_attempt_id: "revenue_metrics-2026-02"
        }),
        Example.attrs(
          :single_window,
          %{
            run: Runs.single_window(),
            run_id: "run_daily_orders_2026_05_19",
            nav_items: Runs.nav_items()
          },
          "One window: no window meter, and no window-run count in the rail."
        ),
        Example.attrs(
          :full_refresh,
          %{
            run: Runs.full_refresh(),
            run_id: "run_full_refresh_sales",
            nav_items: Runs.nav_items()
          },
          "No window at all, and one asset still running."
        ),
        Example.attrs(
          :admission_failure,
          %{
            run: Runs.admission_failure(),
            run_id: "run_backfill_unknown_pool",
            nav_items: Runs.nav_items()
          },
          "Rejected before any asset ran, so the failure has no lane and is called out."
        ),
        Example.attrs(:window_runs, %{
          run: Runs.backfill(:running),
          run_id: "run_backfill_8f2c9d1",
          nav_items: Runs.nav_items(),
          active_mode: :windows
        }),
        Example.attrs(:events, %{
          run: Runs.backfill(:running),
          run_id: "run_backfill_8f2c9d1",
          nav_items: Runs.nav_items(),
          active_mode: :events
        }),
        Example.attrs(
          :not_found,
          %{run: Runs.not_found(), run_id: "run_missing", nav_items: Runs.nav_items()},
          "No such run. Neutral, not an error."
        ),
        Example.attrs(
          :snapshot_unavailable,
          %{run: Runs.unavailable(), run_id: "run_unreadable", nav_items: Runs.nav_items()},
          "The run exists but its snapshot could not be read."
        ),
        Example.attrs(
          :initializing,
          %{
            run: Map.put(Runs.not_found(), :initializing?, true),
            run_id: "run_committed",
            nav_items: Runs.nav_items()
          },
          "Committed but not yet observable. Must not read as not-found."
        )
      ]
    }
  end

  defp schedules do
    empty_schedules = %{
      schedules: [],
      all_schedules: [],
      filters: Schedules.filters(),
      filter_options: %{pipelines: [], windows: []},
      summary: Schedules.summary([]),
      loading: false,
      error: nil,
      nav_items: SchedulesPage.nav_items()
    }

    detail = %{
      schedule: Schedules.schedule(),
      occurrence_preview: Schedules.occurrences(),
      occurrence_error: nil,
      activation_error: nil,
      active_view: :overview,
      loading: false,
      error: nil,
      nav_items: ScheduleDetailPage.nav_items()
    }

    enabled =
      Schedules.schedule(%{
        activation_state: :enabled,
        activation_label: "Enabled",
        activation_tone: :success,
        runtime_state: :idle,
        runtime_label: "Idle",
        effective_enabled?: true
      })

    %{
      "schedules_page/schedules_page" => [
        Example.attrs(:schedules, %{
          schedules: Schedules.list(),
          all_schedules: Schedules.list(),
          filters: Schedules.filters(),
          filter_options: Schedules.filter_options(),
          summary: Schedules.summary(),
          loading: false,
          error: nil,
          nav_items: SchedulesPage.nav_items()
        }),
        Example.attrs(:empty, empty_schedules),
        Example.attrs(:loading, Map.put(empty_schedules, :loading, true)),
        Example.attrs(:error, Map.put(empty_schedules, :error, "load_failed"))
      ],
      "schedule_detail_page/schedule_detail_page" => [
        Example.attrs(:overview, detail),
        Example.attrs(
          :pending_activation_occurrences,
          Map.put(detail, :active_view, :occurrences),
          "Previewed occurrences for a schedule that is not enabled yet."
        ),
        Example.attrs(
          :enabled_occurrences,
          Map.merge(detail, %{schedule: enabled, active_view: :occurrences})
        ),
        Example.attrs(
          :no_occurrences,
          Map.merge(detail, %{
            schedule: enabled,
            occurrence_preview: [],
            active_view: :occurrences
          })
        ),
        Example.attrs(
          :occurrence_error,
          Map.merge(detail, %{
            occurrence_preview: [],
            occurrence_error: "invalid_cron_or_timezone",
            active_view: :occurrences
          })
        ),
        Example.attrs(
          :scheduler_error,
          Map.put(
            detail,
            :schedule,
            Schedules.schedule(%{
              last_scheduler_error: %{
                occurred_label: "May 24 12:02",
                phase_label: "Submit run",
                code_label: "Invalid scheduled window policy",
                message: "Window policy could not be resolved"
              }
            })
          ),
          "The scheduler tried and failed. The failure belongs on the schedule, not only in logs."
        ),
        Example.attrs(
          :current_run,
          Map.put(
            detail,
            :schedule,
            Schedules.schedule(%{
              activation_state: :enabled,
              activation_label: "Enabled",
              activation_tone: :success,
              runtime_state: :running,
              runtime_label: "Running",
              effective_enabled?: true,
              in_flight_run_id: "run_8f3a2c",
              current_run_label: "run_8f3a2c"
            })
          )
        ),
        Example.attrs(
          :activation_failed,
          Map.put(detail, :activation_error, "Could not update schedule. Try again later."),
          "A refused activation belongs next to the state it failed to change."
        ),
        Example.attrs(
          :not_found,
          Map.merge(detail, %{
            schedule: nil,
            occurrence_preview: [],
            error: :not_found
          })
        ),
        Example.attrs(
          :loading,
          Map.merge(detail, %{schedule: nil, occurrence_preview: [], loading: true})
        )
      ]
    }
  end

  defp rebuilds do
    operation = fn state ->
      %{
        operation_id: "rebuild_plan_01",
        root_target_id: "asset:orders",
        state: state,
        phase: :planned,
        reason: "schema changed",
        progress: %{completed: 0, total: 12},
        updated_at: ~U[2026-07-22 12:00:00Z]
      }
    end

    detail_operation = fn state ->
      %{
        operation_id: "rebuild_01",
        root_target_id: "asset:orders",
        state: state,
        phase: :build,
        progress: %{completed: 7, total: 12},
        active_generation_id: "generation_01",
        candidate_generation_id: "generation_02",
        plan_hash: String.duplicate("b", 64),
        cleanup_state: :not_started,
        permissions: %{start: false, cancel: true, retry: false, reconcile: false},
        terminal_error: nil
      }
    end

    items = [
      %{
        target_id: "asset:orders",
        item_id: "item_01",
        window_key: "month:2026-06",
        status: :succeeded,
        attempt_count: 1,
        row_count: 42_018
      },
      %{
        target_id: "asset:orders",
        item_id: "item_02",
        window_key: "month:2026-07",
        status: :running,
        attempt_count: 1,
        row_count: nil
      }
    ]

    %{
      "rebuild_page/rebuilds_page" => [
        Example.attrs(:planning, %{
          operations: [operation.(:planned)],
          target_id: "asset:orders",
          planning?: true,
          has_more?: false
        }),
        Example.attrs(:planned, %{
          operations: [operation.(:planned)],
          target_id: "asset:orders",
          plan: %{
            plan_id: "rebuild_plan_01",
            plan_hash: String.duplicate("a", 64),
            expires_at: ~U[2026-07-22 14:00:00Z],
            permissions: %{start: true}
          },
          planning?: false,
          has_more?: false
        }),
        Example.attrs(:empty, %{
          operations: [],
          target_id: "",
          planning?: false,
          has_more?: false
        }),
        Example.attrs(
          :blocked,
          %{
            operations: [operation.(:failed)],
            target_id: "asset:orders",
            error: "Administrator access is required.",
            planning?: false,
            has_more?: false
          },
          "Not permitted. The control must not appear at all."
        )
      ],
      "rebuild_page/rebuild_detail_page" => [
        Example.attrs(:running, %{
          operation: detail_operation.(:building),
          items: items,
          items_has_more?: true
        }),
        Example.attrs(
          :activation_unknown,
          %{
            operation:
              Map.merge(detail_operation.(:activation_unknown), %{
                phase: :activation,
                unknown_outcome: %{kind: "activation_commit_unknown"},
                permissions: %{start: false, cancel: false, retry: false, reconcile: true}
              }),
            items: items,
            items_has_more?: false
          },
          "The activation outcome is unknown. Reconcile, never retry."
        ),
        Example.attrs(:succeeded, %{
          operation:
            Map.merge(detail_operation.(:succeeded), %{
              phase: :terminal,
              progress: %{completed: 12, total: 12},
              cleanup_state: :complete,
              permissions: %{start: false, cancel: false, retry: false, reconcile: false}
            }),
          items: items,
          items_has_more?: false
        }),
        Example.attrs(:failed, %{
          operation:
            Map.merge(detail_operation.(:failed), %{
              phase: :terminal,
              terminal_error: %{
                code: "candidate_validation_failed",
                message: "Candidate validation failed."
              },
              permissions: %{start: false, cancel: true, retry: true, reconcile: false}
            }),
          items: items,
          items_has_more?: false
        })
      ]
    }
  end

  defp recovery do
    plan = fn permissions ->
      %{
        plan_id: "trp_9c1f4a7e2b8d",
        plan_hash: "sha256:5833baa2c1d94f0ab7e6d2c8f1904ab3",
        expires_at: ~U[2026-07-28 12:00:00Z],
        permissions: permissions,
        payload: %{
          target_generation_id: "tg_2f19c4a8",
          materialization_id: "mat_77b0c31d",
          physical_fingerprint: "733c1d7d8a2b4e619f0c5d3e7a1b8c94",
          source_manifest_id: "mv_5833baa2c1d94f0a"
        }
      }
    end

    %{
      "target_recovery_page/page" => [
        Example.attrs(
          :empty,
          %{},
          "Nothing planned yet: the page refuses to imply a table can be adopted."
        ),
        Example.attrs(:planning, %{
          target_id: "duckdb:main.mart_account_health",
          planning?: true
        }),
        Example.attrs(
          :plan_ready,
          %{
            target_id: "duckdb:main.mart_account_health",
            plan: plan.(%{start: true})
          },
          "The backend granted the start permission, so the action is rendered."
        ),
        Example.attrs(
          :plan_without_permission,
          %{
            target_id: "duckdb:main.mart_account_health",
            plan: plan.(%{start: false})
          },
          "Same evidence, no permission: the control must not appear."
        ),
        Example.attrs(:operation_running, %{
          target_id: "duckdb:main.mart_account_health",
          plan: plan.(%{start: false}),
          operation: %{
            operation_id: "tro_41b8ce07d259",
            state: :running,
            phase: :activating_generation,
            target_id: "duckdb:main.mart_account_health",
            target_generation_id: "tg_2f19c4a8",
            compatibility_result: %{status: :ready},
            permissions: %{reconcile: true}
          }
        }),
        Example.attrs(:error, %{
          target_id: "duckdb:main.unknown_table",
          error: "No proven generation exists for this target."
        })
      ]
    }
  end

  defp errors do
    %{
      "error_page/error_page" => [
        Example.attrs(
          :not_found,
          %{
            title: "Asset not found",
            subtitle: "mart_daily_sales",
            description: "No active catalogue entry matches this asset id.",
            tone: :neutral,
            nav_items: Navigation.items(:assets),
            back_navigate: "/assets",
            back_label: "Back to catalogue"
          },
          "A route that resolves to nothing. Neutral tone: nothing broke."
        ),
        Example.attrs(
          :backend_error,
          %{
            title: "Could not load pipeline",
            subtitle: "Elixir.CrmDemo.Pipelines.CrmDaily",
            description: "backend_unavailable",
            nav_items: Navigation.items(:pipelines),
            back_navigate: "/pipelines",
            back_label: "Back to pipelines"
          },
          "The backend failed. Error tone, and still a way back."
        ),
        Example.attrs(
          :manifest_not_set,
          %{
            title: "No active manifest",
            description: "Publish and activate a manifest to inspect schedules.",
            tone: :warning,
            nav_items: Navigation.items(:schedules),
            back_navigate: "/assets",
            back_label: "Back to catalogue"
          },
          "The control plane is reachable but not configured yet."
        )
      ]
    }
  end
end
