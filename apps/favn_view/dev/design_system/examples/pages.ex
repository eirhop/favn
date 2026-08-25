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

  alias FavnView.AssetCatalogueFilters
  alias FavnView.Auth.Scope
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.LineagePage
  alias FavnView.Components.Navigation
  alias FavnView.Components.PipelineDetailPage
  alias FavnView.Components.PipelinesPage
  alias FavnView.Components.ScheduleDetailPage
  alias FavnView.Components.SchedulesPage
  alias FavnView.Components.RunnersPage
  alias FavnView.Dev.DesignSystem.Example
  alias FavnView.Dev.DesignSystem.Fixtures
  alias FavnView.Dev.DesignSystem.Fixtures.AssetDetail
  alias FavnView.Dev.DesignSystem.Fixtures.RunConfig
  alias FavnView.Dev.DesignSystem.Fixtures.Runs
  alias FavnView.Dev.DesignSystem.Fixtures.RunsList
  alias FavnView.Dev.DesignSystem.Fixtures.Schedules

  @workspace_scope %Scope{
    workspace_id: "workspace-one",
    actor: %{id: "actor-design-system", roles: [:viewer]},
    workspace_configuration: %FavnOrchestrator.WorkspaceConfiguration{
      workspace_id: "workspace-one",
      deployment_id: "deployment-design-system",
      default_timezone: "Etc/UTC",
      default_timezone_source: :application_default
    }
  }

  @doc """
  Every curated page example, keyed by catalogue entry id.
  """
  @spec all() :: %{String.t() => [Example.t()]}
  def all do
    %{}
    |> Map.merge(status())
    |> Map.merge(admin())
    |> Map.merge(account_security())
    |> Map.merge(asset_catalogue())
    |> Map.merge(asset_detail())
    |> Map.merge(lineage())
    |> Map.merge(log_pages())
    |> Map.merge(pipelines())
    |> Map.merge(runs())
    |> Map.merge(runners())
    |> Map.merge(schedules())
    |> Map.merge(rebuilds())
    |> Map.merge(recovery())
    |> Map.merge(errors())
    |> put_workspace_scope()
  end

  defp put_workspace_scope(catalogue) do
    Map.new(catalogue, fn {entry_id, examples} ->
      examples =
        Enum.map(examples, fn
          %Example{attrs: attrs} = example when is_map(attrs) ->
            %{example | attrs: Map.put_new(attrs, :current_scope, @workspace_scope)}

          example ->
            example
        end)

      {entry_id, examples}
    end)
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

  defp admin do
    now = ~U[2026-08-01 10:00:00Z]

    scope = %Scope{
      workspace_id: "workspace-one",
      actor: %{id: "actor-admin", roles: [:admin]},
      session: %{id: "session-current"}
    }

    base = %{
      current_scope: scope,
      operator_workspaces: [
        %{id: "workspace-one", name: "Workspace One", status: :active},
        %{id: "workspace-two", name: "Workspace Two", status: :active}
      ],
      nav_items: Navigation.items(:admin),
      admin_tab: :operators,
      actors: [
        %{
          id: "actor-admin",
          username: "admin@example.com",
          display_name: "Workspace Admin",
          roles: [:admin],
          status: :active,
          membership_status: :active
        },
        %{
          id: "actor-operator",
          username: "operator@example.com",
          display_name: "Data Operator",
          roles: [:operator],
          status: :active,
          membership_status: :active
        }
      ],
      actors_has_more?: false,
      sessions: [
        %{
          id: "session-current",
          actor_id: "actor-admin",
          provider: "password",
          status: :active,
          expires_at: DateTime.add(now, 86_400, :second)
        },
        %{
          id: "session-operator",
          actor_id: "actor-operator",
          provider: "azure_ad",
          status: :active,
          expires_at: DateTime.add(now, 43_200, :second)
        }
      ],
      sessions_has_more?: false,
      audit: [
        %{
          action: "membership.updated",
          subject_kind: "actor",
          subject_id: "actor-operator",
          principal_id: "actor-admin",
          occurred_at: now
        }
      ],
      audit_has_more?: false
    }

    %{
      "admin_page/admin_page" => [
        Example.attrs(
          :operators,
          base,
          "The focused default tab for creating operators and managing workspace membership."
        ),
        Example.attrs(
          :sessions,
          Map.put(base, :admin_tab, :sessions),
          "Session controls are separated from identity changes so revocation is easy to find."
        ),
        Example.attrs(
          :audit,
          Map.put(base, :admin_tab, :audit),
          "Audit history is available without competing with the operational controls."
        )
      ]
    }
  end

  defp account_security do
    scope = %Scope{
      workspace_id: "workspace-one",
      actor: %{id: "actor-viewer", roles: [:viewer]},
      session: %{id: "session-current"}
    }

    %{
      "account_security_page/account_security_page" => [
        Example.attrs(
          :default,
          %{
            current_scope: scope,
            operator_workspaces: [
              %{id: "workspace-one", name: "Workspace One", status: :active}
            ],
            nav_items: Navigation.items()
          },
          "A labelled password form in the shared application shell."
        )
      ]
    }
  end

  defp asset_catalogue do
    filters = AssetCatalogueFilters.defaults()
    assets = AssetCataloguePage.sample_assets()

    base = %{
      assets: assets,
      filters: filters,
      active_mode: :list,
      loading: false,
      error: nil,
      nav_items: AssetCataloguePage.nav_items(:assets),
      connection_options: AssetCataloguePage.connection_options(),
      catalogue_options: AssetCataloguePage.catalogue_options(),
      schema_options: AssetCataloguePage.schema_options(),
      scope_choices: AssetCatalogueFilters.scope_choices(assets, filters)
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
            filters: %{
              search: "orders",
              connection: "duckdb",
              catalogue: "mart",
              schema: "marketing",
              scope: "all"
            }
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
          :overview,
          AssetDetail.base_attrs(),
          "The default screen: what state the asset is in, what feeds it, and the one " <>
            "action worth taking."
        ),
        Example.attrs(
          :full_refresh_asset,
          AssetDetail.attrs(%{
            title: "stg_payments",
            status: "Unknown",
            status_tone: :neutral,
            has_data_windows?: false,
            freshness: FavnView.Components.AssetDetailPage.sample_freshness(:unknown)
          }),
          "An asset with no windows at all, so the rail offers no coverage page."
        ),
        Example.attrs(
          :coverage_complete,
          AssetDetail.coverage_attrs(%{}),
          "The coverage page: every expected day has data, so every cell is quiet."
        ),
        Example.attrs(
          :incomplete_coverage,
          AssetDetail.coverage_attrs(%{
            coverage: AssetDetail.coverage(:incomplete),
            coverage_missing: [8, 15]
          }),
          "Two missing days a week apart, with months either side to step to. The " <>
            "pattern is the point: a list of window keys could not show it."
        ),
        Example.attrs(
          :selected_coverage_periods,
          AssetDetail.coverage_attrs(%{
            coverage: AssetDetail.coverage(:incomplete),
            coverage_missing: [8, 15],
            coverage_selected: AssetDetail.coverage_selection([8])
          }),
          "One day picked for backfill. The button counts the selection rather than " <>
            "every gap."
        ),
        Example.attrs(
          :coverage_at_its_start,
          AssetDetail.coverage_attrs(%{
            coverage: AssetDetail.coverage(:incomplete),
            coverage_missing: [3, 4, 5],
            coverage_navigation: AssetDetail.coverage_navigation_at_start()
          }),
          "The first month coverage has. There is no step back, so the operator can " <>
            "tell they have reached the beginning rather than a button that refuses."
        ),
        Example.attrs(
          :viewer_coverage,
          AssetDetail.coverage_attrs(%{
            can_submit_runs?: false,
            coverage: AssetDetail.coverage(:incomplete),
            coverage_missing: [8, 15]
          }),
          "A viewer sees exactly which days are missing and is told why they cannot " <>
            "fill them. The cells stop being controls rather than looking broken."
        ),
        Example.attrs(
          :unknown_coverage,
          AssetDetail.coverage_attrs(%{
            coverage: AssetDetail.coverage(:unknown),
            coverage_policy: nil
          }),
          "Coverage was never declared. No calendar and no navigator, because an " <>
            "empty grid would read as complete coverage."
        ),
        Example.attrs(
          :coverage_plan_review,
          AssetDetail.coverage_attrs(%{
            coverage: AssetDetail.coverage(:incomplete),
            coverage_missing: [8, 15],
            coverage_plan: AssetDetail.coverage_plan()
          }),
          "The plan names the days in words. The operator confirms what they read."
        ),
        Example.attrs(
          :docs_sql,
          AssetDetail.documentation_attrs(:sql),
          "A SQL asset: the author's own words, then the query and what it reads."
        ),
        Example.attrs(
          :docs_elixir,
          AssetDetail.documentation_attrs(:elixir),
          "An Elixir asset has no query, so it names the function Favn calls instead."
        ),
        Example.attrs(
          :docs_undocumented,
          AssetDetail.documentation_attrs(:undocumented),
          "Nothing authored at all. The page says how to fix that rather than " <>
            "rendering four empty panels."
        ),
        Example.attrs(
          :diagnostics,
          AssetDetail.diagnostics_attrs(%{}),
          "A healthy target says so in a sentence and offers no rebuild, because " <>
            "there is nothing to rebuild."
        ),
        Example.attrs(
          :diagnostics_without_a_table,
          AssetDetail.diagnostics_attrs(%{
            compatibility: AssetDetail.compatibility(:unmanaged),
            coverage: AssetDetail.coverage(:unknown),
            coverage_policy: nil
          }),
          "An asset that manages no table of its own. No verdict, no badge, and no " <>
            "rebuild — the page says why rather than showing an empty one."
        ),
        Example.attrs(
          :rebuild_available,
          AssetDetail.diagnostics_attrs(%{
            compatibility: AssetDetail.compatibility(:rebuild_available)
          }),
          "A rebuild is possible and writes still work."
        ),
        Example.attrs(
          :rebuild_required,
          AssetDetail.diagnostics_attrs(%{
            can_run_asset?: false,
            compatibility: AssetDetail.compatibility(:rebuild_required)
          }),
          "Writes are blocked until the target is rebuilt."
        ),
        Example.attrs(
          :blocked_writes_on_overview,
          AssetDetail.attrs(%{
            can_run_asset?: false,
            compatibility: AssetDetail.compatibility(:rebuild_required)
          }),
          "The overview raises blocked writes itself, because that changes what the " <>
            "operator can do next. A rebuild that blocks nothing stays on diagnostics."
        ),
        Example.attrs(
          :unexpected_drift,
          AssetDetail.diagnostics_attrs(%{
            can_run_asset?: false,
            compatibility: AssetDetail.compatibility(:unexpected_drift)
          }),
          "The physical target changed underneath Favn. Must not look like a normal rebuild."
        ),
        Example.attrs(
          :operator_decision,
          AssetDetail.diagnostics_attrs(%{
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
            run_context_status: :ambiguous
          }),
          "Two pipelines could own this asset, so it cannot be run until one is chosen."
        ),
        Example.attrs(
          :run_config_open,
          AssetDetail.attrs(%{
            run_config_open?: true,
            run_config: RunConfig.default_run_config()
          }),
          "The run dialog, over the page rather than inside a card. It lives at page " <>
            "level because a panel sets a backdrop-filter, which clips a fixed-position " <>
            "child to the card."
        ),
        Example.attrs(
          :run_config_error,
          AssetDetail.attrs(%{
            run_config_open?: true,
            run_config:
              RunConfig.run_config(:refresh_timeline, :day, "2026-06-10", "none", "force_all"),
            run_error: "Choose a period Favn can resolve."
          }),
          "A configuration that cannot be submitted says so inside the dialog, where the " <>
            "field that caused it is."
        ),
        Example.attrs(
          :viewer_cannot_run,
          AssetDetail.attrs(%{can_submit_runs?: false}),
          "A viewer sees the asset and a disabled action that explains itself on hover."
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
          :run_selected,
          AssetDetail.selected_run_attrs(),
          "One run open: the contract now says what that run observed, not what the " <>
            "asset last did. Everything held, so the columns table stays shut."
        ),
        Example.attrs(
          :run_failed,
          AssetDetail.failed_run_attrs(),
          "A check broke and the table drifted. The columns table opens itself, and " <>
            "the difference is named in words rather than left to the reader."
        ),
        Example.attrs(
          :contract_without_a_run,
          AssetDetail.assurance_attrs(),
          "A contract with a composed fragment and a parameterised row-count claim."
        )
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
      refresh: "missing",
      combine_windows: false
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
          "One exact window run with only the asset fields rendered by Flow."
        ),
        Example.attrs(
          :failed_backfill,
          %{
            run: Runs.backfill(:partial),
            run_id: "run_backfill_8f2c9d1",
            nav_items: Runs.nav_items()
          },
          "A failed asset remains visible without loading its detail payload."
        ),
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
        Example.attrs(:window_selector, %{
          run: Runs.backfill(:running),
          run_id: "run_backfill_8f2c9d1",
          nav_items: Runs.nav_items(),
          windows: [
            %{run_id: "run_backfill_8f2c9d1", label: "Feb 1, 2026 – Mar 1, 2026"},
            %{run_id: "run_backfill_91ac3e2", label: "Mar 1, 2026 – Apr 1, 2026"}
          ]
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
        ),
        Example.attrs(:submission_queued, %{
          run: Runs.submission(:queued),
          run_id: "run_submission_crm_reference",
          nav_items: Runs.nav_items()
        }),
        Example.attrs(:submission_preparing, %{
          run: Runs.submission(:preparing),
          run_id: "run_submission_crm_reference",
          nav_items: Runs.nav_items()
        }),
        Example.attrs(
          :submission_failed,
          %{
            run: Runs.submission(:failed),
            run_id: "run_submission_crm_reference",
            nav_items: Runs.nav_items()
          },
          "Preparation failed before admission; the durable failure points to runner diagnostics."
        )
      ],
      "run_asset_attempt_page/run_asset_attempt_page" => [
        Example.attrs(:succeeded, %{
          run_id: "run_backfill_8f2c9d1",
          nav_items: Runs.nav_items(),
          attempt: %{
            id: "orders-2026-02",
            name: "Orders",
            raw_status: :ok,
            status_label: "Succeeded",
            status_tone: :success,
            error_summary: nil,
            output_metadata: %{
              "rows_written" => 82_101,
              "relation" => "warehouse.crm_orders",
              "write_outcome" => "committed"
            },
            facts: [
              %{label: "Started", value: "Jul 23, 2026 10:00:00 UTC"},
              %{label: "Finished", value: "Jul 23, 2026 10:00:34 UTC"},
              %{label: "Duration", value: "34.0 s"}
            ],
            execution_facts: [
              %{label: "Asset", value: "crm.orders"},
              %{label: "Attempt", value: 1},
              %{label: "Stage", value: 1},
              %{label: "Execution pool", value: "default"},
              %{label: "Queue reason", value: "-"},
              %{label: "Window", value: "Feb 2026"},
              %{label: "Run id", value: "run_backfill_8f2c9d1"},
              %{label: "Asset step id", value: "orders-2026-02"}
            ],
            logs_href: "/runs/run_backfill_8f2c9d1/assets/orders-2026-02/logs"
          }
        }),
        Example.attrs(:loading, %{
          run_id: "run_backfill_8f2c9d1",
          nav_items: Runs.nav_items(),
          loading?: true
        }),
        Example.attrs(:not_found, %{
          run_id: "run_backfill_8f2c9d1",
          nav_items: Runs.nav_items(),
          error: "Asset run not found."
        })
      ]
    }
  end

  defp runners do
    now = ~U[2026-07-31 12:00:00Z]

    runner = %{
      runner_instance_id: "runner-local-duckdb",
      runner_pool: "duckdb",
      required_runner_release_id: "rr_local",
      status: :idle,
      supported_task_kinds: [:relation_inspection, :asset_attempt],
      capabilities: ["relation_inspection", "asset_execution"],
      registered_at: now,
      active_task_id: nil
    }

    failed_task = %{
      task_id: "runner-task-inspection",
      task_kind: :relation_inspection,
      status: :failed,
      runner_pool: "duckdb",
      assigned_runner_instance_id: "runner-local-duckdb",
      enqueued_at: now,
      failure: %{
        title: "Driver unavailable",
        message: "failed to initialize DuckDB ADBC connection: driver unavailable",
        remediation: "Set DUCKDB_ADBC_DRIVER and restart the runner.",
        code: "driver_unavailable"
      }
    }

    %{
      "runners_page/runners_page" => [
        Example.attrs(:connected, %{
          overview: %{
            runner_count: 1,
            registry_status: :available,
            runners: [runner],
            tasks: [],
            failures: [],
            observed_at: now
          },
          nav_items: RunnersPage.nav_items()
        }),
        Example.attrs(:failed_task, %{
          overview: %{
            runner_count: 0,
            registry_status: :available,
            runners: [],
            tasks: [failed_task],
            failures: [failed_task],
            observed_at: now
          },
          nav_items: RunnersPage.nav_items()
        }),
        Example.attrs(:empty, %{
          overview: %{
            runner_count: 0,
            registry_status: :available,
            runners: [],
            tasks: [],
            failures: [],
            observed_at: now
          },
          nav_items: RunnersPage.nav_items()
        }),
        Example.attrs(:loading, %{loading: true, nav_items: RunnersPage.nav_items()}),
        Example.attrs(:error, %{
          error: "Runner diagnostics are unavailable",
          nav_items: RunnersPage.nav_items()
        })
      ]
    }
  end

  defp schedules do
    empty_schedules = %{
      schedules: [],
      all_schedules: [],
      filters: Schedules.filters(),
      filter_options: %{pipelines: [], windows: []},
      scope_choices: Schedules.scope_choices([]),
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
          scope_choices: Schedules.scope_choices(),
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
