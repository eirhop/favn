defmodule FavnView.Dev.DesignSystem.Examples.Components do
  @moduledoc """
  Curated examples for the section components in `FavnView.Components`.

  A section component composes elements into one part of a screen: the shell, a
  rail, a log viewer, a badge family with domain meaning. Unlike an element, a
  section is judged against a state the backend can actually be in, so these
  examples are named after those states — an ambiguous run context, an unknown
  activation outcome, a persisted truncation — and the view models come from
  `FavnView.Dev.DesignSystem.Fixtures`.
  """

  use FavnView, :html

  import FavnView.Components.AppShell

  alias FavnView.Components.ModeRail
  alias FavnView.Components.Navigation
  alias FavnView.Components.ScheduleUi
  alias FavnView.Dev.DesignSystem.Example
  alias FavnView.Dev.DesignSystem.Fixtures
  alias FavnView.Dev.DesignSystem.Fixtures.Timeline

  @doc """
  Every curated section example, keyed by catalogue entry id.
  """
  @spec all() :: %{String.t() => [Example.t()]}
  def all do
    %{}
    |> Map.merge(shell())
    |> Map.merge(navigation())
    |> Map.merge(logs())
    |> Map.merge(schedules())
    |> Map.merge(window_actions())
  end

  defp shell do
    %{
      "app_shell/app_shell" => [
        Example.render(
          :list_screen,
          &list_screen/1,
          "A list screen uses title, subtitle, and navigation only."
        ),
        Example.render(
          :detail_screen,
          &detail_screen/1,
          "A detail screen adds a status, a back link, toolbar facts, and a mode rail."
        ),
        Example.render(
          :failing_screen,
          &failing_screen/1,
          "An error tone in the shell status, for a screen whose subject is failing."
        )
      ],
      "theme_toggle/theme_toggle" => [
        Example.attrs(:default, %{}, "The only control that changes the theme.")
      ]
    }
  end

  defp navigation do
    %{
      "nav_rail/nav_rail" => [
        Example.attrs(
          :assets_active,
          %{items: Navigation.items(:assets)},
          "Every item is a live route. There are no placeholder destinations."
        ),
        Example.attrs(:runs_active, %{items: Navigation.items(:runs)}),
        Example.attrs(
          :nothing_active,
          %{items: Navigation.items()},
          "A screen that is not reachable from the rail marks nothing active."
        )
      ],
      "nav_rail/nav_menu" => [
        Example.attrs(
          :closed,
          %{items: Navigation.items(:runs)},
          "The mobile entry point. Hidden from `md` up, where the rail takes over."
        ),
        Example.attrs(
          :open,
          %{items: Navigation.items(:runs), open: true},
          "Labels are visible here because there is room for them."
        )
      ],
      "mode_rail/mode_rail" => [
        Example.attrs(
          :with_disabled_modes,
          %{
            active: :list,
            modes: [
              %{id: :list, label: "List", icon: "hero-list-bullet"},
              %{id: :tree, label: "Tree", icon: "hero-share", disabled: true},
              %{id: :filters, label: "Filters", icon: "hero-funnel", disabled: true},
              %{id: :more, label: "More", icon: "hero-ellipsis-vertical", disabled: true}
            ]
          },
          "A disabled mode stays visible so the screen's shape does not change."
        )
      ]
    }
  end

  defp logs do
    %{
      "log_viewer/log_viewer" => [
        Example.attrs(:mixed_levels, Fixtures.Logs.viewer_attrs()),
        Example.attrs(
          :empty,
          Fixtures.Logs.viewer_attrs(%{logs: []}),
          "Live and empty: waiting, not broken."
        ),
        Example.attrs(
          :multi_line_sql,
          Fixtures.Logs.viewer_attrs(%{logs: Fixtures.Logs.sql_only(), subtitle: "SQL output"}),
          "SQL keeps its line breaks; the viewer must not collapse them."
        ),
        Example.attrs(
          :stacktrace,
          Fixtures.Logs.viewer_attrs(%{
            logs: Fixtures.Logs.stacktrace(),
            subtitle: "Error output"
          })
        ),
        Example.attrs(
          :wrap_on,
          Fixtures.Logs.viewer_attrs(%{logs: Fixtures.Logs.long(), wrap?: true}),
          "Wrapped: no horizontal scrolling, taller rows."
        ),
        Example.attrs(
          :wrap_off,
          Fixtures.Logs.viewer_attrs(%{logs: Fixtures.Logs.long(), wrap?: false}),
          "Unwrapped: one row per entry, horizontal overflow inside the viewer only."
        ),
        Example.attrs(
          :truncated,
          Fixtures.Logs.viewer_attrs(%{logs: Fixtures.Logs.truncated()}),
          "The persisted output hit its limit. The marker must be unmissable."
        )
      ],
      "output_metadata/output_metadata" => [
        Example.attrs(:operational, %{
          id: "output-metadata-operational",
          status: :ok,
          metadata: %{
            rows_written: 0,
            rows_read: 1800,
            relation: "raw.mercatus.reporting_baseline_feeding",
            mode: :monthly_replace,
            partition_month: "2026-04",
            endpoint: "vReportingBaselineFeeding",
            loaded_at: ~U[2026-05-20 18:06:44Z],
            source: %{system: :mercatus}
          }
        }),
        Example.attrs(
          :empty_success,
          %{id: "output-metadata-empty-success", status: :ok, metadata: %{}},
          "Succeeded with nothing to report; not the same as failing."
        ),
        Example.attrs(
          :failed_before_output,
          %{id: "output-metadata-failed", status: :error, metadata: %{}},
          "Failed before producing output, so there is no metadata to show."
        ),
        Example.attrs(
          :quality_warning,
          %{
            id: "output-metadata-sql-quality-warning",
            status: :ok,
            metadata: %{
              quality_status: :warning,
              write_outcome: :written,
              check_results: [
                %{
                  name: :volume_is_reasonable,
                  phase: :before_materialize,
                  outcome: :warned,
                  message: "Incoming volume is below the expected range",
                  metrics: %{"incoming_rows" => 400, "existing_rows" => 1_000},
                  duration_ms: 84
                }
              ]
            }
          },
          "Written despite a warning: the write happened and the warning stands."
        ),
        Example.attrs(
          :no_op,
          %{
            id: "output-metadata-sql-no-op",
            status: :ok,
            metadata: %{
              quality_status: :passed,
              write_outcome: :no_op,
              reason: :has_rows_to_publish,
              check_results: [
                %{
                  name: :has_rows_to_publish,
                  phase: :before_materialize,
                  outcome: :materialization_skipped,
                  message: "No rows were available; the existing target was kept",
                  metrics: %{"incoming_rows" => 0},
                  duration_ms: 31
                }
              ]
            }
          },
          "Nothing was written and the existing target was kept. This must not read as success-with-data."
        ),
        Example.attrs(
          :nested_and_large,
          %{
            id: "output-metadata-nested-large",
            status: :ok,
            initial_rows: 6,
            metadata: large_metadata()
          },
          "Enough keys to need progressive disclosure."
        )
      ]
    }
  end

  defp schedules do
    %{
      "schedule_ui/activation_badge" => [
        Example.attrs(:pending_activation, %{
          state: :pending_activation,
          label: "Pending activation"
        }),
        Example.attrs(:enabled, %{state: :enabled, label: "Enabled"}),
        Example.attrs(:disabled, %{state: :disabled, label: "Disabled"}),
        Example.attrs(:needs_review, %{state: :needs_review, label: "Needs review"}),
        Example.attrs(:retired, %{state: :retired, label: "Retired"})
      ],
      "schedule_ui/runtime_badge" => [
        Example.attrs(:inactive, %{state: :inactive, label: "Inactive"}),
        Example.attrs(:idle, %{state: :idle, label: "Idle"}),
        Example.attrs(:running, %{state: :running, label: "Running"}),
        Example.attrs(:queued, %{state: :queued, label: "Queued"})
      ],
      "schedule_ui/scheduler_error_badge" => [
        Example.attrs(:none, %{error: nil}, "No error: the badge renders nothing at all."),
        Example.attrs(:submit_failure, %{
          error: %{phase_label: "Submit run", message: "Window policy invalid"}
        })
      ],
      "schedule_ui/occurrence_preview_table" => [
        Example.attrs(:preview_rows, %{occurrences: ScheduleUi.sample_occurrences()}),
        Example.attrs(:empty, %{occurrences: []})
      ]
    }
  end

  defp window_actions do
    refresh = List.last(Timeline.refresh_timeline())
    failed = Enum.find(Timeline.refresh_timeline(), &(&1.status == :error))
    data = List.last(Timeline.data_coverage_timeline())

    %{
      "selected_window_actions/selected_window_actions" => [
        Example.attrs(
          :no_selection_full_refresh,
          %{
            selected_window: nil,
            has_data_windows?: false,
            run_config: Timeline.full_refresh_run_config()
          },
          "An asset with no windows can only be refreshed whole."
        ),
        Example.attrs(:no_selection_windowed, %{
          selected_window: nil,
          has_data_windows?: true,
          active_timeline: :refresh,
          run_config: Timeline.default_run_config()
        }),
        Example.attrs(:selected_refresh_period, %{
          selected_window: refresh,
          has_data_windows?: true,
          active_timeline: :refresh,
          run_config: Timeline.default_run_config()
        }),
        Example.attrs(:selected_data_window, %{
          selected_window: data,
          has_data_windows?: true,
          active_timeline: :data_coverage,
          run_config: Timeline.run_config(:data_coverage_timeline, :day, "2026-06-12")
        }),
        Example.attrs(
          :run_config_open,
          %{
            selected_window: nil,
            has_data_windows?: true,
            active_timeline: :refresh,
            run_config_open?: true,
            run_config: Timeline.default_run_config()
          },
          "The editable form, with no window selected."
        ),
        Example.attrs(
          :prefilled_from_failed_run,
          %{
            selected_window: failed,
            has_data_windows?: true,
            active_timeline: :refresh,
            run_config_open?: true,
            run_config:
              Timeline.run_config(:refresh_timeline, :day, "2026-06-10", "none", "force_all")
          },
          "Retrying a failure prefills the config that failure used."
        ),
        Example.attrs(:submitting, %{
          selected_window: refresh,
          has_data_windows?: true,
          submitting_window_run?: true,
          run_config: Timeline.default_run_config()
        }),
        Example.attrs(:submitted, %{
          selected_window: refresh,
          has_data_windows?: true,
          submitted_run_id: "run_01HZ",
          run_config: Timeline.default_run_config()
        }),
        Example.attrs(:submit_error, %{
          selected_window: refresh,
          has_data_windows?: true,
          selected_window_error: "Could not submit run.",
          run_config: Timeline.default_run_config()
        }),
        Example.attrs(
          :invalid_config,
          %{
            selected_window: refresh,
            has_data_windows?: true,
            run_config_open?: true,
            run_config_valid?: false,
            selected_window_error: "force_selected_upstream requires dependencies=all.",
            run_config:
              Timeline.run_config(
                :refresh_timeline,
                :day,
                "2026-06-12",
                "none",
                "force_selected_upstream"
              )
          },
          "An incompatible combination blocks submission and says why."
        ),
        Example.attrs(
          :not_runnable,
          %{
            selected_window:
              Map.merge(data, %{run_enabled?: false, run_disabled_reason: :invalid_window}),
            has_data_windows?: true,
            active_timeline: :data_coverage,
            run_config: Timeline.run_config(:data_coverage_timeline, :day, "2026-06-12")
          },
          "The window cannot be run, so no control implies that it can."
        )
      ]
    }
  end

  defp list_screen(assigns) do
    assigns = assign(assigns, :nav_items, Navigation.items(:assets))

    ~H"""
    <.app_shell
      title="Asset catalogue"
      subtitle="Browse and monitor all assets"
      nav_items={@nav_items}
    >
      <.shell_content />
    </.app_shell>
    """
  end

  defp detail_screen(assigns) do
    assigns = assign(assigns, :nav_items, Navigation.items(:assets))

    ~H"""
    <.app_shell
      title="customer_orders_daily"
      status="Healthy"
      status_tone={:success}
      back_href="/assets"
      back_label="Back to assets"
      facts={[
        %{label: "Connection", value: "duckdb"},
        %{label: "Window", value: "Day Europe/Oslo"},
        %{label: "Last run", value: "6m ago"}
      ]}
      nav_items={@nav_items}
    >
      <:mode_rail>
        <ModeRail.mode_rail
          active={:timeline}
          modes={[
            %{id: :timeline, label: "Timeline", icon: "hero-calendar-days"},
            %{id: :details, label: "Details", icon: "hero-document-text"}
          ]}
        />
      </:mode_rail>
      <.shell_content />
    </.app_shell>
    """
  end

  defp failing_screen(assigns) do
    assigns = assign(assigns, :nav_items, Navigation.items(:runs))

    ~H"""
    <.app_shell
      title="run_c30f914"
      subtitle="dq_orders_nulls · Day Europe/Oslo"
      status="Failed"
      status_tone={:error}
      back_href="/runs"
      back_label="Back to runs"
      nav_items={@nav_items}
    >
      <.shell_content />
    </.app_shell>
    """
  end

  defp shell_content(assigns) do
    ~H"""
    <.panel padding={:lg} class="mx-auto w-full max-w-4xl">
      <h2 class="text-xl font-medium">Shell content slot</h2>
      <p class="mt-2 text-base-content/60">Page components provide the centred content.</p>
    </.panel>
    """
  end

  defp large_metadata do
    Map.merge(
      %{
        rows_inserted: 42,
        rows_updated: 0,
        rows_deleted: 0,
        source: %{system: :warehouse, endpoint: "/v1/orders", empty: []},
        window: %{kind: :month, start_at: "2026-04-01", end_at: "2026-05-01"},
        flags: %{dry_run: false, warnings: []}
      },
      Map.new(1..16, fn index -> {"diagnostic_#{index}", "value #{index}"} end)
    )
  end
end
