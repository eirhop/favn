defmodule FavnView.Dev.DesignSystem.Examples.Elements do
  @moduledoc """
  Curated examples for the `FavnView.UI` element library.

  Elements are the layer that owns colour, border, radius, and spacing, so the
  examples here are chosen to make a mistake in that layer visible: every tone
  in every family, every variant side by side, the long label that must not wrap,
  and the states a screen actually reaches.

  Tone and size matrices are deliberately *not* enumerated here. Those attrs
  declare `values:`, so `?mode=matrix` walks them from the component's own
  metadata and a new tone appears without an edit to this file. What is written
  by hand is what metadata cannot know: realistic copy, slot content, and
  combinations that only matter together.
  """

  use FavnView, :html

  alias FavnView.CoverageCalendar
  alias FavnView.Dev.DesignSystem.Example

  @doc """
  Every curated element example, keyed by catalogue entry id.
  """
  @spec all() :: %{String.t() => [Example.t()]}
  def all do
    %{}
    |> Map.merge(badges())
    |> Map.merge(buttons())
    |> Map.merge(typography())
    |> Map.merge(layout())
    |> Map.merge(surfaces())
    |> Map.merge(data())
    |> Map.merge(fields())
    |> Map.merge(states())
    |> Map.merge(icons())
  end

  defp badges do
    %{
      "badge/badge" => [
        Example.render(
          :tones,
          &badge_tones/1,
          "The full tone vocabulary. Nothing outside this list is a valid colour."
        ),
        Example.render(
          :variants,
          &badge_variants/1,
          "Outline steps back when a row already carries a status badge."
        ),
        Example.render(
          :sizes,
          &badge_sizes/1,
          "The smallest box is the tightest fit: at `xs` a label has 14px of content height to sit in."
        ),
        Example.render(:with_icon, &badge_with_icon/1),
        Example.render(
          :overflow,
          &badge_overflow/1,
          "A long label must not wrap the row it sits in."
        )
      ],
      "badge/status_badge" => [
        Example.render(
          :lifecycle,
          &status_badge_lifecycle/1,
          "The lifecycle state of the thing a row or panel is about."
        ),
        Example.render(
          :domain_statuses,
          &status_badge_domain/1,
          "Domain atoms are normalised by `FavnView.UI.Tokens.tone/1`."
        ),
        Example.attrs(
          :shell_glow,
          %{tone: :success, label: "Healthy", size: :sm, glow: true},
          "`glow` is reserved for the shell's live status, at the size the shell uses."
        )
      ],
      "badge/status_dot" => [
        Example.render(:tones, &status_dot_tones/1, "The dot carries tone without a label box.")
      ]
    }
  end

  defp buttons do
    %{
      "button/button" => [
        Example.render(
          :variants,
          &button_variants/1,
          "One primary per view state; everything else supports it."
        ),
        Example.render(:sizes, &button_sizes/1),
        Example.render(:with_icons, &button_icons/1),
        Example.render(
          :states,
          &button_states/1,
          "Loading keeps the label so the button does not change width."
        ),
        Example.render(:block, &button_block/1)
      ],
      "button/copy_button" => [
        Example.render(
          :labelled_and_bare,
          &copy_button_shapes/1,
          "Labelled beside a value; bare in a table cell, where the column already names it."
        )
      ],
      "button/icon_button" => [
        Example.render(
          :variants,
          &icon_button_variants/1,
          "`label` is required: it is the accessible name and the tooltip."
        ),
        Example.render(:shapes_and_tones, &icon_button_shapes/1),
        Example.render(
          :tooltips,
          &icon_button_tooltips/1,
          "Rail items point the tooltip away from the rail."
        )
      ]
    }
  end

  defp typography do
    %{
      "typography/page_title" => [
        Example.render(
          :page_title,
          &page_title_example/1,
          "The shell owns this; pages do not repeat it."
        )
      ],
      "typography/section_title" => [
        Example.render(:section_title, &section_title_example/1, "A panel or group heading."),
        Example.render(
          :overflow,
          &section_title_overflow/1,
          "Section titles truncate rather than wrap."
        )
      ],
      "typography/eyebrow" => [Example.render(:eyebrow, &eyebrow_example/1)],
      "typography/meta" => [Example.render(:meta, &meta_example/1)]
    }
  end

  defp layout do
    %{
      "layout/stack" => [
        Example.render(
          :page_sections,
          &stack_sections/1,
          "The gap page components use between sections."
        ),
        Example.render(:list_rows, &stack_rows/1, "The gap between list rows.")
      ],
      "layout/inline" => [
        Example.render(
          :badges,
          &inline_badges/1,
          "`inline/1` is the row rhythm: badges and metadata inside a card."
        )
      ],
      "layout/columns" => [
        Example.render(
          :metrics,
          &columns_metrics/1,
          "`columns/1` is for equal-weight children only."
        )
      ]
    }
  end

  defp surfaces do
    %{
      "surface/panel" => [
        Example.render(
          :plain,
          &panel_plain/1,
          "A page section. The default padding is the one every panel should use."
        ),
        Example.render(
          :with_header,
          &panel_with_header/1,
          "A header names the section; actions belong on the same row."
        ),
        Example.render(:with_footer, &panel_with_footer/1),
        Example.render(
          :no_padding,
          &panel_no_padding/1,
          "`padding={:none}` is for panels that own a scroll region or a table."
        )
      ],
      "surface/list_card" => [
        Example.render(:navigable, &list_card_navigable/1, "The whole card is one link."),
        Example.render(:selected, &list_card_selected/1),
        Example.render(
          :inert,
          &list_card_inert/1,
          "No link and no click: a grouped fact, not an affordance."
        ),
        Example.render(:overflow, &list_card_overflow/1)
      ],
      "surface/control_surface" => [
        Example.render(
          :control,
          &control_surface_example/1,
          "The surface compact controls sit on."
        )
      ]
    }
  end

  defp data do
    %{
      "data/data_table" => [
        Example.render(
          :with_row_navigation,
          &data_table_navigable/1,
          "A trailing chevron column appears automatically when rows are navigable."
        ),
        Example.render(:with_actions, &data_table_actions/1),
        Example.render(
          :no_rows,
          &data_table_empty/1,
          "An empty table renders headers only. Pair it with an empty state instead."
        )
      ],
      "data/table_panel" => [
        Example.render(
          :list_screen_standard,
          &table_panel_example/1,
          "The whole list-screen standard: toolbar, then a table whose header pins " <>
            "because the rows scroll. Every list screen composes exactly these three."
        )
      ],
      "data/table_toolbar" => [
        Example.render(
          :search_and_filters,
          &table_toolbar_example/1,
          "Search and selects sit at the end, a result count after them."
        ),
        Example.render(
          :with_scopes,
          &table_toolbar_scopes_example/1,
          "A scope rail carrying counts goes at the start, as on /runs."
        ),
        Example.render(
          :filters_open,
          &table_toolbar_open_example/1,
          "The narrow-screen disclosure opened. Below `lg` the filters are always " <>
            "behind the Filters control, so this is the only way to see them there."
        )
      ],
      "data/stacked_cell" => [
        Example.render(
          :value_and_qualifier,
          &stacked_cell_example/1,
          "The second line is what makes the first legible: an id, a namespace, a timezone."
        )
      ],
      "data/fact_list" => [
        Example.attrs(
          :three_columns,
          %{
            facts: [
              %{label: "Trigger", value: "Schedule"},
              %{label: "Started", value: "Today 06:00"},
              %{label: "Duration", value: "34.5 s"}
            ]
          },
          "The shell toolbar uses this to summarise the current screen."
        ),
        Example.attrs(
          :with_tones,
          %{
            facts: [
              %{label: "Windows", value: "28 / 30"},
              %{label: "Failures", value: 2, tone: :error},
              %{label: "Coverage", value: "Incomplete", tone: :warning},
              %{label: "Compatibility", value: "Compatible", tone: :success}
            ],
            columns: 4
          },
          "A tone marks the one fact that needs attention."
        ),
        Example.attrs(
          :overflow,
          %{
            facts: [
              %{
                label: "Manifest version",
                value: "mv_5833baa2c1d94f0ab7e6d2c8f1904ab35833baa2c1d94f0a"
              },
              %{label: "Runner release", value: "rr_733c1d7d8a2b4e619f0c5d3e7a1b8c94"}
            ],
            columns: 2
          },
          "Values truncate and expose the full text as a title attribute."
        )
      ],
      "data/metric" => [
        Example.render(:summary_band, &metrics/1, "The band a list screen opens with.")
      ],
      "data/outcome_meter" => [
        Example.attrs(
          :mixed,
          %{
            segments: [
              %{tone: :success, count: 9, label: "succeeded"},
              %{tone: :error, count: 1, label: "failed"},
              %{tone: :info, count: 2, label: "running"},
              %{tone: :neutral, count: 2, label: "queued"}
            ],
            summary: "14 assets"
          },
          "One bar replaces four counters, and the legend is the bar's own key."
        ),
        Example.attrs(
          :single_outcome,
          %{
            segments: [
              %{tone: :success, count: 14, label: "succeeded"},
              %{tone: :error, count: 0, label: "failed"},
              %{tone: :info, count: 0, label: "running"}
            ],
            summary: "14 assets"
          },
          "An outcome that did not occur is absent. Nothing spends a row saying zero."
        ),
        Example.attrs(
          :one_failure_in_many,
          %{
            segments: [
              %{tone: :success, count: 199, label: "succeeded"},
              %{tone: :error, count: 1, label: "failed"}
            ],
            summary: "200 assets"
          },
          "A single failure among two hundred keeps a visible sliver rather than rounding away."
        ),
        Example.attrs(
          :compact,
          %{
            segments: [
              %{tone: :success, count: 3, label: "succeeded"},
              %{tone: :error, count: 1, label: "failed"}
            ],
            size: :sm,
            legend?: false
          },
          "Inside a table cell, where the row already carries the counts."
        ),
        Example.attrs(
          :nothing_yet,
          %{segments: [], summary: "No assets"},
          "An empty population renders an empty track, not a broken bar."
        )
      ],
      "data/mono" => [
        Example.attrs(:identifier, %{value: "run_2026_06_12"}, "Identifiers are monospaced.")
      ],
      "data/lineage_graph" => [
        Example.attrs(
          :fan_in,
          %{
            centre: %{label: "customer_orders_daily", icon: "hero-table-cells"},
            inputs: lineage_nodes(3),
            outputs: lineage_nodes(1)
          },
          "Three inputs fan into a shared bus. The edges are drawn, not implied."
        ),
        Example.attrs(
          :one_to_one,
          %{
            centre: %{label: "customer_orders_daily", icon: "hero-table-cells"},
            inputs: lineage_nodes(1),
            outputs: lineage_nodes(1)
          },
          "A single input on each side is one straight line rather than an elbow."
        ),
        Example.attrs(
          :fan_out,
          %{
            centre: %{label: "stg_orders", icon: "hero-cloud-arrow-down"},
            inputs: [],
            outputs: lineage_nodes(4),
            inputs_empty: "Nothing. This asset reads its source directly."
          },
          "A source asset: nothing upstream, and four assets reading it."
        ),
        Example.attrs(
          :isolated,
          %{
            centre: %{label: "scratch_table", icon: "hero-table-cells"},
            inputs: [],
            outputs: [],
            inputs_empty: "Nothing. This asset reads its source directly.",
            outputs_empty: "Nothing yet. No other asset reads this one."
          },
          "Both sides empty. The node still reads as a node rather than a bare label."
        ),
        Example.attrs(
          :undeployed_input,
          %{
            centre: %{label: "customer_orders_daily", icon: "hero-table-cells"},
            inputs: [
              %{label: "stg_orders", icon: "hero-table-cells", navigate: "/assets/stg_orders"},
              %{
                label: "legacy_pricing_with_a_long_name",
                icon: "hero-cube",
                note: "not in this deployment"
              }
            ],
            outputs: lineage_nodes(1)
          },
          "A declared input this deployment does not carry: dashed, unlinked, and " <>
            "named, because a silently shorter list reads as a complete one."
        )
      ],
      "data/run_timeline" => [
        Example.attrs(
          :selected,
          %{
            runs: run_timeline_runs(),
            selected_id: "run-b",
            class: "favn-surface-list rounded-box max-h-96 w-80 p-3"
          },
          "A day heading appears once however many runs it holds, and the selected " <>
            "run is the one in the address bar rather than one the rail remembers."
        ),
        Example.attrs(
          :unselected,
          %{
            runs: run_timeline_runs(),
            class: "favn-surface-list rounded-box max-h-96 w-80 p-3"
          },
          "Nothing selected: the failure still reads as the one entry worth opening."
        ),
        Example.attrs(
          :empty,
          %{
            runs: [],
            empty_label: "This asset has not run yet.",
            class: "favn-surface-list rounded-box w-80 p-3"
          },
          "An asset with no history says so instead of showing an empty rail."
        )
      ],
      "data/coverage_calendar" => [
        Example.attrs(
          :scattered_gaps,
          calendar_attrs(~w(2026-07-08 2026-07-15 2026-07-22), []),
          "Three missing days, one a week apart. The pattern is what a list of " <>
            "window keys cannot show."
        ),
        Example.attrs(
          :consecutive_gaps,
          calendar_attrs(~w(2026-07-13 2026-07-14 2026-07-15 2026-07-16), []),
          "A four-day outage. Same count as three scattered days, entirely " <>
            "different cause."
        ),
        Example.attrs(
          :selected,
          calendar_attrs(
            ~w(2026-07-08 2026-07-15),
            ~w(day:Europe/Oslo:2026-07-08)
          ),
          "One of two missing days picked for backfill. Selection has to beat the " <>
            "missing wash without becoming the loudest thing on the page."
        ),
        Example.attrs(
          :complete,
          calendar_attrs([], []),
          "Every expected day has data. The grid is deliberately quiet, so the eye " <>
            "goes to a gap the moment one appears."
        ),
        Example.attrs(
          :read_only,
          Map.put(calendar_attrs(~w(2026-07-08), []), :on_select, nil),
          "Without an event, missing days are marked but not selectable — the state " <>
            "for a viewer who cannot submit a backfill."
        ),
        Example.attrs(
          :months,
          %{
            layout: :grid,
            columns: 6,
            column_labels: [],
            groups:
              CoverageCalendar.build(%{
                examined: %{
                  kind: :month,
                  timezone: "Europe/Oslo",
                  from: ~U[2025-09-01 00:00:00Z],
                  through: ~U[2026-07-01 00:00:00Z],
                  count: 11
                },
                gaps: [
                  %{
                    window_key: "month:Europe/Oslo:2025-12",
                    kind: :month,
                    start_at: ~U[2025-12-01 00:00:00Z]
                  }
                ]
              }).groups,
            on_select: "toggle_coverage_window"
          },
          "A monthly asset. Cells are months and there are no weekday columns, so " <>
            "the grid never implies a week that does not exist."
        ),
        Example.attrs(
          :empty,
          %{layout: :empty, groups: [], empty_label: "Coverage is not tracked here."},
          "Nothing examined at all. An empty grid would read as complete coverage, " <>
            "so there is no grid."
        )
      ]
    }
  end

  # Built through `CoverageCalendar.build/1` rather than hand-written, so an example
  # cannot show a layout the real derivation never produces.
  defp calendar_attrs(missing_dates, selected) do
    calendar =
      CoverageCalendar.build(%{
        examined: %{
          kind: :day,
          timezone: "Europe/Oslo",
          from: ~U[2026-07-01 00:00:00Z],
          through: ~U[2026-07-31 00:00:00Z],
          count: 31
        },
        gaps: Enum.map(missing_dates, &calendar_gap/1),
        selected: selected
      })

    %{
      layout: calendar.layout,
      columns: calendar.columns,
      column_labels: calendar.column_labels,
      groups: calendar.groups,
      on_select: "toggle_coverage_window"
    }
  end

  defp calendar_gap(date) do
    {:ok, start_at, _offset} = DateTime.from_iso8601(date <> "T00:00:00Z")

    %{
      window_key: "day:Europe/Oslo:" <> date,
      kind: :day,
      timezone: "Europe/Oslo",
      start_at: start_at,
      end_at: DateTime.add(start_at, 1, :day)
    }
  end

  defp lineage_nodes(count) do
    names = ~w(stg_orders stg_customers stg_products dim_calendar)

    Enum.map(0..(count - 1), fn index ->
      name = Enum.at(names, rem(index, length(names)))
      %{label: name, icon: "hero-table-cells", navigate: "/assets/#{name}"}
    end)
  end

  defp run_timeline_runs do
    [
      run_timeline_run("run-d", "Aug 3", "10:25", :success, "Daily Aug 3", "4.2s"),
      run_timeline_run("run-c", "Aug 3", "04:10", :success, "Daily Aug 3", "3.9s"),
      run_timeline_run("run-b", "Aug 2", "10:25", :error, "Daily Aug 2", "1.1s"),
      run_timeline_run("run-a", "Jul 31", "10:25", :info, "Daily Jul 31", nil)
    ]
  end

  defp run_timeline_run(id, day_label, time_label, tone, window_label, duration_label) do
    %{
      id: id,
      patch: "/assets/orders/runs/#{id}",
      status_tone: tone,
      status_label: run_timeline_status_label(tone),
      trigger_label: "Schedule",
      day_label: day_label,
      time_label: time_label,
      duration_label: duration_label,
      window_label: window_label
    }
  end

  defp run_timeline_status_label(:success), do: "Succeeded"
  defp run_timeline_status_label(:error), do: "Failed"
  defp run_timeline_status_label(_tone), do: "Running"

  defp fields do
    %{
      "field/filter_bar" => [
        Example.render(
          :search_and_selects,
          &filter_bar_example/1,
          "Filters submit on change. None of them carries a visible label."
        )
      ],
      "field/input" => [
        Example.attrs(
          :text,
          %{name: "username", label: "Username", value: "operator"},
          "A field always has a visible label, unlike a filter."
        ),
        Example.attrs(:password, %{
          name: "password",
          label: "Password",
          type: "password",
          value: ""
        }),
        Example.attrs(:select, %{
          name: "refresh",
          label: "Refresh mode",
          type: "select",
          value: "auto",
          options: [{"Auto", "auto"}, {"Force all", "force_all"}]
        }),
        Example.attrs(:checkbox, %{
          name: "deps",
          label: "Include dependencies",
          type: "checkbox",
          value: true
        }),
        Example.attrs(
          :with_error,
          %{name: "from", label: "From", value: "2026-13-01", errors: ["is not a valid date"]},
          "An invalid value keeps the input and names the problem."
        )
      ]
    }
  end

  defp states do
    %{
      "state/loading_state" => [
        Example.attrs(
          :loading,
          %{label: "Loading assets"},
          "Replaces the content while the first read is in flight."
        )
      ],
      "state/empty_state" => [
        Example.attrs(
          :empty,
          %{
            title: "No assets found",
            description: "Try changing the search or filters.",
            icon: "hero-magnifying-glass"
          },
          "The request succeeded and returned nothing. Not an error."
        ),
        Example.render(
          :with_action,
          &empty_state_with_action/1,
          "At most one action, and it must actually resolve the emptiness."
        ),
        Example.attrs(:long_text, %{
          title: "No runs match these filters",
          description:
            "Nothing in the selected window, status, and trigger combination produced a run. Widen the window or clear the trigger filter to see more."
        })
      ],
      "state/error_state" => [
        Example.render(
          :with_retry,
          &error_state_with_action/1,
          "The request failed. Always offer a way forward."
        ),
        Example.attrs(
          :warning_tone,
          %{
            tone: :warning,
            title: "Lineage is incomplete",
            description: "The graph exceeded the render limit and was truncated."
          },
          "Warning tone for a degraded read that still returned something usable."
        ),
        Example.attrs(
          :without_action,
          %{title: "Could not load schedules", description: "active_manifest_not_set"},
          "No action when there is genuinely nothing the operator can do here."
        )
      ],
      "state/notice" => [
        Example.render(
          :tones,
          &notice_tones/1,
          "An inline condition inside a panel that otherwise rendered."
        )
      ],
      "state/inline_loading" => [Example.attrs(:refreshing, %{label: "Refreshing"})]
    }
  end

  defp icons do
    %{
      "icon/icon" => [
        Example.render(:sizes, &icon_sizes/1, "Pass `size`, never a `size-*` utility."),
        Example.render(
          :navigation,
          &icon_navigation/1,
          "The icons the primary navigation uses."
        ),
        Example.render(
          :styles,
          &icon_styles/1,
          "Solid and mini variants come from the same plugin."
        )
      ]
    }
  end

  defp badge_tones(assigns) do
    ~H"""
    <.inline gap={:sm}>
      <.badge :for={tone <- Tokens.tones()} tone={tone}>{tone_label(tone)}</.badge>
    </.inline>
    """
  end

  defp badge_variants(assigns) do
    ~H"""
    <.inline gap={:sm}>
      <.badge tone={:info} variant={:soft}>Soft</.badge>

      <.badge tone={:info} variant={:outline}>Outline</.badge>

      <.badge tone={:info} variant={:solid}>Solid</.badge>
    </.inline>
    """
  end

  defp badge_sizes(assigns) do
    ~H"""
    <.inline gap={:sm}>
      <.badge tone={:success} size={:xs}>Healthy</.badge>

      <.badge tone={:success} size={:sm}>Healthy</.badge>

      <.badge tone={:success} size={:md}>Healthy</.badge>
    </.inline>
    """
  end

  defp badge_with_icon(assigns) do
    ~H"""
    <.badge tone={:warning} icon="hero-exclamation-triangle">Rebuild required</.badge>
    """
  end

  defp badge_overflow(assigns) do
    ~H"""
    <.badge tone={:error}>Physical identity mismatch on contract fingerprint</.badge>
    """
  end

  defp status_badge_lifecycle(assigns) do
    ~H"""
    <.inline gap={:sm}>
      <.status_badge tone={:success} label="Healthy" /> <.status_badge tone={:info} label="Running" />
      <.status_badge tone={:warning} label="Stale" /> <.status_badge tone={:error} label="Failed" />
      <.status_badge tone={:neutral} label="Unknown" />
    </.inline>
    """
  end

  defp status_badge_domain(assigns) do
    ~H"""
    <.inline gap={:sm}>
      <.status_badge tone={:succeeded} label="Succeeded" />
      <.status_badge tone={:queued} label="Queued" /> <.status_badge tone={:missed} label="Missed" />
      <.status_badge tone={:blocked} label="Blocked" />
    </.inline>
    """
  end

  defp status_dot_tones(assigns) do
    ~H"""
    <.inline gap={:md}>
      <.status_dot :for={tone <- Tokens.tones()} tone={tone} label={tone_label(tone)} />
    </.inline>
    """
  end

  defp button_variants(assigns) do
    ~H"""
    <.inline gap={:sm}>
      <.button variant={:primary}>Run pipeline</.button> <.button variant={:solid}>Run asset</.button>
      <.button variant={:secondary}>Plan backfill</.button>
      <.button variant={:ghost}>Reset filters</.button>
      <.button variant={:danger}>Cancel run</.button> <.button variant={:link}>Open manifest</.button>
    </.inline>
    """
  end

  defp button_sizes(assigns) do
    ~H"""
    <.inline gap={:sm}>
      <.button size={:xs}>Extra small</.button> <.button size={:sm}>Small</.button>
      <.button size={:md}>Medium</.button> <.button size={:lg}>Large</.button>
    </.inline>
    """
  end

  defp button_icons(assigns) do
    ~H"""
    <.inline gap={:sm}>
      <.button icon="hero-play">Run</.button>
      <.button variant={:secondary} trailing_icon="hero-arrow-right">Next window</.button>
    </.inline>
    """
  end

  defp button_states(assigns) do
    ~H"""
    <.inline gap={:sm}>
      <.button loading>Submitting</.button> <.button disabled>Not permitted</.button>
      <.button variant={:secondary} navigate="/runs">All runs</.button>
    </.inline>
    """
  end

  defp button_block(assigns) do
    ~H"""
    <div class="w-80">
      <.button block>Submit backfill</.button>
    </div>
    """
  end

  defp copy_button_shapes(assigns) do
    ~H"""
    <.inline gap={:sm}>
      <.copy_button value="schedule:MyApp.Pipelines.Daily:daily" label="Copy id" />
      <.copy_button value={~s({"rows": 1204})} label="Copy JSON" variant={:ghost} size={:xs} />
      <.copy_button value="run_8f3a2c" title="Copy run id" size={:xs} />
    </.inline>
    """
  end

  defp icon_button_variants(assigns) do
    ~H"""
    <.inline gap={:sm}>
      <.icon_button icon="hero-arrow-path" label="Reload" variant={:ghost} />
      <.icon_button icon="hero-magnifying-glass" label="Search" variant={:surface} />
      <.icon_button icon="hero-x-mark" label="Cancel run" variant={:danger} />
    </.inline>
    """
  end

  defp icon_button_shapes(assigns) do
    ~H"""
    <.inline gap={:sm}>
      <.icon_button icon="hero-chevron-right" label="Open" shape={:circle} />
      <.icon_button icon="hero-shield-check" label="Recover target" tone={:warning} />
    </.inline>
    """
  end

  defp icon_button_tooltips(assigns) do
    ~H"""
    <.inline gap={:lg}>
      <.icon_button icon="hero-queue-list" label="Pipelines" tooltip={:right} />
      <.icon_button icon="hero-calendar-days" label="Timeline" tooltip={:left} />
    </.inline>
    """
  end

  defp page_title_example(assigns) do
    ~H"""
    <.page_title>customer_orders_daily</.page_title>
    """
  end

  defp section_title_example(assigns) do
    ~H"""
    <.section_title>Coverage</.section_title>
    """
  end

  defp section_title_overflow(assigns) do
    ~H"""
    <div class="max-w-xs">
      <.section_title>favn_reference_workload_warehouse_mart_account_health</.section_title>
    </div>
    """
  end

  defp eyebrow_example(assigns) do
    ~H"""
    <.eyebrow>Coverage</.eyebrow>
    """
  end

  defp meta_example(assigns) do
    ~H"""
    <.meta>Updated 4 minutes ago</.meta>
    """
  end

  defp stack_sections(assigns) do
    ~H"""
    <.stack gap={{:md, :lg}}>
      <div class="favn-surface-list rounded-box p-3 text-sm">Filters</div>

      <div class="favn-surface-list rounded-box p-3 text-sm">Table</div>

      <div class="favn-surface-list rounded-box p-3 text-sm">Pagination</div>
    </.stack>
    """
  end

  defp stack_rows(assigns) do
    ~H"""
    <.stack gap={:sm}>
      <div class="favn-surface-list rounded-box p-3 text-sm">mart_daily_sales</div>

      <div class="favn-surface-list rounded-box p-3 text-sm">stg_orders</div>

      <div class="favn-surface-list rounded-box p-3 text-sm">raw_payments</div>
    </.stack>
    """
  end

  defp inline_badges(assigns) do
    ~H"""
    <.inline gap={:xs}>
      <.status_badge tone={:success} label="Healthy" />
      <.badge tone={:warning} variant={:outline}>Coverage incomplete</.badge>

      <.badge tone={:neutral} variant={:outline}>duckdb</.badge>
    </.inline>
    """
  end

  defp columns_metrics(assigns) do
    ~H"""
    <.columns count={3}>
      <.metric label="Runs" value={128} hint="Last 24 hours" />
      <.metric label="Failures" value={3} tone={:error} hint="2 blocked on drift" />
      <.metric label="Coverage" value="93%" tone={:warning} hint="28 of 30 windows" />
    </.columns>
    """
  end

  defp metrics(assigns) do
    ~H"""
    <.inline gap={:lg}>
      <.metric label="Runs" value={128} hint="Last 24 hours" icon="hero-rocket-launch" />
      <.metric label="Failures" value={3} tone={:error} hint="2 blocked on drift" />
      <.metric label="Coverage" value="93%" tone={:warning} hint="28 of 30 windows" />
    </.inline>
    """
  end

  defp panel_plain(assigns) do
    ~H"""
    <.panel>
      <p class="text-sm text-base-content/70">Panels hold page sections and large content.</p>
    </.panel>
    """
  end

  defp panel_with_header(assigns) do
    ~H"""
    <.panel>
      <:header title="Coverage" subtitle="Last 30 days" icon="hero-calendar-days" />
      <:actions>
        <.button variant={:ghost} icon="hero-arrow-path">Refresh</.button>
      </:actions>

      <p class="text-sm text-base-content/70">28 of 30 windows materialised.</p>
    </.panel>
    """
  end

  defp panel_with_footer(assigns) do
    ~H"""
    <.panel>
      <:header title="Backfill plan" />
      <p class="text-sm text-base-content/70">2 missing windows.</p>

      <:footer>
        <.button>Submit backfill</.button>
      </:footer>
    </.panel>
    """
  end

  defp panel_no_padding(assigns) do
    ~H"""
    <.panel padding={:none}>
      <div class="p-3 text-sm text-base-content/70">No padding: the child controls spacing.</div>
    </.panel>
    """
  end

  defp list_card_navigable(assigns) do
    ~H"""
    <div class="max-w-md">
      <.list_card navigate="/assets/mart_daily_sales">
        <div class="flex items-start justify-between gap-3">
          <div class="min-w-0">
            <h2 class="truncate text-base font-medium">mart_daily_sales</h2>

            <p class="mt-0.5 truncate text-sm text-base-content/60">duckdb · sales · table</p>

            <div class="mt-2"><.status_badge tone={:success} label="Healthy" /></div>
          </div>
          <.icon name="hero-chevron-right" size={:md} class="mt-1 shrink-0 text-base-content/55" />
        </div>
      </.list_card>
    </div>
    """
  end

  defp list_card_selected(assigns) do
    ~H"""
    <div class="max-w-md">
      <.list_card navigate="/assets/stg_orders" selected>
        <h2 class="truncate text-base font-medium">stg_orders</h2>
      </.list_card>
    </div>
    """
  end

  defp list_card_inert(assigns) do
    ~H"""
    <div class="max-w-md">
      <.list_card>
        <p class="text-sm text-base-content/70">Inert row: nothing to open.</p>
      </.list_card>
    </div>
    """
  end

  defp list_card_overflow(assigns) do
    ~H"""
    <div class="max-w-md">
      <.list_card navigate="/assets/long">
        <h2 class="truncate text-base font-medium">
          favn_reference_workload_warehouse_mart_account_health_daily_rollup
        </h2>
      </.list_card>
    </div>
    """
  end

  defp control_surface_example(assigns) do
    ~H"""
    <.control_surface class="p-3">
      <.meta>Control surface</.meta>
    </.control_surface>
    """
  end

  defp data_table_navigable(assigns) do
    assigns = assign(assigns, :rows, table_rows())

    ~H"""
    <.data_table id="runs-table" rows={@rows} row_navigate={&"/runs/#{&1.id}"}>
      <:col :let={run} label="Run">{run.id}</:col>

      <:col :let={run} label="Target">{run.target}</:col>

      <:col :let={run} label="Status">
        <.status_badge tone={run.status} label={to_string(run.status)} />
      </:col>

      <:col :let={run} label="Duration" align={:end}>{run.duration}</:col>
    </.data_table>
    """
  end

  defp data_table_actions(assigns) do
    assigns = assign(assigns, :rows, table_rows())

    ~H"""
    <.data_table id="runs-table-actions" rows={@rows}>
      <:col :let={run} label="Run">{run.id}</:col>

      <:col :let={run} label="Status">
        <.status_badge tone={run.status} label={to_string(run.status)} />
      </:col>

      <:action :let={run}>
        <.icon_button icon="hero-document-text" label={"Logs for " <> run.id} />
      </:action>
    </.data_table>
    """
  end

  defp data_table_empty(assigns) do
    ~H"""
    <.data_table id="runs-table-empty" rows={[]}>
      <:col :let={run} label="Run">{run.id}</:col>

      <:col :let={run} label="Status">{run.status}</:col>
    </.data_table>
    """
  end

  defp table_panel_example(assigns) do
    assigns = assign(assigns, :rows, table_rows())

    ~H"""
    <div class="flex h-96 flex-col">
      <.table_panel count={length(@rows)} count_label="runs">
        <:toolbar>
          <.table_toolbar
            on_change="filter_runs"
            filters_id="panel-example-filters"
            search_name="filters[q]"
            search_label="Search runs"
          >
            <:filters>
              <.select_field
                name="filters[range]"
                label="Time range"
                icon="hero-calendar-days"
                options={[{"Today", "today"}, {"This week", "week"}]}
                value="today"
              />
            </:filters>
          </.table_toolbar>
        </:toolbar>

        <.data_table id="table-panel-example" rows={@rows} row_navigate={&"/runs/#{&1.id}"} fill?>
          <:col :let={run} label="Run" class="w-48">
            <.stacked_cell primary={run.id} secondary={run.target} mono={:primary} />
          </:col>

          <:col :let={run} label="Status" class="w-28">
            <.status_badge tone={run.status} label={to_string(run.status)} />
          </:col>

          <:col :let={run} label="Duration" class="w-24" align={:end}>{run.duration}</:col>
        </.data_table>
      </.table_panel>
    </div>
    """
  end

  defp table_toolbar_example(assigns) do
    ~H"""
    <.table_toolbar
      on_change="filter_assets"
      filters_id="toolbar-example-filters"
      search_name="filters[search]"
      search_label="Search assets"
      on_clear="clear_filters"
      adjusted?={true}
    >
      <:filters>
        <.select_field
          name="filters[connection]"
          label="Connection filter"
          icon="hero-circle-stack"
          options={[{"Connection", "all"}, {"DuckDB", "duckdb"}]}
          value="all"
        />
      </:filters>
    </.table_toolbar>
    """
  end

  defp table_toolbar_open_example(assigns) do
    ~H"""
    <.table_toolbar
      on_change="filter_assets"
      filters_id="toolbar-open-filters"
      filters_open?={true}
      search_name="filters[search]"
      search_label="Search assets"
      on_clear="clear_filters"
      adjusted?={true}
    >
      <:filters>
        <.select_field
          name="filters[catalogue]"
          label="Catalogue filter"
          icon="hero-folder"
          options={[{"Catalogue", "all"}, {"Source", "source"}]}
          value="all"
        />
      </:filters>
    </.table_toolbar>
    """
  end

  defp table_toolbar_scopes_example(assigns) do
    ~H"""
    <.table_toolbar
      on_change="filter_runs"
      filters_id="toolbar-scopes-filters"
      adjusted?={true}
      search_name="filters[q]"
      search_label="Search runs"
    >
      <:scopes>
        <nav class="favn-surface-rail flex flex-wrap items-center gap-0.5 rounded-box p-1">
          <span class="favn-mode-item favn-mode-item-active h-9 rounded-field px-2.5 text-sm font-medium">
            Running <.count_badge count={2} label="runs" tone={:info} />
          </span>

          <span class="favn-mode-item h-9 rounded-field px-2.5 text-sm font-medium favn-text-muted">
            Failed <.count_badge count={1} label="runs" tone={:error} />
          </span>
        </nav>
      </:scopes>
    </.table_toolbar>
    """
  end

  defp stacked_cell_example(assigns) do
    ~H"""
    <.stack gap={:md}>
      <.stacked_cell primary="crm_daily" secondary="schedule-v2:cG1wZWxpbmU" mono={:secondary} />
      <.stacked_cell primary="0 2 * * *" secondary="Etc/UTC" mono={:primary} tone={:muted} />
      <.stacked_cell primary="mart_daily_sales" secondary="table" navigate="/assets/mart" />
    </.stack>
    """
  end

  defp filter_bar_example(assigns) do
    ~H"""
    <.filter_bar on_change="filter_assets">
      <.search_field name="filters[search]" label="Search assets" value="" />
      <.select_field
        name="filters[connection]"
        label="Connection filter"
        icon="hero-circle-stack"
        value="all"
        options={[{"Connection", "all"}, {"DuckDB", "duckdb"}]}
      />
      <.select_field
        name="filters[catalogue]"
        label="Catalogue filter"
        icon="hero-folder"
        value="all"
        options={[{"Catalogue", "all"}, {"Sales", "sales"}]}
      />
    </.filter_bar>
    """
  end

  defp empty_state_with_action(assigns) do
    ~H"""
    <.empty_state
      title="No schedules found"
      description="Deploy a manifest with scheduled pipelines to see them here."
      icon="hero-calendar-days"
    >
      <:action>
        <.button variant={:secondary} navigate="/pipelines">View pipelines</.button>
      </:action>
    </.empty_state>
    """
  end

  defp error_state_with_action(assigns) do
    ~H"""
    <.error_state title="Could not load runs" description="backend_unavailable">
      <:action>
        <.button variant={:secondary}>Retry</.button>
      </:action>
    </.error_state>
    """
  end

  defp notice_tones(assigns) do
    ~H"""
    <.stack gap={:sm}>
      <.notice tone={:warning}>
        Target compatibility blocks writes until the target is rebuilt.
      </.notice>

      <.notice tone={:error}>
        Run cancellation was rejected: the run already reached a terminal status.
      </.notice>

      <.notice tone={:info}>New schedules stay disabled until an operator activates them.</.notice>

      <.notice tone={:success}>All declared windows are materialised.</.notice>
    </.stack>
    """
  end

  defp icon_sizes(assigns) do
    ~H"""
    <.inline gap={:md}>
      <.icon name="hero-sparkles" size={:xs} /> <.icon name="hero-sparkles" size={:sm} />
      <.icon name="hero-sparkles" size={:md} /> <.icon name="hero-sparkles" size={:lg} />
    </.inline>
    """
  end

  defp icon_navigation(assigns) do
    assigns = assign(assigns, :items, FavnView.Components.Navigation.items())

    ~H"""
    <.inline gap={:md}>
      <.icon :for={item <- @items} name={item.icon} size={:md} />
    </.inline>
    """
  end

  defp icon_styles(assigns) do
    ~H"""
    <.inline gap={:md}>
      <.icon name="hero-check-circle" size={:md} />
      <.icon name="hero-check-circle-solid" size={:md} />
      <.icon name="hero-check-circle-mini" size={:md} />
    </.inline>
    """
  end

  defp table_rows do
    [
      %{id: "run_8f2c9d1", target: "mart_daily_sales", status: :succeeded, duration: "34.5 s"},
      %{id: "run_1a77b02", target: "stg_orders", status: :running, duration: "8.1 s"},
      %{id: "run_c30f914", target: "dq_orders_nulls", status: :failed, duration: "2.4 s"}
    ]
  end

  defp tone_label(tone), do: tone |> Atom.to_string() |> String.capitalize()
end
