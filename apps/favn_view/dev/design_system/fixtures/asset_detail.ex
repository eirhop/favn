defmodule FavnView.Dev.DesignSystem.Fixtures.AssetDetail do
  @moduledoc """
  View models for the asset detail page and its timeline panel.

  The asset detail screen is the densest surface Favn has: a refresh timeline, a
  freshness timeline, a data-coverage timeline, coverage evidence, target
  compatibility, a contract, and a run-config form, each with its own states. The
  states are the point — `:rebuild_required` and `:unexpected_drift` must not
  look alike, and `:unknown` coverage must not look like complete coverage — so
  every one of them is a fixture rather than a variation of prose.

  `base_attrs/0` is the healthy screen. Every other fixture is that map with the
  one thing under test changed, so a diff between two examples is the difference
  being demonstrated.
  """

  alias FavnView.Components.AssetDetailPage
  alias FavnView.Dev.DesignSystem.Fixtures.Timeline

  @hash_a String.duplicate("a", 64)
  @hash_b String.duplicate("b", 64)
  @hash_c String.duplicate("c", 64)
  @hash_d String.duplicate("d", 64)
  @hash_e String.duplicate("e", 64)

  @doc """
  The healthy asset detail screen.
  """
  @spec base_attrs() :: map()
  def base_attrs do
    %{
      title: "customer_orders_daily",
      status: "Healthy",
      status_tone: :success,
      window_range: "May 14 - Jun 12",
      refresh_window_range: "May 14 - Jun 12",
      freshness_window_range: "May 14 - Jun 12",
      data_coverage_window_range: "May 14 - Jun 12",
      refresh_timeline_label: "Monthly run anchors",
      refresh_cadence_label: "Monthly run anchors Europe/Oslo",
      freshness_timeline_label: "Daily freshness periods",
      freshness_cadence_label: "Daily freshness Europe/Oslo",
      data_coverage_timeline_label: "Daily data windows",
      active_timeline: :refresh,
      has_freshness_timeline?: true,
      has_data_windows?: true,
      can_run_asset?: true,
      run_contexts: [List.last(run_contexts())],
      selected_run_context: List.last(run_contexts()),
      run_context_status: :selected,
      nav_items: AssetDetailPage.sample_nav_items(),
      refresh_timeline: Timeline.refresh_timeline(),
      freshness_timeline: Timeline.freshness_timeline(),
      data_coverage_timeline: Timeline.data_coverage_timeline(),
      freshness: AssetDetailPage.sample_freshness(:fresh),
      coverage: coverage(:complete),
      coverage_policy: coverage_policy(),
      coverage_gaps: [],
      coverage_pagination: coverage_pagination(false),
      compatibility: compatibility(:ready),
      active_mode: :overview,
      asset_id: "customer_orders_daily",
      runs: runs(),
      selected_run_id: nil,
      selected_run: nil,
      selected_window: nil,
      run_config_open?: false,
      command_resource: "asset:customer_orders_daily",
      run_config: Timeline.default_run_config()
    }
  end

  @doc """
  Four runs across three days, so the spine shows a repeated day and a gap.
  """
  @spec runs() :: [map()]
  def runs do
    [
      run("run-d", "Jun 12", "10:25", :success, "Daily Jun 12", "4.2s"),
      run("run-c", "Jun 12", "04:10", :success, "Daily Jun 12", "3.9s"),
      run("run-b", "Jun 11", "10:25", :error, "Daily Jun 11", "1.1s"),
      run("run-a", "Jun 9", "10:25", :success, "Daily Jun 9", "4.0s")
    ]
  end

  @doc """
  One selected run: a succeeded one whose contract and checks all held.
  """
  @spec selected_run() :: {:ok, map()}
  def selected_run do
    {:ok,
     %{
       run_id: "run-d",
       target_id: "asset:customer_orders_daily",
       status: :ok,
       submit_kind: :schedule,
       trigger: %{},
       started_at: ~U[2026-06-12 10:25:04Z],
       finished_at: ~U[2026-06-12 10:25:08Z],
       duration_ms: 4_210,
       window: %{kind: :day, value: "2026-06-12", label: "Daily Jun 12", range: "Jun 12, 2026"},
       error: nil,
       assurance: assurance(),
       asset_result: %{
         status: :ok,
         stage: 0,
         started_at: ~U[2026-06-12 10:25:04Z],
         finished_at: ~U[2026-06-12 10:25:08Z],
         duration_ms: 4_210,
         attempt_count: 1,
         max_attempts: 3,
         error: nil,
         meta: %{
           "rows_written" => 1_284,
           "relation" => "mart.customer_orders_daily",
           "write_outcome" => "written",
           "quality_status" => "passed"
         }
       },
       runtime_inputs: [
         %{
           node_key: {{CrmDemo.Assets.CustomerOrdersDaily, :asset}, nil},
           resolver: CrmDemo.Resolvers.LandedOrders,
           input_identity: "landing/orders/2026-06-12.csv",
           payload_fingerprint: @hash_a,
           source_run_id: "run-c",
           source_node_key: nil,
           source_payload_fingerprint: @hash_b
         }
       ]
     }}
  end

  defp run(id, day_label, time_label, tone, window_label, duration_label) do
    %{
      id: id,
      patch: "/assets/customer_orders_daily/runs/#{id}",
      status: (tone == :error && :error) || :ok,
      status_tone: tone,
      status_label: (tone == :error && "Failed") || "Succeeded",
      trigger_label: "Schedule",
      started_at: ~U[2026-06-12 10:25:04Z],
      day_label: day_label,
      time_label: time_label,
      duration_label: duration_label,
      window_label: window_label
    }
  end

  @doc """
  `base_attrs/0` with the given keys replaced.
  """
  @spec attrs(map()) :: map()
  def attrs(overrides) when is_map(overrides), do: Map.merge(base_attrs(), overrides)

  @doc """
  `base_attrs/0` on the coverage page, with the given keys replaced.

  Coverage evidence only renders there, so a fixture that changed `:coverage` while
  staying on the overview would demonstrate nothing.
  """
  @spec coverage_attrs(map()) :: map()
  def coverage_attrs(overrides) when is_map(overrides) do
    attrs(Map.merge(%{active_mode: :coverage, active_timeline: :data_coverage}, overrides))
  end

  @doc """
  `base_attrs/0` on the diagnostics page, with the given keys replaced.

  Compatibility renders in full only there. The overview shows it just when it
  blocks writes, so a `:rebuild_available` fixture left on the overview would be a
  page with nothing on it.
  """
  @spec diagnostics_attrs(map()) :: map()
  def diagnostics_attrs(overrides) when is_map(overrides) do
    attrs(Map.put(overrides, :active_mode, :diagnostics))
  end

  @doc """
  The overview mode showing one freshness state.
  """
  @spec freshness_attrs(atom()) :: map()
  def freshness_attrs(state) do
    attrs(%{
      active_mode: :overview,
      selected_window: List.last(Timeline.refresh_timeline()),
      freshness: AssetDetailPage.sample_freshness(state)
    })
  end

  @doc """
  The window timeline panel on its own, with both timelines available.
  """
  @spec window_timeline_panel_attrs() :: map()
  def window_timeline_panel_attrs do
    base_attrs()
    |> Map.take([
      :window_range,
      :refresh_window_range,
      :freshness_window_range,
      :data_coverage_window_range,
      :refresh_timeline_label,
      :refresh_cadence_label,
      :freshness_timeline_label,
      :freshness_cadence_label,
      :data_coverage_timeline_label,
      :active_timeline,
      :has_freshness_timeline?,
      :has_data_windows?,
      :can_run_asset?,
      :refresh_timeline,
      :freshness_timeline,
      :data_coverage_timeline,
      :freshness,
      :selected_window,
      :run_config_open?,
      :command_resource,
      :run_config
    ])
  end

  @doc """
  Coverage evidence.

  `:unknown` is deliberately a different shape: coverage that was never declared
  has no counts, and a fixture that invented zeroes would let the page render a
  reassuring "0 missing".
  """
  @spec coverage(:complete | :incomplete | :unknown) :: map()
  def coverage(:complete) do
    %{
      status: :complete,
      evaluated_at: ~U[2026-07-22 12:00:00Z],
      expected_count: 22,
      covered_count: 22,
      missing_count: 0,
      last_expected_window: %{start_at: ~U[2026-07-21 00:00:00Z]}
    }
  end

  def coverage(:incomplete) do
    %{
      status: :incomplete,
      evaluated_at: ~U[2026-07-22 12:00:00Z],
      expected_count: 22,
      covered_count: 20,
      missing_count: 2,
      last_expected_window: %{start_at: ~U[2026-07-21 00:00:00Z]}
    }
  end

  def coverage(:unknown), do: %{status: :unknown, unknown_reason: :coverage_not_declared}

  @doc """
  The declared coverage policy.
  """
  @spec coverage_policy() :: map()
  def coverage_policy do
    %{
      timezone: "Europe/Oslo",
      timezone_source: :application_default,
      declared_from: ~U[2026-07-01 00:00:00Z],
      effective_from: ~U[2026-07-01 00:00:00Z],
      availability_delay_seconds: 21_600
    }
  end

  @doc """
  Two missing windows.
  """
  @spec coverage_gaps() :: [map()]
  def coverage_gaps do
    [
      %{window_key: "day:Europe/Oslo:2026-07-08"},
      %{window_key: "day:Europe/Oslo:2026-07-15"}
    ]
  end

  @doc """
  Coverage-gap pagination, with or without a further page.
  """
  @spec coverage_pagination(boolean()) :: map()
  def coverage_pagination(has_more) do
    %{
      limit: 100,
      has_more: has_more,
      next_cursor: if(has_more, do: "opaque-next-page-cursor")
    }
  end

  @doc """
  A backfill plan awaiting review.
  """
  @spec coverage_plan() :: map()
  def coverage_plan do
    %{plan_hash: @hash_a, window_count: 2, windows: coverage_gaps()}
  end

  @doc """
  Target compatibility, in each state the operator has to tell apart.

  `blocks_writes?` is the one that changes what the operator may do, so it is
  set per state rather than inferred from the status name.
  """
  @spec compatibility(atom()) :: map()
  def compatibility(:ready) do
    %{
      status: :ready,
      reason_code: "compatible",
      diff: %{},
      active_generation_id: "generation-orders-v1",
      desired_descriptor_hash: @hash_a,
      physical_fingerprint: @hash_b,
      persisted?: true,
      blocks_writes?: false
    }
  end

  def compatibility(:rebuild_available) do
    Map.merge(compatibility(:ready), %{
      status: :rebuild_available,
      reason_code: "execution_package_changed",
      diff: %{execution_package_hash: %{active: @hash_c, desired: @hash_d}}
    })
  end

  def compatibility(:rebuild_required) do
    Map.merge(compatibility(:ready), %{
      status: :rebuild_required,
      reason_code: "incompatible_descriptor",
      diff: %{
        window_identity: %{
          active: %{kind: :day, timezone: "Europe/Oslo"},
          desired: %{kind: :month, timezone: "Europe/Oslo"}
        }
      },
      blocks_writes?: true
    })
  end

  def compatibility(:unexpected_drift) do
    Map.merge(compatibility(:ready), %{
      status: :unexpected_drift,
      reason_code: "physical_fingerprint_mismatch",
      diff: %{physical_fingerprint: %{active: @hash_b, observed: @hash_e}},
      blocks_writes?: true
    })
  end

  def compatibility(:operator_decision) do
    Map.merge(compatibility(:ready), %{
      status: :operator_decision,
      reason_code: "unmanaged_physical_target",
      active_generation_id: nil,
      physical_fingerprint: nil,
      diff: %{},
      blocks_writes?: true
    })
  end

  @doc """
  Two pipeline run contexts, which is what makes the run context ambiguous.
  """
  @spec run_contexts() :: [map()]
  def run_contexts do
    [
      %{
        id: "pipeline:manual",
        label: "MyApp.Pipelines.Manual / monthly",
        href: "/assets/orders?run_context=pipeline%3Amanual",
        timezone: "Etc/UTC",
        policy: %{kind: :month, anchor: :previous_complete_period}
      },
      %{
        id: "pipeline:scheduled",
        label: "MyApp.Pipelines.Scheduled / monthly",
        href: "/assets/orders?run_context=pipeline%3Ascheduled",
        timezone: "Europe/Oslo",
        policy: %{kind: :month, anchor: :current_period}
      }
    ]
  end

  @doc """
  The runs mode with nothing selected, showing the contract on its own.
  """
  @spec assurance_attrs() :: map()
  def assurance_attrs, do: attrs(%{active_mode: :runs, assurance: assurance()})

  @doc """
  The runs mode with one succeeded run open beside the spine.
  """
  @spec selected_run_attrs() :: map()
  def selected_run_attrs do
    attrs(%{
      active_mode: :runs,
      assurance: assurance(),
      selected_run_id: "run-d",
      selected_run: selected_run()
    })
  end

  @doc """
  The runs mode with a failed run open: a check broke and the table drifted.
  """
  @spec failed_run_attrs() :: map()
  def failed_run_attrs do
    {:ok, run} = selected_run()

    failed =
      {:ok,
       %{
         run
         | run_id: "run-b",
           status: :error,
           started_at: ~U[2026-06-11 10:25:04Z],
           finished_at: ~U[2026-06-11 10:25:05Z],
           duration_ms: 1_100,
           window: %{
             kind: :day,
             value: "2026-06-11",
             label: "Daily Jun 11",
             range: "Jun 11, 2026"
           },
           error: %{message: "check orders_have_customer failed: 17 orders name no customer"},
           assurance: assurance(:mismatch),
           asset_result: %{
             run.asset_result
             | status: :error,
               meta: %{"write_outcome" => "rolled_back", "quality_status" => "failed"}
           }
       }}

    attrs(%{
      active_mode: :runs,
      assurance: assurance(:mismatch),
      selected_run_id: "run-b",
      selected_run: failed
    })
  end

  @doc """
  Assurance evidence for one run.

  `:passed` carries four checks rather than the two generated row-count claims,
  because the checks table has to be shown holding hand-written checks too — with
  only claims in it, a layout that cannot scale past two rows looks fine.
  """
  @spec assurance(:passed | :mismatch) :: map()
  def assurance(state \\ :passed)

  def assurance(:passed) do
    %{
      quality_status: :passed,
      write_outcome: :written,
      latest_run_id: "run-d",
      contract_validation: contract_validation(:matched),
      checks: [
        declared_check(:orders_have_customer, :passed),
        declared_check(:no_future_dates, :passed)
      ],
      contract: contract()
    }
  end

  def assurance(:mismatch) do
    %{
      quality_status: :failed,
      write_outcome: :rolled_back,
      latest_run_id: "run-b",
      contract_validation: contract_validation(:mismatch),
      checks: [
        declared_check(:orders_have_customer, :failed),
        declared_check(:no_future_dates, :passed)
      ],
      contract: contract()
    }
  end

  defp contract do
    fragment = MyApp.Contracts.AuditMetadata

    %{
      grain: %{by: [:order_id], description: "one customer order"},
      unique_keys: [[:order_id]],
      row_counts: [
        %{
          claim_id: "row_count.equals.param.expected_rows",
          equals: %{source: :param, name: :expected_rows},
          min: nil,
          max: nil,
          when: nil,
          on_violation: :fail,
          latest_result: %{outcome: :passed, metrics: %{actual: 1_284}}
        },
        %{
          claim_id: "row_count.min.1",
          equals: nil,
          min: 1,
          max: nil,
          when: :target_exists,
          on_violation: :skip_materialization,
          latest_result: %{outcome: :condition_skipped, metrics: %{}}
        }
      ],
      compositions: [
        %{module: fragment, start_index: 1, columns: [:processed_at, :favn_run_id]}
      ],
      columns: [
        contract_column(:order_id, :integer, %{kind: :local}),
        contract_column(:processed_at, :datetime, %{kind: :fragment, module: fragment}),
        contract_column(:favn_run_id, :string, %{kind: :fragment, module: fragment})
      ]
    }
  end

  defp declared_check(name, outcome) do
    %{
      name: name,
      origin: :asset,
      claim_id: nil,
      phase: :after_materialize,
      when: nil,
      on_violation: :fail,
      message: check_message(name),
      latest_result: %{outcome: outcome, metrics: %{actual: check_actual(name, outcome)}}
    }
  end

  defp check_message(:orders_have_customer), do: "every order names a customer"
  defp check_message(:no_future_dates), do: "no order is dated in the future"

  defp check_actual(_name, :passed), do: 0
  defp check_actual(_name, _outcome), do: 17

  defp contract_validation(:matched) do
    %{
      status: :passed,
      expected_columns: [:order_id, :processed_at, :favn_run_id],
      observed_columns: [
        observed_column(:order_id, :integer),
        observed_column(:processed_at, :datetime),
        observed_column(:favn_run_id, :string)
      ],
      differences: [],
      observed_column_count: 3,
      observed_truncated?: false
    }
  end

  defp contract_validation(:mismatch) do
    %{
      status: :failed,
      expected_columns: [:order_id, :processed_at, :favn_run_id],
      observed_columns: [
        observed_column(:order_id, :integer),
        observed_column(:processed_at, :string)
      ],
      differences: [
        %{kind: :type_mismatch, column: :processed_at, expected: :datetime, observed: :string},
        %{kind: :missing_column, column: :favn_run_id}
      ],
      observed_column_count: 2,
      observed_truncated?: false
    }
  end

  defp observed_column(name, type) do
    %{
      name: name,
      type: type,
      native_type: to_string(type),
      nullable?: false,
      nullability_observed?: true
    }
  end

  defp contract_column(name, type, origin) do
    %{
      name: name,
      type: type,
      nullable?: false,
      description: nil,
      tags: [],
      via: nil,
      sources: [],
      origin: origin
    }
  end
end
