defmodule FavnView.Dev.DesignSystem.Fixtures.Schedules do
  @moduledoc """
  Schedule view models for the design system.

  A schedule is judged on the states an operator has to tell apart at a glance:
  waiting for approval, approved and running, approved but changed since, and
  switched off. Each is a named fixture, so an example says which state it shows
  instead of showing four rows of plausible data.

  These view models have the shape `FavnView.ScheduleDetailLive` and
  `FavnView.SchedulesLive` build from the orchestrator facade. Nothing here
  reaches past the facade — the fixtures are literals.
  """

  @doc """
  One schedule detail view model, `attrs` merged over the defaults.

  Defaults to `:pending_activation`, the state a schedule is in the first time
  an operator sees it: published by the manifest, and not yet allowed to run.
  """
  @spec schedule(map()) :: map()
  def schedule(attrs \\ %{}) when is_map(attrs) do
    Map.merge(
      %{
        id: "schedule:MyApp.Pipelines.Daily:daily",
        schedule_label: "daily",
        pipeline_label: "MyApp.Pipelines.Daily",
        cron: "0 6 * * *",
        timezone: "Europe/Oslo",
        window_label: "Day Europe/Oslo",
        overlap: :forbid,
        missed: :skip,
        manifest_active?: true,
        activation_state: :pending_activation,
        activation_label: "Pending activation",
        activation_tone: :warning,
        runtime_state: :inactive,
        runtime_label: "Inactive",
        effective_enabled?: false,
        next_due_label: "May 25 06:00",
        last_evaluated_label: "-",
        last_due_label: "-",
        last_submitted_label: "-",
        queued_due_label: "-",
        updated_label: "May 24 12:00",
        in_flight_run_id: nil,
        current_run_label: nil,
        last_scheduler_error: nil
      },
      attrs
    )
  end

  @doc """
  Occurrence previews covering the three outcomes a preview can report.

  Upcoming, queued behind an overlap policy, and not going to run at all.
  """
  @spec occurrences() :: [map()]
  def occurrences do
    [
      %{
        due_at: ~U[2026-05-25 06:00:00Z],
        due_label: "May 25 06:00",
        timezone: "Europe/Oslo",
        window_label: "May 24 00:00 -> May 25 00:00",
        status: :upcoming,
        status_label: "Upcoming",
        notes: []
      },
      %{
        due_at: ~U[2026-05-26 06:00:00Z],
        due_label: "May 26 06:00",
        timezone: "Europe/Oslo",
        window_label: "May 25 00:00 -> May 26 00:00",
        status: :queued,
        status_label: "Queued",
        notes: ["Queued due to overlap policy"]
      },
      %{
        due_at: ~U[2026-05-27 06:00:00Z],
        due_label: "May 27 06:00",
        timezone: "Europe/Oslo",
        window_label: "May 26 00:00 -> May 27 00:00",
        status: :disabled,
        status_label: "Disabled",
        notes: ["Will not submit until enabled"]
      }
    ]
  end

  @doc """
  The schedule list, one row per activation state worth distinguishing.
  """
  @spec list() :: [map()]
  def list do
    [
      %{
        id: "schedule:MyApp.Pipelines.Daily:daily",
        route_id: "s-c2NoZWR1bGU6TXlBcHAuUGlwZWxpbmVzLkRhaWx5OmRhaWx5",
        schedule_label: "daily",
        pipeline_label: "MyApp.Pipelines.Daily",
        cron: "0 6 * * *",
        timezone: "Europe/Oslo",
        window_label: "Day Europe/Oslo",
        overlap: :forbid,
        missed: :skip,
        activation_state: :pending_activation,
        activation_label: "Pending activation",
        runtime_state: :inactive,
        runtime_label: "Inactive",
        next_due_label: "May 25 06:00",
        last_submitted_label: "-",
        in_flight_run_id: nil,
        current_run_label: nil,
        last_scheduler_error: nil,
        updated_label: "May 24 12:00"
      },
      %{
        id: "schedule:MyApp.Pipelines.Marketing:refresh",
        route_id: "s-c2NoZWR1bGU6TXlBcHAuUGlwZWxpbmVzLk1hcmtldGluZzpyZWZyZXNo",
        schedule_label: "refresh",
        pipeline_label: "MyApp.Pipelines.Marketing",
        cron: "*/15 * * * *",
        timezone: "Etc/UTC",
        window_label: "No window",
        overlap: :allow,
        missed: :one,
        activation_state: :enabled,
        activation_label: "Enabled",
        runtime_state: :running,
        runtime_label: "Running",
        next_due_label: "May 24 12:15",
        last_submitted_label: "May 24 12:00",
        in_flight_run_id: "run_8f3a2c",
        current_run_label: "run_8f3a2c",
        last_scheduler_error: nil,
        updated_label: "May 24 12:01"
      },
      %{
        id: "schedule:MyApp.Pipelines.Hourly:hourly",
        route_id: "s-c2NoZWR1bGU6TXlBcHAuUGlwZWxpbmVzLkhvdXJseTpob3VybHk",
        schedule_label: "hourly",
        pipeline_label: "MyApp.Pipelines.Hourly",
        cron: "0 * * * *",
        timezone: "Etc/UTC",
        window_label: "Hour Etc/UTC",
        overlap: :queue_one,
        missed: :one,
        activation_state: :needs_review,
        activation_label: "Needs review",
        runtime_state: :inactive,
        runtime_label: "Inactive",
        next_due_label: "May 24 13:00",
        last_submitted_label: "May 24 11:00",
        in_flight_run_id: nil,
        current_run_label: nil,
        last_scheduler_error: %{phase_label: "Submit run", message: "Window policy invalid"},
        updated_label: "May 24 12:03"
      },
      %{
        id: "schedule:MyApp.Pipelines.Monthly:monthly",
        route_id: "s-c2NoZWR1bGU6TXlBcHAuUGlwZWxpbmVzLk1vbnRobHk6bW9udGhseQ",
        schedule_label: "monthly",
        pipeline_label: "MyApp.Pipelines.Monthly",
        cron: "0 5 1 * *",
        timezone: "Europe/Oslo",
        window_label: "Month Europe/Oslo",
        overlap: :queue_one,
        missed: :one,
        activation_state: :disabled,
        activation_label: "Disabled",
        runtime_state: :queued,
        runtime_label: "Queued",
        next_due_label: "Jun 1 05:00",
        last_submitted_label: "May 1 05:00",
        in_flight_run_id: nil,
        current_run_label: nil,
        last_scheduler_error: nil,
        updated_label: "May 24 12:04"
      }
    ]
  end

  @doc """
  The scope choices counted from `entries`, the way the LiveView counts them.
  """
  @spec scope_choices([map()]) :: [map()]
  def scope_choices(entries \\ list()) do
    FavnView.ScheduleFilters.scope_choices(entries, filters())
  end

  @doc "The list filters in their default, nothing-selected state."
  @spec filters() :: map()
  def filters do
    %{
      "search" => "",
      "activation_state" => "all",
      "runtime_state" => "all",
      "pipeline" => "all",
      "window" => "all"
    }
  end

  @doc "Filter options for the pipelines and windows in `list/0`."
  @spec filter_options() :: map()
  def filter_options do
    %{
      pipelines: [{"MyApp.Pipelines.Daily", "MyApp.Pipelines.Daily"}],
      windows: [{"Day", "day"}, {"No window", "none"}]
    }
  end

  @doc """
  One schedule action fixture, for the header control examples.

  Only the two fields the control reads, so an example cannot accidentally
  depend on the rest of a schedule.
  """
  @spec actions(atom()) :: map()
  def actions(activation_state) when is_atom(activation_state) do
    %{id: "pipeline:CrmDemo.Pipelines.Daily:default", activation_state: activation_state}
  end
end
