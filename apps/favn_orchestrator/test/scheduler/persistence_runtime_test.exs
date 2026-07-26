defmodule FavnOrchestrator.Scheduler.PersistenceRuntimeTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest.Schedule
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Persistence.Results.ScheduleClaim
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Scheduler.PersistenceRuntime

  defmodule MissingRuns do
    alias FavnOrchestrator.Persistence.Error

    def get(_context, _run_id), do: {:error, Error.new(:not_found, "run not found")}
  end

  defmodule PendingSubmission do
    def get(_context, _run_id), do: {:ok, %{status: :queued}}
  end

  test "automatic ticks pause without crashing during runtime maintenance" do
    token = Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false)
    assert {:ok, ^token} = Lifecycle.begin_maintenance(:runner_replacement, token)

    on_exit(fn ->
      Lifecycle.end_maintenance(token)
    end)

    runtime =
      start_supervised!(
        {PersistenceRuntime,
         name: :"scheduler-maintenance-#{System.unique_integer([:positive])}",
         workspace_ids: ["workspace"],
         auto_tick?: false}
      )

    send(runtime, :tick)

    assert Process.alive?(runtime)
    assert {:ok, %{last_tick_at: nil, last_error: nil}} = GenServer.call(runtime, :diagnostics)
  end

  for overlap <- [:forbid, :queue_one] do
    test "#{overlap} keeps overlap protection while admission is still queued" do
      {:ok, context} =
        WorkspaceContext.new("workspace", "scheduler-test", [:customer_operator])

      cursor = %{"in_flight_run_id" => "run-queued", "queued_due_at" => nil}

      assert ^cursor =
               PersistenceRuntime.reconcile_in_flight(
                 context,
                 cursor,
                 MissingRuns,
                 PendingSubmission
               )

      now = ~U[2026-07-26 12:05:00Z]
      claim = schedule_claim(now, cursor)
      entry = schedule_entry(unquote(overlap))

      assert {[], _next_due_at, next_cursor} =
               PersistenceRuntime.evaluation(entry, claim, cursor, now)

      assert next_cursor["in_flight_run_id"] == "run-queued"
    end
  end

  defp schedule_claim(now, cursor) do
    %ScheduleClaim{
      workspace_id: "workspace",
      deployment_id: "deployment",
      pipeline_target_id: "pipeline",
      schedule_id: "schedule",
      next_due_at: DateTime.add(now, -60, :second),
      cursor: cursor,
      version: 1,
      owner_id: "owner",
      claim_generation: 1,
      claim_expires_at: DateTime.add(now, 30, :second)
    }
  end

  defp schedule_entry(overlap) do
    %{
      module: __MODULE__.Pipeline,
      id: :pipeline,
      schedule_fingerprint: "fingerprint",
      schedule: %Schedule{
        module: __MODULE__.Schedules,
        name: :every_minute,
        ref: {__MODULE__.Schedules, :every_minute},
        cron: "* * * * *",
        timezone: "Etc/UTC",
        overlap: overlap,
        missed: :skip
      }
    }
  end
end
