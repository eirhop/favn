defmodule FavnOrchestrator.Storage.SchedulerStateCodecTest do
  use ExUnit.Case, async: true

  alias Favn.Scheduler.State, as: SchedulerState
  alias FavnOrchestrator.Storage.SchedulerStateCodec

  test "normalizes scheduler key and map payload" do
    now = DateTime.utc_now()

    state = %{
      schedule_fingerprint: "fp",
      last_evaluated_at: now,
      last_due_at: now,
      last_submitted_due_at: now,
      in_flight_run_id: "run_1",
      queued_due_at: now,
      updated_at: now,
      version: 2
    }

    assert {:ok, {MyApp.Pipeline, :daily}} =
             SchedulerStateCodec.normalize_key({MyApp.Pipeline, :daily})

    assert {:ok, normalized} = SchedulerStateCodec.normalize_state(state)
    assert normalized.version == 2
    assert normalized.in_flight_run_id == "run_1"
  end

  test "normalizes scheduler struct payload" do
    scheduler_state = %SchedulerState{
      pipeline_module: MyApp.Pipeline,
      schedule_id: :daily,
      schedule_fingerprint: "fp"
    }

    assert {:ok, normalized} = SchedulerStateCodec.normalize_state(scheduler_state)
    assert normalized.schedule_fingerprint == "fp"
    refute Map.has_key?(normalized, :pipeline_module)
  end

  test "rejects invalid scheduler key and payload fields" do
    assert {:error, {:invalid_scheduler_key, :bad}} = SchedulerStateCodec.normalize_key(:bad)

    assert {:error, {:invalid_scheduler_field, :version, 0}} =
             SchedulerStateCodec.normalize_state(%{version: 0})

    assert {:error, {:invalid_scheduler_field, :last_due_at, "bad"}} =
             SchedulerStateCodec.normalize_state(%{last_due_at: "bad"})
  end
end
