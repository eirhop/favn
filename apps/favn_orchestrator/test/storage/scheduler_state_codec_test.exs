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

  test "encodes scheduler state as explicit storage DTO" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    assert {:ok, payload} =
             SchedulerStateCodec.encode_state(%SchedulerState{
               pipeline_module: MyApp.Pipeline,
               schedule_id: :daily,
               schedule_fingerprint: "fp",
               last_evaluated_at: now,
               last_due_at: now,
               last_submitted_due_at: now,
               in_flight_run_id: "run_1",
               queued_due_at: now,
               updated_at: now,
               version: 3
             })

    decoded = Jason.decode!(payload)

    assert decoded["format"] == "favn.scheduler_state.storage"
    assert decoded["schema_version"] == 1
    assert decoded["state"]["schedule_fingerprint"] == "fp"
    assert decoded["state"]["last_due_at"] == DateTime.to_iso8601(now)
    refute Map.has_key?(decoded["state"], "pipeline_module")
    refute Map.has_key?(decoded["state"], "schedule_id")
    refute Map.has_key?(decoded["state"], "version")
    refute payload =~ "__type__"
    refute payload =~ "__struct__"
  end

  test "decodes scheduler state storage DTO to normalized runtime values" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    payload =
      Jason.encode!(%{
        "format" => "favn.scheduler_state.storage",
        "schema_version" => 1,
        "state" => %{
          "schedule_fingerprint" => "fp",
          "last_due_at" => DateTime.to_iso8601(now),
          "in_flight_run_id" => "run_1"
        }
      })

    assert {:ok, decoded} = SchedulerStateCodec.decode_state(payload)
    assert decoded.schedule_fingerprint == "fp"
    assert decoded.last_due_at == now
    assert decoded.in_flight_run_id == "run_1"
    assert decoded.last_evaluated_at == nil
    assert decoded.version == nil
  end

  test "builds scheduler state struct from row identity and decoded state" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    assert {:ok, %SchedulerState{} = state} =
             SchedulerStateCodec.build_state(
               {MyApp.Pipeline, :daily},
               4,
               %{schedule_fingerprint: "fp", last_due_at: now}
             )

    assert state.pipeline_module == MyApp.Pipeline
    assert state.schedule_id == :daily
    assert state.version == 4
    assert state.last_due_at == now
  end

  test "rejects invalid scheduler state storage DTOs" do
    old_payload = Jason.encode!(%{"format" => "json-v1", "value" => %{}})

    assert {:error, {:invalid_scheduler_state_dto, _}} =
             SchedulerStateCodec.decode_state(old_payload)

    unsupported_version =
      Jason.encode!(%{
        "format" => "favn.scheduler_state.storage",
        "schema_version" => 2,
        "state" => %{}
      })

    assert {:error, {:unsupported_scheduler_state_schema_version, 2}} =
             SchedulerStateCodec.decode_state(unsupported_version)

    unknown_field =
      Jason.encode!(%{
        "format" => "favn.scheduler_state.storage",
        "schema_version" => 1,
        "state" => %{"extra" => true}
      })

    assert {:error, {:unknown_scheduler_state_fields, ["extra"]}} =
             SchedulerStateCodec.decode_state(unknown_field)

    bad_datetime =
      Jason.encode!(%{
        "format" => "favn.scheduler_state.storage",
        "schema_version" => 1,
        "state" => %{"last_due_at" => "bad"}
      })

    assert {:error, {:invalid_scheduler_state_field, :last_due_at, "bad"}} =
             SchedulerStateCodec.decode_state(bad_datetime)

    assert {:error, {:invalid_scheduler_state_json, _}} =
             SchedulerStateCodec.decode_state("not-json")

    assert {:error, {:invalid_scheduler_field, :version, 0}} =
             SchedulerStateCodec.build_state({MyApp.Pipeline, :daily}, 0, %{})
  end
end
