Code.require_file("../../../favn_test_support/fixtures/runner_task_persistence.exs", __DIR__)

defmodule FavnStoragePostgres.StorageV2.WriteEvidenceTest do
  use ExUnit.Case, async: true
  alias Favn.Contracts, as: C
  alias FavnStoragePostgres.RunnerTasks.WriteEvidence
  alias FavnTestSupport.RunnerTaskPersistence, as: Fixture

  test "candidate absence requires an explicit successful read of the exact relation after stop" do
    version = Fixture.version()
    ref = hd(version.manifest.assets).ref

    {_kind, request, _result} =
      Enum.find(Fixture.tasks(version), &(elem(&1, 0) == :generation_discard))

    {_kind, _payload, marker_result} =
      Enum.find(Fixture.tasks(version), &(elem(&1, 0) == :generation_marker_read))

    stopped = ~U[2026-09-04 12:00:00Z]
    command = %{disposition: :observe_generation, stopped_at: stopped}

    base = %{
      status: :succeeded,
      data_state: :available,
      enqueued_at: DateTime.add(stopped, 1),
      terminal_at: DateTime.add(stopped, 2)
    }

    task = %{task_kind: :generation_discard, write_target_id: request.target_id, payload: request}

    marker =
      Map.merge(base, %{
        task_kind: :generation_marker_read,
        payload: %C.GenerationMarkerReadRequest{manifest: version, asset_ref: ref},
        result: marker_result
      })

    inspection =
      Map.merge(base, %{
        task_kind: :relation_inspection,
        payload: %C.RelationInspectionRequest{
          relation: request.candidate_relation,
          include: [:relation]
        },
        result: %C.RelationInspectionResult{
          relation_ref: request.candidate_relation,
          required_runner_release_id: request.required_runner_release_id,
          relation: nil,
          inspected_at: base.terminal_at
        }
      })

    assert {:ok, %{"disposition" => "candidate_absent"}} =
             WriteEvidence.validate(task, command, [marker, inspection])

    invalid = [
      %{inspection | payload: %{inspection.payload | include: [:columns]}},
      %{inspection | payload: %{inspection.payload | asset_ref: ref}},
      %{inspection | result: %{inspection.result | relation_ref: request.active_relation}},
      %{
        inspection
        | result: %{
            inspection.result
            | warnings: [%{code: :relation_failed, message: "unavailable"}]
          }
      },
      %{
        inspection
        | result: %{
            inspection.result
            | warnings: [%{code: "relation_failed", message: "unavailable"}]
          }
      },
      %{inspection | result: %{inspection.result | error: %{message: "unavailable"}}},
      %{inspection | enqueued_at: DateTime.add(stopped, -1)}
    ]

    for observation <- invalid,
        do: assert({:error, _} = WriteEvidence.validate(task, command, [marker, observation]))

    still_active = %{
      marker
      | result: %{
          marker.result
          | marker: %{
              marker.result.marker
              | active_generation_id: request.candidate_generation_id
            }
        }
    }

    assert {:error, _} = WriteEvidence.validate(task, command, [still_active, inspection])
  end

  test "asset resolution accepts only administrator-verified no-effect, without invented success" do
    task = %{task_kind: :asset_attempt}

    assert {:ok, %{"disposition" => "administrator_verified_no_effect"}} =
             WriteEvidence.validate(task, %{disposition: :verified_no_effect}, [])

    assert {:error, _} = WriteEvidence.validate(task, %{disposition: :observe_generation}, [])
    assert {:error, _} = WriteEvidence.validate(task, %{disposition: :verified_no_effect}, [%{}])
  end
end
