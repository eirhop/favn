defmodule FavnOrchestrator.ManifestInspectionAdmissionTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.ManifestInspectionAdmission

  test "queues fairly and recovers a slot when its process exits" do
    name = :"manifest_inspection_admission_#{System.unique_integer([:positive])}"
    start_supervised!({ManifestInspectionAdmission, limit: 1, name: name})
    parent = self()

    first =
      spawn(fn ->
        ManifestInspectionAdmission.with_slot(
          fn ->
            send(parent, :first_started)

            receive do
              :stop -> :ok
            end
          end,
          name
        )
      end)

    assert_receive :first_started

    second =
      Task.async(fn ->
        ManifestInspectionAdmission.with_slot(fn -> send(parent, :second_started) end, name)
      end)

    refute_receive :second_started, 20
    Process.exit(first, :kill)
    assert_receive :second_started
    Task.await(second)
  end
end
