defmodule FavnOrchestrator.ManifestInspectionAdmissionTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.ManifestInspectionAdmission
  alias FavnOrchestrator.RuntimeConfig

  test "runtime limits reject invalid values" do
    for value <- [0, 33, -1, "4", nil] do
      assert {:error,
              {:invalid_runtime_config, {:manifest_inspection_concurrency, :out_of_range}}} =
               RuntimeConfig.normalize(manifest_inspection_concurrency: value)
    end

    assert {:ok, %{manifest_inspection_concurrency: 32}} = RuntimeConfig.normalize([])
  end

  test "the configured shared cap admits more work only after a caller finishes" do
    {:ok, config} = RuntimeConfig.normalize(manifest_inspection_concurrency: 2)
    name = :"manifest_inspection_admission_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ManifestInspectionAdmission, limit: config.manifest_inspection_concurrency, name: name}
    )

    parent = self()

    tasks =
      for index <- 1..6 do
        Task.async(fn ->
          ManifestInspectionAdmission.with_slot(
            fn ->
              send(parent, {:admitted, self(), index})

              receive do
                :finish -> :ok
              end
            end,
            name
          )
        end)
      end

    assert_receive {:admitted, first, _}
    assert_receive {:admitted, second, _}
    refute_receive {:admitted, _, _}, 20
    send(first, :finish)
    assert_receive {:admitted, third, _}
    refute_receive {:admitted, _, _}, 20
    send(second, :finish)
    send(third, :finish)

    for _ <- 1..3 do
      assert_receive {:admitted, pid, _}
      send(pid, :finish)
    end

    Enum.each(tasks, &Task.await/1)
    assert :sys.get_state(name).active == %{}
    assert :queue.is_empty(:sys.get_state(name).waiting)
  end

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
