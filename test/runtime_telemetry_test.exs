defmodule Favn.RuntimeTelemetryTest do
  use ExUnit.Case

  alias Favn.Test.Fixtures.Assets.Runner.RunnerAssets

  setup do
    state = Favn.TestSetup.capture_state()
    handler_id = "runtime-telemetry-#{System.unique_integer([:positive])}"

    :ok = Favn.TestSetup.setup_asset_modules([RunnerAssets], reload_graph?: true)
    :ok = Favn.TestSetup.configure_storage_adapter(Favn.Storage.Adapter.Memory, [])
    :ok = Favn.TestSetup.clear_memory_storage_adapter()

    events = [
      [:favn, :runtime, :run, :start],
      [:favn, :runtime, :run, :stop],
      [:favn, :runtime, :run, :exception],
      [:favn, :runtime, :step, :start],
      [:favn, :runtime, :step, :stop],
      [:favn, :runtime, :step, :exception],
      [:favn, :runtime, :storage, :put_run],
      [:favn, :runtime, :pubsub, :publish]
    ]

    parent = self()

    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        fn event, measurements, metadata, _config ->
          send(parent, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)

      Favn.TestSetup.restore_state(state, reload_graph?: true, clear_storage_adapter_env?: true)
    end)

    :ok
  end

  test "successful run emits runtime lifecycle telemetry" do
    assert {:ok, run_id} = Favn.run({RunnerAssets, :final})
    assert {:ok, %Favn.Run{}} = Favn.await_run(run_id)

    assert_receive {:telemetry, [:favn, :runtime, :run, :start], _measurements,
                    %{run_id: ^run_id}}

    assert_receive {:telemetry, [:favn, :runtime, :step, :start], _measurements,
                    %{run_id: ^run_id, ref: _ref}}

    assert_receive {:telemetry, [:favn, :runtime, :step, :stop], measurements,
                    %{run_id: ^run_id, ref: _ref}}

    assert is_integer(measurements.duration_ms)

    assert_receive {:telemetry, [:favn, :runtime, :run, :stop], _measurements, %{run_id: ^run_id}}

    assert_receive {:telemetry, [:favn, :runtime, :storage, :put_run],
                    %{duration_ms: duration_ms}, %{operation: :put_run, result: :ok}}

    assert is_integer(duration_ms)

    assert_receive {:telemetry, [:favn, :runtime, :pubsub, :publish], %{duration_ms: publish_ms},
                    %{run_id: ^run_id, result: :ok}}

    assert is_integer(publish_ms)
  end

  test "failed run emits exception telemetry" do
    assert {:ok, run_id} = Favn.run({RunnerAssets, :crashes})
    assert {:error, %Favn.Run{status: :error}} = Favn.await_run(run_id)

    assert_receive {:telemetry, [:favn, :runtime, :step, :exception], _measurements,
                    %{run_id: ^run_id, error_class: :exception}}

    assert_receive {:telemetry, [:favn, :runtime, :run, :exception], _measurements,
                    %{run_id: ^run_id, error_class: :run_failed}}
  end
end
