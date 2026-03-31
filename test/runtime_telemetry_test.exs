defmodule Favn.RuntimeTelemetryTest do
  use ExUnit.Case

  alias Favn.Test.Fixtures.Assets.Runner.RunnerAssets

  defmodule RaisingStore do
    @behaviour Favn.Storage.Adapter

    @impl true
    def child_spec(_opts), do: :none

    @impl true
    def put_run(_run, _opts), do: raise("boom")

    @impl true
    def get_run(_run_id, _opts), do: {:ok, nil}

    @impl true
    def list_runs(_opts, _adapter_opts), do: {:ok, []}
  end

  def handle_event(event, measurements, metadata, parent) do
    send(parent, {:telemetry, event, measurements, metadata})
  end

  setup do
    state = Favn.TestSetup.capture_state()
    pubsub_before = Application.get_env(:favn, :pubsub_name)

    :ok = Favn.TestSetup.setup_asset_modules([RunnerAssets], reload_graph?: true)
    :ok = Favn.TestSetup.configure_storage_adapter(Favn.Storage.Adapter.Memory, [])
    :ok = Favn.TestSetup.clear_memory_storage_adapter()

    handler_id = "runtime-telemetry-#{System.unique_integer([:positive])}"

    events = [
      [:favn, :runtime, :run, :created],
      [:favn, :runtime, :run, :start],
      [:favn, :runtime, :run, :stop],
      [:favn, :runtime, :run, :exception],
      [:favn, :runtime, :run, :cancel_requested],
      [:favn, :runtime, :run, :cancelled],
      [:favn, :runtime, :run, :timeout_triggered],
      [:favn, :runtime, :run, :timed_out],
      [:favn, :runtime, :step, :ready],
      [:favn, :runtime, :step, :start],
      [:favn, :runtime, :step, :stop],
      [:favn, :runtime, :step, :exception],
      [:favn, :runtime, :step, :retry],
      [:favn, :runtime, :step, :cancelled],
      [:favn, :runtime, :step, :timed_out],
      [:favn, :runtime, :storage, :put_run],
      [:favn, :runtime, :storage, :get_run],
      [:favn, :runtime, :storage, :list_runs],
      [:favn, :runtime, :pubsub, :publish],
      [:favn, :runtime, :executor, :start_step],
      [:favn, :runtime, :executor, :cancel_step],
      [:favn, :runtime, :coordinator, :dispatch],
      [:favn, :runtime, :coordinator, :admission]
    ]

    :ok = :telemetry.attach_many(handler_id, events, &__MODULE__.handle_event/4, self())

    on_exit(fn ->
      :telemetry.detach(handler_id)

      if is_nil(pubsub_before) do
        Application.delete_env(:favn, :pubsub_name)
      else
        Application.put_env(:favn, :pubsub_name, pubsub_before)
      end

      Favn.TestSetup.restore_state(state, reload_graph?: true, clear_storage_adapter_env?: true)
    end)

    :ok
  end

  test "emits run_created/start/stop, step lifecycle, executor start, and dispatch telemetry" do
    assert {:ok, run_id} = Favn.run({RunnerAssets, :final})
    assert {:ok, %Favn.Run{}} = Favn.await_run(run_id)

    events = telemetry_events_for(run_id)

    assert_count(events, [:favn, :runtime, :run, :created], 1)
    assert_count(events, [:favn, :runtime, :run, :start], 1)
    assert_count(events, [:favn, :runtime, :run, :stop], 1)
    assert has_event?(events, [:favn, :runtime, :step, :ready], fn _m, md -> is_tuple(md.ref) end)

    assert has_event?(events, [:favn, :runtime, :step, :start], fn m, md ->
             is_number(m.attempt) and is_number(m.max_attempts) and is_tuple(md.ref)
           end)

    assert has_event?(events, [:favn, :runtime, :step, :stop], fn m, md ->
             is_number(m.duration_ms) and is_tuple(md.ref)
           end)

    assert has_event?(events, [:favn, :runtime, :executor, :start_step], fn m, md ->
             is_number(m.duration_ms) and md.result == :ok and is_tuple(md.ref)
           end)

    assert has_event?(events, [:favn, :runtime, :coordinator, :dispatch], fn m, md ->
             is_number(m.duration_ms) and md.result == :ok
           end)

    assert has_event?(events, [:favn, :runtime, :run, :stop], fn m, md ->
             is_number(m.duration_ms) and md.run_id == run_id
           end)
  end

  test "emits exception and retry telemetry for failing and retried flows" do
    assert {:ok, fail_run_id} = Favn.run({RunnerAssets, :crashes})
    assert {:error, %Favn.Run{status: :error}} = Favn.await_run(fail_run_id)

    fail_events = telemetry_events_for(fail_run_id)

    assert has_event?(fail_events, [:favn, :runtime, :step, :exception], fn m, md ->
             is_number(m.duration_ms) and md.error_kind == :error and md.error_class == :exception
           end)

    assert has_event?(fail_events, [:favn, :runtime, :run, :exception], fn m, md ->
             is_number(m.duration_ms) and md.error_class == :run_failed and
               md.terminal_reason_kind == :failed
           end)

    assert {:ok, retry_run_id} =
             Favn.run({RunnerAssets, :transient_then_ok}, retry: [max_attempts: 2, delay_ms: 0])

    assert {:ok, %Favn.Run{status: :ok}} = Favn.await_run(retry_run_id)

    retry_events = telemetry_events_for(retry_run_id)

    assert has_event?(retry_events, [:favn, :runtime, :step, :retry], fn m, md ->
             is_number(m.attempt) and is_number(m.max_attempts) and is_tuple(md.ref)
           end)
  end

  test "emits cancellation and timeout telemetry including executor cancel and admission" do
    assert {:ok, cancel_run_id} = Favn.run({RunnerAssets, :slow_asset}, timeout_ms: 1_000)
    assert {:ok, :cancelling} = Favn.cancel_run(cancel_run_id)
    assert {:error, %Favn.Run{status: :cancelled}} = Favn.await_run(cancel_run_id)

    cancel_events = telemetry_events_for(cancel_run_id)

    assert has_event?(cancel_events, [:favn, :runtime, :run, :cancel_requested], fn _m, md ->
             md.terminal_reason_kind == :cancel_requested
           end)

    assert has_event?(cancel_events, [:favn, :runtime, :run, :cancelled], fn m, md ->
             is_number(m.duration_ms) and md.terminal_reason_kind == :cancelled
           end)

    assert has_event?(cancel_events, [:favn, :runtime, :executor, :cancel_step], fn m, md ->
             is_number(m.duration_ms) and md.result == :ok and is_tuple(md.ref)
           end)

    assert has_event?(cancel_events, [:favn, :runtime, :coordinator, :admission], fn m, md ->
             is_number(m.duration_ms) and md.result == :closed
           end)

    assert {:ok, timeout_run_id} = Favn.run({RunnerAssets, :slow_asset}, timeout_ms: 10)
    assert {:error, %Favn.Run{status: :timed_out}} = Favn.await_run(timeout_run_id)

    timeout_events = telemetry_events_for(timeout_run_id)

    assert has_event?(timeout_events, [:favn, :runtime, :run, :timeout_triggered], fn m, md ->
             is_number(m.duration_ms) and md.terminal_reason_kind == :timed_out
           end)

    assert has_event?(timeout_events, [:favn, :runtime, :run, :timed_out], fn m, md ->
             is_number(m.duration_ms) and md.terminal_reason_kind == :timed_out
           end)

    assert has_event?(timeout_events, [:favn, :runtime, :step, :timed_out], fn _m, md ->
             is_tuple(md.ref)
           end)
  end

  test "emits storage telemetry for success and failure paths" do
    assert {:ok, _run_id} = Favn.run({RunnerAssets, :final})

    events = drain_telemetry()

    assert has_event?(events, [:favn, :runtime, :storage, :put_run], fn m, md ->
             is_number(m.duration_ms) and md.result == :ok and md.run_id != nil
           end)

    Application.put_env(:favn, :storage_adapter, Unknown.Adapter)

    assert {:error, {:store_error, {:invalid_storage_adapter, Unknown.Adapter}}} =
             Favn.get_run("x")

    invalid_adapter_events = drain_telemetry()

    assert has_event?(invalid_adapter_events, [:favn, :runtime, :storage, :get_run], fn m, md ->
             is_number(m.duration_ms) and md.result == :error and
               md.error_class == :invalid_adapter
           end)

    Application.put_env(:favn, :storage_adapter, RaisingStore)

    assert {:error, {:store_error, {:raised, %RuntimeError{message: "boom"}}}} =
             Favn.Storage.put_run(%Favn.Run{id: "raise-1", status: :running})

    raised_events = drain_telemetry()

    assert has_event?(raised_events, [:favn, :runtime, :storage, :put_run], fn _m, md ->
             md.result == :error and md.error_class == :adapter_raise and md.error_kind == :error
           end)
  end

  test "emits pubsub telemetry for failure paths" do
    Application.put_env(:favn, :pubsub_name, MissingPubSub)

    assert {:error, {:raised, %ArgumentError{}}} =
             Favn.Runtime.Events.publish_run_event("r1", :run_started, %{
               seq: 1,
               entity: :run,
               status: :running,
               data: %{}
             })

    events = drain_telemetry()

    assert has_event?(events, [:favn, :runtime, :pubsub, :publish], fn m, md ->
             is_number(m.duration_ms) and md.result == :error and
               md.error_class in [:publish_raise, :publish_exit]
           end)
  end

  defp telemetry_events_for(run_id) do
    Process.sleep(10)

    drain_telemetry()
    |> Enum.filter(fn {_event, _measurements, metadata} -> metadata[:run_id] == run_id end)
  end

  defp drain_telemetry(acc \\ []) do
    receive do
      {:telemetry, event, measurements, metadata} ->
        drain_telemetry([{event, measurements, metadata} | acc])
    after
      20 ->
        Enum.reverse(acc)
    end
  end

  defp has_event?(events, event_name, validator) do
    Enum.any?(events, fn {event, measurements, metadata} ->
      event == event_name and validator.(measurements, metadata)
    end)
  end

  defp assert_count(events, event_name, expected_count) do
    count = Enum.count(events, fn {event, _measurements, _metadata} -> event == event_name end)
    assert count == expected_count
  end
end
