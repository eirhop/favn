defmodule Favn.EventsTest do
  use ExUnit.Case

  test "subscribes and unsubscribes per run topic" do
    run_id = "run-events-1"
    ref = {__MODULE__, :asset_a}

    assert :ok = Favn.subscribe_run(run_id)

    assert :ok =
             Favn.Runtime.Events.publish_run_event(run_id, :step_finished, %{
               seq: 1,
               ref: ref,
               stage: 2,
               payload: %{duration_ms: 12}
             })

    assert_receive {:favn_run_event,
                    %{
                      event: :step_finished,
                      run_id: ^run_id,
                      seq: 1,
                      ref: ^ref,
                      stage: 2,
                      payload: %{duration_ms: 12}
                    }}

    assert :ok = Favn.unsubscribe_run(run_id)

    assert :ok =
             Favn.Runtime.Events.publish_run_event(run_id, :run_finished, %{seq: 2, payload: %{}})

    refute_receive {:favn_run_event, %{event: :run_finished, run_id: ^run_id, seq: 2}}
  end
end
