defmodule FavnOrchestrator.Scheduler.PersistenceRuntimeTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Scheduler.PersistenceRuntime

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
end
