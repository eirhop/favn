defmodule FavnOrchestrator.BoundedDispatcherTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.BoundedDispatcher

  test "run_many honors explicit concurrency" do
    {:ok, tracker} = Agent.start_link(fn -> %{active: 0, max_active: 0} end)

    on_exit(fn ->
      if Process.alive?(tracker), do: Agent.stop(tracker)
    end)

    assert {:ok, [:ok, :ok, :ok, :ok, :ok]} =
             BoundedDispatcher.run_many(
               1..5,
               fn _item ->
                 Agent.update(tracker, fn state ->
                   active = state.active + 1
                   %{active: active, max_active: max(state.max_active, active)}
                 end)

                 Process.sleep(20)

                 Agent.update(tracker, fn state -> %{state | active: state.active - 1} end)
                 :ok
               end,
               max_concurrency: 2
             )

    assert Agent.get(tracker, & &1.max_active) <= 2
  end

  test "run returns explicit timeout error" do
    assert {:error, {:dispatcher_timeout, 1}} =
             BoundedDispatcher.run(
               fn ->
                 Process.sleep(50)
                 :ok
               end,
               timeout_ms: 1
             )
  end

  test "run returns task exit error" do
    assert {:error, {:dispatcher_task_exit, {%RuntimeError{message: "boom"}, _stack}}} =
             BoundedDispatcher.run(fn -> raise "boom" end, timeout_ms: 1_000)
  end
end
