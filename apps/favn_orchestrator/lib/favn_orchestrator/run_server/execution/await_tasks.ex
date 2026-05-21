defmodule FavnOrchestrator.RunServer.Execution.AwaitTasks do
  @moduledoc """
  BEAM process bookkeeping for runner await workers.

  This module owns spawn/monitor/ref tracking, per-await deadlines, stale reply
  flushing, and worker termination. It does not know about pipeline stages,
  retries, admission policy, or materialization claim semantics; callers provide
  cleanup callbacks for killed entries.
  """

  @await_task_timeout_buffer_ms 2_000

  @type entry :: map()
  @type await_result :: term()
  @type event :: {entry(), await_result()}
  @type cleanup_fun :: (entry() -> :ok)

  @type await :: %{
          required(:pid) => pid(),
          required(:monitor_ref) => reference(),
          required(:deadline_ms) => integer(),
          required(:entry) => entry()
        }

  @type t :: %__MODULE__{
          replies: %{optional(reference()) => await()},
          monitors: %{optional(reference()) => reference()}
        }

  defstruct replies: %{}, monitors: %{}

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec start([entry()], pos_integer(), (entry() -> await_result())) :: t()
  def start(entries, timeout_ms, await_fun)
      when is_list(entries) and is_integer(timeout_ms) and timeout_ms > 0 and
             is_function(await_fun, 1) do
    parent = self()

    Enum.reduce(entries, new(), fn entry, %__MODULE__{} = acc ->
      reply_ref = make_ref()

      {pid, monitor_ref} =
        spawn_monitor(fn ->
          send(parent, {reply_ref, await_fun.(entry)})
        end)

      await = %{
        pid: pid,
        monitor_ref: monitor_ref,
        deadline_ms: await_deadline(timeout_ms),
        entry: entry
      }

      %{
        acc
        | replies: Map.put(acc.replies, reply_ref, await),
          monitors: Map.put(acc.monitors, monitor_ref, reply_ref)
      }
    end)
  end

  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{replies: replies}), do: map_size(replies) == 0

  @spec merge(t(), t()) :: t()
  def merge(%__MODULE__{} = left, %__MODULE__{} = right) do
    %__MODULE__{
      replies: Map.merge(left.replies, right.replies),
      monitors: Map.merge(left.monitors, right.monitors)
    }
  end

  @spec receive_next(t(), cleanup_fun()) :: {:events, [event()], t()}
  def receive_next(%__MODULE__{replies: replies, monitors: monitors} = tasks, timeout_cleanup_fun)
      when is_function(timeout_cleanup_fun, 1) do
    receive_timeout_ms = next_receive_timeout_ms(tasks)

    receive do
      {reply_ref, result} when is_map_key(replies, reply_ref) ->
        {%{monitor_ref: monitor_ref, entry: entry}, next_replies} = Map.pop!(replies, reply_ref)
        Process.demonitor(monitor_ref, [:flush])

        {:events, [{entry, result}],
         %{tasks | replies: next_replies, monitors: Map.delete(monitors, monitor_ref)}}

      {:DOWN, monitor_ref, :process, _pid, reason} when is_map_key(monitors, monitor_ref) ->
        {reply_ref, next_monitors} = Map.pop!(monitors, monitor_ref)
        {%{entry: entry}, next_replies} = Map.pop!(replies, reply_ref)

        {:events, [{entry, await_exit_to_error(reason)}],
         %{tasks | replies: next_replies, monitors: next_monitors}}
    after
      receive_timeout_ms ->
        timeout_expired(tasks, timeout_cleanup_fun)
    end
  end

  @spec drain_available(t()) :: {[event()], t()}
  def drain_available(%__MODULE__{} = tasks), do: drain_available(tasks, [])

  @spec timeout_expired(t(), cleanup_fun()) :: {:events, [event()], t()}
  def timeout_expired(%__MODULE__{replies: replies, monitors: monitors} = tasks, cleanup_fun)
      when is_function(cleanup_fun, 1) do
    now = System.monotonic_time(:millisecond)

    {timed_out, next_replies, next_monitors} =
      Enum.reduce(replies, {[], %{}, monitors}, fn {reply_ref, await},
                                                   {timed_out, next_replies, next_monitors} ->
        if await.deadline_ms <= now do
          Process.exit(await.pid, :kill)
          Process.demonitor(await.monitor_ref, [:flush])
          flush_reply(reply_ref)
          :ok = cleanup_fun.(await.entry)

          {[{await.entry, {:error, :timeout}} | timed_out], next_replies,
           Map.delete(next_monitors, await.monitor_ref)}
        else
          {timed_out, Map.put(next_replies, reply_ref, await), next_monitors}
        end
      end)

    {:events, Enum.reverse(timed_out), %{tasks | replies: next_replies, monitors: next_monitors}}
  end

  @spec stop(t(), cleanup_fun()) :: :ok
  def stop(%__MODULE__{replies: replies}, cleanup_fun) when is_function(cleanup_fun, 1) do
    Enum.each(replies, fn {reply_ref, %{pid: pid, monitor_ref: monitor_ref, entry: entry}} ->
      Process.exit(pid, :kill)
      Process.demonitor(monitor_ref, [:flush])
      flush_reply(reply_ref)
      :ok = cleanup_fun.(entry)
    end)
  end

  defp drain_available(%__MODULE__{replies: replies, monitors: monitors} = tasks, acc) do
    receive do
      {reply_ref, result} when is_map_key(replies, reply_ref) ->
        {%{monitor_ref: monitor_ref, entry: entry}, next_replies} = Map.pop!(replies, reply_ref)
        Process.demonitor(monitor_ref, [:flush])

        drain_available(
          %{tasks | replies: next_replies, monitors: Map.delete(monitors, monitor_ref)},
          [{entry, result} | acc]
        )

      {:DOWN, monitor_ref, :process, _pid, reason} when is_map_key(monitors, monitor_ref) ->
        {reply_ref, next_monitors} = Map.pop!(monitors, monitor_ref)
        {%{entry: entry}, next_replies} = Map.pop!(replies, reply_ref)

        drain_available(
          %{tasks | replies: next_replies, monitors: next_monitors},
          [{entry, await_exit_to_error(reason)} | acc]
        )
    after
      0 -> {Enum.reverse(acc), tasks}
    end
  end

  defp next_receive_timeout_ms(%__MODULE__{replies: replies}) do
    now = System.monotonic_time(:millisecond)

    replies
    |> Map.values()
    |> Enum.map(& &1.deadline_ms)
    |> Enum.min(fn -> now end)
    |> Kernel.-(now)
    |> max(0)
  end

  defp await_deadline(timeout_ms),
    do: System.monotonic_time(:millisecond) + timeout_ms + @await_task_timeout_buffer_ms

  defp flush_reply(reply_ref) do
    receive do
      {^reply_ref, _result} -> :ok
    after
      0 -> :ok
    end
  end

  defp await_exit_to_error(reason) do
    {:error, %{type: :await_task_failed, kind: :exit, reason: inspect(reason)}}
  end
end
