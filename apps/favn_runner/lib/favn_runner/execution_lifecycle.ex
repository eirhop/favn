defmodule FavnRunner.ExecutionLifecycle do
  @moduledoc false

  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias FavnRunner.ExecutionLifecycle.Execution
  alias FavnRunner.ResultRetention

  @default_max_completed_executions 1_000
  @default_max_completed_bytes 64 * 1_024 * 1_024
  @default_max_completed_execution_bytes 1 * 1_024 * 1_024
  @default_max_logs_per_execution 500
  @default_max_events_per_execution 500
  @default_max_log_bytes_per_execution 1 * 1_024 * 1_024
  @default_max_event_bytes_per_execution 1 * 1_024 * 1_024

  @type execution_id :: String.t()

  @type waiter :: %{
          required(:from) => GenServer.from(),
          required(:timer_ref) => reference(),
          required(:monitor_ref) => reference()
        }

  @type retention_policy :: %{
          required(:max_completed_executions) => non_neg_integer(),
          required(:max_completed_bytes) => non_neg_integer(),
          required(:max_completed_execution_bytes) => non_neg_integer(),
          required(:max_logs_per_execution) => non_neg_integer(),
          required(:max_events_per_execution) => non_neg_integer(),
          required(:max_log_bytes_per_execution) => non_neg_integer(),
          required(:max_event_bytes_per_execution) => non_neg_integer()
        }

  @type counters :: %{
          required(:evicted_completed_executions) => non_neg_integer(),
          required(:evicted_completed_bytes) => non_neg_integer(),
          required(:truncated_completed_executions) => non_neg_integer(),
          required(:dropped_logs) => non_neg_integer(),
          required(:dropped_events) => non_neg_integer()
        }

  @type t :: %__MODULE__{
          executions: %{optional(execution_id()) => Execution.t()},
          monitor_to_execution: %{optional(reference()) => execution_id()},
          waiters: %{optional(execution_id()) => [waiter()]},
          waiter_monitor_to_waiter: %{optional(reference()) => {execution_id(), GenServer.from()}},
          log_subscribers: %{optional(execution_id()) => MapSet.t(pid())},
          subscriber_to_monitor: %{optional(pid()) => reference()},
          subscriber_monitor_to_pid: %{optional(reference()) => pid()},
          subscriber_executions: %{optional(pid()) => MapSet.t(execution_id())},
          completed_order: :queue.queue(execution_id()),
          completed_bytes: non_neg_integer(),
          retention: retention_policy(),
          counters: counters()
        }

  defstruct executions: %{},
            monitor_to_execution: %{},
            waiters: %{},
            waiter_monitor_to_waiter: %{},
            log_subscribers: %{},
            subscriber_to_monitor: %{},
            subscriber_monitor_to_pid: %{},
            subscriber_executions: %{},
            completed_order: :queue.new(),
            completed_bytes: 0,
            retention: %{
              max_completed_executions: @default_max_completed_executions,
              max_completed_bytes: @default_max_completed_bytes,
              max_completed_execution_bytes: @default_max_completed_execution_bytes,
              max_logs_per_execution: @default_max_logs_per_execution,
              max_events_per_execution: @default_max_events_per_execution,
              max_log_bytes_per_execution: @default_max_log_bytes_per_execution,
              max_event_bytes_per_execution: @default_max_event_bytes_per_execution
            },
            counters: %{
              evicted_completed_executions: 0,
              evicted_completed_bytes: 0,
              truncated_completed_executions: 0,
              dropped_logs: 0,
              dropped_events: 0
            }

  @spec new(keyword()) :: t()
  def new(opts \\ []) when is_list(opts) do
    %__MODULE__{retention: retention_policy(opts)}
  end

  @doc false
  @spec worker_result_budget(t()) :: non_neg_integer()
  def worker_result_budget(%__MODULE__{} = lifecycle),
    do: div(lifecycle.retention.max_completed_execution_bytes * 3, 4)

  @spec fetch_execution(t(), execution_id()) :: {:ok, Execution.t()} | :error
  def fetch_execution(%__MODULE__{} = lifecycle, execution_id) when is_binary(execution_id) do
    Map.fetch(lifecycle.executions, execution_id)
  end

  @spec fetch_result(t(), execution_id()) ::
          {:ok, RunnerResult.t()} | {:error, :execution_not_found | :not_completed}
  def fetch_result(%__MODULE__{} = lifecycle, execution_id) when is_binary(execution_id) do
    case Map.fetch(lifecycle.executions, execution_id) do
      {:ok, %Execution{status: :completed, result: %RunnerResult{} = result}} -> {:ok, result}
      {:ok, %Execution{status: :running}} -> {:error, :not_completed}
      :error -> {:error, :execution_not_found}
    end
  end

  @spec put_running(t(), execution_id(), RunnerWork.t(), pid(), reference()) :: t()
  def put_running(%__MODULE__{} = lifecycle, execution_id, %RunnerWork{} = work, pid, monitor_ref)
      when is_binary(execution_id) and is_pid(pid) and is_reference(monitor_ref) do
    execution = Execution.running(execution_id, work, pid, monitor_ref, DateTime.utc_now())

    %{
      lifecycle
      | executions: Map.put(lifecycle.executions, execution_id, execution),
        monitor_to_execution: Map.put(lifecycle.monitor_to_execution, monitor_ref, execution_id)
    }
  end

  @spec put_completed(t(), execution_id(), RunnerWork.t(), RunnerResult.t()) :: t()
  def put_completed(
        %__MODULE__{} = lifecycle,
        execution_id,
        %RunnerWork{} = work,
        %RunnerResult{} = result
      )
      when is_binary(execution_id) do
    {result, retained_bytes, truncated?} = compact_result(lifecycle, result)

    execution =
      Execution.completed(
        execution_id,
        work,
        result,
        DateTime.utc_now(),
        retained_bytes,
        truncated?
      )

    {execution, dropped_logs, dropped_events} = bound_completed_execution(lifecycle, execution)

    lifecycle
    |> count_truncation(truncated?)
    |> count_buffer_drops(dropped_logs, dropped_events)
    |> put_completed_execution(execution_id, execution)
    |> prune_completed()
  end

  @spec add_waiter(t(), execution_id(), GenServer.from(), reference(), reference()) :: t()
  def add_waiter(%__MODULE__{} = lifecycle, execution_id, from, timer_ref, monitor_ref)
      when is_binary(execution_id) and is_reference(timer_ref) and is_reference(monitor_ref) do
    waiter = %{from: from, timer_ref: timer_ref, monitor_ref: monitor_ref}

    %{
      lifecycle
      | waiters: Map.update(lifecycle.waiters, execution_id, [waiter], &[waiter | &1]),
        waiter_monitor_to_waiter:
          Map.put(lifecycle.waiter_monitor_to_waiter, monitor_ref, {execution_id, from})
    }
  end

  @spec pop_waiter(t(), execution_id(), GenServer.from()) :: {[waiter()], t()}
  def pop_waiter(%__MODULE__{} = lifecycle, execution_id, from) when is_binary(execution_id) do
    {matched, remaining_waiters} =
      split_waiters(lifecycle.waiters, execution_id, &(&1.from == from))

    waiter_monitor_to_waiter =
      Enum.reduce(matched, lifecycle.waiter_monitor_to_waiter, fn waiter, acc ->
        Map.delete(acc, waiter.monitor_ref)
      end)

    {matched,
     %{lifecycle | waiters: remaining_waiters, waiter_monitor_to_waiter: waiter_monitor_to_waiter}}
  end

  @spec remove_waiter_monitor(t(), reference()) :: {[waiter()], t()}
  def remove_waiter_monitor(%__MODULE__{} = lifecycle, monitor_ref)
      when is_reference(monitor_ref) do
    case Map.pop(lifecycle.waiter_monitor_to_waiter, monitor_ref) do
      {nil, waiter_monitor_to_waiter} ->
        {[], %{lifecycle | waiter_monitor_to_waiter: waiter_monitor_to_waiter}}

      {{execution_id, from}, waiter_monitor_to_waiter} ->
        {matched, waiters} = split_waiters(lifecycle.waiters, execution_id, &(&1.from == from))

        {matched,
         %{lifecycle | waiters: waiters, waiter_monitor_to_waiter: waiter_monitor_to_waiter}}
    end
  end

  @spec subscribe_logs(t(), execution_id(), pid(), reference()) ::
          {:ok, [term()], [reference()], t()} | {:error, term(), [reference()], t()}
  def subscribe_logs(%__MODULE__{} = lifecycle, execution_id, subscriber, monitor_ref)
      when is_binary(execution_id) and is_pid(subscriber) and is_reference(monitor_ref) do
    case Map.fetch(lifecycle.executions, execution_id) do
      {:ok, %Execution{status: :running, logs: logs}} ->
        {lifecycle, unused_monitor_refs} =
          add_log_subscriber(lifecycle, execution_id, subscriber, monitor_ref)

        {:ok, Enum.reverse(logs), unused_monitor_refs, lifecycle}

      {:ok, %Execution{status: :completed, logs: logs}} ->
        {:ok, Enum.reverse(logs), [monitor_ref], lifecycle}

      :error ->
        {:error, :execution_not_found, [monitor_ref], lifecycle}
    end
  end

  @spec unsubscribe_logs(t(), execution_id(), pid()) :: {[reference()], t()}
  def unsubscribe_logs(%__MODULE__{} = lifecycle, execution_id, subscriber)
      when is_binary(execution_id) and is_pid(subscriber) do
    remove_log_subscription(lifecycle, execution_id, subscriber)
  end

  @spec remove_subscriber_monitor(t(), reference()) :: t()
  def remove_subscriber_monitor(%__MODULE__{} = lifecycle, monitor_ref)
      when is_reference(monitor_ref) do
    case Map.pop(lifecycle.subscriber_monitor_to_pid, monitor_ref) do
      {nil, subscriber_monitor_to_pid} ->
        %{lifecycle | subscriber_monitor_to_pid: subscriber_monitor_to_pid}

      {subscriber, subscriber_monitor_to_pid} ->
        execution_ids = Map.get(lifecycle.subscriber_executions, subscriber, MapSet.new())

        log_subscribers =
          Enum.reduce(execution_ids, lifecycle.log_subscribers, fn execution_id, acc ->
            update_execution_subscribers(acc, execution_id, &MapSet.delete(&1, subscriber))
          end)

        %{
          lifecycle
          | log_subscribers: log_subscribers,
            subscriber_to_monitor: Map.delete(lifecycle.subscriber_to_monitor, subscriber),
            subscriber_monitor_to_pid: subscriber_monitor_to_pid,
            subscriber_executions: Map.delete(lifecycle.subscriber_executions, subscriber)
        }
    end
  end

  @spec append_log(t(), execution_id(), term()) :: {[pid()], t()}
  def append_log(%__MODULE__{} = lifecycle, execution_id, entry) when is_binary(execution_id) do
    subscribers =
      lifecycle.log_subscribers |> Map.get(execution_id, MapSet.new()) |> MapSet.to_list()

    lifecycle =
      update_execution(lifecycle, execution_id, fn execution ->
        {logs, dropped} =
          bounded_prepend(
            execution.logs,
            entry,
            lifecycle.retention.max_logs_per_execution,
            lifecycle.retention.max_log_bytes_per_execution
          )

        {%{execution | logs: logs, dropped_log_count: execution.dropped_log_count + dropped},
         :dropped_logs, dropped}
      end)

    {subscribers, lifecycle}
  end

  @spec append_event(t(), execution_id(), term()) :: t()
  def append_event(%__MODULE__{} = lifecycle, execution_id, event) when is_binary(execution_id) do
    update_execution(lifecycle, execution_id, fn execution ->
      {events, dropped} =
        bounded_prepend(
          execution.events,
          event,
          lifecycle.retention.max_events_per_execution,
          lifecycle.retention.max_event_bytes_per_execution
        )

      {%{
         execution
         | events: events,
           dropped_event_count: execution.dropped_event_count + dropped
       }, :dropped_events, dropped}
    end)
  end

  @spec finalize(t(), execution_id(), RunnerResult.t()) :: {[waiter()], [reference()], t()}
  def finalize(%__MODULE__{} = lifecycle, execution_id, %RunnerResult{} = result)
      when is_binary(execution_id) do
    case Map.fetch(lifecycle.executions, execution_id) do
      {:ok, %Execution{status: :running} = execution} ->
        {result, retained_bytes, truncated?} = compact_result(lifecycle, result)

        completed =
          Execution.complete(
            execution,
            result,
            DateTime.utc_now(),
            retained_bytes,
            truncated?
          )

        {completed, dropped_logs, dropped_events} =
          bound_completed_execution(lifecycle, completed)

        {waiters, lifecycle} = pop_all_waiters(lifecycle, execution_id)

        {subscriber_monitor_refs, lifecycle} =
          remove_all_execution_subscribers(lifecycle, execution_id)

        lifecycle =
          %{
            lifecycle
            | monitor_to_execution:
                Map.delete(lifecycle.monitor_to_execution, execution.monitor_ref)
          }
          |> count_truncation(truncated?)
          |> count_buffer_drops(dropped_logs, dropped_events)
          |> put_completed_execution(execution_id, completed)
          |> prune_completed()

        worker_monitor_refs =
          if is_reference(execution.monitor_ref), do: [execution.monitor_ref], else: []

        {waiters, worker_monitor_refs ++ subscriber_monitor_refs, lifecycle}

      _other ->
        {[], [], lifecycle}
    end
  end

  @spec pop_worker_monitor(t(), reference()) :: {execution_id() | nil, t()}
  def pop_worker_monitor(%__MODULE__{} = lifecycle, monitor_ref) when is_reference(monitor_ref) do
    {execution_id, monitor_to_execution} = Map.pop(lifecycle.monitor_to_execution, monitor_ref)
    {execution_id, %{lifecycle | monitor_to_execution: monitor_to_execution}}
  end

  @spec diagnostics(t()) :: map()
  def diagnostics(%__MODULE__{} = lifecycle) do
    executions = Map.values(lifecycle.executions)

    %{
      in_flight_executions: Enum.count(executions, &(&1.status == :running)),
      completed_executions: Enum.count(executions, &(&1.status == :completed)),
      waiters: map_size(lifecycle.waiter_monitor_to_waiter),
      log_subscribers: map_size(lifecycle.subscriber_to_monitor),
      log_subscriptions:
        lifecycle.log_subscribers
        |> Map.values()
        |> Enum.reduce(0, &(MapSet.size(&1) + &2)),
      retention:
        lifecycle.retention
        |> Map.merge(lifecycle.counters)
        |> Map.put(:completed_bytes, lifecycle.completed_bytes)
    }
  end

  defp retention_policy(opts) do
    opts
    |> Keyword.get(:retention, [])
    |> then(fn retention ->
      %{
        max_completed_executions:
          retention_value(retention, :max_completed_executions, @default_max_completed_executions),
        max_completed_bytes:
          retention_value(retention, :max_completed_bytes, @default_max_completed_bytes),
        max_completed_execution_bytes:
          retention_value(
            retention,
            :max_completed_execution_bytes,
            @default_max_completed_execution_bytes
          ),
        max_logs_per_execution:
          retention_value(retention, :max_logs_per_execution, @default_max_logs_per_execution),
        max_events_per_execution:
          retention_value(retention, :max_events_per_execution, @default_max_events_per_execution),
        max_log_bytes_per_execution:
          retention_value(
            retention,
            :max_log_bytes_per_execution,
            @default_max_log_bytes_per_execution
          ),
        max_event_bytes_per_execution:
          retention_value(
            retention,
            :max_event_bytes_per_execution,
            @default_max_event_bytes_per_execution
          )
      }
    end)
  end

  defp retention_value(retention, key, default) when is_list(retention) do
    retention
    |> Keyword.get(key, default)
    |> normalize_non_negative_integer(default)
  end

  defp retention_value(retention, key, default) when is_map(retention) do
    retention
    |> Map.get(key, default)
    |> normalize_non_negative_integer(default)
  end

  defp retention_value(_retention, _key, default), do: default

  defp normalize_non_negative_integer(value, _default) when is_integer(value) and value >= 0,
    do: value

  defp normalize_non_negative_integer(_value, default), do: default

  defp put_completed_execution(lifecycle, execution_id, execution) do
    previous_bytes =
      case Map.get(lifecycle.executions, execution_id) do
        %Execution{status: :completed, retained_bytes: bytes} -> bytes
        _execution -> 0
      end

    %{
      lifecycle
      | executions: Map.put(lifecycle.executions, execution_id, execution),
        completed_order: :queue.in(execution_id, lifecycle.completed_order),
        completed_bytes: lifecycle.completed_bytes - previous_bytes + execution.retained_bytes
    }
  end

  defp prune_completed(%__MODULE__{} = lifecycle) do
    if completed_count(lifecycle) > lifecycle.retention.max_completed_executions or
         lifecycle.completed_bytes > lifecycle.retention.max_completed_bytes do
      case :queue.out(lifecycle.completed_order) do
        {{:value, execution_id}, completed_order} ->
          lifecycle = %{lifecycle | completed_order: completed_order}

          case Map.fetch(lifecycle.executions, execution_id) do
            {:ok, %Execution{status: :completed}} ->
              lifecycle
              |> evict_completed_execution(execution_id)
              |> prune_completed()

            _other ->
              prune_completed(lifecycle)
          end

        {:empty, _queue} ->
          lifecycle
      end
    else
      lifecycle
    end
  end

  defp completed_count(%__MODULE__{} = lifecycle) do
    Enum.count(lifecycle.executions, fn {_id, execution} -> execution.status == :completed end)
  end

  defp evict_completed_execution(%__MODULE__{} = lifecycle, execution_id) do
    retained_bytes =
      case Map.get(lifecycle.executions, execution_id) do
        %Execution{retained_bytes: bytes} -> bytes
        _execution -> 0
      end

    %{
      lifecycle
      | executions: Map.delete(lifecycle.executions, execution_id),
        completed_bytes: max(lifecycle.completed_bytes - retained_bytes, 0),
        waiters: Map.delete(lifecycle.waiters, execution_id),
        log_subscribers: Map.delete(lifecycle.log_subscribers, execution_id),
        counters:
          lifecycle.counters
          |> Map.update!(:evicted_completed_executions, &(&1 + 1))
          |> Map.update!(:evicted_completed_bytes, &(&1 + retained_bytes))
    }
  end

  defp compact_result(lifecycle, result) do
    ResultRetention.compact(result, worker_result_budget(lifecycle))
  end

  defp bound_completed_execution(lifecycle, execution) do
    Execution.bound_retained(execution, lifecycle.retention.max_completed_execution_bytes)
  end

  defp count_truncation(lifecycle, true) do
    %{
      lifecycle
      | counters: Map.update!(lifecycle.counters, :truncated_completed_executions, &(&1 + 1))
    }
  end

  defp count_truncation(lifecycle, false), do: lifecycle

  defp count_buffer_drops(lifecycle, dropped_logs, dropped_events) do
    %{
      lifecycle
      | counters:
          lifecycle.counters
          |> Map.update!(:dropped_logs, &(&1 + dropped_logs))
          |> Map.update!(:dropped_events, &(&1 + dropped_events))
    }
  end

  defp split_waiters(waiters, execution_id, fun) do
    case Map.fetch(waiters, execution_id) do
      {:ok, execution_waiters} ->
        {matched, remaining} = Enum.split_with(execution_waiters, fun)

        waiters =
          if remaining == [] do
            Map.delete(waiters, execution_id)
          else
            Map.put(waiters, execution_id, remaining)
          end

        {matched, waiters}

      :error ->
        {[], waiters}
    end
  end

  defp pop_all_waiters(%__MODULE__{} = lifecycle, execution_id) do
    {waiters, remaining_waiters} = Map.pop(lifecycle.waiters, execution_id, [])

    waiter_monitor_to_waiter =
      Enum.reduce(waiters, lifecycle.waiter_monitor_to_waiter, fn waiter, acc ->
        Map.delete(acc, waiter.monitor_ref)
      end)

    {waiters,
     %{lifecycle | waiters: remaining_waiters, waiter_monitor_to_waiter: waiter_monitor_to_waiter}}
  end

  defp add_log_subscriber(lifecycle, execution_id, subscriber, monitor_ref) do
    case Map.fetch(lifecycle.subscriber_to_monitor, subscriber) do
      {:ok, _existing_monitor_ref} ->
        {[monitor_ref], lifecycle}

      :error ->
        lifecycle = %{
          lifecycle
          | subscriber_to_monitor:
              Map.put(lifecycle.subscriber_to_monitor, subscriber, monitor_ref),
            subscriber_monitor_to_pid:
              Map.put(lifecycle.subscriber_monitor_to_pid, monitor_ref, subscriber)
        }

        {[], lifecycle}
    end
    |> then(fn {unused_monitor_refs, lifecycle} ->
      log_subscribers =
        Map.update(
          lifecycle.log_subscribers,
          execution_id,
          MapSet.new([subscriber]),
          fn subscribers ->
            MapSet.put(subscribers, subscriber)
          end
        )

      subscriber_executions =
        Map.update(
          lifecycle.subscriber_executions,
          subscriber,
          MapSet.new([execution_id]),
          fn execution_ids ->
            MapSet.put(execution_ids, execution_id)
          end
        )

      {%{
         lifecycle
         | log_subscribers: log_subscribers,
           subscriber_executions: subscriber_executions
       }, unused_monitor_refs}
    end)
  end

  defp remove_log_subscription(lifecycle, execution_id, subscriber) do
    log_subscribers =
      update_execution_subscribers(
        lifecycle.log_subscribers,
        execution_id,
        &MapSet.delete(&1, subscriber)
      )

    subscriber_executions =
      update_subscriber_executions(
        lifecycle.subscriber_executions,
        subscriber,
        &MapSet.delete(&1, execution_id)
      )

    lifecycle = %{
      lifecycle
      | log_subscribers: log_subscribers,
        subscriber_executions: subscriber_executions
    }

    maybe_remove_subscriber_monitor(lifecycle, subscriber)
  end

  defp remove_all_execution_subscribers(lifecycle, execution_id) do
    subscribers = Map.get(lifecycle.log_subscribers, execution_id, MapSet.new())

    Enum.reduce(subscribers, {[], lifecycle}, fn subscriber, {monitor_refs, lifecycle} ->
      {removed_monitor_refs, lifecycle} =
        remove_log_subscription(lifecycle, execution_id, subscriber)

      {monitor_refs ++ removed_monitor_refs, lifecycle}
    end)
  end

  defp maybe_remove_subscriber_monitor(lifecycle, subscriber) do
    execution_ids = Map.get(lifecycle.subscriber_executions, subscriber, MapSet.new())

    if MapSet.size(execution_ids) == 0 do
      case Map.pop(lifecycle.subscriber_to_monitor, subscriber) do
        {nil, subscriber_to_monitor} ->
          {[], %{lifecycle | subscriber_to_monitor: subscriber_to_monitor}}

        {monitor_ref, subscriber_to_monitor} ->
          lifecycle = %{
            lifecycle
            | subscriber_to_monitor: subscriber_to_monitor,
              subscriber_monitor_to_pid:
                Map.delete(lifecycle.subscriber_monitor_to_pid, monitor_ref),
              subscriber_executions: Map.delete(lifecycle.subscriber_executions, subscriber)
          }

          {[monitor_ref], lifecycle}
      end
    else
      {[], lifecycle}
    end
  end

  defp update_execution_subscribers(log_subscribers, execution_id, fun) do
    case Map.fetch(log_subscribers, execution_id) do
      {:ok, subscribers} ->
        subscribers = fun.(subscribers)

        if MapSet.size(subscribers) == 0 do
          Map.delete(log_subscribers, execution_id)
        else
          Map.put(log_subscribers, execution_id, subscribers)
        end

      :error ->
        log_subscribers
    end
  end

  defp update_subscriber_executions(subscriber_executions, subscriber, fun) do
    case Map.fetch(subscriber_executions, subscriber) do
      {:ok, execution_ids} ->
        execution_ids = fun.(execution_ids)

        if MapSet.size(execution_ids) == 0 do
          Map.delete(subscriber_executions, subscriber)
        else
          Map.put(subscriber_executions, subscriber, execution_ids)
        end

      :error ->
        subscriber_executions
    end
  end

  defp update_execution(%__MODULE__{} = lifecycle, execution_id, fun) do
    case Map.fetch(lifecycle.executions, execution_id) do
      {:ok, %Execution{} = execution} ->
        {execution, counter_key, dropped} = fun.(execution)

        %{
          lifecycle
          | executions: Map.put(lifecycle.executions, execution_id, execution),
            counters: Map.update!(lifecycle.counters, counter_key, &(&1 + dropped))
        }

      :error ->
        lifecycle
    end
  end

  defp bounded_prepend(entries, entry, max_entries, max_bytes) do
    entries = [entry | entries]

    case max_entries do
      0 ->
        {[], length(entries)}

      max_entries when length(entries) > max_entries ->
        {kept, dropped} = Enum.split(entries, max_entries)
        drop_until_within_bytes(kept, max_bytes, length(dropped))

      _max_entries ->
        drop_until_within_bytes(entries, max_bytes, 0)
    end
  end

  defp drop_until_within_bytes(entries, max_bytes, dropped) do
    if entries != [] and :erlang.external_size(entries) > max_bytes do
      entries
      |> Enum.drop(-1)
      |> drop_until_within_bytes(max_bytes, dropped + 1)
    else
      {entries, dropped}
    end
  end
end
