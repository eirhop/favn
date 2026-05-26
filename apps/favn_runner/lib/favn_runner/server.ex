defmodule FavnRunner.Server do
  @moduledoc """
  Runner protocol server for manifest registration and work execution.
  """

  use GenServer

  alias Favn.Connection.Registry, as: ConnectionRegistry
  alias Favn.Connection.Resolved
  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RunnerCancellation
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias Favn.SQL.SessionPool
  alias FavnRunner.ExecutionAdmission
  alias FavnRunner.ExecutionLifecycle
  alias FavnRunner.Inspection
  alias FavnRunner.ManifestResolver
  alias FavnRunner.ManifestStore
  alias FavnRunner.SQLRuntimePreflight
  alias FavnRunner.Worker

  @type execution_id :: String.t()

  @type state :: %{
          lifecycle: ExecutionLifecycle.t(),
          admission: ExecutionAdmission.t(),
          queue: :queue.queue(map()),
          queue_monitors: %{optional(reference()) => reference()}
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec register_manifest(Version.t(), keyword()) :: :ok | {:error, term()}
  def register_manifest(%Version{} = version, opts \\ []) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:register_manifest, version})
  end

  @spec submit_work(RunnerWork.t(), keyword()) :: {:ok, execution_id()} | {:error, term()}
  def submit_work(%RunnerWork{} = work, opts \\ []) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:submit_work, work}, submit_call_timeout_ms(opts))
  catch
    :exit, {:timeout, _call} ->
      {:error,
       RunnerError.normalize(:runner_submit_timeout,
         kind: :boundary,
         type: :runner_submit_timeout,
         retryable?: nil
       )}
  end

  @spec await_result(execution_id(), timeout(), keyword()) ::
          {:ok, RunnerResult.t()} | {:error, term()}
  def await_result(execution_id, timeout, opts \\ [])

  def await_result(execution_id, timeout, opts)
      when is_binary(execution_id) and is_integer(timeout) and timeout > 0 and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:await_result, execution_id, timeout}, timeout + 1_000)
  end

  def await_result(_execution_id, _timeout, _opts), do: {:error, :invalid_await_args}

  @spec cancel_work(execution_id(), RunnerCancellation.t(), keyword()) ::
          {:ok, RunnerCancellation.outcome()} | {:error, RunnerError.t()}
  def cancel_work(execution_id, reason \\ %{}, opts \\ [])

  def cancel_work(execution_id, reason, opts)
      when is_binary(execution_id) and is_map(reason) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)

    GenServer.call(
      server,
      {:cancel_work, execution_id, RunnerCancellation.from_map(reason)},
      cancel_call_timeout_ms(opts)
    )
  catch
    :exit, {:timeout, _call} ->
      {:error,
       RunnerError.normalize(:runner_cancel_timeout,
         kind: :boundary,
         type: :runner_cancel_timeout,
         retryable?: nil
       )}
  end

  def cancel_work(_execution_id, _reason, _opts) do
    {:error,
     RunnerError.normalize(:invalid_cancel_args,
       kind: :boundary,
       type: :invalid_cancel_args,
       retryable?: false
     )}
  end

  @spec subscribe_execution_logs(execution_id(), pid(), keyword()) :: :ok | {:error, term()}
  def subscribe_execution_logs(execution_id, subscriber, opts \\ [])

  def subscribe_execution_logs(execution_id, subscriber, opts)
      when is_binary(execution_id) and is_pid(subscriber) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:subscribe_execution_logs, execution_id, subscriber})
  end

  def subscribe_execution_logs(_execution_id, _subscriber, _opts),
    do: {:error, :invalid_log_subscription_args}

  @spec unsubscribe_execution_logs(execution_id(), pid(), keyword()) :: :ok
  def unsubscribe_execution_logs(execution_id, subscriber, opts \\ [])

  def unsubscribe_execution_logs(execution_id, subscriber, opts)
      when is_binary(execution_id) and is_pid(subscriber) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:unsubscribe_execution_logs, execution_id, subscriber})
  end

  def unsubscribe_execution_logs(_execution_id, _subscriber, _opts), do: :ok

  @spec inspect_relation(RelationInspectionRequest.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def inspect_relation(%RelationInspectionRequest{} = request, opts \\ []) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:inspect_relation, request}, Keyword.get(opts, :timeout, 15_000))
  end

  @spec diagnostics(keyword()) :: {:ok, map()} | {:error, term()}
  def diagnostics(opts \\ []) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)

    case Process.whereis(server) do
      nil -> {:error, :runner_not_available}
      _pid -> GenServer.call(server, :diagnostics)
    end
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       lifecycle: ExecutionLifecycle.new(opts),
       admission: ExecutionAdmission.new(opts),
       queue: :queue.new(),
       queue_monitors: %{}
     }}
  end

  @impl true
  def handle_call({:register_manifest, %Version{} = version}, _from, state) do
    reply = ManifestStore.register(version, server: FavnRunner.ManifestStore)
    {:reply, reply, state}
  end

  def handle_call({:submit_work, %RunnerWork{} = work}, from, state) do
    reply =
      with {:ok, asset_ref} <- ManifestResolver.resolve_target_ref(work),
           {:ok, version} <-
             ManifestStore.fetch(work.manifest_version_id, work.manifest_content_hash,
               server: FavnRunner.ManifestStore
             ),
           {:ok, asset} <- ManifestResolver.resolve_asset(version, asset_ref),
           execution_id <- new_execution_id() do
        case SQLRuntimePreflight.run(work, version) do
          :ok ->
            case admit_worker(state) do
              {:ok, admission} ->
                start_admitted_worker(execution_id, work, version, asset, %{
                  state
                  | admission: admission
                })

              {{:error, %RunnerError{} = error}, next_state} ->
                enqueue_or_reject_submit(
                  next_state,
                  from,
                  execution_id,
                  work,
                  version,
                  asset,
                  error
                )
            end

          {:error, diagnostic} ->
            lifecycle =
              ExecutionLifecycle.put_completed(
                state.lifecycle,
                execution_id,
                work,
                preflight_failed_result(work, version, diagnostic)
              )

            {{:ok, execution_id}, %{state | lifecycle: lifecycle}}
        end
      end

    case reply do
      {{:ok, execution_id}, next_state} -> {:reply, {:ok, execution_id}, next_state}
      {{:error, reason}, next_state} -> {:reply, {:error, reason}, next_state}
      {:noreply, next_state} -> {:noreply, next_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:await_result, execution_id, timeout}, from, state) do
    case ExecutionLifecycle.fetch_result(state.lifecycle, execution_id) do
      {:ok, %RunnerResult{} = result} ->
        {:reply, {:ok, result}, state}

      {:error, :not_completed} ->
        waiter_monitor_ref = Process.monitor(elem(from, 0))
        timer_ref = Process.send_after(self(), {:await_timeout, execution_id, from}, timeout)

        lifecycle =
          ExecutionLifecycle.add_waiter(
            state.lifecycle,
            execution_id,
            from,
            timer_ref,
            waiter_monitor_ref
          )

        {:noreply, %{state | lifecycle: lifecycle}}

      {:error, :execution_not_found} ->
        {:reply, {:error, :execution_not_found}, state}
    end
  end

  def handle_call({:cancel_work, execution_id, reason}, _from, state) do
    case ExecutionLifecycle.fetch_execution(state.lifecycle, execution_id) do
      {:ok, %{status: :completed}} ->
        {:reply,
         {:ok,
          RunnerCancellation.outcome(:already_completed,
            execution_id: execution_id,
            runner_status: :already_completed,
            native_status: :not_applicable
          )}, state}

      {:ok, %{status: :running, pid: pid, work: work}} ->
        _ = DynamicSupervisor.terminate_child(FavnRunner.WorkerSupervisor, pid)

        result = cancelled_result(work, reason)
        next_state = finalize_execution(state, execution_id, result)

        {:reply,
         {:ok,
          RunnerCancellation.outcome(:acknowledged,
            execution_id: execution_id,
            runner_status: :beam_worker_stopped,
            native_status: :native_cancel_unknown
          )}, next_state}

      :error ->
        {:reply,
         {:ok,
          RunnerCancellation.outcome(:not_found,
            execution_id: execution_id,
            runner_status: :not_found,
            native_status: :native_cancel_unknown
          )}, state}
    end
  end

  def handle_call({:subscribe_execution_logs, execution_id, subscriber}, _from, state) do
    monitor_ref = Process.monitor(subscriber)

    case ExecutionLifecycle.subscribe_logs(state.lifecycle, execution_id, subscriber, monitor_ref) do
      {:ok, replay_entries, cleanup_monitor_refs, lifecycle} ->
        cleanup_monitor_refs(cleanup_monitor_refs)

        Enum.each(replay_entries, fn entry ->
          send(subscriber, {:runner_log_entry, execution_id, entry})
        end)

        {:reply, :ok, %{state | lifecycle: lifecycle}}

      {:error, :execution_not_found, cleanup_monitor_refs, lifecycle} ->
        cleanup_monitor_refs(cleanup_monitor_refs)
        {:reply, {:error, :execution_not_found}, %{state | lifecycle: lifecycle}}
    end
  end

  def handle_call({:unsubscribe_execution_logs, execution_id, subscriber}, _from, state) do
    {cleanup_monitor_refs, lifecycle} =
      ExecutionLifecycle.unsubscribe_logs(state.lifecycle, execution_id, subscriber)

    cleanup_monitor_refs(cleanup_monitor_refs)
    {:reply, :ok, %{state | lifecycle: lifecycle}}
  end

  def handle_call({:inspect_relation, %RelationInspectionRequest{} = request}, _from, state) do
    reply =
      with {:ok, version} <-
             ManifestStore.fetch(request.manifest_version_id, request.manifest_content_hash,
               server: FavnRunner.ManifestStore
             ) do
        Inspection.inspect_relation(request, version)
      end

    {:reply, reply, state}
  end

  def handle_call(:diagnostics, _from, state) do
    reply =
      {:ok,
       state.lifecycle
       |> ExecutionLifecycle.diagnostics()
       |> Map.merge(%{
         available?: true,
         server: __MODULE__,
         admission: admission_diagnostics(state),
         data_plane: data_plane_diagnostics()
       })}

    {:reply, reply, state}
  end

  @impl true
  def handle_info({:runner_event, execution_id, event}, state) do
    lifecycle = ExecutionLifecycle.append_event(state.lifecycle, execution_id, event)
    {:noreply, %{state | lifecycle: lifecycle}}
  end

  def handle_info({:runner_log_entry, execution_id, entry}, state) do
    {subscribers, lifecycle} = ExecutionLifecycle.append_log(state.lifecycle, execution_id, entry)

    Enum.each(subscribers, fn subscriber ->
      send(subscriber, {:runner_log_entry, execution_id, entry})
    end)

    {:noreply, %{state | lifecycle: lifecycle}}
  end

  def handle_info({:runner_result, execution_id, %RunnerResult{} = result}, state) do
    case ExecutionLifecycle.fetch_execution(state.lifecycle, execution_id) do
      {:ok, %{status: :running}} ->
        {:noreply, finalize_execution(state, execution_id, result)}

      {:ok, %{status: :completed}} ->
        {:noreply, state}

      :error ->
        {:noreply, state}
    end
  end

  def handle_info({:await_timeout, execution_id, from}, state) do
    {waiters, lifecycle} = ExecutionLifecycle.pop_waiter(state.lifecycle, execution_id, from)

    Enum.each(waiters, fn %{from: waiter_from, monitor_ref: monitor_ref} ->
      cleanup_monitor_refs([monitor_ref])
      GenServer.reply(waiter_from, {:error, :timeout})
    end)

    {:noreply, %{state | lifecycle: lifecycle}}
  end

  def handle_info({:queued_submit_timeout, queue_ref}, state) do
    {entries, state} = pop_queued_submit(state, queue_ref)

    Enum.each(entries, fn entry ->
      GenServer.reply(
        entry.from,
        {:error,
         RunnerError.normalize(:runner_queue_timeout,
           kind: :boundary,
           type: :runner_queue_timeout,
           retryable?: true
         )}
      )
    end)

    {:noreply, state}
  end

  def handle_info({:DOWN, monitor_ref, :process, _pid, reason}, state) do
    if Map.has_key?(state.queue_monitors, monitor_ref) do
      {_entries, state} = pop_queued_submit_by_monitor(state, monitor_ref)
      {:noreply, state}
    else
      handle_lifecycle_down(monitor_ref, reason, state)
    end
  end

  defp handle_lifecycle_down(monitor_ref, reason, state) do
    {execution_id, lifecycle} =
      ExecutionLifecycle.pop_worker_monitor(state.lifecycle, monitor_ref)

    state = %{state | lifecycle: lifecycle}

    cond do
      is_binary(execution_id) ->
        case ExecutionLifecycle.fetch_execution(state.lifecycle, execution_id) do
          {:ok, %{status: :running, work: work}} ->
            {:noreply, finalize_execution(state, execution_id, crashed_result(work, reason))}

          _other ->
            {:noreply, state}
        end

      true ->
        {waiters, lifecycle} =
          ExecutionLifecycle.remove_waiter_monitor(state.lifecycle, monitor_ref)

        if waiters != [] do
          Enum.each(waiters, fn %{timer_ref: timer_ref} -> Process.cancel_timer(timer_ref) end)
          {:noreply, %{state | lifecycle: lifecycle}}
        else
          lifecycle = ExecutionLifecycle.remove_subscriber_monitor(lifecycle, monitor_ref)
          {:noreply, %{state | lifecycle: lifecycle}}
        end
    end
  end

  defp start_worker(execution_id, work, version, asset) do
    child_spec = %{
      id: {Worker, execution_id},
      start:
        {Worker, :start_link,
         [
           %{
             server: self(),
             execution_id: execution_id,
             work: work,
             version: version,
             asset: asset
           }
         ]},
      restart: :temporary,
      shutdown: 5000,
      type: :worker
    }

    DynamicSupervisor.start_child(FavnRunner.WorkerSupervisor, child_spec)
  end

  defp start_admitted_worker(execution_id, work, version, asset, state) do
    case start_worker(execution_id, work, version, asset) do
      {:ok, pid} ->
        monitor_ref = Process.monitor(pid)

        lifecycle =
          ExecutionLifecycle.put_running(
            state.lifecycle,
            execution_id,
            work,
            pid,
            monitor_ref
          )

        {{:ok, execution_id}, %{state | lifecycle: lifecycle}}

      {:error, reason} ->
        {{:error, reason}, state}
    end
  end

  defp enqueue_or_reject_submit(state, _from, _execution_id, _work, _version, _asset, error),
    do: {{:error, error}, state}

  defp admit_worker(state) do
    case ExecutionAdmission.admit(state.admission, active_worker_count(state)) do
      {:ok, admission} ->
        {:ok, admission}

      {:error, %RunnerError{} = error, admission} ->
        {{:error, error}, %{state | admission: admission}}
    end
  end

  defp admission_diagnostics(state) do
    ExecutionAdmission.diagnostics(
      state.admission,
      active_worker_count(state),
      queued_worker_count(state)
    )
  end

  defp queued_worker_count(state), do: :queue.len(state.queue)

  defp active_worker_count(state) do
    state.lifecycle.executions
    |> Map.values()
    |> Enum.count(&(&1.status == :running))
  end

  defp finalize_execution(state, execution_id, %RunnerResult{} = result) do
    {waiters, monitor_refs, lifecycle} =
      ExecutionLifecycle.finalize(state.lifecycle, execution_id, result)

    cleanup_monitor_refs(monitor_refs)

    Enum.each(waiters, fn %{from: from, timer_ref: timer_ref, monitor_ref: monitor_ref} ->
      _ = Process.cancel_timer(timer_ref)
      cleanup_monitor_refs([monitor_ref])
      GenServer.reply(from, {:ok, result})
    end)

    %{state | lifecycle: lifecycle}
    |> drain_submit_queue()
  end

  defp drain_submit_queue(state) do
    if active_worker_count(state) < state.admission.max_active_workers do
      case :queue.out(state.queue) do
        {{:value, entry}, queue} ->
          state = %{
            state
            | queue: queue,
              queue_monitors: Map.delete(state.queue_monitors, entry.monitor_ref)
          }

          _ = Process.cancel_timer(entry.timer_ref)
          Process.demonitor(entry.monitor_ref, [:flush])

          case start_admitted_worker(
                 entry.execution_id,
                 entry.work,
                 entry.version,
                 entry.asset,
                 state
               ) do
            {{:ok, execution_id}, next_state} ->
              GenServer.reply(entry.from, {:ok, execution_id})
              drain_submit_queue(next_state)

            {{:error, reason}, next_state} ->
              GenServer.reply(entry.from, {:error, reason})
              drain_submit_queue(next_state)
          end

        {:empty, _queue} ->
          state
      end
    else
      state
    end
  end

  defp pop_queued_submit(state, queue_ref) do
    pop_queued_submit_by(state, &(&1.ref == queue_ref))
  end

  defp pop_queued_submit_by_monitor(state, monitor_ref) do
    pop_queued_submit_by(state, &(&1.monitor_ref == monitor_ref))
  end

  defp pop_queued_submit_by(state, predicate) when is_function(predicate, 1) do
    {matched, remaining} =
      state.queue
      |> :queue.to_list()
      |> Enum.split_with(predicate)

    Enum.each(matched, fn entry ->
      _ = Process.cancel_timer(entry.timer_ref)
      Process.demonitor(entry.monitor_ref, [:flush])
    end)

    queue_monitors =
      Enum.reduce(matched, state.queue_monitors, fn entry, acc ->
        Map.delete(acc, entry.monitor_ref)
      end)

    {matched, %{state | queue: :queue.from_list(remaining), queue_monitors: queue_monitors}}
  end

  defp cleanup_monitor_refs(monitor_refs) when is_list(monitor_refs),
    do: Enum.each(monitor_refs, &Process.demonitor(&1, [:flush]))

  defp cancelled_result(work, reason) do
    %RunnerResult{
      run_id: work.run_id,
      manifest_version_id: work.manifest_version_id,
      manifest_content_hash: work.manifest_content_hash,
      status: :cancelled,
      asset_results: [],
      error: RunnerError.cancelled(reason),
      metadata: RunnerWork.lifecycle_metadata(work)
    }
  end

  defp crashed_result(work, reason) do
    %RunnerResult{
      run_id: work.run_id,
      manifest_version_id: work.manifest_version_id,
      manifest_content_hash: work.manifest_content_hash,
      status: :error,
      asset_results: [],
      error: RunnerError.normalize(reason, kind: :exit, type: :worker_crash),
      metadata: RunnerWork.lifecycle_metadata(work)
    }
  end

  defp preflight_failed_result(%RunnerWork{} = work, %Version{} = version, diagnostic) do
    %RunnerResult{
      run_id: work.run_id,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      status: :error,
      asset_results: [],
      error:
        RunnerError.normalize(diagnostic,
          kind: :preflight,
          type: :missing_runtime_config,
          message: Map.get(diagnostic, :message, "runner preflight failed"),
          details: Map.get(diagnostic, :details, %{}),
          retryable?: false
        ),
      metadata: Map.put(RunnerWork.lifecycle_metadata(work), :preflight, :sql_runtime_config)
    }
  end

  defp data_plane_diagnostics do
    case ConnectionRegistry.list(registry_name: FavnRunner.ConnectionRegistry) do
      connections when is_list(connections) ->
        %{
          status: :ok,
          connection_count: length(connections),
          connections: Enum.map(connections, &connection_diagnostics/1),
          session_pool: SessionPool.diagnostics()
        }
    end
  rescue
    exception ->
      %{
        status: :error,
        reason: %{kind: :raised, exception: exception.__struct__}
      }
  catch
    kind, reason ->
      %{status: :error, reason: redact(%{kind: kind, reason: reason})}
  end

  defp connection_diagnostics(%Resolved{} = resolved) do
    base = %{
      name: resolved.name,
      adapter: resolved.adapter,
      module: resolved.module,
      required_keys: resolved.required_keys,
      secret_fields: resolved.secret_fields,
      schema_keys: resolved.schema_keys,
      config: safe_connection_config(resolved.config || %{})
    }

    adapter_connection_diagnostics(resolved, base)
  end

  defp adapter_connection_diagnostics(%Resolved{} = resolved, base) do
    if is_atom(resolved.adapter) and function_exported?(resolved.adapter, :diagnostics, 2) do
      case resolved.adapter.diagnostics(resolved, []) do
        {:ok, details} -> Map.merge(base, %{status: :ok, details: redact(details)})
        {:error, reason} -> Map.merge(base, %{status: :error, reason: redact(reason)})
      end
    else
      Map.merge(base, %{status: :unknown, summary: :adapter_diagnostics_not_supported})
    end
  rescue
    exception ->
      Map.merge(base, %{status: :error, reason: %{kind: :raised, exception: exception.__struct__}})
  catch
    kind, reason ->
      Map.merge(base, %{status: :error, reason: redact(%{kind: kind, reason: reason})})
  end

  defp safe_connection_config(config) when is_map(config) do
    %{
      production?: Map.get(config, :production?),
      duckdb_storage: Map.get(config, :duckdb_storage),
      database_path: if(Map.has_key?(config, :database), do: :redacted, else: nil)
    }
  end

  defp redact(value) when is_map(value) do
    Map.new(value, fn {key, map_value} -> {key, redact(key, map_value)} end)
  end

  defp redact(value) when is_list(value), do: Enum.map(value, &redact/1)

  defp redact(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&redact/1)
    |> List.to_tuple()
  end

  defp redact(value) when is_atom(value), do: value
  defp redact(value) when is_integer(value), do: value
  defp redact(value) when is_boolean(value), do: value
  defp redact(value) when is_binary(value), do: value
  defp redact(nil), do: nil
  defp redact(value), do: inspect(value)

  defp redact(key, _value)
       when key in [:token, :tokens, :password, :secret, :database, :database_path],
       do: "[REDACTED]"

  defp redact(key, value) when is_atom(key) do
    if sensitive_key?(Atom.to_string(key)), do: "[REDACTED]", else: redact(value)
  end

  defp redact(key, value) when is_binary(key) do
    if sensitive_key?(key), do: "[REDACTED]", else: redact(value)
  end

  defp redact(_key, value), do: redact(value)

  defp sensitive_key?(key) when is_binary(key) do
    key = String.downcase(key)

    String.contains?(key, "token") or String.contains?(key, "password") or
      String.contains?(key, "secret") or String.contains?(key, "credential") or
      String.contains?(key, "database")
  end

  defp submit_call_timeout_ms(opts) do
    pos_int_opt(
      opts,
      :submit_call_timeout_ms,
      Application.get_env(:favn_runner, :submit_call_timeout_ms, 35_000)
    )
  end

  defp cancel_call_timeout_ms(opts) do
    pos_int_opt(
      opts,
      :cancel_call_timeout_ms,
      Application.get_env(:favn_runner, :cancel_call_timeout_ms, 5_000)
    )
  end

  defp pos_int_opt(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> value
      _other -> default
    end
  end

  defp new_execution_id do
    "rx_" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end
end
