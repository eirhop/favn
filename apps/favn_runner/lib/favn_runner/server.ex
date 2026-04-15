defmodule FavnRunner.Server do
  @moduledoc """
  Runner protocol server for manifest registration and work execution.
  """

  use GenServer

  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias FavnRunner.ManifestResolver
  alias FavnRunner.ManifestStore
  alias FavnRunner.Worker

  @type execution_id :: String.t()

  @type waiter :: %{
          required(:from) => GenServer.from(),
          required(:timer_ref) => reference()
        }

  @type execution_state :: %{
          required(:work) => RunnerWork.t(),
          required(:status) => :running | :completed,
          optional(:pid) => pid(),
          optional(:monitor_ref) => reference(),
          optional(:result) => RunnerResult.t(),
          optional(:events) => [term()]
        }

  @type state :: %{
          executions: %{required(execution_id()) => execution_state()},
          monitor_to_execution: %{required(reference()) => execution_id()},
          waiters: %{required(execution_id()) => [waiter()]}
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, %{}, name: name)
  end

  @spec register_manifest(Version.t(), keyword()) :: :ok | {:error, term()}
  def register_manifest(%Version{} = version, opts \\ []) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:register_manifest, version})
  end

  @spec submit_work(RunnerWork.t(), keyword()) :: {:ok, execution_id()} | {:error, term()}
  def submit_work(%RunnerWork{} = work, opts \\ []) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:submit_work, work})
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

  @spec cancel_work(execution_id(), map(), keyword()) :: :ok | {:error, term()}
  def cancel_work(execution_id, reason \\ %{}, opts \\ [])

  def cancel_work(execution_id, reason, opts)
      when is_binary(execution_id) and is_map(reason) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:cancel_work, execution_id, reason})
  end

  def cancel_work(_execution_id, _reason, _opts), do: {:error, :invalid_cancel_args}

  @impl true
  def init(_args) do
    {:ok, %{executions: %{}, monitor_to_execution: %{}, waiters: %{}}}
  end

  @impl true
  def handle_call({:register_manifest, %Version{} = version}, _from, state) do
    reply = ManifestStore.register(version, server: FavnRunner.ManifestStore)
    {:reply, reply, state}
  end

  def handle_call({:submit_work, %RunnerWork{} = work}, _from, state) do
    reply =
      with {:ok, asset_ref} <- ManifestResolver.resolve_target_ref(work),
           {:ok, version} <-
             ManifestStore.fetch(work.manifest_version_id, work.manifest_content_hash,
               server: FavnRunner.ManifestStore
             ),
           {:ok, asset} <- ManifestResolver.resolve_asset(version, asset_ref),
           execution_id <- new_execution_id(),
           {:ok, pid} <- start_worker(execution_id, work, version, asset) do
        monitor_ref = Process.monitor(pid)

        execution = %{
          work: work,
          status: :running,
          pid: pid,
          monitor_ref: monitor_ref,
          events: []
        }

        next_state =
          state
          |> put_in([:executions, execution_id], execution)
          |> put_in([:monitor_to_execution, monitor_ref], execution_id)

        {{:ok, execution_id}, next_state}
      end

    case reply do
      {{:ok, execution_id}, next_state} -> {:reply, {:ok, execution_id}, next_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:await_result, execution_id, timeout}, from, state) do
    case Map.fetch(state.executions, execution_id) do
      {:ok, %{status: :completed, result: %RunnerResult{} = result}} ->
        {:reply, {:ok, result}, state}

      {:ok, %{status: :running}} ->
        timer_ref = Process.send_after(self(), {:await_timeout, execution_id, from}, timeout)
        waiters = [%{from: from, timer_ref: timer_ref} | Map.get(state.waiters, execution_id, [])]
        {:noreply, put_in(state, [:waiters, execution_id], waiters)}

      :error ->
        {:reply, {:error, :execution_not_found}, state}
    end
  end

  def handle_call({:cancel_work, execution_id, reason}, _from, state) do
    case Map.fetch(state.executions, execution_id) do
      {:ok, %{status: :completed}} ->
        {:reply, :ok, state}

      {:ok, %{status: :running, pid: pid, work: work, monitor_ref: monitor_ref}} ->
        _ = DynamicSupervisor.terminate_child(FavnRunner.WorkerSupervisor, pid)

        result = cancelled_result(work, reason)
        next_state = finalize_execution(state, execution_id, result)

        next_state = %{
          next_state
          | monitor_to_execution: Map.delete(next_state.monitor_to_execution, monitor_ref)
        }

        {:reply, :ok, next_state}

      :error ->
        {:reply, {:error, :execution_not_found}, state}
    end
  end

  @impl true
  def handle_info({:runner_event, execution_id, event}, state) do
    next_state =
      update_in(state, [:executions, execution_id, :events], fn
        nil -> [event]
        events -> [event | events]
      end)

    {:noreply, next_state}
  end

  def handle_info({:runner_result, execution_id, %RunnerResult{} = result}, state) do
    case Map.fetch(state.executions, execution_id) do
      {:ok, %{status: :running}} ->
        {:noreply, finalize_execution(state, execution_id, result)}

      {:ok, %{status: :completed}} ->
        {:noreply, state}

      :error ->
        {:noreply, state}
    end
  end

  def handle_info({:await_timeout, execution_id, from}, state) do
    {waiters, remaining_waiters} = pop_waiter(state.waiters, execution_id, from)

    Enum.each(waiters, fn %{from: waiter_from} ->
      GenServer.reply(waiter_from, {:error, :timeout})
    end)

    {:noreply, %{state | waiters: remaining_waiters}}
  end

  def handle_info({:DOWN, monitor_ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitor_to_execution, monitor_ref) do
      {nil, monitor_to_execution} ->
        {:noreply, %{state | monitor_to_execution: monitor_to_execution}}

      {execution_id, monitor_to_execution} ->
        next_state = %{state | monitor_to_execution: monitor_to_execution}

        case Map.fetch(next_state.executions, execution_id) do
          {:ok, %{status: :running, work: work}} ->
            crash_result = crashed_result(work, reason)
            {:noreply, finalize_execution(next_state, execution_id, crash_result)}

          _other ->
            {:noreply, next_state}
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

  defp finalize_execution(state, execution_id, %RunnerResult{} = result) do
    execution = Map.get(state.executions, execution_id, %{})

    next_state =
      put_in(state, [:executions, execution_id], %{
        work: execution[:work],
        status: :completed,
        result: result,
        events: execution[:events] || []
      })

    {waiters, remaining_waiters} = Map.pop(next_state.waiters, execution_id, [])

    Enum.each(waiters, fn %{from: from, timer_ref: timer_ref} ->
      _ = Process.cancel_timer(timer_ref)
      GenServer.reply(from, {:ok, result})
    end)

    %{next_state | waiters: remaining_waiters}
  end

  defp pop_waiter(waiters, execution_id, from) do
    case Map.fetch(waiters, execution_id) do
      {:ok, execution_waiters} ->
        {matched, remaining} = Enum.split_with(execution_waiters, &(&1.from == from))

        next_waiters =
          if remaining == [] do
            Map.delete(waiters, execution_id)
          else
            Map.put(waiters, execution_id, remaining)
          end

        {matched, next_waiters}

      :error ->
        {[], waiters}
    end
  end

  defp cancelled_result(work, reason) do
    %RunnerResult{
      run_id: work.run_id,
      manifest_version_id: work.manifest_version_id,
      manifest_content_hash: work.manifest_content_hash,
      status: :cancelled,
      asset_results: [],
      error: {:cancelled, reason},
      metadata: work.metadata
    }
  end

  defp crashed_result(work, reason) do
    %RunnerResult{
      run_id: work.run_id,
      manifest_version_id: work.manifest_version_id,
      manifest_content_hash: work.manifest_content_hash,
      status: :error,
      asset_results: [],
      error: {:worker_crash, reason},
      metadata: work.metadata
    }
  end

  defp new_execution_id do
    "rx_" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end
end
