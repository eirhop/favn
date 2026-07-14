defmodule FavnOrchestrator.Scheduler.Runtime do
  @moduledoc false

  use GenServer

  require Logger

  alias Favn.Manifest.Index
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.Scheduler.Diagnostics
  alias FavnOrchestrator.Scheduler.Evaluator
  alias FavnOrchestrator.Scheduler.ManifestEntries
  alias FavnOrchestrator.Scheduler.StateStore

  @default_tick_ms 15_000
  @default_call_timeout_ms 5_000

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc false
  @spec reload(GenServer.server()) :: :ok | {:error, term()}
  def reload(server \\ __MODULE__), do: call_runtime(server, :reload)

  @doc false
  @spec tick(GenServer.server()) :: :ok | {:error, term()}
  def tick(server \\ __MODULE__), do: call_runtime(server, :tick)

  @doc false
  @spec scheduled(GenServer.server()) :: [map()] | {:error, term()}
  def scheduled(server \\ __MODULE__), do: call_runtime(server, :scheduled)

  @doc false
  @spec diagnostics(GenServer.server()) :: {:ok, map()} | {:error, term()}
  def diagnostics(server \\ __MODULE__), do: call_runtime(server, :diagnostics)

  @doc false
  @spec inspect_entries(GenServer.server()) ::
          [FavnOrchestrator.SchedulerEntry.t()] | {:error, term()}
  def inspect_entries(server \\ __MODULE__), do: call_runtime(server, :inspect_entries)

  @impl true
  def init(opts) do
    tick_ms = positive_integer(Keyword.get(opts, :tick_ms), configured_tick_ms())
    auto_tick? = Keyword.get(opts, :auto_tick?, true) == true

    case load_runtime(tick_ms, auto_tick?) do
      {:ok, state} ->
        schedule_next_tick(state)
        emit_scheduler_loaded(state)
        {:ok, state}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call(:reload, _from, state) do
    with {:ok, flushed} <- StateStore.flush_dirty(state),
         {:ok, next} <- load_runtime(flushed.tick_ms, flushed.auto_tick?) do
      emit_scheduler_loaded(next)
      {:reply, :ok, next}
    else
      {:error, reason, next} ->
        {:reply, {:error, reason}, next}

      {:error, reason} ->
        {:reply, {:error, reason}, StateStore.record_failure(state, reason)}
    end
  end

  def handle_call(:tick, _from, state) do
    case Evaluator.evaluate(state) do
      {:ok, next} -> {:reply, :ok, next}
      {:error, reason, next} -> {:reply, {:error, reason}, next}
    end
  end

  def handle_call(:scheduled, _from, state) do
    entries = state.entries |> Map.values() |> Enum.sort_by(&inspect(&1.module))
    {:reply, entries, state}
  end

  def handle_call(:diagnostics, _from, state),
    do: {:reply, {:ok, Diagnostics.payload(state)}, state}

  def handle_call(:inspect_entries, _from, state),
    do: {:reply, Diagnostics.entries(state, DateTime.utc_now()), state}

  @impl true
  def handle_info(:tick, state) do
    next =
      case Evaluator.evaluate(state) do
        {:ok, next} ->
          next

        {:error, reason, next} ->
          Logger.error("scheduler tick failed reason=#{safe_diagnostic(reason)}")

          next
      end

    schedule_next_tick(next)
    {:noreply, next}
  end

  defp call_runtime(server, message) do
    GenServer.call(server, message, call_timeout_ms())
  catch
    :exit, {:timeout, _call} -> {:error, {:scheduler_call_timeout, message}}
    :exit, {:noproc, _call} -> {:error, :scheduler_not_running}
  end

  defp load_runtime(tick_ms, auto_tick?) do
    with {:ok, version, index} <- load_active_manifest_index(),
         {:ok, entries} <- ManifestEntries.discover(version, index),
         {:ok, states} <- StateStore.load(entries) do
      {:ok,
       %{
         entries: entries,
         states: states,
         dirty_states: %{},
         last_persist_error: nil,
         tick_ms: tick_ms,
         auto_tick?: auto_tick?,
         version: version
       }}
    else
      {:empty, reason} when reason in [:active_manifest_not_set, :manifest_version_not_found] ->
        {:ok, empty_runtime_state(tick_ms, auto_tick?)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp empty_runtime_state(tick_ms, auto_tick?) do
    %{
      entries: %{},
      states: %{},
      dirty_states: %{},
      last_persist_error: nil,
      tick_ms: tick_ms,
      auto_tick?: auto_tick?,
      version: nil
    }
  end

  defp load_active_manifest_index do
    with {:ok, manifest_version_id} <- ManifestStore.get_active_manifest(),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         {:ok, index} <- Index.build_from_version(version) do
      {:ok, version, index}
    else
      {:error, reason} when reason in [:active_manifest_not_set, :manifest_version_not_found] ->
        {:empty, reason}

      {:error, _reason} = error ->
        error
    end
  end

  defp emit_scheduler_loaded(state) do
    OperationalEvents.emit(:scheduler_loaded, %{entry_count: map_size(state.entries)}, %{
      manifest_version_id: manifest_version_id(state.version),
      auto_tick?: state.auto_tick?
    })
  end

  defp schedule_next_tick(%{auto_tick?: true, tick_ms: tick_ms}) do
    Process.send_after(self(), :tick, next_tick_delay_ms(tick_ms))
  end

  defp schedule_next_tick(_state), do: :ok

  defp manifest_version_id(nil), do: nil
  defp manifest_version_id(version), do: version.manifest_version_id

  defp configured_tick_ms do
    case Application.get_env(:favn_orchestrator, :scheduler, []) do
      opts when is_list(opts) -> positive_integer(Keyword.get(opts, :tick_ms), @default_tick_ms)
      _other -> @default_tick_ms
    end
  end

  defp call_timeout_ms do
    Application.get_env(
      :favn_orchestrator,
      :scheduler_call_timeout_ms,
      @default_call_timeout_ms
    )
    |> positive_integer(@default_call_timeout_ms)
  end

  defp positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, default), do: default

  defp next_tick_delay_ms(base_tick_ms) do
    now = DateTime.utc_now()
    ms_to_next_minute = 60_000 - now.second * 1_000 - div(elem(now.microsecond, 0), 1_000)
    max(100, min(base_tick_ms, ms_to_next_minute))
  end

  defp safe_diagnostic(reason) do
    reason
    |> Redaction.redact_operational_bounded()
    |> inspect(limit: 20, printable_limit: 2_000)
  end
end
