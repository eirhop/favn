defmodule FavnOrchestrator.Storage.Adapter.Memory do
  @moduledoc false

  use GenServer

  @behaviour Favn.Storage.Adapter

  alias Favn.Manifest.Version
  alias Favn.Scheduler.State, as: SchedulerState
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.RunEventCodec
  alias FavnOrchestrator.Storage.RunStateCodec
  alias FavnOrchestrator.Storage.SchedulerStateCodec
  alias FavnOrchestrator.Storage.WriteSemantics

  @type state :: %{
          manifests: %{required(String.t()) => Version.t()},
          active_manifest_version_id: String.t() | nil,
          runs: %{required(String.t()) => RunState.t()},
          run_events: %{required(String.t()) => [map()]},
          scheduler_states: %{required({module(), atom() | nil}) => map()},
          coverage_baselines: %{required(String.t()) => CoverageBaseline.t()},
          backfill_windows: %{required({String.t(), module(), String.t()}) => BackfillWindow.t()},
          asset_window_states: %{required({module(), atom(), String.t()}) => AssetWindowState.t()}
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, %{}, name: name)
  end

  @spec reset(keyword()) :: :ok
  def reset(opts \\ []) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, :reset)
  end

  @impl true
  def child_spec(opts) when is_list(opts) do
    runtime_name = runtime_name(opts)

    if runtime_started?(runtime_name) do
      :none
    else
      start_opts =
        opts
        |> Keyword.delete(:server)
        |> Keyword.put(:name, runtime_name)

      {:ok,
       %{
         id: {__MODULE__, runtime_name},
         start: {__MODULE__, :start_link, [start_opts]},
         type: :worker,
         restart: :permanent,
         shutdown: 5000
       }}
    end
  end

  @spec scheduler_child_spec(keyword()) :: Favn.Storage.Adapter.child_spec_result()
  def scheduler_child_spec(opts \\ []) when is_list(opts) do
    child_spec(opts)
  end

  @impl true
  def put_manifest_version(%Version{} = version, opts \\ []) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_manifest_version, version})
  end

  @impl true
  def get_manifest_version(manifest_version_id, opts \\ []) when is_binary(manifest_version_id) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_manifest_version, manifest_version_id})
  end

  @impl true
  def list_manifest_versions(opts \\ []) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, :list_manifest_versions)
  end

  @impl true
  def set_active_manifest_version(manifest_version_id, opts \\ [])
      when is_binary(manifest_version_id) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:set_active_manifest_version, manifest_version_id})
  end

  @impl true
  def get_active_manifest_version(opts \\ []) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, :get_active_manifest_version)
  end

  @impl true
  def put_run(%RunState{} = run, opts \\ []) when is_list(opts) do
    with {:ok, normalized} <- RunStateCodec.normalize(run) do
      server = Keyword.get(opts, :server, __MODULE__)
      GenServer.call(server, {:put_run, normalized})
    end
  end

  @impl true
  def persist_run_transition(%RunState{} = run, event, opts)
      when is_map(event) and is_list(opts) do
    with {:ok, normalized_run} <- RunStateCodec.normalize(run),
         {:ok, normalized_event} <- RunEventCodec.normalize(run.id, event),
         :ok <- validate_transition_alignment(normalized_run, normalized_event) do
      server = Keyword.get(opts, :server, __MODULE__)
      GenServer.call(server, {:persist_run_transition, normalized_run, normalized_event})
    end
  end

  @impl true
  def get_run(run_id, opts \\ []) when is_binary(run_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_run, run_id})
  end

  @impl true
  def list_runs(run_opts \\ [], adapter_opts \\ [])
      when is_list(run_opts) and is_list(adapter_opts) do
    server = Keyword.get(adapter_opts, :server, __MODULE__)
    GenServer.call(server, {:list_runs, run_opts})
  end

  @impl true
  def append_run_event(run_id, event, opts \\ [])
      when is_binary(run_id) and is_map(event) and is_list(opts) do
    with {:ok, normalized} <- RunEventCodec.normalize(run_id, event) do
      server = Keyword.get(opts, :server, __MODULE__)
      GenServer.call(server, {:append_run_event, run_id, normalized})
    end
  end

  @impl true
  def list_run_events(run_id, opts \\ []) when is_binary(run_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_run_events, run_id})
  end

  @impl true
  def put_scheduler_state(key, scheduler_state, opts)
      when is_map(scheduler_state) and is_list(opts) do
    with {:ok, normalized_key} <- SchedulerStateCodec.normalize_key(key),
         {:ok, normalized_state} <- SchedulerStateCodec.normalize_state(scheduler_state) do
      server = Keyword.get(opts, :server, __MODULE__)
      GenServer.call(server, {:put_scheduler_state, normalized_key, normalized_state})
    end
  end

  @spec put_scheduler_state(SchedulerState.t(), keyword()) :: :ok | {:error, term()}
  def put_scheduler_state(%SchedulerState{} = scheduler_state, opts) when is_list(opts) do
    key = {scheduler_state.pipeline_module, scheduler_state.schedule_id}

    payload =
      scheduler_state
      |> Map.from_struct()
      |> Map.drop([:pipeline_module, :schedule_id])

    put_scheduler_state(key, payload, opts)
  end

  @impl true
  def get_scheduler_state(key, opts \\ []) when is_list(opts) do
    with {:ok, normalized_key} <- SchedulerStateCodec.normalize_key(key) do
      server = Keyword.get(opts, :server, __MODULE__)
      GenServer.call(server, {:get_scheduler_state, normalized_key})
    end
  end

  @spec get_scheduler_state(module(), atom() | nil, keyword()) ::
          {:ok, SchedulerState.t() | nil} | {:error, term()}
  def get_scheduler_state(pipeline_module, schedule_id, opts)
      when is_atom(pipeline_module) and is_list(opts) do
    case get_scheduler_state({pipeline_module, schedule_id}, opts) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, state} when is_map(state) ->
        {:ok,
         struct(
           SchedulerState,
           Map.merge(state, %{pipeline_module: pipeline_module, schedule_id: schedule_id})
         )}

      other ->
        other
    end
  end

  @impl true
  def put_coverage_baseline(%CoverageBaseline{} = baseline, opts) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_coverage_baseline, baseline})
  end

  @impl true
  def get_coverage_baseline(baseline_id, opts) when is_binary(baseline_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_coverage_baseline, baseline_id})
  end

  @impl true
  def list_coverage_baselines(filters, opts) when is_list(filters) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_coverage_baselines, filters})
  end

  @impl true
  def put_backfill_window(%BackfillWindow{} = window, opts) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_backfill_window, window})
  end

  @impl true
  def get_backfill_window(backfill_run_id, pipeline_module, window_key, opts)
      when is_binary(backfill_run_id) and is_atom(pipeline_module) and is_binary(window_key) and
             is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_backfill_window, {backfill_run_id, pipeline_module, window_key}})
  end

  @impl true
  def list_backfill_windows(filters, opts) when is_list(filters) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_backfill_windows, filters})
  end

  @impl true
  def put_asset_window_state(%AssetWindowState{} = state, opts) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_asset_window_state, state})
  end

  @impl true
  def get_asset_window_state(asset_ref_module, asset_ref_name, window_key, opts)
      when is_atom(asset_ref_module) and is_atom(asset_ref_name) and is_binary(window_key) and
             is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)

    GenServer.call(
      server,
      {:get_asset_window_state, {asset_ref_module, asset_ref_name, window_key}}
    )
  end

  @impl true
  def list_asset_window_states(filters, opts) when is_list(filters) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_asset_window_states, filters})
  end

  @impl true
  def replace_backfill_read_models(
        scope,
        coverage_baselines,
        backfill_windows,
        asset_window_states,
        opts
      )
      when is_list(scope) and is_list(coverage_baselines) and is_list(backfill_windows) and
             is_list(asset_window_states) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)

    GenServer.call(
      server,
      {:replace_backfill_read_models, scope, coverage_baselines, backfill_windows,
       asset_window_states}
    )
  end

  @impl true
  def init(_args) do
    {:ok, initial_state()}
  end

  defp initial_state do
    %{
      manifests: %{},
      active_manifest_version_id: nil,
      runs: %{},
      run_events: %{},
      scheduler_states: %{},
      coverage_baselines: %{},
      backfill_windows: %{},
      asset_window_states: %{}
    }
  end

  @impl true
  def handle_call(:reset, _from, _state) do
    {:reply, :ok, initial_state()}
  end

  def handle_call({:put_manifest_version, %Version{} = version}, _from, state) do
    next_state =
      case Map.fetch(state.manifests, version.manifest_version_id) do
        {:ok, %Version{content_hash: hash}} when hash == version.content_hash ->
          state

        {:ok, %Version{}} ->
          state

        :error ->
          put_in(state, [:manifests, version.manifest_version_id], version)
      end

    reply =
      case Map.fetch(state.manifests, version.manifest_version_id) do
        {:ok, %Version{content_hash: hash}} when hash != version.content_hash ->
          {:error, :manifest_version_conflict}

        _ ->
          :ok
      end

    {:reply, reply, next_state}
  end

  def handle_call({:get_manifest_version, manifest_version_id}, _from, state) do
    reply =
      case Map.fetch(state.manifests, manifest_version_id) do
        {:ok, %Version{} = version} -> {:ok, version}
        :error -> {:error, :manifest_version_not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:list_manifest_versions, _from, state) do
    versions =
      state.manifests
      |> Map.values()
      |> Enum.sort_by(& &1.manifest_version_id)

    {:reply, {:ok, versions}, state}
  end

  def handle_call({:set_active_manifest_version, manifest_version_id}, _from, state) do
    case Map.has_key?(state.manifests, manifest_version_id) do
      true ->
        {:reply, :ok, %{state | active_manifest_version_id: manifest_version_id}}

      false ->
        {:reply, {:error, :manifest_version_not_found}, state}
    end
  end

  def handle_call(:get_active_manifest_version, _from, state) do
    reply =
      case state.active_manifest_version_id do
        nil -> {:error, :active_manifest_not_set}
        manifest_version_id -> {:ok, manifest_version_id}
      end

    {:reply, reply, state}
  end

  def handle_call({:put_run, %RunState{} = incoming}, _from, state) do
    {reply, runs} = put_run_with_semantics(state.runs, incoming)

    normalized_reply = if reply == :idempotent, do: :ok, else: reply

    {:reply, normalized_reply, %{state | runs: runs}}
  end

  def handle_call({:persist_run_transition, %RunState{} = run, event}, _from, state) do
    {run_reply, runs} = put_run_with_semantics(state.runs, run)

    case run_reply do
      run_write_result when run_write_result in [:ok, :idempotent] ->
        current = Map.get(state.run_events, run.id, [])

        case append_event_with_semantics(current, event) do
          {:ok, event_write_result, next_events} ->
            next_state = %{
              state
              | runs: runs,
                run_events: Map.put(state.run_events, run.id, next_events)
            }

            result =
              case {run_write_result, event_write_result} do
                {:idempotent, :idempotent} -> :idempotent
                _ -> :ok
              end

            {:reply, result, next_state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get_run, run_id}, _from, state) do
    reply =
      case Map.fetch(state.runs, run_id) do
        {:ok, %RunState{} = run} -> {:ok, run}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:list_runs, run_opts}, _from, state) do
    runs =
      state.runs
      |> Map.values()
      |> filter_runs(run_opts)
      |> Enum.sort_by(& &1.updated_at, {:desc, DateTime})
      |> maybe_limit_runs(run_opts)

    {:reply, {:ok, runs}, state}
  end

  def handle_call({:append_run_event, run_id, event}, _from, state) do
    current = Map.get(state.run_events, run_id, [])

    case append_event_with_semantics(current, event) do
      {:ok, _event_write_result, next_events} ->
        {:reply, :ok, put_in(state, [:run_events, run_id], next_events)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:list_run_events, run_id}, _from, state) do
    events = Map.get(state.run_events, run_id, [])
    {:reply, {:ok, events}, state}
  end

  def handle_call({:put_scheduler_state, key, scheduler_state}, _from, state) do
    current = Map.get(state.scheduler_states, key)
    incoming_version = Map.get(scheduler_state, :version)

    reply =
      case {current, incoming_version} do
        {nil, 1} ->
          :ok

        {nil, _other} ->
          {:error, :invalid_scheduler_version}

        {%{version: version}, incoming} when is_integer(version) and incoming == version + 1 ->
          :ok

        {%{version: _version}, _incoming} ->
          {:error, :stale_scheduler_state}
      end

    next_state =
      case reply do
        :ok ->
          put_in(state, [:scheduler_states, key], scheduler_state)

        _ ->
          state
      end

    {:reply, reply, next_state}
  end

  def handle_call({:get_scheduler_state, key}, _from, state) do
    value =
      case Map.get(state.scheduler_states, key) do
        nil ->
          nil

        stored when is_map(stored) ->
          {pipeline_module, schedule_id} = key

          struct(
            SchedulerState,
            Map.merge(stored, %{pipeline_module: pipeline_module, schedule_id: schedule_id})
          )
      end

    {:reply, {:ok, value}, state}
  end

  def handle_call({:put_coverage_baseline, %CoverageBaseline{} = baseline}, _from, state) do
    {:reply, :ok, put_in(state, [:coverage_baselines, baseline.baseline_id], baseline)}
  end

  def handle_call({:get_coverage_baseline, baseline_id}, _from, state) do
    {:reply, fetch_or_not_found(state.coverage_baselines, baseline_id), state}
  end

  def handle_call({:list_coverage_baselines, filters}, _from, state) do
    rows =
      state.coverage_baselines
      |> Map.values()
      |> filter_by(filters)
      |> Enum.sort_by(&{DateTime.to_unix(&1.updated_at, :microsecond) * -1, &1.baseline_id})
      |> offset_and_fetch(filters)

    {:reply, {:ok, Page.from_fetched(rows, page_opts(filters))}, state}
  end

  def handle_call({:put_backfill_window, %BackfillWindow{} = window}, _from, state) do
    key = {window.backfill_run_id, window.pipeline_module, window.window_key}
    {:reply, :ok, put_in(state, [:backfill_windows, key], window)}
  end

  def handle_call({:get_backfill_window, key}, _from, state) do
    {:reply, fetch_or_not_found(state.backfill_windows, key), state}
  end

  def handle_call({:list_backfill_windows, filters}, _from, state) do
    rows =
      state.backfill_windows
      |> Map.values()
      |> filter_by(filters)
      |> Enum.sort_by(
        &{DateTime.to_unix(&1.window_start_at, :microsecond), &1.backfill_run_id,
         Atom.to_string(&1.pipeline_module), &1.window_key}
      )
      |> offset_and_fetch(filters)

    {:reply, {:ok, Page.from_fetched(rows, page_opts(filters))}, state}
  end

  def handle_call({:put_asset_window_state, %AssetWindowState{} = window_state}, _from, state) do
    key = {window_state.asset_ref_module, window_state.asset_ref_name, window_state.window_key}
    {:reply, :ok, put_in(state, [:asset_window_states, key], window_state)}
  end

  def handle_call({:get_asset_window_state, key}, _from, state) do
    {:reply, fetch_or_not_found(state.asset_window_states, key), state}
  end

  def handle_call({:list_asset_window_states, filters}, _from, state) do
    rows =
      state.asset_window_states
      |> Map.values()
      |> filter_by(filters)
      |> Enum.sort_by(
        &{DateTime.to_unix(&1.updated_at, :microsecond) * -1, Atom.to_string(&1.asset_ref_module),
         Atom.to_string(&1.asset_ref_name), &1.window_key}
      )
      |> offset_and_fetch(filters)

    {:reply, {:ok, Page.from_fetched(rows, page_opts(filters))}, state}
  end

  def handle_call(
        {:replace_backfill_read_models, scope, coverage_baselines, backfill_windows,
         asset_window_states},
        _from,
        state
      ) do
    next_state = %{
      state
      | coverage_baselines:
          state.coverage_baselines
          |> reject_scoped(scope)
          |> put_coverage_baselines(coverage_baselines),
        backfill_windows:
          state.backfill_windows
          |> reject_scoped(scope)
          |> put_backfill_windows(backfill_windows),
        asset_window_states:
          state.asset_window_states
          |> reject_scoped(scope)
          |> put_asset_window_states(asset_window_states)
    }

    {:reply, :ok, next_state}
  end

  defp put_run_with_semantics(runs, %RunState{} = incoming) do
    case Map.fetch(runs, incoming.id) do
      :error ->
        {{:ok, :ok}, Map.put(runs, incoming.id, incoming)}

      {:ok, %RunState{} = existing} ->
        case WriteSemantics.decide(
               existing.event_seq,
               existing.snapshot_hash,
               incoming.event_seq,
               incoming.snapshot_hash
             ) do
          :replace -> {{:ok, :ok}, Map.put(runs, incoming.id, incoming)}
          :idempotent -> {{:ok, :idempotent}, runs}
          {:error, reason} -> {{:error, reason}, runs}
        end
    end
    |> normalize_put_run_reply()
  end

  defp normalize_put_run_reply({{:ok, result}, runs}) when result in [:ok, :idempotent],
    do: {result, runs}

  defp normalize_put_run_reply({{:error, reason}, runs}), do: {{:error, reason}, runs}

  defp filter_runs(runs, run_opts) do
    case Keyword.get(run_opts, :status) do
      nil -> runs
      status -> Enum.filter(runs, &(&1.status == status))
    end
  end

  defp fetch_or_not_found(values, key) do
    case Map.fetch(values, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, :not_found}
    end
  end

  defp filter_by(values, filters) do
    filters = Keyword.drop(filters, [:limit, :offset])

    Enum.filter(values, fn value ->
      Enum.all?(filters, fn {key, expected} -> Map.get(value, key) == expected end)
    end)
  end

  defp reject_scoped(_values, []), do: %{}

  defp reject_scoped(values, scope) when is_map(values) do
    values
    |> Enum.reject(fn {_key, value} -> scoped?(value, scope) end)
    |> Map.new()
  end

  defp scoped?(value, scope) do
    Enum.all?(scope, fn {key, expected} -> Map.get(value, key) == expected end)
  end

  defp put_coverage_baselines(values, baselines) do
    Enum.reduce(baselines, values, fn %CoverageBaseline{} = baseline, acc ->
      Map.put(acc, baseline.baseline_id, baseline)
    end)
  end

  defp put_backfill_windows(values, windows) do
    Enum.reduce(windows, values, fn %BackfillWindow{} = window, acc ->
      Map.put(acc, {window.backfill_run_id, window.pipeline_module, window.window_key}, window)
    end)
  end

  defp put_asset_window_states(values, states) do
    Enum.reduce(states, values, fn %AssetWindowState{} = window_state, acc ->
      Map.put(
        acc,
        {window_state.asset_ref_module, window_state.asset_ref_name, window_state.window_key},
        window_state
      )
    end)
  end

  defp offset_and_fetch(values, filters) do
    opts = page_opts(filters)

    values
    |> Enum.drop(Keyword.fetch!(opts, :offset))
    |> Enum.take(Keyword.fetch!(opts, :limit) + 1)
  end

  defp page_opts(filters) do
    {:ok, opts} = Page.normalize_opts(filters)
    opts
  end

  defp maybe_limit_runs(runs, run_opts) do
    case Keyword.get(run_opts, :limit) do
      limit when is_integer(limit) and limit > 0 -> Enum.take(runs, limit)
      _ -> runs
    end
  end

  defp validate_transition_alignment(%RunState{} = run, event) when is_map(event) do
    cond do
      Map.get(event, :run_id) != run.id ->
        {:error, :invalid_run_event_run_id}

      Map.get(event, :sequence) != run.event_seq ->
        {:error, :invalid_run_event_sequence}

      true ->
        :ok
    end
  end

  defp append_event_with_semantics(current_events, event) when is_list(current_events) do
    sequence = Map.get(event, :sequence)

    existing = Enum.find(current_events, &(Map.get(&1, :sequence) == sequence))

    case WriteSemantics.decide_run_event_append(existing, event) do
      :insert ->
        {:ok, :ok, Enum.sort_by(current_events ++ [event], &Map.get(&1, :sequence, 0))}

      :idempotent ->
        {:ok, :idempotent, current_events}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp runtime_name(opts) do
    Keyword.get(opts, :server, Keyword.get(opts, :name, __MODULE__))
  end

  defp runtime_started?(name) when is_atom(name), do: Process.whereis(name) != nil
  defp runtime_started?(_name), do: false
end
