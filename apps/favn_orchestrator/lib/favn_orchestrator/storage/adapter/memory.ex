defmodule FavnOrchestrator.Storage.Adapter.Memory do
  @moduledoc false

  use GenServer

  @behaviour Favn.Storage.Adapter

  alias Favn.Manifest.Version
  alias Favn.Scheduler.State, as: SchedulerState
  alias FavnOrchestrator.RunState

  @type state :: %{
          manifests: %{required(String.t()) => Version.t()},
          active_manifest_version_id: String.t() | nil,
          runs: %{required(String.t()) => RunState.t()},
          run_events: %{required(String.t()) => [map()]},
          scheduler_states: %{required({module(), atom() | nil}) => map()}
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
    {:ok,
     %{
       id: __MODULE__,
       start: {__MODULE__, :start_link, [opts]},
       type: :worker,
       restart: :permanent,
       shutdown: 5000
     }}
  end

  @spec scheduler_child_spec(keyword()) :: {:ok, Supervisor.child_spec()} | :none
  def scheduler_child_spec(opts \\ []) when is_list(opts) do
    if Process.whereis(__MODULE__) do
      :none
    else
      child_spec(opts)
    end
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
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_run, run})
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
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:append_run_event, run_id, event})
  end

  @impl true
  def list_run_events(run_id, opts \\ []) when is_binary(run_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_run_events, run_id})
  end

  @impl true
  def put_scheduler_state({pipeline_module, schedule_id} = key, scheduler_state, opts)
      when is_atom(pipeline_module) and is_map(scheduler_state) and is_list(opts) do
    _ = schedule_id
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_scheduler_state, key, scheduler_state})
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
  def get_scheduler_state({pipeline_module, schedule_id} = key, opts \\ [])
      when is_atom(pipeline_module) and is_list(opts) do
    _ = schedule_id
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_scheduler_state, key})
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
  def init(_args) do
    {:ok,
     %{
       manifests: %{},
       active_manifest_version_id: nil,
       runs: %{},
       run_events: %{},
       scheduler_states: %{}
     }}
  end

  @impl true
  def handle_call(:reset, _from, _state) do
    {:reply, :ok,
     %{
       manifests: %{},
       active_manifest_version_id: nil,
       runs: %{},
       run_events: %{},
       scheduler_states: %{}
     }}
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
    {:reply, reply, %{state | runs: runs}}
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
    sequence = Map.get(event, :sequence)

    reply =
      if Enum.any?(current, &(Map.get(&1, :sequence) == sequence)) do
        {:error, :conflicting_event_sequence}
      else
        :ok
      end

    next_events =
      case reply do
        :ok ->
          current
          |> Kernel.++([event])
          |> Enum.sort_by(&Map.get(&1, :sequence, 0))

        _ ->
          current
      end

    {:reply, reply, put_in(state, [:run_events, run_id], next_events)}
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
        {nil, nil} ->
          :ok

        {nil, 1} ->
          :ok

        {nil, _other} ->
          {:error, :invalid_scheduler_version}

        {%{version: version}, nil} when is_integer(version) ->
          :ok

        {%{version: version}, incoming} when is_integer(version) and incoming == version + 1 ->
          :ok

        {%{version: _version}, _incoming} ->
          {:error, :stale_scheduler_state}
      end

    next_state =
      case reply do
        :ok ->
          new_version =
            case {current, incoming_version} do
              {%{version: version}, nil} when is_integer(version) -> version + 1
              {nil, nil} -> 1
              {_value, value} -> value
            end

          put_in(state, [:scheduler_states, key], Map.put(scheduler_state, :version, new_version))

        _ ->
          state
      end

    {:reply, reply, next_state}
  end

  def handle_call({:get_scheduler_state, key}, _from, state) do
    {:reply, {:ok, Map.get(state.scheduler_states, key)}, state}
  end

  defp put_run_with_semantics(runs, %RunState{} = incoming) do
    case Map.fetch(runs, incoming.id) do
      :error ->
        {:ok, Map.put(runs, incoming.id, incoming)}

      {:ok, %RunState{} = existing} when incoming.event_seq > existing.event_seq ->
        {:ok, Map.put(runs, incoming.id, incoming)}

      {:ok, %RunState{} = existing} when incoming.event_seq < existing.event_seq ->
        {{:error, :stale_write}, runs}

      {:ok, %RunState{} = existing} when incoming.snapshot_hash == existing.snapshot_hash ->
        {:ok, runs}

      {:ok, %RunState{}} ->
        {{:error, :conflicting_snapshot}, runs}
    end
    |> normalize_put_run_reply()
  end

  defp normalize_put_run_reply({:ok, runs}), do: {:ok, runs}
  defp normalize_put_run_reply({{:error, reason}, runs}), do: {{:error, reason}, runs}

  defp filter_runs(runs, run_opts) do
    case Keyword.get(run_opts, :status) do
      nil -> runs
      status -> Enum.filter(runs, &(&1.status == status))
    end
  end

  defp maybe_limit_runs(runs, run_opts) do
    case Keyword.get(run_opts, :limit) do
      limit when is_integer(limit) and limit > 0 -> Enum.take(runs, limit)
      _ -> runs
    end
  end
end
