defmodule Favn.Storage.Adapter.Memory do
  @moduledoc """
  In-memory storage adapter for runtime run records.

  This adapter is intended for development and testing:

    * node-local (per BEAM node)
    * non-durable (data is lost on restart)
    * deterministic listing for predictable assertions
  """

  use GenServer
  @behaviour Favn.Storage.Adapter

  alias Favn.Run
  alias Favn.Scheduler.State, as: SchedulerState
  alias Favn.Storage.RunWriteSemantics
  alias Favn.Storage.SnapshotHash

  @table_name __MODULE__.Table
  @scheduler_table __MODULE__.SchedulerTable

  @impl true
  @spec child_spec(keyword()) :: {:ok, Supervisor.child_spec()} | :none
  def child_spec(opts \\ []) do
    {:ok,
     %{
       id: __MODULE__,
       start: {__MODULE__, :start_link, [opts]},
       type: :worker,
       restart: :permanent,
       shutdown: 5000
     }}
  end

  @impl true
  @spec scheduler_child_spec(keyword()) :: {:ok, Supervisor.child_spec()} | :none
  def scheduler_child_spec(_opts), do: :none

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(state) do
    _table = :ets.new(@table_name, [:set, :named_table, :public, read_concurrency: true])

    _scheduler_table =
      :ets.new(@scheduler_table, [:set, :named_table, :public, read_concurrency: true])

    {:ok, state}
  end

  @impl true
  @spec put_run(Run.t(), keyword()) :: :ok | {:error, term()}
  def put_run(%Run{} = run, _opts) do
    with {:ok, incoming_hash} <- SnapshotHash.for_run(run, allow_fallback_term: true) do
      case :ets.lookup(@table_name, run.id) do
        [] ->
          inserted_seq = System.unique_integer([:monotonic, :positive])
          updated_seq = System.unique_integer([:monotonic, :positive])

          true =
            :ets.insert(
              @table_name,
              {run.id, run, inserted_seq, updated_seq, run.event_seq, incoming_hash}
            )

          :ok

        [stored] ->
          handle_existing_run(stored, run, incoming_hash)
      end
    end
  rescue
    error -> {:error, error}
  end

  defp handle_existing_run(
         {_id, _stored_run, inserted_seq, _updated_seq, stored_event_seq, stored_hash},
         %Run{} = incoming_run,
         incoming_hash
       ) do
    case RunWriteSemantics.decide(
           stored_event_seq,
           stored_hash,
           incoming_run.event_seq,
           incoming_hash
         ) do
      :replace ->
        updated_seq = System.unique_integer([:monotonic, :positive])

        true =
          :ets.insert(
            @table_name,
            {incoming_run.id, incoming_run, inserted_seq, updated_seq, incoming_run.event_seq,
             incoming_hash}
          )

        :ok

      :idempotent ->
        :ok

      {:error, reason} ->
        {:error, reason}

      :insert ->
        :ok
    end
  end

  defp handle_existing_run(
         {_id, %Run{} = stored_run, inserted_seq, updated_seq},
         %Run{} = incoming_run,
         incoming_hash
       ) do
    with {:ok, stored_hash} <- SnapshotHash.for_run(stored_run, allow_fallback_term: true) do
      handle_existing_run(
        {incoming_run.id, stored_run, inserted_seq, updated_seq, stored_run.event_seq,
         stored_hash},
        incoming_run,
        incoming_hash
      )
    end
  end

  @impl true
  @spec get_run(Favn.run_id(), keyword()) :: {:ok, Run.t()} | {:error, :not_found | term()}
  def get_run(run_id, _opts) do
    case :ets.lookup(@table_name, run_id) do
      [{^run_id, run, _inserted_seq, _updated_seq, _event_seq, _snapshot_hash}] -> {:ok, run}
      [{^run_id, run, _inserted_seq, _updated_seq}] -> {:ok, run}
      [] -> {:error, :not_found}
    end
  rescue
    error -> {:error, error}
  end

  @impl true
  @spec list_runs(Favn.list_runs_opts(), keyword()) :: {:ok, [Run.t()]} | {:error, term()}
  def list_runs(opts, _adapter_opts) when is_list(opts) do
    status = Keyword.get(opts, :status)
    limit = Keyword.get(opts, :limit)

    runs =
      @table_name
      |> :ets.tab2list()
      |> Enum.map(fn
        {_id, run, _inserted_seq, updated_seq, _event_seq, _snapshot_hash} -> {run, updated_seq}
        {_id, run, _inserted_seq, updated_seq} -> {run, updated_seq}
      end)
      |> maybe_filter_status(status)
      |> Enum.sort_by(&sort_key/1, :desc)
      |> Enum.map(&elem(&1, 0))
      |> maybe_limit(limit)

    {:ok, runs}
  rescue
    error -> {:error, error}
  end

  defp maybe_filter_status(runs, nil), do: runs

  defp maybe_filter_status(runs, status) do
    Enum.filter(runs, fn {run, _seq} -> run.status == status end)
  end

  defp maybe_limit(runs, nil), do: runs
  defp maybe_limit(runs, limit), do: Enum.take(runs, limit)

  defp sort_key({run, updated_seq}) do
    {updated_seq, run.id}
  end

  @impl true
  @spec put_scheduler_state(SchedulerState.t(), keyword()) :: :ok | {:error, term()}
  def put_scheduler_state(%SchedulerState{} = state, _opts) do
    key = {state.pipeline_module, state.schedule_id}
    true = :ets.insert(@scheduler_table, {key, state})
    :ok
  rescue
    error -> {:error, error}
  end

  @impl true
  @spec get_scheduler_state(module(), atom() | nil, keyword()) ::
          {:ok, SchedulerState.t() | nil} | {:error, term()}
  def get_scheduler_state(pipeline_module, schedule_id, _opts) when is_atom(pipeline_module) do
    key = {pipeline_module, schedule_id}

    case :ets.lookup(@scheduler_table, key) do
      [{^key, %SchedulerState{} = state}] ->
        {:ok, state}

      [] when is_nil(schedule_id) ->
        @scheduler_table
        |> :ets.tab2list()
        |> Enum.find_value(fn
          {{^pipeline_module, _stored_schedule_id}, %SchedulerState{} = state} -> state
          _ -> nil
        end)
        |> then(&{:ok, &1})

      [] ->
        {:ok, nil}
    end
  rescue
    error -> {:error, error}
  end
end
