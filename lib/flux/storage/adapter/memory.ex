defmodule Flux.Storage.Adapter.Memory do
  @moduledoc """
  In-memory storage adapter for runtime run records.

  This adapter is intended for development and testing:

    * node-local (per BEAM node)
    * non-durable (data is lost on restart)
    * deterministic listing for predictable assertions
  """

  use GenServer

  alias Flux.Run

  @table_name __MODULE__.Table

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

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(state) do
    _table = :ets.new(@table_name, [:set, :named_table, :public, read_concurrency: true])
    {:ok, state}
  end

  @spec put_run(Run.t(), keyword()) :: :ok | {:error, term()}
  def put_run(%Run{} = run, _opts) do
    inserted_seq =
      case :ets.lookup(@table_name, run.id) do
        [{_id, _stored_run, existing_inserted_seq}] -> existing_inserted_seq
        [] -> System.unique_integer([:monotonic, :positive])
      end

    true = :ets.insert(@table_name, {run.id, run, inserted_seq})
    :ok
  rescue
    error -> {:error, error}
  end

  @spec get_run(Flux.run_id(), keyword()) :: {:ok, Run.t()} | {:error, :not_found | term()}
  def get_run(run_id, _opts) do
    case :ets.lookup(@table_name, run_id) do
      [{^run_id, run, _inserted_seq}] -> {:ok, run}
      [] -> {:error, :not_found}
    end
  rescue
    error -> {:error, error}
  end

  @spec list_runs(Flux.list_runs_opts(), keyword()) :: {:ok, [Run.t()]} | {:error, term()}
  def list_runs(opts, _adapter_opts) when is_list(opts) do
    status = Keyword.get(opts, :status)
    limit = Keyword.get(opts, :limit)

    runs =
      @table_name
      |> :ets.tab2list()
      |> Enum.map(fn {_id, run, inserted_seq} -> {run, inserted_seq} end)
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

  defp sort_key({run, inserted_seq}) do
    {run.started_at, inserted_seq, run.id}
  end
end
