defmodule FavnDuckdb.TestSupport do
  @moduledoc false

  @events_key {__MODULE__, :events}
  @modes_key {__MODULE__, :modes}

  def start_events do
    Process.put(@events_key, [])
    :ok
  end

  def record(event) do
    events = Process.get(@events_key, [])
    key = System.unique_integer([:positive, :monotonic])
    Process.put(@events_key, [{key, event} | events])
    :ok
  end

  def events do
    @events_key
    |> Process.get([])
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
  end

  def put_mode(key, value) do
    modes = Process.get(@modes_key, %{})
    Process.put(@modes_key, Map.put(modes, key, value))
    :ok
  end

  def put_modes(keys, value) do
    Enum.each(keys, &put_mode(&1, value))
    :ok
  end

  def mode(key, default), do: Map.get(Process.get(@modes_key, %{}), key, default)

  def reset do
    Process.delete(@events_key)
    Process.delete(@modes_key)
    :ok
  end
end

defmodule FavnDuckdb.TestSupport.FakeClient do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      @behaviour Favn.SQL.Adapter.DuckDB.Client

      @impl true
      def open(_database), do: {:ok, make_ref()}

      @impl true
      def connection(_db_ref), do: {:ok, make_ref()}

      @impl true
      def query(_conn_ref, _sql, _params), do: {:ok, make_ref()}

      @impl true
      def fetch_all(_result_ref), do: []

      @impl true
      def columns(_result_ref), do: []

      @impl true
      def begin_transaction(_conn_ref), do: :ok

      @impl true
      def commit(_conn_ref), do: :ok

      @impl true
      def rollback(_conn_ref), do: :ok

      @impl true
      def appender(_conn_ref, _table_name, _schema), do: {:ok, make_ref()}

      @impl true
      def appender_add_rows(_appender_ref, _rows), do: :ok

      @impl true
      def appender_flush(_appender_ref), do: :ok

      @impl true
      def appender_close(_appender_ref), do: :ok

      @impl true
      def release(_resource), do: :ok

      defoverridable open: 1,
                     connection: 1,
                     query: 3,
                     fetch_all: 1,
                     columns: 1,
                     begin_transaction: 1,
                     commit: 1,
                     rollback: 1,
                     appender: 3,
                     appender_add_rows: 2,
                     appender_flush: 1,
                     appender_close: 1,
                     release: 1
    end
  end
end
