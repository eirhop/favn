defmodule Favn.SQL.Adapter.DuckDB.ADBC.Client.ADBC do
  @moduledoc false

  @behaviour Favn.SQL.Adapter.DuckDB.ADBC.Client

  alias Adbc.{Column, Connection, Database, Result}

  @impl true
  def open(database, opts) do
    database_opts(database, opts)
    |> Database.start_link()
  end

  @impl true
  def connection(db), do: Connection.start_link(database: db)

  @impl true
  def query(conn, sql, params), do: Connection.query(conn, sql, params)

  @impl true
  def execute(conn, sql, params), do: Connection.execute(conn, sql, params)

  @impl true
  def fetch_all(%Result{} = result, max_rows) do
    case bounded_result?(result, max_rows) do
      :ok -> result_to_rows(result)
      {:error, reason} -> {:error, reason}
    end
  end

  def fetch_all(_result, _max_rows), do: {:error, :invalid_result}

  @impl true
  def columns(%Result{} = result) do
    result
    |> Result.to_columns()
    |> Map.keys()
    |> Enum.map(&to_string/1)
  end

  def columns(_result), do: {:error, :invalid_result}

  @impl true
  def bulk_insert(conn, rows, opts) when is_list(rows) and is_list(opts) do
    with {:ok, columns} <- rows_to_columns(rows) do
      Connection.bulk_insert(conn, columns, opts)
    end
  end

  @impl true
  def begin_transaction(conn), do: execute_transaction_statement(conn, "BEGIN TRANSACTION")

  @impl true
  def commit(conn), do: execute_transaction_statement(conn, "COMMIT")

  @impl true
  def rollback(conn), do: execute_transaction_statement(conn, "ROLLBACK")

  @impl true
  def release(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid, :normal, 5_000), else: :ok
  catch
    :exit, {:noproc, _} -> :ok
    :exit, _reason -> :ok
  end

  def release(_resource), do: :ok

  defp database_opts(nil, opts), do: database_opts(":memory:", opts)
  defp database_opts(":memory:", opts), do: driver_opts(opts)
  defp database_opts(path, opts) when is_binary(path), do: Keyword.put(driver_opts(opts), :path, path)
  defp database_opts(_database, _opts), do: [driver: :duckdb]

  defp driver_opts(opts) do
    configured = FavnDuckdbADBC.Runtime.driver_opts()
    driver = Keyword.get(opts, :driver, Keyword.get(configured, :driver, :duckdb))
    entrypoint = Keyword.get(opts, :entrypoint, Keyword.get(configured, :entrypoint))

    [driver: driver]
    |> maybe_put(:entrypoint, entrypoint)
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp bounded_result?(%Result{num_rows: num_rows}, max_rows)
       when is_integer(num_rows) and num_rows > max_rows,
       do: {:error, {:result_row_limit_exceeded, num_rows, max_rows}}

  defp bounded_result?(_result, _max_rows), do: :ok

  defp result_to_rows(%Result{} = result) do
    columns = Result.to_map(result)

    columns
    |> Map.values()
    |> case do
      [] -> []
      [first | _] -> rows_from_columns(columns, length(first))
    end
  end

  defp rows_from_columns(columns, row_count) do
    if row_count == 0 do
      []
    else
      for index <- 0..(row_count - 1)//1 do
        Map.new(columns, fn {column, values} -> {to_string(column), Enum.at(values, index)} end)
      end
    end
  end

  defp rows_to_columns([]), do: {:ok, []}

  defp rows_to_columns([%{} | _] = rows) do
    names = rows |> Enum.flat_map(&Map.keys/1) |> Enum.uniq()

    columns =
      Enum.map(names, fn name ->
        values = Enum.map(rows, &Map.get(&1, name))
        Column.new(values, name: to_string(name))
      end)

    {:ok, columns}
  end

  defp rows_to_columns(rows) when is_list(rows) do
    {:error, {:unsupported_bulk_rows, rows}}
  end

  defp execute_transaction_statement(conn, sql) do
    case Connection.execute(conn, sql, []) do
      {:ok, _rows_affected} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
