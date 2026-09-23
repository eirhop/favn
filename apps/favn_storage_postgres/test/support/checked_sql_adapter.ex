defmodule FavnStoragePostgres.TestSupport.CheckedSQLAdapter do
  @moduledoc false
  alias Favn.SQL.{Capabilities, Result}

  def connect(resolved, _) do
    Process.put({__MODULE__, :transaction_outcome}, resolved.config[:transaction_outcome])
    options = resolved.config.database_url |> Ecto.Repo.Supervisor.parse_url()
    {:ok, pid} = Postgrex.start_link(Keyword.put(options, :ssl, false))
    {:ok, {pid, resolved.config.observer}}
  end

  def disconnect({pid, _}, _), do: GenServer.stop(pid)

  def capabilities(_, _),
    do: {:ok, %Capabilities{transactions: :supported, replace_table: :supported}}

  def relation(_, _, _), do: {:ok, nil}

  def execute(conn, sql, opts), do: query(conn, sql, opts)

  def query({pid, _}, sql, opts) do
    result = Postgrex.query!(pid, IO.iodata_to_binary(sql), Keyword.get(opts, :params, []))

    {:ok,
     %Result{
       kind: :query,
       command: to_string(result.command),
       rows_affected: result.num_rows,
       columns: result.columns || [],
       rows: Enum.map(result.rows || [], &Map.new(Enum.zip(result.columns, &1)))
     }}
  end

  def columns({pid, _}, relation, _) do
    %{rows: rows} =
      Postgrex.query!(
        pid,
        "SELECT attname FROM pg_attribute WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped",
        [relation.name]
      )

    {:ok,
     Enum.with_index(rows, 1)
     |> Enum.map(fn {[name], position} ->
       %Favn.SQL.Column{name: name, position: position, data_type: "VARCHAR", nullable?: true}
     end)}
  end

  def transaction({pid, observer} = conn, fun, _) do
    Postgrex.query!(pid, "BEGIN", [])

    case fun.(conn) do
      {:ok, value} ->
        Postgrex.query!(pid, "COMMIT", [])
        {:ok, value}

      {:error, error} ->
        Postgrex.query!(pid, "ROLLBACK", [])

        %{rows: [[0]]} =
          Postgrex.query!(
            pid,
            "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'favn_check_candidate_%'",
            []
          )

        send(observer, {:confirmed_sql_rollback, error})

        error =
          if Process.get({__MODULE__, :transaction_outcome}) == :rejected_commit do
            %Favn.SQL.Error{
              type: :transaction_conflict,
              operation: :transaction,
              message: "native rejected commit",
              retryable?: false,
              details: %{
                transaction_stage: :commit,
                transaction_outcome: :rolled_back,
                transaction_retry_attempts: 4,
                transaction_retry_stop: "attempt_limit"
              }
            }
          else
            error
          end

        {:error,
         %{
           error
           | details:
               Map.merge(error.details, %{
                 transaction_stage:
                   if(error.type == :transaction_conflict, do: :commit, else: :body),
                 transaction_outcome: :rolled_back
               })
         }}
    end
  end

  def materialize_in_transaction(_, _, _), do: raise("failed check reached materialization")
end
