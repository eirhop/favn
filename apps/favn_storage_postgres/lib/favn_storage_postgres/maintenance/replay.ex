defmodule FavnStoragePostgres.Maintenance.Replay do
  @moduledoc false
  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Error
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Repo

  def validate_timestamp!(timestamp) do
    %{rows: [[now]]} = SQL.query!(Repo, "SELECT clock_timestamp()", [])
    window = FavnOrchestrator.Retention.Policy.command_window_seconds()

    if not match?(%DateTime{}, timestamp) or
         DateTime.compare(timestamp, DateTime.add(now, -window, :second)) == :lt or
         DateTime.compare(timestamp, DateTime.add(now, 300, :second)) == :gt do
      Repo.rollback(Error.new(:invalid, "command is outside the replay window"))
    end

    :ok
  end

  def read(fun, options \\ []) do
    case Repo.transaction(
           fn ->
             %{rows: [[isolation]]} = SQL.query!(Repo, "SHOW transaction_isolation", [])

             unless isolation in ["repeatable read", "serializable"] do
               SQL.query!(Repo, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", [])
             end

             fun.()
           end,
           options
         ) do
      {:ok, result} -> result
      {:error, error} -> {:error, error}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  def check!(workspace_id, stream, cursor) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT publication_id, batch_offset FROM favn_control.retention_floors
        WHERE ($1::text IS NULL OR workspace_id = $1) AND stream = ANY($2::text[])
        ORDER BY publication_id DESC, batch_offset DESC LIMIT 1
        """,
        [workspace_id, List.wrap(stream)]
      )

    floor =
      case rows do
        [] -> {0, 0}
        [[id, offset]] -> {id, offset}
      end

    if cursor && {cursor.publication_id, Map.get(cursor, :batch_offset, 0)} < floor do
      Repo.rollback(
        Error.new(:expired, "history cursor expired", details: %{reason_code: "history_expired"})
      )
    end

    floor
  end

  def advance!(workspace_id, stream, publication_id, offset) do
    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.retention_floors (workspace_id, stream, publication_id, batch_offset)
      VALUES ($1,$2,$3,$4)
      ON CONFLICT (workspace_id, stream) DO UPDATE
      SET publication_id = EXCLUDED.publication_id, batch_offset = EXCLUDED.batch_offset
      WHERE (retention_floors.publication_id, retention_floors.batch_offset) <
            (EXCLUDED.publication_id, EXCLUDED.batch_offset)
      """,
      [workspace_id, stream, publication_id, offset]
    )
  end
end
