defmodule FavnStoragePostgres.Bootstrap.Lock do
  @moduledoc false

  @spec acquire(DBConnection.conn(), String.t()) ::
          :ok | {:error, :operation_in_progress | :lock_failed}
  def acquire(connection, database) do
    key = key(database)

    case Postgrex.query(connection, "SELECT pg_catalog.pg_try_advisory_lock($1)", [key]) do
      {:ok, %{rows: [[true]]}} -> :ok
      {:ok, %{rows: [[false]]}} -> {:error, :operation_in_progress}
      {:error, _reason} -> {:error, :lock_failed}
    end
  end

  @spec release(DBConnection.conn(), String.t()) :: :ok | {:error, :lock_lost}
  def release(connection, database) do
    case Postgrex.query(
           connection,
           "SELECT pg_catalog.pg_advisory_unlock($1)",
           [key(database)]
         ) do
      {:ok, %{rows: [[true]]}} -> :ok
      {:ok, %{rows: [[false]]}} -> {:error, :lock_lost}
      {:error, _reason} -> {:error, :lock_lost}
    end
  end

  defp key(database) do
    <<unsigned::unsigned-64, _rest::binary>> =
      :crypto.hash(:sha256, "favn:postgres-bootstrap:" <> database)

    if unsigned > 9_223_372_036_854_775_807,
      do: unsigned - 18_446_744_073_709_551_616,
      else: unsigned
  end
end
