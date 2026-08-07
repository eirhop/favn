defmodule FavnStoragePostgres.Bootstrap.Lock do
  @moduledoc false

  @spec acquire(pid(), String.t()) :: :ok | {:error, :operation_in_progress | :lock_failed}
  def acquire(connection, database) do
    key = key(database)

    case Postgrex.query(connection, "SELECT pg_catalog.pg_try_advisory_lock($1)", [key]) do
      {:ok, %{rows: [[true]]}} -> :ok
      {:ok, %{rows: [[false]]}} -> {:error, :operation_in_progress}
      {:error, _reason} -> {:error, :lock_failed}
    end
  end

  @spec release(pid(), String.t()) :: :ok
  def release(connection, database) do
    _result =
      Postgrex.query(connection, "SELECT pg_catalog.pg_advisory_unlock($1)", [key(database)])

    :ok
  end

  defp key(database) do
    <<unsigned::unsigned-64, _rest::binary>> =
      :crypto.hash(:sha256, "favn:postgres-bootstrap:" <> database)

    if unsigned > 9_223_372_036_854_775_807,
      do: unsigned - 18_446_744_073_709_551_616,
      else: unsigned
  end
end
