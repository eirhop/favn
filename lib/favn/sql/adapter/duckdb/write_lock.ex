defmodule Favn.SQL.Adapter.DuckDB.WriteLock do
  @moduledoc false

  @spec with_lock(atom() | nil, (-> result)) :: result when result: var
  def with_lock(connection, fun) when is_function(fun, 0) do
    lock_key = {:favn_duckdb_write_lock, connection || :anonymous}

    :global.trans(lock_key, fun, [node()])
  end
end
