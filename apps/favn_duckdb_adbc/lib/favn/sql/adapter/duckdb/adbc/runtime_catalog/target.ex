defmodule Favn.SQL.Adapter.DuckDB.ADBC.RuntimeCatalog.Target do
  @moduledoc false
  alias Favn.SQL.{Client, Error}

  def resolve(session, %{catalog: c, schema: s} = ref, opts) when is_binary(c) and is_binary(s) do
    case query(
           session,
           "SELECT database_name FROM duckdb_databases() WHERE #{fold("database_name")}=#{fold("?")} LIMIT 2",
           [c],
           opts
         ) do
      {:ok, [%{"database_name" => actual}]} -> {:ok, %{ref | catalog: actual}}
      {:ok, _} -> error()
      error -> error
    end
  end

  # Two-part SQL identifiers can mean either schema.table or catalog.table.
  # Require both parts when a catalog was explicitly authored.
  def resolve(_session, %{catalog: c, schema: nil}, _opts) when is_binary(c), do: error()

  def resolve(session, ref, opts) do
    with :ok <- unambiguous_schema(session, ref, opts),
         {:ok, rows} <-
           query(
             session,
             """
             SELECT table_catalog AS catalog, table_schema AS schema, table_name AS name
             FROM information_schema.tables
             WHERE #{fold("table_name")}=#{fold("?")} AND (? IS NULL OR #{fold("table_schema")}=#{fold("?")})
               AND (? IS NOT NULL OR in_search_path(table_catalog, table_schema))
             LIMIT 2
             """,
             [ref.name, ref.schema, ref.schema, ref.schema],
             opts
           ) do
      case rows do
        [] -> destination(session, ref, opts)
        [row] -> bind_existing(session, ref, row, opts)
        _ -> error()
      end
    end
  end

  defp unambiguous_schema(_session, %{schema: nil}, _opts), do: :ok

  defp unambiguous_schema(session, ref, opts) do
    case query(
           session,
           "SELECT database_name FROM duckdb_databases() WHERE #{fold("database_name")}=#{fold("?")}",
           [ref.schema],
           opts
         ) do
      {:ok, []} -> :ok
      {:ok, _} -> error()
      error -> error
    end
  end

  defp bind_existing(session, ref, row, opts) do
    # A unique metadata candidate is accepted only if the original reference binds.
    # Do not emulate DuckDB's parser or silently choose an ambiguous catalog.
    sql = "SELECT 1 FROM " <> qualified(ref) <> " LIMIT 0"

    with {:ok, _} <- query(session, sql, [], opts),
         do: {:ok, %{ref | catalog: row["catalog"], schema: row["schema"], name: row["name"]}}
  end

  defp destination(session, ref, opts) do
    with {:ok, [%{"catalog" => c, "schema" => s}]} <-
           query(
             session,
             "SELECT current_database() AS catalog, current_schema() AS schema",
             [],
             opts
           ) do
      if is_nil(ref.schema) do
        {:ok, %{ref | catalog: c, schema: s}}
      else
        with {:ok, schemas} <-
               query(
                 session,
                 """
                 SELECT catalog_name AS catalog, schema_name AS schema
                 FROM information_schema.schemata WHERE #{fold("schema_name")}=#{fold("?")}
                 ORDER BY (catalog_name=?) DESC LIMIT 2
                 """,
                 [ref.schema, c],
                 opts
               ) do
          case schemas do
            [%{"catalog" => ^c} = row | _] -> {:ok, %{ref | catalog: c, schema: row["schema"]}}
            [row] -> {:ok, %{ref | catalog: row["catalog"], schema: row["schema"]}}
            _ -> error()
          end
        end
      end
    end
  end

  # DuckDB identifiers fold ASCII letters only; Unicode lowercasing could
  # redirect a reference that the native binder would reject.
  defp fold(expression),
    do: "translate(#{expression}, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')"

  defp query({adapter, conn}, sql, params, opts) do
    case adapter.query(conn, sql, Keyword.put(opts, :params, params)) do
      {:ok, result} -> {:ok, result.rows}
      error -> error
    end
  end

  defp query(session, sql, params, opts) do
    case Client.query(session, sql, Keyword.put(opts, :params, params)) do
      {:ok, result} -> {:ok, result.rows}
      error -> error
    end
  end

  defp qualified(ref),
    do:
      [ref.catalog, ref.schema, ref.name]
      |> Enum.reject(&is_nil/1)
      |> Enum.map_join(".", &("\"" <> String.replace(&1, "\"", "\"\"") <> "\""))

  defp error,
    do:
      {:error,
       %Error{
         type: :runtime_catalog_ambiguous_target,
         message:
           "Runtime publication requires an unambiguous native target; qualify its catalog and schema",
         retryable?: false
       }}
end
