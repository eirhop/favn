defmodule FavnReferenceWorkload.Client.DuckDBJSONLoader do
  @moduledoc """
  Minimal DuckDB client for loading JSON payload rows into owned relations.

  Raw assets pass only the relation metadata they need from `ctx`, rather than
  the whole runtime context.

  Best-practice point shown here:

  - the asset extracts `ctx.asset.relation` and passes only that value here
  - this client needs relation name, schema, and connection, but it does not
    need pipeline, params, attempts, or other runtime details
  - using `%Favn.RelationRef{}` as the client boundary keeps responsibilities
    narrow and explicit
  """

  alias Favn.RelationRef
  alias Favn.SQLClient

  @payload_dir ".favn/data/reference_workload_api"

  @spec replace_relation_from_rows(RelationRef.t(), atom(), [map()], iodata()) ::
          :ok | {:error, term()}
  def replace_relation_from_rows(%RelationRef{} = relation, dataset, rows, select_sql)
      when is_atom(dataset) and is_list(rows) do
    with {:ok, payload_path} <- write_payload(dataset, rows),
         {:ok, session} <- SQLClient.connect(relation.connection) do
      result =
        with {:ok, _} <- SQLClient.query(session, create_schema_sql(relation), []),
             {:ok, _} <-
               SQLClient.query(
                 session,
                 replace_table_sql(relation, payload_path, select_sql),
                 []
               ) do
          :ok
        else
          {:error, reason} -> {:error, reason}
        end

      SQLClient.disconnect(session)
      result
    end
  end

  @spec replace_relation_from_sql(RelationRef.t(), iodata()) :: :ok | {:error, term()}
  def replace_relation_from_sql(%RelationRef{} = relation, select_sql) do
    with {:ok, session} <- SQLClient.connect(relation.connection) do
      result =
        with {:ok, _} <- SQLClient.query(session, create_schema_sql(relation), []),
             {:ok, _} <- SQLClient.query(session, replace_table_sql(relation, select_sql), []) do
          :ok
        else
          {:error, reason} -> {:error, reason}
        end

      SQLClient.disconnect(session)
      result
    end
  end

  defp write_payload(dataset, rows) do
    with :ok <- File.mkdir_p(@payload_dir),
         {:ok, payload} <- Jason.encode(rows),
         path <- Path.expand(Path.join(@payload_dir, "#{dataset}.json")),
         :ok <- File.write(path, payload) do
      {:ok, path}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp replace_table_sql(%RelationRef{} = relation, payload_path, select_sql) do
    source_sql = read_json_sql(payload_path)

    relation
    |> replace_table_sql(IO.iodata_to_binary(select_sql))
    |> String.replace("__RAW_JSON_SOURCE__", source_sql)
  end

  defp replace_table_sql(%RelationRef{} = relation, select_sql) do
    schema = quote_ident(relation.schema)
    name = quote_ident(relation.name)

    """
    create or replace table #{schema}.#{name} as
    #{IO.iodata_to_binary(select_sql)}
    """
  end

  defp create_schema_sql(%RelationRef{} = relation),
    do: ["create schema if not exists ", quote_ident(relation.schema)]

  defp read_json_sql(payload_path) do
    escaped = String.replace(payload_path, "'", "''")
    "read_json('#{escaped}', auto_detect = true)"
  end

  defp quote_ident(identifier),
    do: ["\"", String.replace(to_string(identifier), "\"", "\"\""), "\""]
end
