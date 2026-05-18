defmodule Favn.Dev.DataInspection do
  @moduledoc """
  Lightweight local SQL data inspection helpers.

  The helpers back `mix favn.inspect relation`, `mix favn.inspect partitions`,
  and `mix favn.query`. They resolve configured local Favn connections, parse
  simple relation strings into `Favn.RelationRef`, and execute read-only
  inspection through `Favn.SQL.Client`.
  """

  alias Favn.Connection.Loader
  alias Favn.RelationRef
  alias Favn.SQL.Client
  alias Favn.SQL.Result

  @default_limit 50
  @read_only_keywords ~w(select with show describe explain pragma values)
  @mutating_keywords ~w(
    alter analyze attach begin call checkpoint comment commit copy create delete detach drop execute
    grant import insert install load merge replace reset revoke rollback set truncate update use vacuum
  )

  @type relation_opts :: [connection: atom() | String.t()]
  @type query_opts :: [
          connection: atom() | String.t(),
          allow_write: boolean(),
          limit: pos_integer()
        ]

  @doc """
  Inspects a relation and returns relation metadata, columns, row count, and a
  small sample when supported by the configured SQL adapter.
  """
  @spec inspect_relation(String.t(), relation_opts()) :: {:ok, map()} | {:error, term()}
  def inspect_relation(relation, opts \\ []) when is_binary(relation) and is_list(opts) do
    client = Keyword.get(opts, :client, Client)

    with {:ok, relation_ref} <- parse_relation(relation, opts),
         {:ok, session} <- client.connect(relation_ref.connection, connect_opts(relation_ref)) do
      result =
        with {:ok, relation_info} <- client.relation(session, relation_ref),
             {:ok, columns} <- client.columns(session, relation_ref) do
          row_count = optional_introspection(fn -> client.row_count(session, relation_ref) end)

          sample =
            optional_introspection(fn ->
              client.sample(session, relation_ref, limit: @default_limit)
            end)

          {:ok,
           %{
             relation: relation_ref,
             metadata: relation_info,
             columns: columns,
             row_count: row_count,
             sample: sample
           }}
        end

      client.disconnect(session)
      result
    end
  end

  @doc """
  Returns partition-like metadata for a relation when the adapter exposes it.
  """
  @spec inspect_partitions(String.t(), relation_opts()) :: {:ok, map()} | {:error, term()}
  def inspect_partitions(relation, opts \\ []) when is_binary(relation) and is_list(opts) do
    client = Keyword.get(opts, :client, Client)

    with {:ok, relation_ref} <- parse_relation(relation, opts),
         {:ok, session} <- client.connect(relation_ref.connection, connect_opts(relation_ref)) do
      result =
        case client.table_metadata(session, relation_ref) do
          {:ok, metadata} ->
            {:ok, %{relation: relation_ref, metadata: metadata, partitions: partitions(metadata)}}

          {:error, reason} ->
            {:error, reason}
        end

      client.disconnect(session)
      result
    end
  end

  @doc """
  Runs a SQL query against a local connection.

  Queries are read-only by default. Pass `allow_write: true` only for deliberate
  local mutation.
  """
  @spec query(String.t(), query_opts()) :: {:ok, map()} | {:error, term()}
  def query(sql, opts \\ []) when is_binary(sql) and is_list(opts) do
    client = Keyword.get(opts, :client, Client)
    limit = Keyword.get(opts, :limit, @default_limit)

    with :ok <- validate_limit(limit),
         :ok <- validate_read_only(sql, opts),
         {:ok, connection} <- resolve_connection(Keyword.get(opts, :connection)),
         {:ok, session} <- client.connect(connection) do
      result =
        case client.query(session, sql, []) do
          {:ok, %Result{} = sql_result} ->
            {:ok,
             %{
               connection: connection,
               result: sql_result,
               displayed_rows: Enum.take(sql_result.rows || [], limit),
               display_limit: limit
             }}

          {:ok, other} ->
            {:ok,
             %{connection: connection, result: other, displayed_rows: [], display_limit: limit}}

          {:error, reason} ->
            {:error, reason}
        end

      client.disconnect(session)
      result
    end
  end

  @doc """
  Parses a relation string into a `Favn.RelationRef` and resolves its connection.

  Accepted relation forms are `name`, `schema.name`, and
  `catalog.schema.name`.
  """
  @spec parse_relation(String.t(), relation_opts()) :: {:ok, RelationRef.t()} | {:error, term()}
  def parse_relation(relation, opts \\ []) when is_binary(relation) and is_list(opts) do
    with {:ok, attrs} <- relation_attrs(relation),
         {:ok, connection} <- resolve_connection(Keyword.get(opts, :connection)) do
      {:ok, RelationRef.new!(Map.put(attrs, :connection, connection))}
    end
  rescue
    error in ArgumentError -> {:error, Exception.message(error)}
  end

  @doc """
  Validates that SQL is read-only unless `allow_write: true` is set.
  """
  @spec validate_read_only(String.t(), query_opts()) :: :ok | {:error, term()}
  def validate_read_only(sql, opts \\ []) when is_binary(sql) and is_list(opts) do
    if Keyword.get(opts, :allow_write, false) do
      :ok
    else
      validate_read_only_statement(sql)
    end
  end

  defp relation_attrs(relation) do
    parts = relation |> String.trim() |> String.split(".", trim: false)

    cond do
      Enum.any?(parts, &(&1 == "")) ->
        {:error, "relation must be name, schema.name, or catalog.schema.name"}

      length(parts) == 1 ->
        [name] = parts
        {:ok, %{name: name}}

      length(parts) == 2 ->
        [schema, name] = parts
        {:ok, %{schema: schema, name: name}}

      length(parts) == 3 ->
        [catalog, schema, name] = parts
        {:ok, %{catalog: catalog, schema: schema, name: name}}

      true ->
        {:error, "relation must be name, schema.name, or catalog.schema.name"}
    end
  end

  defp resolve_connection(nil) do
    with {:ok, connections} <- Loader.load() do
      case Map.keys(connections) do
        [connection] ->
          {:ok, connection}

        [] ->
          {:error, "no Favn SQL connections are configured; pass --connection"}

        connections ->
          {:error,
           "multiple Favn SQL connections configured (#{format_connections(connections)}); pass --connection"}
      end
    end
  end

  defp resolve_connection(connection) when is_atom(connection) do
    with {:ok, connections} <- Loader.load() do
      validate_connection(connection, connections)
    end
  end

  defp resolve_connection(connection) when is_binary(connection) do
    with {:ok, connections} <- Loader.load() do
      connections
      |> Map.keys()
      |> Enum.find(&(Atom.to_string(&1) == connection))
      |> case do
        nil ->
          {:error,
           "connection #{inspect(connection)} is not configured; available: #{format_connections(Map.keys(connections))}"}

        connection ->
          {:ok, connection}
      end
    end
  end

  defp validate_connection(connection, connections) do
    if Map.has_key?(connections, connection) do
      {:ok, connection}
    else
      {:error,
       "connection #{inspect(connection)} is not configured; available: #{format_connections(Map.keys(connections))}"}
    end
  end

  defp format_connections(connections) do
    connections |> Enum.sort() |> Enum.map_join(", ", &inspect/1)
  end

  defp optional_introspection(fun) do
    case fun.() do
      {:ok, value} -> value
      {:error, reason} -> {:error, reason}
    end
  end

  defp partitions(metadata) when is_map(metadata) do
    Map.get(metadata, :partitions) || Map.get(metadata, "partitions") || []
  end

  defp validate_limit(limit) when is_integer(limit) and limit > 0, do: :ok

  defp validate_limit(limit),
    do: {:error, "limit must be a positive integer, got: #{inspect(limit)}"}

  defp connect_opts(%RelationRef{catalog: catalog}) when is_binary(catalog) and catalog != "" do
    [required_catalogs: [catalog]]
  end

  defp connect_opts(%RelationRef{}), do: []

  defp validate_read_only_statement(sql) do
    sanitized = sanitize_sql(sql)

    statements =
      sanitized |> String.split(";") |> Enum.map(&String.trim/1) |> Enum.reject(&(&1 == ""))

    cond do
      statements == [] ->
        {:error, "query must not be empty"}

      length(statements) > 1 ->
        {:error, "query must contain a single statement unless --allow-write is passed"}

      mutating_keyword = find_mutating_keyword(hd(statements)) ->
        {:error,
         "query appears to mutate data with #{String.upcase(mutating_keyword)}; pass --allow-write to run it locally"}

      first_keyword(hd(statements)) in @read_only_keywords ->
        :ok

      true ->
        {:error,
         "query must start with a read-only statement (SELECT, WITH, SHOW, DESCRIBE, EXPLAIN, PRAGMA, VALUES) unless --allow-write is passed"}
    end
  end

  defp find_mutating_keyword(statement) do
    Enum.find(@mutating_keywords, fn keyword ->
      Regex.match?(~r/(^|\W)#{keyword}(\W|$)/i, statement)
    end)
  end

  defp first_keyword(statement) do
    case Regex.run(~r/^\s*([a-z_]+)/i, statement) do
      [_, keyword] -> String.downcase(keyword)
      _other -> nil
    end
  end

  defp sanitize_sql(sql) do
    sql
    |> String.replace(~r/'(?:''|[^'])*'/, "''")
    |> String.replace(~r/"(?:""|[^"])*"/, ~s(""))
    |> String.replace(~r/--[^\n\r]*/, " ")
    |> String.replace(~r{/\*.*?\*/}s, " ")
  end
end
