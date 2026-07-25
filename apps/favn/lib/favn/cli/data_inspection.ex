defmodule Favn.CLI.DataInspection do
  @moduledoc """
  Structured SQL data inspection helpers for local Favn projects.

  The helpers back `mix favn.inspect relation` and `mix favn.inspect partitions`.
  They resolve configured local Favn connections, parse
  simple relation strings into `Favn.RelationRef`, and execute inspection through
  `Favn.SQL.Client`.

  The public Mix tasks load `.env` before evaluating consumer runtime config,
  without starting the consumer application. This module starts
  `:favn_sql_runtime` before opening SQL client sessions so `mix favn.inspect`
  has a supervised `Favn.SQL.SessionPool`.
  """

  alias Favn.Connection.Loader
  alias Favn.RelationRef
  alias Favn.SQL.Client

  @default_limit 50

  @type relation_opts :: [connection: atom() | String.t()]

  @doc """
  Inspects a relation and returns relation metadata, columns, row count, and a
  small sample when supported by the configured SQL adapter.
  """
  @spec inspect_relation(String.t(), relation_opts()) :: {:ok, map()} | {:error, term()}
  def inspect_relation(relation, opts \\ []) when is_binary(relation) and is_list(opts) do
    client = Keyword.get(opts, :client, Client)

    with {:ok, relation_ref} <- parse_relation(relation, opts),
         :ok <- ensure_sql_runtime_started(),
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
         :ok <- ensure_sql_runtime_started(),
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

  defp ensure_sql_runtime_started do
    case Application.ensure_all_started(:favn_sql_runtime) do
      {:ok, _apps} -> :ok
      {:error, reason} -> {:error, reason}
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

  defp connect_opts(%RelationRef{catalog: catalog}) when is_binary(catalog) and catalog != "" do
    [required_catalogs: [catalog]]
  end

  defp connect_opts(%RelationRef{}), do: []
end
