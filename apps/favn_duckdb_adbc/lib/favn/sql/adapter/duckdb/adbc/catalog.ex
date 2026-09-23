defmodule Favn.SQL.Adapter.DuckDB.ADBC.Catalog do
  @moduledoc false
  @behaviour Favn.SQL.Catalog.Backend
  alias Favn.Catalog.{Artifact, Projection}
  alias Favn.Manifest.Serializer
  alias Favn.Semantic.Artifact, as: SemanticArtifact
  alias Favn.Semantic.Catalog, as: Semantics
  alias Favn.SQL.Catalog.Request
  alias Favn.SQL.{Client, Error}

  @impl true
  def applications, do: [:adbc]

  @impl true
  def qualify(session, request, deadline) do
    case query(
           session,
           "SELECT type FROM duckdb_databases() WHERE database_name = ?",
           [request.catalog],
           deadline
         ) do
      {:ok, [%{"type" => type}]} when type in ["duckdb", "ducklake"] -> :ok
      _ -> {:error, :unsupported_catalog_publication}
    end
  end

  @impl true
  def publish(session, request, deadline) do
    Client.transaction(
      session,
      fn tx ->
        with :ok <- bootstrap(tx, request, deadline),
             {:ok, previous} <- receipt(tx, request, deadline) do
          if previous do
            {:ok, Map.put(previous, "outcome", "replayed")}
          else
            install(tx, request, deadline)
          end
        end
      end,
      deadline: deadline
    )
    |> classify_conflict()
  end

  @impl true
  def rebuild(session, request, deadline) do
    Client.transaction(
      session,
      fn tx ->
        with :ok <- verify_schema(tx, request, deadline, bookkeeping()),
             {:ok, selected} <- selections(tx, request, deadline),
             :ok <- validate_selections(selected),
             :ok <-
               execute(
                 tx,
                 "UPDATE " <>
                   table(request, "selection") <>
                   " SET revision = revision",
                 [],
                 deadline
               ),
             {:ok, rows} <- retained_documents(tx, request, deadline),
             :ok <- validate_releases(rows, selected),
             :ok <-
               each(Enum.sort(Projection.columns()), fn {name, fields} ->
                 with :ok <-
                        execute(tx, "DROP TABLE IF EXISTS " <> table(request, name), [], deadline),
                      do: create_table(tx, request, name, fields, deadline)
               end),
             :ok <-
               each(rows, fn row ->
                 with {:ok, projection} <- retained_projection(row),
                      do: install_rows(tx, request, projection, deadline)
               end) do
          {:ok,
           %{
             "outcome" => "rebuilt",
             "operation_id" => request.operation_id,
             "releases" => length(rows),
             "selections" => selected
           }}
        end
      end,
      deadline: deadline
    )
    |> classify_conflict()
  end

  defp retained_documents(session, request, deadline) do
    with {:ok, [%{"count" => count, "bytes" => bytes}]} <-
           query(
             session,
             "SELECT COUNT(*) AS count, COALESCE(SUM(octet_length(encode(document))), 0)::BIGINT AS bytes FROM " <>
               table(request, "release"),
             [],
             deadline
           ) do
      if count <= 10_000 and bytes <= 134_217_728 do
        with {:ok, rows} <-
               query(
                 session,
                 "SELECT context, version, identity, document FROM " <>
                   table(request, "release") <> " ORDER BY context, version LIMIT 10001",
                 [],
                 deadline
               ) do
          if length(rows) == count, do: {:ok, rows}, else: error(:catalog_integrity_failure)
        end
      else
        error(:catalog_rebuild_limit_exceeded)
      end
    end
  end

  defp validate_selections(selected) do
    if Enum.all?(selected, fn {_context, value} ->
         case value do
           %{"version" => nil, "revision" => 0} ->
             true

           %{"version" => version, "revision" => revision}
           when is_binary(version) and is_integer(revision) ->
             match?(
               {:ok, _},
               Request.expectation(version <> ":" <> to_string(revision))
             )

           _ ->
             false
         end
       end), do: :ok, else: error(:catalog_integrity_failure)
  end

  defp validate_releases(rows, selected) do
    keys = Enum.map(rows, &{&1["context"], &1["version"]})

    if length(rows) <= 10_000 and length(Enum.uniq(keys)) == length(keys) and
         Enum.all?(selected, fn {context, value} ->
           is_nil(value["version"]) or {context, value["version"]} in keys
         end) do
      each(rows, fn row ->
        case retained_projection(row) do
          {:ok, _} -> :ok
          error -> error
        end
      end)
    else
      error(:catalog_integrity_failure)
    end
  end

  defp retained_projection(row) do
    decoder =
      case row["context"] do
        "manifest" -> Artifact
        "semantic" -> SemanticArtifact
        _ -> nil
      end

    with false <- is_nil(decoder),
         {:ok, artifact} <- decoder.decode(row["document"]),
         {:ok, projection} <- Projection.build(artifact),
         true <-
           projection.version == row["version"] and projection.identity == row["identity"] and
             projection.document == row["document"] do
      {:ok, projection}
    else
      _ -> error(:catalog_integrity_failure)
    end
  end

  @impl true
  def observe(session, request, deadline) do
    with {:ok, tables} <-
           query(
             session,
             "SELECT table_name FROM information_schema.tables WHERE table_catalog = ? AND table_schema = ? AND table_name = 'catalog_schema'",
             [request.catalog, request.schema],
             deadline
           ) do
      if tables == [] do
        {:ok, Map.new(["manifest", "semantic"], &{&1, %{"version" => nil, "revision" => 0}})}
      else
        with :ok <- verify_schema(session, request, deadline),
             do: selections(session, request, deadline)
      end
    end
  end

  alias Favn.SQL.Adapter.DuckDB.ADBC.Rejection

  # Only native errors proven to reject this transaction are conflicts. An
  # arbitrary commit/rollback failure remains an unknown outcome.
  defp classify_conflict({:error, %Error{} = reason} = result) do
    if Rejection.rejected_conflict?(reason), do: error(:catalog_conflict), else: result
  end

  defp classify_conflict(result), do: result

  @impl true
  def reconcile(session, request, deadline) do
    with :ok <- verify_schema(session, request, deadline),
         {:ok, receipt} <- receipt(session, request, deadline) do
      if receipt,
        do: {:ok, Map.put(receipt, "outcome", "replayed")},
        else: {:error, :publication_outcome_unknown}
    end
  end

  defp install(session, request, deadline) do
    with {:ok, selected} <- selections(session, request, deadline),
         :ok <- expectations(request, selected),
         :ok <- each(request.projections, &install_projection(session, request, &1, deadline)),
         :ok <- install_macros(session, request, deadline),
         :ok <- each(request.projections, &select(session, request, &1, deadline)),
         {:ok, current} <- selections(session, request, deadline),
         {:ok, sizes} <-
           query(
             session,
             "SELECT COUNT(*) AS releases, CAST(COALESCE(SUM(octet_length(encode(document))), 0) AS BIGINT) AS bytes FROM " <>
               table(request, "release"),
             [],
             deadline
           ) do
      receipt = %{
        "outcome" => "committed",
        "operation_id" => request.operation_id,
        "schema_version" => 1,
        "compatibility" => "unknown",
        "selections" => current,
        "artifacts" => Map.new(request.projections, &{&1.kind, &1.identity}),
        "retained" => hd(sizes)
      }

      with :ok <-
             execute(
               session,
               "INSERT INTO " <> table(request, "receipt") <> " VALUES (?, ?)",
               [request.operation_id, json(receipt)],
               deadline
             ),
           do: {:ok, receipt}
    end
  end

  defp bootstrap(session, request, deadline) do
    with {:ok, existing} <-
           query(
             session,
             "SELECT table_name FROM information_schema.tables WHERE table_catalog = ? AND table_schema = ?",
             [request.catalog, request.schema],
             deadline
           ) do
      reserved = Map.keys(columns())
      names = Enum.map(existing, & &1["table_name"])

      cond do
        "catalog_schema" in names ->
          verify_schema(session, request, deadline)

        Enum.any?(names, &(&1 in reserved)) ->
          error(:catalog_schema_conflict)

        true ->
          with :ok <-
                 execute(session, "CREATE SCHEMA IF NOT EXISTS " <> scope(request), [], deadline),
               :ok <-
                 each(Enum.sort(columns()), fn {name, fields} ->
                   create_table(session, request, name, fields, deadline)
                 end),
               :ok <-
                 execute(
                   session,
                   "INSERT INTO " <> table(request, "catalog_schema") <> " VALUES (1)",
                   [],
                   deadline
                 ),
               :ok <-
                 execute(
                   session,
                   "INSERT INTO " <>
                     table(request, "selection") <>
                     " VALUES ('manifest', NULL, 0), ('semantic', NULL, 0)",
                   [],
                   deadline
                 ) do
            :ok
          end
      end
    end
  end

  defp verify_schema(session, request, deadline, expected \\ columns()) do
    with {:ok, [%{"version" => 1}]} <-
           query(
             session,
             "SELECT version FROM " <> table(request, "catalog_schema") <> " LIMIT 2",
             [],
             deadline
           ),
         {:ok, fields} <-
           query(
             session,
             "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_catalog = ? AND table_schema = ? ORDER BY table_name, ordinal_position",
             [request.catalog, request.schema],
             deadline
           ) do
      actual =
        fields
        |> Enum.filter(&Map.has_key?(expected, &1["table_name"]))
        |> Enum.group_by(& &1["table_name"], &{&1["column_name"], &1["data_type"]})

      if actual == expected, do: :ok, else: error(:catalog_schema_conflict)
    else
      _ -> error(:catalog_schema_conflict)
    end
  end

  defp columns, do: Map.merge(Projection.columns(), bookkeeping())

  defp bookkeeping do
    %{
      "catalog_schema" => [{"version", "INTEGER"}],
      "selection" => [{"context", "VARCHAR"}, {"version", "VARCHAR"}, {"revision", "BIGINT"}],
      "release" => [
        {"context", "VARCHAR"},
        {"version", "VARCHAR"},
        {"identity", "VARCHAR"},
        {"document", "VARCHAR"}
      ],
      "receipt" => [{"operation_id", "VARCHAR"}, {"document", "VARCHAR"}]
    }
  end

  defp selections(session, request, deadline) do
    with {:ok, rows} <-
           query(
             session,
             "SELECT context, version, revision FROM " <>
               table(request, "selection") <> " LIMIT 3",
             [],
             deadline
           ) do
      if Enum.sort(Enum.map(rows, & &1["context"])) == ["manifest", "semantic"] do
        {:ok, Map.new(rows, &{&1["context"], Map.take(&1, ["version", "revision"])})}
      else
        error(:catalog_integrity_failure)
      end
    end
  end

  defp expectations(request, selected) do
    if Enum.all?(request.expectations, fn {kind, expected} -> selected[kind] == expected end),
      do: :ok,
      else:
        {:error,
         %Error{
           type: :catalog_conflict,
           message: "Catalog selection changed",
           details: %{expected: request.expectations, observed: selected}
         }}
  end

  defp select(session, request, projection, deadline) do
    expected = request.expectations[projection.kind]

    sql =
      "UPDATE " <>
        table(request, "selection") <>
        " SET version = ?, revision = revision + 1 WHERE context = ? AND version IS NOT DISTINCT FROM ? AND revision = ?"

    case Client.execute(session, sql,
           params: [
             projection.version,
             projection.kind,
             expected["version"],
             expected["revision"]
           ],
           deadline: deadline
         ) do
      {:ok, %{rows_affected: 1}} -> :ok
      {:ok, _} -> error(:catalog_conflict)
      error -> error
    end
  end

  defp install_projection(session, request, projection, deadline) do
    with {:ok, rows} <-
           query(
             session,
             "SELECT identity, document FROM " <>
               table(request, "release") <>
               " WHERE context = ? AND version = ? LIMIT 2",
             [projection.kind, projection.version],
             deadline
           ) do
      case rows do
        [] ->
          with :ok <-
                 install_rows(session, request, projection, deadline) do
            execute(
              session,
              "INSERT INTO " <> table(request, "release") <> " VALUES (?, ?, ?, ?)",
              [projection.kind, projection.version, projection.identity, projection.document],
              deadline
            )
          end

        [%{"identity" => identity, "document" => document}]
        when identity == projection.identity and document == projection.document ->
          :ok

        _ ->
          error(:catalog_integrity_failure)
      end
    end
  end

  defp create_table(session, request, name, fields, deadline) do
    definition = Enum.map_join(fields, ", ", fn {key, type} -> quote_id(key) <> " " <> type end)

    execute(
      session,
      "CREATE TABLE " <> table(request, name) <> " (" <> definition <> ")",
      [],
      deadline
    )
  end

  defp install_rows(session, request, projection, deadline) do
    each(Enum.sort(projection.tables), fn {name, rows} ->
      insert_batches(
        session,
        table(request, name),
        target_rows(name, rows, request.catalog),
        deadline
      )
    end)
  end

  defp target_rows("metric", rows, catalog), do: Enum.map(rows, &List.replace_at(&1, 5, catalog))
  defp target_rows(_, rows, _), do: rows

  defp insert_batches(session, table, rows, deadline) do
    rows
    |> batches()
    |> each(fn batch ->
      placeholders = "(" <> Enum.map_join(hd(batch), ",", fn _ -> "?" end) <> ")"

      execute(
        session,
        "INSERT INTO " <>
          table <> " VALUES " <> Enum.map_join(batch, ",", fn _ -> placeholders end),
        List.flatten(batch),
        deadline
      )
    end)
  end

  defp batches(rows) do
    Enum.chunk_while(
      rows,
      {[], 0, 0},
      fn row, {batch, count, size} ->
        bytes = byte_size(json(row))

        if count == 500 or size + bytes > 1_048_576 do
          {:cont, Enum.reverse(batch), {[row], 1, bytes}}
        else
          {:cont, {[row | batch], count + 1, size + bytes}}
        end
      end,
      fn
        {[], _, _} -> {:cont, []}
        {batch, _, _} -> {:cont, Enum.reverse(batch), {[], 0, 0}}
      end
    )
  end

  defp install_macros(_, %{semantic: nil}, _), do: :ok

  defp install_macros(session, request, deadline) do
    namespace = Semantics.namespace(request.semantic)
    macro_scope = quote_id(request.catalog) <> "." <> quote_id(namespace)
    marker = macro_scope <> ".\"catalog_artifact\""

    with {:ok, schemas} <-
           query(
             session,
             "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = ? AND schema_name = ?",
             [request.catalog, namespace],
             deadline
           ),
         {:ok, document} <- Favn.Semantic.Artifact.encode(request.semantic) do
      case schemas do
        [] ->
          with :ok <- execute(session, "CREATE SCHEMA " <> macro_scope, [], deadline),
               :ok <-
                 each(
                   Semantics.macros(request.semantic, request.catalog),
                   &execute(session, &1, [], deadline)
                 ),
               {:ok, definitions} <-
                 macro_definitions(session, request.catalog, namespace, deadline),
               :ok <-
                 execute(
                   session,
                   "CREATE TABLE " <> marker <> " (document VARCHAR, definitions VARCHAR)",
                   [],
                   deadline
                 ) do
            execute(
              session,
              "INSERT INTO " <> marker <> " VALUES (?, ?)",
              [document, json(definitions)],
              deadline
            )
          end

        [_] ->
          with {:ok, [%{"document" => ^document, "definitions" => stored}]} <-
                 query(
                   session,
                   "SELECT document, definitions FROM " <> marker <> " LIMIT 2",
                   [],
                   deadline
                 ),
               {:ok, definitions} <-
                 macro_definitions(session, request.catalog, namespace, deadline),
               true <- stored == json(definitions) do
            :ok
          else
            _ -> error(:catalog_integrity_failure)
          end

        _ ->
          error(:catalog_integrity_failure)
      end
    end
  end

  defp macro_definitions(session, catalog, namespace, deadline),
    do:
      query(
        session,
        "SELECT function_name, parameters, macro_definition FROM duckdb_functions() WHERE database_name = ? AND schema_name = ? AND function_type = 'macro' ORDER BY function_name",
        [catalog, namespace],
        deadline
      )

  defp receipt(session, request, deadline) do
    with {:ok, rows} <-
           query(
             session,
             "SELECT document FROM " <>
               table(request, "receipt") <> " WHERE operation_id = ? LIMIT 2",
             [request.operation_id],
             deadline
           ) do
      case rows do
        [] ->
          {:ok, nil}

        [%{"document" => document}] ->
          case Jason.decode(document) do
            {:ok, %{"operation_id" => id, "schema_version" => 1} = result}
            when id == request.operation_id ->
              {:ok, result}

            _ ->
              error(:catalog_integrity_failure)
          end

        _ ->
          error(:catalog_integrity_failure)
      end
    end
  end

  defp each(values, fun),
    do:
      Enum.reduce_while(values, :ok, fn value, :ok ->
        case fun.(value) do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
      end)

  defp execute(session, sql, params, deadline) do
    case Client.execute(session, sql, params: params, deadline: deadline) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  defp query(session, sql, params, deadline) do
    case Client.query(session, sql, params: params, deadline: deadline) do
      {:ok, result} -> {:ok, result.rows}
      error -> error
    end
  end

  defp error(type),
    do: {:error, %Error{type: type, message: "Catalog operation rejected", retryable?: false}}

  defp table(request, name), do: scope(request) <> "." <> quote_id(name)
  defp scope(request), do: quote_id(request.catalog) <> "." <> quote_id(request.schema)
  defp quote_id(value), do: "\"" <> String.replace(value, "\"", "\"\"") <> "\""
  defp json(value), do: Serializer.encode_canonical!(value)
end
