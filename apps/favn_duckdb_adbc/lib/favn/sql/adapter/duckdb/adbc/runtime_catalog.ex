defmodule Favn.SQL.Adapter.DuckDB.ADBC.RuntimeCatalog do
  @moduledoc false
  @behaviour Favn.SQL.RuntimeCatalog
  alias Favn.Manifest.Serializer
  alias Favn.RuntimeCatalog.Publication
  alias Favn.Semantic.Snapshot
  alias Favn.SQL.{Client, Error}

  @schema "favn_runtime"
  @impl true
  def resolve(session, relation, opts), do: __MODULE__.Target.resolve(session, relation, opts)

  @impl true
  def prepare(session, publication, relation, opts) do
    with {:ok, relation} <- resolve(session, relation, opts),
         false <- String.downcase(relation.schema || "") == @schema,
         {:ok, catalog} <- catalog(session, relation, opts),
         :ok <- bootstrap(session, catalog, opts),
         :ok <- duplicate(session, catalog, publication, relation, opts),
         :ok <- lock_target(session, catalog, publication, opts),
         :ok <- coverage_scope(session, catalog, publication, opts) do
      {:ok, %{catalog: catalog, publication: publication, relation: relation}}
    else
      true -> error(:runtime_catalog_reserved_schema)
      {:error, _} = error -> error
    end
  end

  @impl true
  def record(session, %{catalog: c, publication: p, relation: relation}, output, opts) do
    now = DateTime.utc_now()
    contract = output.contract
    contract_id = Snapshot.digest("rc_", contract)
    mutation = output.mutation
    coverage_support = coverage_support(p, mutation)
    checks = Enum.map(output.check_results, &check/1)

    with {:ok, {expiry, until, inclusive}} <- Publication.expiry(p, now),
         :ok <- put_contract(session, c, contract_id, contract, opts),
         :ok <-
           insert(
             session,
             c,
             "publication",
             [
               [
                 p.publication_id,
                 request_hash(p, relation),
                 p.workspace_id,
                 p.target_id,
                 p.generation_id,
                 p.asset_ref,
                 p.run_id,
                 p.step_id,
                 p.attempt,
                 p.manifest_id,
                 p.manifest_hash,
                 p.runner_release,
                 c,
                 relation.schema,
                 relation.name,
                 iso(now),
                 p.freshness_key,
                 json(p.policy),
                 expiry,
                 iso(until),
                 inclusive,
                 contract_id,
                 mutation,
                 coverage_support,
                 quality(checks),
                 json(checks),
                 json(p.coverage)
               ]
             ],
             opts
           ),
         :ok <- replace_state(session, c, p, mutation, opts),
         :ok <- update_windows(session, c, p, mutation, coverage_support, opts) do
      {:ok,
       %{
         "publication_id" => p.publication_id,
         "published_at" => iso(now),
         "fresh_until" => iso(until)
       }}
    end
  end

  def activate(_session, %{workspace_id: nil}, _opts), do: :ok

  def activate(session, request, opts) do
    with {:ok, relation} <- resolve(session, request.stable_relation, opts),
         {:ok, c} <- catalog(session, relation, opts),
         :ok <- verify(session, c, opts),
         {:ok, [%{"revision" => revision, "generation_id" => previous}]} <-
           query(
             session,
             "SELECT revision, generation_id FROM " <>
               table(c, "asset_state") <>
               " WHERE workspace_id = ? AND target_id = ? AND scope_key = '__target__' LIMIT 2",
             [request.workspace_id, request.logical_target_id],
             opts
           ),
         true <- is_nil(previous) or previous == request.expected_active_generation_id,
         {:ok, [%{"count" => count}]} <-
           query(
             session,
             "SELECT count(*) AS count FROM " <>
               table(c, "asset_state") <>
               " WHERE workspace_id = ? AND target_id = ? AND generation_id = ? AND scope_key <> '__target__'",
             [request.workspace_id, request.logical_target_id, request.candidate_generation_id],
             opts
           ),
         true <- count > 0 do
      cas(
        session,
        "UPDATE " <>
          table(c, "asset_state") <>
          " SET generation_id = ?, revision = revision + 1 WHERE workspace_id = ? AND target_id = ? AND scope_key = '__target__' AND revision = ?",
        [
          request.candidate_generation_id,
          request.workspace_id,
          request.logical_target_id,
          revision
        ],
        opts
      )
    else
      {:error, _} = e -> e
      _ -> error(:runtime_catalog_generation_conflict)
    end
  end

  defp catalog(session, relation, opts) do
    with {:ok, [%{"catalog" => c}]} <-
           query(
             session,
             "SELECT COALESCE(?, current_database()) AS catalog",
             [relation.catalog],
             opts
           ),
         {:ok, [%{"type" => type}]} <-
           query(
             session,
             "SELECT type FROM duckdb_databases() WHERE database_name = ?",
             [c],
             opts
           ),
         true <- type in ["duckdb", "ducklake"] do
      {:ok, c}
    else
      _ -> error(:unsupported_runtime_catalog)
    end
  end

  defp bootstrap(session, c, opts) do
    with {:ok, existing} <-
           query(
             session,
             "SELECT table_name FROM information_schema.tables WHERE table_catalog = ? AND table_schema = ?",
             [c, @schema],
             opts
           ) do
      cond do
        existing == [] ->
          with :ok <- execute(session, "CREATE SCHEMA IF NOT EXISTS " <> scope(c), [], opts),
               :ok <-
                 each(columns(), fn {name, fields} ->
                   execute(
                     session,
                     "CREATE TABLE " <>
                       table(c, name) <>
                       " (" <>
                       Enum.map_join(fields, ", ", fn {field, type} ->
                         id(field) <> " " <> type
                       end) <> ")",
                     [],
                     opts
                   )
                 end),
               :ok <-
                 execute(
                   session,
                   "INSERT INTO " <> table(c, "runtime_schema") <> " VALUES (1, 0)",
                   [],
                   opts
                 ),
               :ok <-
                 each(views(c), fn {name, sql} ->
                   execute(session, "CREATE VIEW " <> table(c, name) <> " AS " <> sql, [], opts)
                 end),
               do: :ok

        Enum.any?(existing, &(&1["table_name"] == "runtime_schema")) ->
          verify(session, c, opts)

        true ->
          error(:runtime_catalog_schema_conflict)
      end
    end
  end

  defp verify(session, c, opts) do
    with {:ok, [%{"version" => 1}]} <-
           query(
             session,
             "SELECT version FROM " <> table(c, "runtime_schema") <> " LIMIT 2",
             [],
             opts
           ),
         {:ok, fields} <-
           query(
             session,
             "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_catalog = ? AND table_schema = ? ORDER BY table_name, ordinal_position",
             [c, @schema],
             opts
           ) do
      actual =
        fields
        |> Enum.filter(&Map.has_key?(columns(), &1["table_name"]))
        |> Enum.group_by(& &1["table_name"], &{&1["column_name"], &1["data_type"]})

      view_columns = %{
        "freshness" => Enum.map(columns()["publication"], &elem(&1, 0)) ++ ["time_freshness"],
        "check_result" => ["publication_id", "asset_ref", "check_name", "phase", "outcome"],
        "coverage" => Enum.map(columns()["window_state"], &elem(&1, 0))
      }

      names = fields |> Enum.group_by(& &1["table_name"], & &1["column_name"])
      valid_views = Enum.all?(view_columns, fn {name, expected} -> names[name] == expected end)

      valid_names =
        Enum.sort(Map.keys(names)) == Enum.sort(Map.keys(columns()) ++ Map.keys(view_columns))

      if actual == columns() and valid_views and valid_names,
        do: :ok,
        else: error(:runtime_catalog_schema_conflict)
    else
      _ -> error(:runtime_catalog_schema_conflict)
    end
  end

  defp duplicate(session, c, p, relation, opts) do
    expected_hash = request_hash(p, relation)

    with {:ok, rows} <-
           query(
             session,
             "SELECT request_hash FROM " <>
               table(c, "publication") <> " WHERE publication_id = ? LIMIT 2",
             [p.publication_id],
             opts
           ) do
      case rows do
        [] -> :ok
        [%{"request_hash" => ^expected_hash}] -> error(:already_published)
        _ -> error(:runtime_catalog_integrity_failure)
      end
    end
  end

  defp lock_target(session, c, p, opts) do
    where = " WHERE workspace_id = ? AND target_id = ? AND scope_key = '__target__'"

    with {:ok, rows} <-
           query(
             session,
             "SELECT generation_id, revision FROM " <>
               table(c, "asset_state") <> where <> " LIMIT 2",
             [p.workspace_id, p.target_id],
             opts
           ) do
      case rows do
        [] ->
          with :ok <- introduce(session, c, opts),
               do:
                 insert(
                   session,
                   c,
                   "asset_state",
                   [
                     [
                       p.workspace_id,
                       p.target_id,
                       if(p.candidate, do: nil, else: p.generation_id),
                       "__target__",
                       nil,
                       0
                     ]
                   ],
                   opts
                 )

        [%{"generation_id" => generation, "revision" => revision}]
        when p.candidate or is_nil(generation) or generation == p.generation_id ->
          cas(
            session,
            "UPDATE " <>
              table(c, "asset_state") <>
              " SET generation_id = ?, revision = revision + 1" <> where <> " AND revision = ?",
            [
              if(p.candidate, do: generation, else: p.generation_id),
              p.workspace_id,
              p.target_id,
              revision
            ],
            opts
          )

        _ ->
          error(:runtime_catalog_generation_conflict)
      end
    end
  end

  # First introduction of an immutable contract/target needs conflict authority
  # on DuckLake, which has no unique constraints. Existing targets use their row.
  defp introduce(session, c, opts) do
    with {:ok, [%{"revision" => revision}]} <-
           query(
             session,
             "SELECT revision FROM " <> table(c, "runtime_schema") <> " LIMIT 2",
             [],
             opts
           ),
         do:
           cas(
             session,
             "UPDATE " <>
               table(c, "runtime_schema") <> " SET revision = revision + 1 WHERE revision = ?",
             [revision],
             opts
           )
  end

  defp put_contract(session, c, hash, contract, opts) do
    document = json(contract)

    with {:ok, rows} <-
           query(
             session,
             "SELECT document FROM " <>
               table(c, "contract_snapshot") <> " WHERE contract_id = ? LIMIT 2",
             [hash],
             opts
           ) do
      case rows do
        [] ->
          with :ok <- introduce(session, c, opts),
               do: insert(session, c, "contract_snapshot", [[hash, document]], opts)

        [%{"document" => ^document}] ->
          :ok

        _ ->
          error(:runtime_catalog_integrity_failure)
      end
    end
  end

  defp replace_state(session, c, p, mutation, opts) do
    scope_key =
      if String.starts_with?(p.freshness_key, "calendar:"),
        do: "latest",
        else: p.freshness_key |> String.split("|calendar:", parts: 2) |> hd()

    {predicate, params} =
      if mutation == "replace",
        do: {"scope_key <> '__target__'", [p.workspace_id, p.target_id, p.generation_id]},
        else: {"scope_key = ?", [p.workspace_id, p.target_id, p.generation_id, scope_key]}

    with :ok <-
           execute(
             session,
             "DELETE FROM " <>
               table(c, "asset_state") <>
               " WHERE workspace_id = ? AND target_id = ? AND generation_id = ? AND " <> predicate,
             params,
             opts
           ),
         do:
           insert(
             session,
             c,
             "asset_state",
             [[p.workspace_id, p.target_id, p.generation_id, scope_key, p.publication_id, 0]],
             opts
           )
  end

  defp coverage_scope(session, c, %{windows: [w | _]} = p, opts) do
    with {:ok, rows} <-
           query(
             session,
             "SELECT DISTINCT window_kind, timezone FROM " <>
               table(c, "window_state") <>
               " WHERE workspace_id = ? AND target_id = ? AND generation_id = ? LIMIT 2",
             [p.workspace_id, p.target_id, p.generation_id],
             opts
           ) do
      if Enum.all?(rows, &(&1["window_kind"] == w["kind"] and &1["timezone"] == w["timezone"])),
        do: :ok,
        else: error(:runtime_catalog_coverage_conflict)
    end
  end

  defp coverage_scope(_, _, _, _), do: :ok

  defp update_windows(session, c, p, mutation, support, opts) do
    where = " WHERE workspace_id = ? AND target_id = ? AND generation_id = ?"
    params = [p.workspace_id, p.target_id, p.generation_id]

    clear =
      if mutation == "replace" or support == "unsupported",
        do: execute(session, "DELETE FROM " <> table(c, "window_state") <> where, params, opts),
        else: :ok

    with :ok <- clear do
      windows = if support == "supported", do: p.windows, else: []

      with :ok <-
             each(Enum.chunk_every(windows, 500), fn batch ->
               execute(
                 session,
                 "DELETE FROM " <>
                   table(c, "window_state") <>
                   where <>
                   " AND (start_at, end_at) IN (" <>
                   Enum.map_join(batch, ",", fn _ -> "(?, ?)" end) <> ")",
                 params ++ Enum.flat_map(batch, &[&1["start_at"], &1["end_at"]]),
                 opts
               )
             end),
           do:
             insert(
               session,
               c,
               "window_state",
               Enum.map(windows, fn w ->
                 params ++
                   [w["kind"], w["timezone"], w["start_at"], w["end_at"], p.publication_id]
               end),
               opts
             )
    end
  end

  defp coverage_support(%{coverage: nil, windows: []}, _), do: "not_applicable"
  defp coverage_support(_, "replace_groups"), do: "unsupported"
  defp coverage_support(%{windows: []}, _), do: "unknown"
  defp coverage_support(_, _), do: "supported"

  defp check(c),
    do: %{
      "name" => to_string(c.name),
      "phase" => to_string(c.phase),
      "outcome" => to_string(c.outcome)
    }

  defp quality(checks) do
    evaluated = Enum.filter(checks, &(&1["outcome"] in ["passed", "warned", "failed", "errored"]))

    cond do
      evaluated == [] -> "not_checked"
      Enum.any?(evaluated, &(&1["outcome"] != "passed")) -> "warning"
      true -> "passed"
    end
  end

  defp columns do
    %{
      "runtime_schema" => [{"version", "INTEGER"}, {"revision", "BIGINT"}],
      "contract_snapshot" => [{"contract_id", "VARCHAR"}, {"document", "VARCHAR"}],
      "publication" =>
        Enum.map(
          ~w(publication_id request_hash workspace_id target_id generation_id asset_ref run_id step_id),
          &{&1, "VARCHAR"}
        ) ++
          [{"attempt", "BIGINT"}] ++
          Enum.map(
            ~w(manifest_id manifest_hash runner_release relation_catalog relation_schema relation_name),
            &{&1, "VARCHAR"}
          ) ++
          [{"published_at", "TIMESTAMP WITH TIME ZONE"}] ++
          Enum.map(~w(freshness_key policy expiry_kind), &{&1, "VARCHAR"}) ++
          [{"fresh_until", "TIMESTAMP WITH TIME ZONE"}, {"inclusive", "BOOLEAN"}] ++
          Enum.map(
            ~w(contract_id mutation coverage_support quality_status checks coverage_declaration),
            &{&1, "VARCHAR"}
          ),
      "asset_state" =>
        Enum.map(
          ~w(workspace_id target_id generation_id scope_key publication_id),
          &{&1, "VARCHAR"}
        ) ++ [{"revision", "BIGINT"}],
      "window_state" =>
        Enum.map(~w(workspace_id target_id generation_id window_kind timezone), &{&1, "VARCHAR"}) ++
          [
            {"start_at", "TIMESTAMP WITH TIME ZONE"},
            {"end_at", "TIMESTAMP WITH TIME ZONE"},
            {"publication_id", "VARCHAR"}
          ]
    }
  end

  defp views(c) do
    current =
      " FROM " <>
        table(c, "asset_state") <>
        " s JOIN " <>
        table(c, "asset_state") <>
        " t ON t.workspace_id=s.workspace_id AND t.target_id=s.target_id AND t.scope_key='__target__' AND t.generation_id=s.generation_id JOIN " <>
        table(c, "publication") <> " p ON p.publication_id=s.publication_id"

    %{
      "freshness" =>
        "SELECT p.*, CASE WHEN expiry_kind='none' THEN 'fresh' WHEN expiry_kind='always' THEN 'always' WHEN expiry_kind='unknown' THEN 'unknown' WHEN current_timestamp < fresh_until OR (inclusive AND current_timestamp = fresh_until) THEN 'fresh' ELSE 'expired' END AS time_freshness" <>
          current,
      "check_result" =>
        "SELECT p.publication_id,p.asset_ref,json_extract_string(j.value,'$.name') AS check_name,json_extract_string(j.value,'$.phase') AS phase,json_extract_string(j.value,'$.outcome') AS outcome" <>
          current <> ", json_each(p.checks) j",
      "coverage" =>
        "SELECT w.* FROM " <>
          table(c, "window_state") <>
          " w JOIN " <>
          table(c, "asset_state") <>
          " t ON t.workspace_id=w.workspace_id AND t.target_id=w.target_id AND t.generation_id=w.generation_id AND t.scope_key='__target__'"
    }
  end

  defp insert(_, _, _, [], _), do: :ok

  defp insert(session, c, name, rows, opts) do
    rows
    |> Enum.chunk_every(500)
    |> each(fn batch ->
      if byte_size(json(batch)) > 1_048_576,
        do: error(:runtime_catalog_batch_too_large),
        else:
          execute(
            session,
            "INSERT INTO " <>
              table(c, name) <>
              " VALUES " <>
              Enum.map_join(batch, ",", fn row ->
                "(" <> Enum.map_join(row, ",", fn _ -> "?" end) <> ")"
              end),
            List.flatten(batch),
            opts
          )
    end)
  end

  defp cas({adapter, conn}, sql, params, opts) do
    case adapter.execute(conn, sql, Keyword.put(opts, :params, params)) do
      {:ok, %{rows_affected: 1}} -> :ok
      {:ok, _} -> error(:runtime_catalog_conflict)
      error -> error
    end
  end

  defp cas(session, sql, params, opts) do
    case Client.execute(session, sql, Keyword.put(opts, :params, params)) do
      {:ok, %{rows_affected: 1}} -> :ok
      {:ok, _} -> error(:runtime_catalog_conflict)
      error -> error
    end
  end

  defp execute(session, sql, params, opts) do
    case Client.execute(session, sql, Keyword.put(opts, :params, params)) do
      {:ok, _} -> :ok
      error -> error
    end
  end

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

  defp each(values, f),
    do:
      Enum.reduce_while(values, :ok, fn v, :ok ->
        case f.(v) do
          :ok -> {:cont, :ok}
          e -> {:halt, e}
        end
      end)

  defp error(reason),
    do: {:error, %Error{type: reason, message: "Runtime publication rejected", retryable?: false}}

  defp request_hash(p, relation),
    do: Snapshot.digest("ri_", [Map.from_struct(p), Map.from_struct(relation)])

  defp scope(c), do: id(c) <> "." <> id(@schema)
  defp table(c, name), do: scope(c) <> "." <> id(name)
  defp id(s), do: "\"" <> String.replace(s, "\"", "\"\"") <> "\""
  defp json(v), do: Serializer.encode_canonical!(v)
  defp iso(nil), do: nil
  defp iso(v), do: DateTime.to_iso8601(v)
end
