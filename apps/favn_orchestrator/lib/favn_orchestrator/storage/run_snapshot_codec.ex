defmodule FavnOrchestrator.Storage.RunSnapshotCodec do
  @moduledoc false

  alias Favn.Manifest.Identity
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.PayloadCodec
  alias FavnOrchestrator.Storage.RunStateCodec

  @max_manifest_atom_length 128
  @max_manifest_module_length 512

  # Favn-owned run snapshot atoms are fixed here; consumer module/name atoms come only from
  # the associated manifest record.
  @internal_atom_strings [
    "action",
    "asset",
    "asset_dependencies",
    "asset_ref",
    "asset_results",
    "attempt",
    "attempt_count",
    "attempts",
    "all",
    "anchor_ranges",
    "anchor_window",
    "backfill_range",
    "cancelled",
    "config",
    "dependencies",
    "downstream",
    "duration_ms",
    "error",
    "event_seq",
    "execution_id",
    "finished_at",
    "id",
    "in_flight_execution_ids",
    "inserted_at",
    "kind",
    "lineage_depth",
    "manual",
    "manifest_content_hash",
    "manifest_version_id",
    "max_attempts",
    "meta",
    "metadata",
    "name",
    "next_attempt",
    "next_retry_at",
    "nil",
    "node_key",
    "node_stages",
    "nodes",
    "none",
    "ok",
    "outputs",
    "params",
    "parent_run_id",
    "pipeline",
    "pipeline_context",
    "pipeline_dependencies",
    "pipeline_module",
    "pipeline_submit_ref",
    "pipeline_target_refs",
    "plan",
    "ref",
    "relation",
    "rerun_of_run_id",
    "resolved_refs",
    "result",
    "retry_backoff_ms",
    "retrying",
    "root_run_id",
    "run",
    "run_finished",
    "run_kind",
    "runner_execution_id",
    "rows_written",
    "runner_metadata",
    "schedule",
    "snapshot_hash",
    "source",
    "source_run_id",
    "stage",
    "stages",
    "started_at",
    "status",
    "submit_ref",
    "submit_kind",
    "target_node_keys",
    "target_refs",
    "terminal_event_type",
    "timeout_ms",
    "topo_order",
    "trigger",
    "updated_at",
    "upstream",
    "window"
  ]

  @type run_record :: %{
          required(:run_blob) => String.t(),
          required(:manifest_version_id) => String.t()
        }

  @type manifest_record :: %{
          required(:manifest_version_id) => String.t(),
          required(:content_hash) => String.t(),
          required(:manifest_json) => String.t()
        }

  @spec decode_run(run_record(), manifest_record() | nil) ::
          {:ok, RunState.t()} | {:error, term()}
  def decode_run(%{run_blob: payload, manifest_version_id: manifest_version_id}, manifest_record)
      when is_binary(payload) and is_binary(manifest_version_id) do
    with {:ok, manifest_record} <- validate_manifest_record(manifest_record, manifest_version_id),
         {:ok, allowed_atom_strings} <- allowed_atom_strings(manifest_record),
         {:ok, decoded} <-
           PayloadCodec.decode(payload, allowed_atom_strings: allowed_atom_strings),
         %RunState{} = run_state <- decoded,
         :ok <- validate_run_manifest(run_state, manifest_version_id),
         :ok <- validate_run_content_hash(run_state, manifest_record),
         {:ok, normalized} <- RunStateCodec.normalize(run_state) do
      {:ok, normalized}
    else
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_run_payload, other}}
    end
  end

  def decode_run(run_record, _manifest_record), do: {:error, {:invalid_run_record, run_record}}

  defp validate_manifest_record(nil, manifest_version_id),
    do: {:error, {:missing_manifest_version, manifest_version_id}}

  defp validate_manifest_record(
         %{manifest_version_id: manifest_version_id} = record,
         manifest_version_id
       ),
       do: {:ok, record}

  defp validate_manifest_record(%{manifest_version_id: other}, manifest_version_id),
    do: {:error, {:run_manifest_mismatch, manifest_version_id, other}}

  defp validate_manifest_record(record, _manifest_version_id),
    do: {:error, {:invalid_manifest_record, record}}

  defp allowed_atom_strings(manifest_record) do
    with {:ok, manifest_atoms} <- manifest_atom_strings(manifest_record) do
      {:ok, Enum.uniq(@internal_atom_strings ++ manifest_atoms)}
    end
  end

  defp manifest_atom_strings(%{content_hash: content_hash, manifest_json: manifest_json})
       when is_binary(content_hash) and is_binary(manifest_json) do
    with {:ok, decoded} <- decode_manifest_json(manifest_json),
         :ok <- validate_manifest_content_hash(decoded, content_hash),
         {:ok, atoms} <- manifest_atom_strings_from_manifest(decoded) do
      {:ok, Enum.uniq(atoms)}
    end
  end

  defp manifest_atom_strings(record), do: {:error, {:invalid_manifest_record, record}}

  defp decode_manifest_json(manifest_json) do
    case JSON.decode(manifest_json) do
      {:ok, decoded} when is_map(decoded) -> {:ok, decoded}
      {:ok, decoded} -> {:error, {:invalid_manifest_json_root, decoded}}
      {:error, reason} -> {:error, {:invalid_manifest_json, reason}}
    end
  end

  defp validate_manifest_content_hash(decoded_manifest, content_hash) do
    case Identity.hash_manifest(decoded_manifest) do
      {:ok, ^content_hash} -> :ok
      {:ok, computed} -> {:error, {:manifest_content_hash_mismatch, content_hash, computed}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp manifest_atom_strings_from_manifest(%{} = manifest) do
    with {:ok, asset_atoms} <- manifest_records_atoms(Map.get(manifest, "assets"), &asset_atoms/1),
         {:ok, pipeline_atoms} <-
           manifest_records_atoms(Map.get(manifest, "pipelines"), &pipeline_atoms/1),
         {:ok, schedule_atoms} <-
           manifest_records_atoms(Map.get(manifest, "schedules"), &schedule_atoms/1),
         {:ok, graph_atoms} <- graph_atoms(Map.get(manifest, "graph")) do
      {:ok, asset_atoms ++ pipeline_atoms ++ schedule_atoms ++ graph_atoms}
    end
  end

  defp manifest_records_atoms(records, fun) when is_list(records), do: collect_atoms(records, fun)
  defp manifest_records_atoms(_records, _fun), do: {:ok, []}

  defp asset_atoms(%{} = asset) do
    collect_atom_groups([
      module_atom(Map.get(asset, "module")),
      manifest_atom(Map.get(asset, "name")),
      ref_atoms(Map.get(asset, "ref")),
      refs_atoms(Map.get(asset, "depends_on")),
      refs_atoms(Map.get(asset, "relation_inputs"))
    ])
  end

  defp asset_atoms(_asset), do: {:ok, []}

  defp pipeline_atoms(%{} = pipeline) do
    collect_atom_groups([
      module_atom(Map.get(pipeline, "module")),
      manifest_atom(Map.get(pipeline, "name")),
      selector_atoms(Map.get(pipeline, "selectors"))
    ])
  end

  defp pipeline_atoms(_pipeline), do: {:ok, []}

  defp schedule_atoms(%{} = schedule) do
    collect_atom_groups([
      module_atom(Map.get(schedule, "module")),
      manifest_atom(Map.get(schedule, "name")),
      ref_atoms(Map.get(schedule, "pipeline")),
      ref_atoms(Map.get(schedule, "pipeline_ref"))
    ])
  end

  defp schedule_atoms(_schedule), do: {:ok, []}

  defp graph_atoms(%{} = graph) do
    collect_atom_groups([
      refs_atoms(Map.get(graph, "nodes")),
      refs_atoms(Map.get(graph, "topo_order")),
      edge_atoms(Map.get(graph, "edges"))
    ])
  end

  defp graph_atoms(_graph), do: {:ok, []}

  defp selector_atoms(selectors) when is_list(selectors) do
    collect_atoms(selectors, &selector_atom/1)
  end

  defp selector_atoms(_selectors), do: {:ok, []}

  defp selector_atom([_kind, ref]), do: ref_atoms(ref)
  defp selector_atom(%{"value" => ref}), do: ref_atoms(ref)
  defp selector_atom(%{"ref" => ref}), do: ref_atoms(ref)
  defp selector_atom(%{"module" => _module, "name" => _name} = ref), do: ref_atoms(ref)
  defp selector_atom(_selector), do: {:ok, []}

  defp edge_atoms(edges) when is_list(edges) do
    collect_atoms(edges, fn edge ->
      case edge do
        [left, right] ->
          collect_atom_groups([ref_atoms(left), ref_atoms(right)])

        %{"from" => left, "to" => right} ->
          collect_atom_groups([ref_atoms(left), ref_atoms(right)])

        _edge ->
          {:ok, []}
      end
    end)
  end

  defp edge_atoms(_edges), do: {:ok, []}

  defp refs_atoms(refs) when is_list(refs), do: collect_atoms(refs, &ref_atoms/1)
  defp refs_atoms(_refs), do: {:ok, []}

  defp ref_atoms(%{"module" => module, "name" => name}) do
    collect_atom_groups([module_atom(module), manifest_atom(name)])
  end

  defp ref_atoms([module, name]),
    do: collect_atom_groups([module_atom(module), manifest_atom(name)])

  defp ref_atoms(_ref), do: {:ok, []}

  defp collect_atoms(values, fun) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case fun.(value) do
        {:ok, atoms} -> {:cont, {:ok, acc ++ atoms}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp collect_atom_groups(groups) do
    Enum.reduce_while(groups, {:ok, []}, fn
      {:ok, atoms}, {:ok, acc} -> {:cont, {:ok, acc ++ atoms}}
      {:error, reason}, _acc -> {:halt, {:error, reason}}
    end)
  end

  defp module_atom(nil), do: {:ok, []}

  defp module_atom(value) when is_binary(value) do
    if valid_manifest_module?(value),
      do: {:ok, [value]},
      else: {:error, {:invalid_manifest_module, value}}
  end

  defp module_atom(value), do: {:error, {:invalid_manifest_module, value}}

  defp manifest_atom(nil), do: {:ok, []}

  defp manifest_atom(value) when is_binary(value) do
    if valid_manifest_atom?(value),
      do: {:ok, [value]},
      else: {:error, {:invalid_manifest_atom, value}}
  end

  defp manifest_atom(value), do: {:error, {:invalid_manifest_atom, value}}

  defp valid_manifest_module?(value) when is_binary(value) do
    byte_size(value) <= @max_manifest_module_length and
      Regex.match?(~r/^Elixir\.[A-Z][A-Za-z0-9_]*(\.[A-Z][A-Za-z0-9_]*)*$/, value)
  end

  defp valid_manifest_atom?(value) when is_binary(value) do
    byte_size(value) in 1..@max_manifest_atom_length and
      Regex.match?(~r/^[A-Za-z_][A-Za-z0-9_]*[!?=]?$/, value)
  end

  defp validate_run_manifest(
         %RunState{manifest_version_id: manifest_version_id},
         manifest_version_id
       ),
       do: :ok

  defp validate_run_manifest(%RunState{manifest_version_id: other}, manifest_version_id),
    do: {:error, {:run_manifest_mismatch, manifest_version_id, other}}

  defp validate_run_content_hash(%RunState{manifest_content_hash: content_hash}, %{
         content_hash: content_hash
       }),
       do: :ok

  defp validate_run_content_hash(%RunState{manifest_content_hash: run_hash}, %{
         content_hash: manifest_hash
       }),
       do: {:error, {:run_manifest_content_hash_mismatch, manifest_hash, run_hash}}
end
