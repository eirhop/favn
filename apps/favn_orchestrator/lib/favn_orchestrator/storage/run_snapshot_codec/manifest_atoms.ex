defmodule FavnOrchestrator.Storage.RunSnapshotCodec.ManifestAtoms do
  @moduledoc false

  alias Favn.Manifest.Identity

  @max_atom_count 100_000
  @max_atom_length 128
  @max_module_length 512

  @spec extract(map()) :: {:ok, MapSet.t(String.t())} | {:error, term()}
  def extract(%{content_hash: content_hash, manifest_index_json: manifest_index_json})
      when is_binary(content_hash) and is_binary(manifest_index_json) do
    with {:ok, manifest} <- decode_manifest(manifest_index_json),
         :ok <- validate_content_hash(manifest, content_hash),
         {:ok, atoms} <- atoms_from_manifest(manifest),
         atoms = MapSet.new(atoms),
         :ok <- validate_count(atoms) do
      {:ok, atoms}
    end
  end

  def extract(%{content_hash: content_hash, atom_strings: atom_strings})
      when is_binary(content_hash) and is_list(atom_strings) do
    atoms = MapSet.new(atom_strings)

    with true <- byte_size(content_hash) == 64,
         true <- Enum.all?(atoms, &valid_persisted_atom?/1),
         :ok <- validate_count(atoms) do
      {:ok, atoms}
    else
      _invalid -> {:error, :invalid_manifest_atom_inventory}
    end
  end

  def extract(record), do: {:error, {:invalid_manifest_record, record}}

  defp decode_manifest(manifest_index_json) do
    case JSON.decode(manifest_index_json) do
      {:ok, decoded} when is_map(decoded) -> {:ok, decoded}
      {:ok, decoded} -> {:error, {:invalid_manifest_index_json_root, decoded}}
      {:error, reason} -> {:error, {:invalid_manifest_index_json, reason}}
    end
  end

  defp validate_content_hash(manifest, content_hash) do
    case Identity.hash_manifest(manifest) do
      {:ok, ^content_hash} -> :ok
      {:ok, computed} -> {:error, {:manifest_content_hash_mismatch, content_hash, computed}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp atoms_from_manifest(manifest) do
    collect_groups([
      records_atoms(Map.get(manifest, "assets"), &asset_atoms/1),
      records_atoms(Map.get(manifest, "pipelines"), &pipeline_atoms/1),
      records_atoms(Map.get(manifest, "schedules"), &schedule_atoms/1),
      graph_atoms(Map.get(manifest, "graph"))
    ])
  end

  defp records_atoms(records, fun) when is_list(records), do: collect(records, fun)
  defp records_atoms(_records, _fun), do: {:ok, []}

  defp asset_atoms(%{} = asset) do
    collect_groups([
      module_atom(Map.get(asset, "module")),
      manifest_atom(Map.get(asset, "name")),
      manifest_atom(Map.get(asset, "execution_pool")),
      settings_atoms(Map.get(asset, "settings")),
      ref_atoms(Map.get(asset, "ref")),
      refs_atoms(Map.get(asset, "depends_on")),
      refs_atoms(Map.get(asset, "relation_inputs"))
    ])
  end

  defp asset_atoms(_asset), do: {:ok, []}

  defp pipeline_atoms(%{} = pipeline) do
    collect_groups([
      module_atom(Map.get(pipeline, "module")),
      manifest_atom(Map.get(pipeline, "name")),
      manifest_atom(Map.get(pipeline, "execution_pool")),
      manifest_atom(Map.get(pipeline, "source")),
      manifest_atoms(Map.get(pipeline, "outputs")),
      settings_atoms(Map.get(pipeline, "settings")),
      selector_atoms(Map.get(pipeline, "selectors"))
    ])
  end

  defp pipeline_atoms(_pipeline), do: {:ok, []}

  defp schedule_atoms(%{} = schedule) do
    collect_groups([
      module_atom(Map.get(schedule, "module")),
      manifest_atom(Map.get(schedule, "name")),
      ref_atoms(Map.get(schedule, "pipeline")),
      ref_atoms(Map.get(schedule, "pipeline_ref"))
    ])
  end

  defp schedule_atoms(_schedule), do: {:ok, []}

  defp graph_atoms(%{} = graph) do
    collect_groups([
      refs_atoms(Map.get(graph, "nodes")),
      refs_atoms(Map.get(graph, "topo_order")),
      edge_atoms(Map.get(graph, "edges"))
    ])
  end

  defp graph_atoms(_graph), do: {:ok, []}

  defp selector_atoms(selectors) when is_list(selectors), do: collect(selectors, &selector_atom/1)
  defp selector_atoms(_selectors), do: {:ok, []}

  defp settings_atoms(%{} = settings), do: collect(Map.keys(settings), &setting_atom/1)
  defp settings_atoms(_settings), do: {:ok, []}

  defp manifest_atoms(values) when is_list(values), do: collect(values, &manifest_atom/1)
  defp manifest_atoms(_values), do: {:ok, []}

  defp selector_atom([kind, value]) when kind in [:asset, "asset"], do: ref_atoms(value)
  defp selector_atom([kind, value]) when kind in [:module, "module"], do: module_atom(value)

  defp selector_atom([kind, value]) when kind in [:tag, "tag", :category, "category"],
    do: manifest_atom(value)

  defp selector_atom(%{"module" => kind, "name" => value}) when kind in [:asset, "asset"],
    do: ref_atoms(value)

  defp selector_atom(%{"module" => kind, "name" => value}) when kind in [:module, "module"],
    do: module_atom(value)

  defp selector_atom(%{"module" => kind, "name" => value})
       when kind in [:tag, "tag", :category, "category"],
       do: manifest_atom(value)

  defp selector_atom(%{"value" => ref}), do: ref_atoms(ref)
  defp selector_atom(%{"ref" => ref}), do: ref_atoms(ref)
  defp selector_atom(%{"module" => _module, "name" => _name} = ref), do: ref_atoms(ref)
  defp selector_atom(_selector), do: {:ok, []}

  defp edge_atoms(edges) when is_list(edges) do
    collect(edges, fn
      [left, right] -> collect_groups([ref_atoms(left), ref_atoms(right)])
      %{"from" => left, "to" => right} -> collect_groups([ref_atoms(left), ref_atoms(right)])
      _edge -> {:ok, []}
    end)
  end

  defp edge_atoms(_edges), do: {:ok, []}

  defp refs_atoms(refs) when is_list(refs), do: collect(refs, &ref_atoms/1)
  defp refs_atoms(_refs), do: {:ok, []}

  defp ref_atoms(%{"module" => module, "name" => name}),
    do: collect_groups([module_atom(module), manifest_atom(name)])

  defp ref_atoms([module, name]),
    do: collect_groups([module_atom(module), manifest_atom(name)])

  defp ref_atoms(_ref), do: {:ok, []}

  defp collect(values, fun) do
    values
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, reversed} ->
      case fun.(value) do
        {:ok, atoms} -> {:cont, {:ok, Enum.reverse(atoms, reversed)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> reverse_result()
  end

  defp collect_groups(groups) do
    groups
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, atoms}, {:ok, reversed} -> {:cont, {:ok, Enum.reverse(atoms, reversed)}}
      {:error, reason}, _acc -> {:halt, {:error, reason}}
    end)
    |> reverse_result()
  end

  defp reverse_result({:ok, reversed}), do: {:ok, Enum.reverse(reversed)}
  defp reverse_result({:error, reason}), do: {:error, reason}

  defp module_atom(nil), do: {:ok, []}

  defp module_atom(value) when is_binary(value) do
    if byte_size(value) <= @max_module_length and
         Regex.match?(~r/^Elixir\.[A-Z][A-Za-z0-9_]*(\.[A-Z][A-Za-z0-9_]*)*$/, value) do
      {:ok, [value]}
    else
      {:error, {:invalid_manifest_module, value}}
    end
  end

  defp module_atom(value), do: {:error, {:invalid_manifest_module, value}}

  defp manifest_atom(nil), do: {:ok, []}

  defp manifest_atom(value) when is_binary(value) do
    if byte_size(value) in 1..@max_atom_length and
         Regex.match?(~r/^[A-Za-z_][A-Za-z0-9_]*[!?=]?$/, value) do
      {:ok, [value]}
    else
      {:error, {:invalid_manifest_atom, value}}
    end
  end

  defp manifest_atom(value), do: {:error, {:invalid_manifest_atom, value}}

  defp setting_atom(value) when is_binary(value) do
    if Favn.Settings.valid_key_string?(value),
      do: {:ok, [value]},
      else: {:error, {:invalid_manifest_setting_key, value}}
  end

  defp setting_atom(value), do: {:error, {:invalid_manifest_setting_key, value}}

  defp validate_count(atoms) do
    if MapSet.size(atoms) <= @max_atom_count,
      do: :ok,
      else: {:error, :manifest_atom_limit_exceeded}
  end

  defp valid_persisted_atom?(value) when is_binary(value),
    do: byte_size(value) in 1..@max_module_length

  defp valid_persisted_atom?(_value), do: false
end
