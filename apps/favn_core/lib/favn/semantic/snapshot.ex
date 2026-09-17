defmodule Favn.Semantic.Snapshot do
  @moduledoc """
  Public data-contract projection used by an independent semantic build.

  Callers supply already compiled asset maps enriched with their `:contract`.
  This module never loads customer modules or runner releases. Its closed JSON
  records retain schema, relationships, declared lineage and dependencies, while
  excluding execution SQL, runtime settings, secrets and source locations.
  """

  alias Favn.Manifest.Serializer
  alias Favn.SQL.Contract

  @max_bytes 64 * 1024 * 1024

  @doc "Projects compiled assets to deterministic, JSON-compatible records."
  @spec build([map()]) :: {:ok, [map()]} | {:error, atom()}
  def build(assets) when is_list(assets) and length(assets) <= 10_000 do
    records = assets |> Enum.map(&asset_record/1) |> Enum.sort_by(& &1["ref"])
    refs = Enum.map(records, & &1["ref"])

    cond do
      refs != Enum.uniq(refs) ->
        {:error, :duplicate_asset}

      byte_size(Serializer.encode_canonical!(records)) > @max_bytes ->
        {:error, :snapshot_too_large}

      true ->
        {:ok, records}
    end
  rescue
    _error in [ArgumentError, KeyError, FunctionClauseError] -> {:error, :invalid_snapshot}
  end

  def build(_assets), do: {:error, :invalid_snapshot}

  @doc "Formats an existing asset reference without creating atoms."
  @spec ref({module(), atom()}) :: String.t()
  def ref({module, name}) when is_atom(module) and is_atom(name),
    do: Enum.join(Module.split(module), ".") <> "." <> Atom.to_string(name)

  @doc "Hashes canonical public data with a domain-specific identity prefix."
  @spec digest(String.t(), term()) :: String.t()
  def digest(prefix, value),
    do:
      prefix <>
        Base.encode16(:crypto.hash(:sha256, Serializer.encode_canonical!(value)), case: :lower)

  defp asset_record(asset) do
    contract = Map.get(asset, :contract)
    if contract, do: Contract.validate!(contract)

    record = %{
      "ref" => ref(Map.fetch!(asset, :ref)),
      "kind" => to_string(Map.fetch!(asset, :type)),
      "relation" => relation(Map.get(asset, :relation)),
      "dependencies" => asset |> Map.get(:depends_on, []) |> Enum.map(&ref/1) |> Enum.sort(),
      "contract" => if(contract, do: contract_record(contract), else: nil)
    }

    Map.put(record, "fingerprint", digest("ac_", record))
  end

  defp relation(nil), do: nil

  defp relation(value) do
    %{
      "connection_ref" => optional_string(Map.get(value, :connection)),
      "catalog" => Map.get(value, :catalog),
      "schema" => Map.get(value, :schema),
      "name" => Map.fetch!(value, :name)
    }
  end

  defp contract_record(contract) do
    %{
      "grain" => if(contract.grain, do: Enum.map(contract.grain.by, &to_string/1), else: []),
      "grain_description" => if(contract.grain, do: contract.grain.description),
      "columns" => Enum.with_index(contract.columns, 1) |> Enum.map(&column/1),
      "compositions" =>
        Enum.map(contract.compositions, fn composition ->
          %{
            "module" => Enum.join(Module.split(composition.module), "."),
            "start_index" => composition.start_index,
            "columns" => Enum.map(composition.columns, &to_string/1)
          }
        end),
      "unique_keys" => Enum.map(contract.unique_keys, &key/1),
      "row_counts" => Enum.map(contract.row_counts, &row_count/1),
      "relationships" => contract |> Map.get(:relationships, []) |> Enum.map(&relationship/1)
    }
  end

  defp column({column, ordinal}) do
    %{
      "name" => to_string(column.name),
      "ordinal" => ordinal,
      "type" => to_string(column.type),
      "nullable" => column.nullable?,
      "description" => column.description,
      "renamed_from" => optional_string(column.renamed_from),
      "tags" => column.tags,
      "via" => optional_string(column.via),
      "sources" => Enum.map(column.sources, &lineage/1)
    }
  end

  defp lineage(source) do
    %{
      "kind" => to_string(source.kind),
      "asset_ref" => if(source.asset_ref, do: ref(source.asset_ref)),
      "dataset" => source.dataset,
      "column" => to_string(source.column)
    }
  end

  defp key(value) do
    %{
      "columns" => Enum.map(value.columns, &to_string/1)
    }
  end

  defp row_count(value) do
    Map.new([:equals, :min, :max, :when, :on_violation], fn key ->
      item = Map.get(value, key)

      normalized =
        if is_struct(item), do: %{"parameter" => to_string(item.name)}, else: json_scalar(item)

      {to_string(key), normalized}
    end)
  end

  defp relationship(value) do
    %{
      "name" => to_string(value.name),
      "target" => ref(value.target),
      "on" =>
        Enum.map(value.on, fn {local, remote} ->
          %{"source" => to_string(local), "target" => to_string(remote)}
        end),
      "cardinality" => to_string(value.cardinality),
      "on_violation" => to_string(value.on_violation)
    }
  end

  defp json_scalar(value) when is_atom(value) and not is_nil(value), do: to_string(value)
  defp json_scalar(value), do: value
  defp optional_string(nil), do: nil
  defp optional_string(value), do: to_string(value)
end
