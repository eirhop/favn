defmodule FavnOrchestrator.Storage.PayloadCodec do
  @moduledoc false

  @format "json-v1"
  @allowed_struct_modules %{
    "Elixir.Favn.Backfill.RangeRequest" => :"Elixir.Favn.Backfill.RangeRequest",
    "Elixir.Favn.Contracts.RelationInspectionRequest" =>
      :"Elixir.Favn.Contracts.RelationInspectionRequest",
    "Elixir.Favn.Contracts.RelationInspectionResult" =>
      :"Elixir.Favn.Contracts.RelationInspectionResult",
    "Elixir.Favn.Contracts.RunnerEvent" => :"Elixir.Favn.Contracts.RunnerEvent",
    "Elixir.Favn.Contracts.RunnerResult" => :"Elixir.Favn.Contracts.RunnerResult",
    "Elixir.Favn.Contracts.RunnerWork" => :"Elixir.Favn.Contracts.RunnerWork",
    "Elixir.Favn.Manifest" => :"Elixir.Favn.Manifest",
    "Elixir.Favn.Manifest.Asset" => :"Elixir.Favn.Manifest.Asset",
    "Elixir.Favn.Manifest.Catalog" => :"Elixir.Favn.Manifest.Catalog",
    "Elixir.Favn.Manifest.Graph" => :"Elixir.Favn.Manifest.Graph",
    "Elixir.Favn.Manifest.Pipeline" => :"Elixir.Favn.Manifest.Pipeline",
    "Elixir.Favn.Manifest.Schedule" => :"Elixir.Favn.Manifest.Schedule",
    "Elixir.Favn.Manifest.SQLExecution" => :"Elixir.Favn.Manifest.SQLExecution",
    "Elixir.Favn.Manifest.Version" => :"Elixir.Favn.Manifest.Version",
    "Elixir.Favn.Plan" => :"Elixir.Favn.Plan",
    "Elixir.Favn.Pipeline.Definition" => :"Elixir.Favn.Pipeline.Definition",
    "Elixir.Favn.Pipeline.Resolution" => :"Elixir.Favn.Pipeline.Resolution",
    "Elixir.Favn.RelationRef" => :"Elixir.Favn.RelationRef",
    "Elixir.Favn.Run" => :"Elixir.Favn.Run",
    "Elixir.Favn.Run.AssetResult" => :"Elixir.Favn.Run.AssetResult",
    "Elixir.Favn.Run.Context" => :"Elixir.Favn.Run.Context",
    "Elixir.Favn.RuntimeConfig.Ref" => :"Elixir.Favn.RuntimeConfig.Ref",
    "Elixir.Favn.Scheduler.State" => :"Elixir.Favn.Scheduler.State",
    "Elixir.Favn.SQL.Definition" => :"Elixir.Favn.SQL.Definition",
    "Elixir.Favn.SQL.Definition.Param" => :"Elixir.Favn.SQL.Definition.Param",
    "Elixir.Favn.SQL.Column" => :"Elixir.Favn.SQL.Column",
    "Elixir.Favn.SQL.Relation" => :"Elixir.Favn.SQL.Relation",
    "Elixir.Favn.SQL.RelationRef" => :"Elixir.Favn.SQL.RelationRef",
    "Elixir.Favn.SQL.Template" => :"Elixir.Favn.SQL.Template",
    "Elixir.Favn.SQL.Template.AssetRef" => :"Elixir.Favn.SQL.Template.AssetRef",
    "Elixir.Favn.SQL.Template.Call" => :"Elixir.Favn.SQL.Template.Call",
    "Elixir.Favn.SQL.Template.DefinitionRef" => :"Elixir.Favn.SQL.Template.DefinitionRef",
    "Elixir.Favn.SQL.Template.Fragment" => :"Elixir.Favn.SQL.Template.Fragment",
    "Elixir.Favn.SQL.Template.Placeholder" => :"Elixir.Favn.SQL.Template.Placeholder",
    "Elixir.Favn.SQL.Template.Position" => :"Elixir.Favn.SQL.Template.Position",
    "Elixir.Favn.SQL.Template.Relation" => :"Elixir.Favn.SQL.Template.Relation",
    "Elixir.Favn.SQL.Template.Requirements" => :"Elixir.Favn.SQL.Template.Requirements",
    "Elixir.Favn.SQL.Template.Span" => :"Elixir.Favn.SQL.Template.Span",
    "Elixir.Favn.SQL.Template.Text" => :"Elixir.Favn.SQL.Template.Text",
    "Elixir.Favn.Triggers.Schedule" => :"Elixir.Favn.Triggers.Schedule",
    "Elixir.Favn.Window.Anchor" => :"Elixir.Favn.Window.Anchor",
    "Elixir.Favn.Window.Policy" => :"Elixir.Favn.Window.Policy",
    "Elixir.Favn.Window.Request" => :"Elixir.Favn.Window.Request",
    "Elixir.Favn.Window.Runtime" => :"Elixir.Favn.Window.Runtime",
    "Elixir.Favn.Window.Spec" => :"Elixir.Favn.Window.Spec",
    "Elixir.FavnOrchestrator.Backfill.AssetWindowState" =>
      :"Elixir.FavnOrchestrator.Backfill.AssetWindowState",
    "Elixir.FavnOrchestrator.Backfill.BackfillWindow" =>
      :"Elixir.FavnOrchestrator.Backfill.BackfillWindow",
    "Elixir.FavnOrchestrator.Backfill.CoverageBaseline" =>
      :"Elixir.FavnOrchestrator.Backfill.CoverageBaseline",
    "Elixir.FavnOrchestrator.RunEvent" => :"Elixir.FavnOrchestrator.RunEvent",
    "Elixir.FavnOrchestrator.RunState" => :"Elixir.FavnOrchestrator.RunState",
    "Elixir.FavnOrchestrator.SchedulerEntry" => :"Elixir.FavnOrchestrator.SchedulerEntry",
    "Elixir.MapSet" => :"Elixir.MapSet"
  }

  @type encoded_value :: map() | list() | String.t() | number() | boolean() | nil

  @spec encode(term()) :: {:ok, String.t()} | {:error, term()}
  def encode(value) do
    payload = %{"format" => @format, "value" => encode_term(value)}
    {:ok, Jason.encode!(payload)}
  rescue
    error -> {:error, {:payload_encode_failed, error}}
  end

  @spec decode(String.t(), keyword()) :: {:ok, term()} | {:error, term()}
  def decode(payload, opts \\ []) when is_binary(payload) and is_list(opts) do
    allowed_atom_strings = allowed_atom_strings(opts)

    with {:ok, decoded} <- Jason.decode(payload),
         %{"format" => @format, "value" => value} <- decoded,
         {:ok, term} <- decode_term(value, allowed_atom_strings) do
      {:ok, term}
    else
      {:error, reason} -> {:error, {:payload_decode_failed, reason}}
      other -> {:error, {:invalid_payload_root, other}}
    end
  end

  defp encode_term(%DateTime{} = datetime) do
    %{"__type__" => "datetime", "value" => DateTime.to_iso8601(datetime)}
  end

  defp encode_term(%_{} = struct) do
    %{
      "__type__" => "struct",
      "module" => Atom.to_string(struct.__struct__),
      "fields" => encode_term(Map.from_struct(struct))
    }
  end

  defp encode_term(map) when is_map(map) do
    entries =
      map
      |> Enum.map(fn {key, value} ->
        encoded_key = encode_term(key)
        encoded_value = encode_term(value)
        {Jason.encode!(encoded_key), [encoded_key, encoded_value]}
      end)
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(&elem(&1, 1))

    %{"__type__" => "map", "entries" => entries}
  end

  defp encode_term(list) when is_list(list), do: Enum.map(list, &encode_term/1)

  defp encode_term(tuple) when is_tuple(tuple) do
    %{"__type__" => "tuple", "items" => tuple |> Tuple.to_list() |> Enum.map(&encode_term/1)}
  end

  defp encode_term(atom) when is_atom(atom) do
    %{"__type__" => "atom", "value" => Atom.to_string(atom)}
  end

  defp encode_term(value) when is_binary(value), do: value
  defp encode_term(value) when is_number(value), do: value
  defp encode_term(value) when is_boolean(value), do: value
  defp encode_term(nil), do: nil

  defp allowed_atom_strings(opts) when is_list(opts) do
    opts
    |> Keyword.get(:allowed_atom_strings, [])
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> MapSet.new()
  end

  defp decode_term(%{"__type__" => "datetime", "value" => value}, _allowed_atom_strings)
       when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      _ -> {:error, {:invalid_datetime, value}}
    end
  end

  defp decode_term(
         %{"__type__" => "struct", "module" => module, "fields" => fields},
         allowed_atom_strings
       )
       when is_binary(module) do
    with {:ok, decoded_fields} <- decode_term(fields, allowed_atom_strings),
         :ok <- validate_struct_fields(decoded_fields, fields),
         {:ok, module_atom} <- allowed_struct_module(module),
         :ok <- validate_struct_module(module_atom, module) do
      {:ok, struct(module_atom, decoded_fields)}
    else
      error -> error
    end
  rescue
    error -> {:error, {:invalid_struct_decode, module, error}}
  end

  defp decode_term(%{"__type__" => "map", "entries" => entries}, allowed_atom_strings)
       when is_list(entries) do
    Enum.reduce_while(entries, {:ok, %{}}, fn
      [encoded_key, encoded_value], {:ok, acc} ->
        with {:ok, key} <- decode_term(encoded_key, allowed_atom_strings),
             {:ok, value} <- decode_term(encoded_value, allowed_atom_strings) do
          {:cont, {:ok, Map.put(acc, key, value)}}
        else
          {:error, reason} -> {:halt, {:error, reason}}
        end

      entry, _acc ->
        {:halt, {:error, {:invalid_map_entry, entry}}}
    end)
  end

  defp decode_term(%{"__type__" => "tuple", "items" => items}, allowed_atom_strings)
       when is_list(items) do
    with {:ok, decoded_items} <- decode_list(items, allowed_atom_strings) do
      {:ok, List.to_tuple(decoded_items)}
    end
  end

  defp decode_term(%{"__type__" => "atom", "value" => value}, allowed_atom_strings)
       when is_binary(value) do
    decode_atom(value, allowed_atom_strings)
  end

  defp decode_term(%{"__type__" => type} = value, _allowed_atom_strings),
    do: {:error, {:unsupported_payload_type, type, value}}

  defp decode_term(list, allowed_atom_strings) when is_list(list),
    do: decode_list(list, allowed_atom_strings)

  defp decode_term(value, _allowed_atom_strings) when is_binary(value), do: {:ok, value}
  defp decode_term(value, _allowed_atom_strings) when is_number(value), do: {:ok, value}
  defp decode_term(value, _allowed_atom_strings) when is_boolean(value), do: {:ok, value}
  defp decode_term(nil, _allowed_atom_strings), do: {:ok, nil}

  defp decode_term(other, _allowed_atom_strings),
    do: {:error, {:unsupported_payload_value, other}}

  defp decode_list(list, allowed_atom_strings) do
    Enum.reduce_while(list, {:ok, []}, fn value, {:ok, acc} ->
      case decode_term(value, allowed_atom_strings) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_atom(value, allowed_atom_strings) when is_binary(value) do
    {:ok, String.to_existing_atom(value)}
  rescue
    ArgumentError -> decode_allowed_atom(value, allowed_atom_strings)
  end

  # sobelow_skip ["DOS.StringToAtom"]
  defp decode_allowed_atom(value, allowed_atom_strings) do
    if MapSet.member?(allowed_atom_strings, value) do
      {:ok, String.to_atom(value)}
    else
      {:error, {:unknown_atom, value}}
    end
  end

  defp validate_struct_fields(decoded_fields, _raw_fields) when is_map(decoded_fields), do: :ok

  defp validate_struct_fields(_decoded_fields, raw_fields),
    do: {:error, {:invalid_struct_fields, raw_fields}}

  defp allowed_struct_module(raw_module) when is_binary(raw_module) do
    case Map.fetch(@allowed_struct_modules, raw_module) do
      {:ok, module} -> {:ok, module}
      :error -> {:error, {:unsupported_struct_module, raw_module}}
    end
  end

  defp validate_struct_module(module_atom, raw_module) when is_atom(module_atom) do
    case Code.ensure_loaded(module_atom) do
      {:module, ^module_atom} ->
        if function_exported?(module_atom, :__struct__, 0) do
          :ok
        else
          {:error, {:invalid_struct_module, raw_module}}
        end

      _other ->
        {:error, {:invalid_struct_module, raw_module}}
    end
  end
end
