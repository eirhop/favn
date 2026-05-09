defmodule FavnOrchestrator.Storage.Freshness.AssetFreshnessStateCodec do
  @moduledoc false

  alias Favn.Window.Key, as: WindowKey
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Storage.JsonSafe

  @format "favn.freshness.asset_freshness_state.storage.v1"

  @spec encode(AssetFreshnessState.t()) :: {:ok, String.t()} | {:error, term()}
  def encode(%AssetFreshnessState{} = state) do
    {:ok, Jason.encode!(to_dto(state))}
  rescue
    error -> {:error, {:asset_freshness_state_encode_failed, error}}
  end

  @spec decode(String.t()) :: {:ok, AssetFreshnessState.t()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    with {:ok, %{"format" => @format, "schema_version" => 1} = dto} <- Jason.decode(payload),
         {:ok, asset_ref_module} <- existing_atom(Map.get(dto, "asset_ref_module")),
         {:ok, asset_ref_name} <- existing_atom(Map.get(dto, "asset_ref_name")),
         {:ok, latest_success_node_key} <-
           node_key_from_dto(Map.get(dto, "latest_success_node_key")),
         {:ok, latest_success_at} <- optional_datetime(Map.get(dto, "latest_success_at")),
         {:ok, latest_attempt_at} <- optional_datetime(Map.get(dto, "latest_attempt_at")),
         {:ok, updated_at} <- datetime(Map.get(dto, "updated_at")),
         {:ok, input_versions} <- input_versions_from_dto(Map.get(dto, "input_versions", [])),
         {:ok, metadata} <- map_field(dto, "metadata") do
      AssetFreshnessState.new(%{
        asset_ref_module: asset_ref_module,
        asset_ref_name: asset_ref_name,
        freshness_key: Map.get(dto, "freshness_key"),
        status: Map.get(dto, "status"),
        freshness_version: Map.get(dto, "freshness_version"),
        latest_success_run_id: Map.get(dto, "latest_success_run_id"),
        latest_success_node_key: latest_success_node_key,
        latest_success_at: latest_success_at,
        latest_attempt_run_id: Map.get(dto, "latest_attempt_run_id"),
        latest_attempt_status: Map.get(dto, "latest_attempt_status"),
        latest_attempt_at: latest_attempt_at,
        manifest_version_id: Map.get(dto, "manifest_version_id"),
        manifest_content_hash: Map.get(dto, "manifest_content_hash"),
        input_versions: input_versions,
        metadata: metadata,
        updated_at: updated_at
      })
    else
      {:ok, other} -> {:error, {:invalid_asset_freshness_state_dto, other}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp to_dto(%AssetFreshnessState{} = state) do
    %{
      "format" => @format,
      "schema_version" => 1,
      "asset_ref_module" => Atom.to_string(state.asset_ref_module),
      "asset_ref_name" => Atom.to_string(state.asset_ref_name),
      "freshness_key" => state.freshness_key,
      "status" => Atom.to_string(state.status),
      "freshness_version" => state.freshness_version,
      "latest_success_run_id" => state.latest_success_run_id,
      "latest_success_node_key" => node_key_to_dto(state.latest_success_node_key),
      "latest_success_at" => datetime_to_dto(state.latest_success_at),
      "latest_attempt_run_id" => state.latest_attempt_run_id,
      "latest_attempt_status" => atom_to_string(state.latest_attempt_status),
      "latest_attempt_at" => datetime_to_dto(state.latest_attempt_at),
      "manifest_version_id" => state.manifest_version_id,
      "manifest_content_hash" => state.manifest_content_hash,
      "input_versions" => input_versions_to_dto(state.input_versions),
      "metadata" => JsonSafe.data(state.metadata || %{}),
      "updated_at" => datetime_to_dto(state.updated_at)
    }
  end

  defp input_versions_to_dto(input_versions) when is_list(input_versions) do
    input_versions
    |> Enum.map(&input_version_to_dto/1)
    |> Enum.reject(&is_nil/1)
  end

  defp input_versions_to_dto(input_versions) when is_map(input_versions) do
    input_versions
    |> Enum.map(fn
      {node_key, version} when is_tuple(node_key) ->
        input_version_to_dto(%{upstream_node_key: node_key, freshness_version: version})

      {_key, %{} = input_version} ->
        input_version_to_dto(input_version)

      _other ->
        nil
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp input_versions_to_dto(_input_versions), do: []

  defp input_version_to_dto(%{} = input_version) do
    upstream_node_key = field(input_version, :upstream_node_key)
    upstream_ref = field(input_version, :upstream_ref) || ref_from_node_key(upstream_node_key)

    %{
      "upstream_ref" => ref_to_dto(upstream_ref),
      "upstream_node_key" => node_key_to_dto(upstream_node_key),
      "freshness_version" => field(input_version, :freshness_version),
      "success_run_id" => field(input_version, :success_run_id)
    }
  end

  defp input_version_to_dto(_input_version), do: nil

  defp input_versions_from_dto(values) when is_list(values) do
    values
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
      case input_version_from_dto(value) do
        {:ok, input_version} -> {:cont, {:ok, [input_version | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, input_versions} -> {:ok, Enum.reverse(input_versions)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp input_versions_from_dto(_values), do: {:ok, []}

  defp input_version_from_dto(%{} = dto) do
    with {:ok, upstream_ref} <- optional_ref_from_dto(Map.get(dto, "upstream_ref")),
         {:ok, upstream_node_key} <- node_key_from_dto(Map.get(dto, "upstream_node_key")) do
      {:ok,
       %{
         upstream_ref: upstream_ref,
         upstream_node_key: upstream_node_key,
         freshness_version: Map.get(dto, "freshness_version"),
         success_run_id: Map.get(dto, "success_run_id")
       }}
    end
  end

  defp input_version_from_dto(value), do: {:error, {:invalid_input_version, value}}

  defp node_key_to_dto({ref, identity}) do
    %{"ref" => ref_to_dto(ref), "identity" => node_identity_to_dto(identity)}
  end

  defp node_key_to_dto(nil), do: nil
  defp node_key_to_dto(_value), do: nil

  defp node_key_from_dto(nil), do: {:ok, nil}

  defp node_key_from_dto(%{"ref" => ref, "identity" => identity}) do
    with {:ok, decoded_ref} <- ref_from_dto(ref),
         {:ok, decoded_identity} <- node_identity_from_dto(identity) do
      {:ok, {decoded_ref, decoded_identity}}
    end
  end

  defp node_key_from_dto(value), do: {:error, {:invalid_node_key, value}}

  defp node_identity_to_dto(nil), do: nil

  defp node_identity_to_dto(%{kind: _kind, start_at_us: _start_at_us, timezone: _timezone} = key) do
    %{"type" => "window_key", "value" => WindowKey.encode(key)}
  end

  defp node_identity_to_dto(identity), do: %{"type" => "json", "value" => JsonSafe.data(identity)}

  defp node_identity_from_dto(nil), do: {:ok, nil}

  defp node_identity_from_dto(%{"type" => "window_key", "value" => value}),
    do: WindowKey.decode(value)

  defp node_identity_from_dto(%{"type" => "json", "value" => value}), do: {:ok, value}
  defp node_identity_from_dto(value), do: {:error, {:invalid_node_identity, value}}

  defp ref_to_dto({module, name}) when is_atom(module) and is_atom(name) do
    %{"module" => Atom.to_string(module), "name" => Atom.to_string(name)}
  end

  defp ref_to_dto(_value), do: nil

  defp optional_ref_from_dto(nil), do: {:ok, nil}
  defp optional_ref_from_dto(value), do: ref_from_dto(value)

  defp ref_from_dto(%{"module" => module, "name" => name}) do
    with {:ok, module_atom} <- existing_atom(module),
         {:ok, name_atom} <- existing_atom(name) do
      {:ok, {module_atom, name_atom}}
    end
  end

  defp ref_from_dto(value), do: {:error, {:invalid_ref, value}}

  defp field(map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  defp ref_from_node_key({ref, _identity}), do: ref
  defp ref_from_node_key(_node_key), do: nil

  defp atom_to_string(nil), do: nil
  defp atom_to_string(value) when is_atom(value), do: Atom.to_string(value)

  defp datetime_to_dto(nil), do: nil
  defp datetime_to_dto(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp optional_datetime(nil), do: {:ok, nil}
  defp optional_datetime(value), do: datetime(value)

  defp datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, reason} -> {:error, {:invalid_datetime, value, reason}}
    end
  end

  defp datetime(value), do: {:error, {:invalid_datetime, value}}

  defp existing_atom(value) when is_binary(value) do
    {:ok, String.to_existing_atom(value)}
  rescue
    ArgumentError -> {:error, {:unknown_atom, value}}
  end

  defp existing_atom(value), do: {:error, {:invalid_atom, value}}

  defp map_field(dto, field) do
    case Map.fetch(dto, field) do
      {:ok, value} when is_map(value) -> {:ok, value}
      {:ok, value} -> {:error, {:invalid_dto_field, field, value}}
      :error -> {:error, {:missing_dto_field, field}}
    end
  end
end
