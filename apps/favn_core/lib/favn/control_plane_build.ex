defmodule Favn.ControlPlaneBuild do
  @moduledoc """
  Deterministic identity for one production control-plane image input set.

  Filesystem discovery and OCI operations belong to local tooling. This module
  owns only validation, canonical ordering, and hashing so CI and maintainer
  builds share one platform-independent identity contract.
  """

  @schema_version 1
  @sha256 ~r/\A[0-9a-f]{64}\z/

  @type input_record :: %{
          required(:path) => String.t(),
          required(:sha256) => String.t(),
          required(:size) => non_neg_integer()
        }

  @type identity :: %{
          required(String.t()) => String.t() | pos_integer() | [String.t()]
        }

  @type descriptor :: %{
          required(:schema_version) => pos_integer(),
          required(:control_plane_build_id) => String.t(),
          required(:identity) => identity(),
          required(:inputs) => [input_record()]
        }

  @type error ::
          :invalid_control_plane_identity
          | :invalid_control_plane_input
          | {:duplicate_control_plane_input, String.t()}

  @doc "Returns the only supported control-plane build descriptor version."
  @spec current_schema_version() :: pos_integer()
  def current_schema_version, do: @schema_version

  @doc "Builds and hashes one validated, canonically ordered input descriptor."
  @spec new([map()], map()) :: {:ok, descriptor()} | {:error, error()}
  def new(inputs, identity) when is_list(inputs) and is_map(identity) do
    with {:ok, identity} <- normalize_identity(identity),
         {:ok, inputs} <- normalize_inputs(inputs) do
      payload = %{
        "schema_version" => @schema_version,
        "identity" => identity,
        "inputs" => inputs
      }

      {:ok,
       %{
         schema_version: @schema_version,
         control_plane_build_id: sha256(canonical_binary(payload)),
         identity: identity,
         inputs: inputs
       }}
    end
  end

  def new(_inputs, _identity), do: {:error, :invalid_control_plane_identity}

  @doc "Recomputes the ID of an already normalized descriptor payload."
  @spec compute_id([map()], map()) :: {:ok, String.t()} | {:error, error()}
  def compute_id(inputs, identity) do
    with {:ok, descriptor} <- new(inputs, identity) do
      {:ok, descriptor.control_plane_build_id}
    end
  end

  defp normalize_identity(identity) do
    identity
    |> Enum.reduce_while({:ok, %{}}, fn
      {key, value}, {:ok, acc} when is_binary(key) and key != "" ->
        case normalize_identity_value(value) do
          {:ok, normalized} -> {:cont, {:ok, Map.put(acc, key, normalized)}}
          :error -> {:halt, {:error, :invalid_control_plane_identity}}
        end

      _entry, _acc ->
        {:halt, {:error, :invalid_control_plane_identity}}
    end)
    |> case do
      {:ok, normalized} when map_size(normalized) > 0 -> {:ok, normalized}
      _invalid -> {:error, :invalid_control_plane_identity}
    end
  end

  defp normalize_identity_value(value) when is_binary(value) and value != "", do: {:ok, value}
  defp normalize_identity_value(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp normalize_identity_value(values) when is_list(values) do
    if values != [] and Enum.all?(values, &(is_binary(&1) and &1 != "")) do
      {:ok, Enum.sort(values)}
    else
      :error
    end
  end

  defp normalize_identity_value(_value), do: :error

  defp normalize_inputs(inputs) do
    inputs
    |> Enum.reduce_while({:ok, %{}}, fn input, {:ok, acc} ->
      with {:ok, normalized} <- normalize_input(input),
           false <- Map.has_key?(acc, normalized.path) do
        {:cont, {:ok, Map.put(acc, normalized.path, normalized)}}
      else
        true -> {:halt, {:error, {:duplicate_control_plane_input, input_path(input)}}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, records} when map_size(records) > 0 ->
        {:ok, records |> Map.values() |> Enum.sort_by(& &1.path)}

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_control_plane_input}
    end
  end

  defp normalize_input(input) when is_map(input) do
    path = field(input, :path)
    digest = field(input, :sha256)
    size = field(input, :size)

    if safe_relative_path?(path) and valid_digest?(digest) and is_integer(size) and size >= 0 do
      {:ok, %{path: path, sha256: digest, size: size}}
    else
      {:error, :invalid_control_plane_input}
    end
  end

  defp normalize_input(_input), do: {:error, :invalid_control_plane_input}

  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
  defp input_path(input) when is_map(input), do: field(input, :path) || "invalid"
  defp input_path(_input), do: "invalid"

  defp safe_relative_path?(path) when is_binary(path) and path != "" do
    Path.type(path) == :relative and
      not String.contains?(path, "\\") and
      not Enum.any?(Path.split(path), &(&1 in ["", ".", ".."]))
  end

  defp safe_relative_path?(_path), do: false

  defp valid_digest?(digest), do: is_binary(digest) and Regex.match?(@sha256, digest)

  defp canonical_binary(value), do: :erlang.term_to_binary(canonical(value), [:deterministic])

  defp canonical(map) when is_map(map) do
    {:map,
     map
     |> Enum.map(fn {key, value} -> {to_string(key), canonical(value)} end)
     |> Enum.sort_by(&elem(&1, 0))}
  end

  defp canonical(list) when is_list(list), do: {:list, Enum.map(list, &canonical/1)}
  defp canonical(value) when is_binary(value), do: {:string, value}
  defp canonical(value) when is_integer(value), do: {:integer, value}

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
