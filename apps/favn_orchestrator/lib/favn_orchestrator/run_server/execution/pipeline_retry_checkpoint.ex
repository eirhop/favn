defmodule FavnOrchestrator.RunServer.Execution.PipelineRetryCheckpoint do
  @moduledoc false

  import Bitwise

  @encoding "stage_bitset_v1"

  @spec encode([term()], [term()]) :: {:ok, map()} | {:error, :invalid_retry_selection}
  def encode(stage_node_keys, retry_node_keys)
      when is_list(stage_node_keys) and is_list(retry_node_keys) do
    retry_set = MapSet.new(retry_node_keys)
    stage_set = MapSet.new(stage_node_keys)

    if MapSet.size(stage_set) == length(stage_node_keys) and
         MapSet.subset?(retry_set, stage_set) do
      bits = encode_bits(stage_node_keys, retry_set)

      {:ok,
       %{
         encoding: @encoding,
         stage_size: length(stage_node_keys),
         retry_count: MapSet.size(retry_set),
         bits: Base.url_encode64(bits, padding: false)
       }}
    else
      {:error, :invalid_retry_selection}
    end
  end

  def encode(_stage_node_keys, _retry_node_keys), do: {:error, :invalid_retry_selection}

  @spec decode(map(), [term()]) :: {:ok, [term()]} | {:error, :invalid_retry_checkpoint}
  def decode(checkpoint, stage_node_keys) when is_map(checkpoint) and is_list(stage_node_keys) do
    with @encoding <- field(checkpoint, :encoding),
         stage_size when stage_size == length(stage_node_keys) <- field(checkpoint, :stage_size),
         retry_count when is_integer(retry_count) and retry_count >= 0 <-
           field(checkpoint, :retry_count),
         bits when is_binary(bits) <- field(checkpoint, :bits),
         {:ok, decoded} <- Base.url_decode64(bits, padding: false),
         true <- byte_size(decoded) == div(stage_size + 7, 8),
         selected <- decode_bits(stage_node_keys, decoded),
         true <- length(selected) == retry_count do
      {:ok, selected}
    else
      _invalid -> {:error, :invalid_retry_checkpoint}
    end
  end

  def decode(_checkpoint, _stage_node_keys), do: {:error, :invalid_retry_checkpoint}

  defp encode_bits(stage_node_keys, retry_set) do
    stage_node_keys
    |> Enum.with_index()
    |> Enum.chunk_every(8)
    |> Enum.map(fn chunk ->
      Enum.reduce(chunk, 0, fn {node_key, index}, byte ->
        if MapSet.member?(retry_set, node_key),
          do: byte ||| 1 <<< rem(index, 8),
          else: byte
      end)
    end)
    |> :erlang.list_to_binary()
  end

  defp decode_bits(stage_node_keys, bits) do
    stage_node_keys
    |> Enum.with_index()
    |> Enum.filter(fn {_node_key, index} ->
      (:binary.at(bits, div(index, 8)) &&& 1 <<< rem(index, 8)) != 0
    end)
    |> Enum.map(&elem(&1, 0))
  end

  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end
