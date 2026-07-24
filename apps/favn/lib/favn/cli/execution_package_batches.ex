defmodule Favn.CLI.ExecutionPackageBatches do
  @moduledoc false

  alias Favn.Manifest.Serializer

  @default_max_count 100
  @default_max_compressed_bytes 8 * 1024 * 1024
  @default_max_decompressed_bytes 32 * 1024 * 1024

  @spec build([term()], keyword()) :: {:ok, [[map()]]} | {:error, term()}
  def build(packages, opts \\ []) when is_list(packages) and is_list(opts) do
    limits = %{
      max_count: Keyword.get(opts, :max_count, @default_max_count),
      max_compressed_bytes:
        Keyword.get(opts, :max_compressed_bytes, @default_max_compressed_bytes),
      max_decompressed_bytes:
        Keyword.get(opts, :max_decompressed_bytes, @default_max_decompressed_bytes)
    }

    with :ok <- validate_limits(limits),
         {:ok, items} <- encode_items(packages),
         batches <- estimated_batches(items, limits),
         {:ok, exact_batches} <- enforce_exact_limits(batches, limits) do
      {:ok, exact_batches}
    end
  end

  defp validate_limits(limits) do
    if Enum.all?(limits, fn {_key, value} -> is_integer(value) and value > 0 end),
      do: :ok,
      else: {:error, :invalid_execution_package_batch_limits}
  end

  defp encode_items(packages) do
    packages
    |> Enum.reduce_while({:ok, []}, fn package, {:ok, items} ->
      case Serializer.encode_manifest(package) do
        {:ok, encoded} ->
          item = %{
            value: JSON.decode!(encoded),
            decompressed_bytes: byte_size(encoded),
            compressed_bytes: encoded |> :zlib.gzip() |> byte_size()
          }

          {:cont, {:ok, [item | items]}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, items} -> {:ok, Enum.reverse(items)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp estimated_batches(items, limits) do
    {batches, current, _metrics} =
      Enum.reduce(items, {[], [], empty_metrics()}, fn item, {batches, current, metrics} ->
        next_metrics = add_metrics(metrics, item)

        if current != [] and exceeds_limits?(next_metrics, limits) do
          {[Enum.reverse(current) | batches], [item.value], add_metrics(empty_metrics(), item)}
        else
          {batches, [item.value | current], next_metrics}
        end
      end)

    batches = if current == [], do: batches, else: [Enum.reverse(current) | batches]
    Enum.reverse(batches)
  end

  defp empty_metrics, do: %{count: 0, compressed_bytes: 0, decompressed_bytes: 15}

  defp add_metrics(metrics, item) do
    %{
      count: metrics.count + 1,
      compressed_bytes: metrics.compressed_bytes + item.compressed_bytes,
      decompressed_bytes:
        metrics.decompressed_bytes + item.decompressed_bytes +
          if(metrics.count > 0, do: 1, else: 0)
    }
  end

  defp exceeds_limits?(metrics, limits) do
    metrics.count > limits.max_count or
      metrics.compressed_bytes > limits.max_compressed_bytes or
      metrics.decompressed_bytes > limits.max_decompressed_bytes
  end

  defp enforce_exact_limits(batches, limits) do
    Enum.reduce_while(batches, {:ok, []}, fn batch, {:ok, acc} ->
      case split_to_fit(batch, limits) do
        {:ok, fitted} -> {:cont, {:ok, Enum.reverse(fitted, acc)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, batches} -> {:ok, Enum.reverse(batches)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp split_to_fit(batch, limits) do
    payload = JSON.encode!(%{packages: batch})

    if length(batch) <= limits.max_count and
         byte_size(payload) <= limits.max_decompressed_bytes and
         byte_size(:zlib.gzip(payload)) <= limits.max_compressed_bytes do
      {:ok, [batch]}
    else
      split_oversized_batch(batch, limits)
    end
  end

  defp split_oversized_batch([_package], _limits),
    do: {:error, :execution_package_exceeds_publication_request_limits}

  defp split_oversized_batch(batch, limits) do
    {left, right} = Enum.split(batch, div(length(batch), 2))

    with {:ok, left_batches} <- split_to_fit(left, limits),
         {:ok, right_batches} <- split_to_fit(right, limits) do
      {:ok, left_batches ++ right_batches}
    end
  end
end
