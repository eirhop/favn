defmodule Favn.CLI.ExecutionPackageBatchesTest do
  use ExUnit.Case, async: true

  alias Favn.CLI.ExecutionPackageBatches

  test "batches satisfy count, expanded JSON, and gzip request budgets" do
    packages =
      Enum.map(1..5, fn index ->
        %{
          content_hash: String.duplicate(Integer.to_string(index), 64),
          sql_execution: %{
            sql: Base.encode64(:crypto.strong_rand_bytes(384)),
            template: %{index: index}
          }
        }
      end)

    canonical = Enum.map(packages, &canonical/1)
    single_payloads = Enum.map(canonical, &JSON.encode!(%{packages: [&1]}))

    max_decompressed = single_payloads |> Enum.map(&byte_size/1) |> Enum.max()

    max_compressed =
      single_payloads
      |> Enum.map(&(:zlib.gzip(&1) |> byte_size()))
      |> Enum.max()

    assert {:ok, batches} =
             ExecutionPackageBatches.build(packages,
               max_count: 3,
               max_decompressed_bytes: max_decompressed,
               max_compressed_bytes: max_compressed
             )

    assert length(batches) > 1
    assert Enum.concat(batches) == canonical

    Enum.each(batches, fn batch ->
      payload = JSON.encode!(%{packages: batch})

      assert length(batch) <= 3
      assert byte_size(payload) <= max_decompressed
      assert byte_size(:zlib.gzip(payload)) <= max_compressed
    end)
  end

  defp canonical(package) do
    package
    |> Favn.Manifest.Serializer.encode_manifest!()
    |> JSON.decode!()
  end
end
