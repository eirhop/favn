Code.require_file("../support/sql_package_fixture.exs", __DIR__)

defmodule Favn.Manifest.SQLPackageSizeTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerTask.PersistenceCodec
  alias Favn.Manifest.{ExecutionPackage, Serializer}
  alias Favn.SQL.Template.{Call, Text}
  alias FavnTestSupport.SQLPackageFixture, as: Fixture

  test "ordinary SQL grows with source bytes and keeps generous headroom below 1 MiB" do
    for {columns, budget} <- [{10, 4_096}, {100, 16_384}, {1_000, 147_456}, {5_000, 786_432}] do
      package = Fixture.package(columns)
      source_bytes = Fixture.source_bytes(package)
      bytes = byte_size(:erlang.term_to_binary(package, [:deterministic]))

      assert bytes < budget
      assert bytes < 4 * source_bytes + 4_096
      assert Fixture.count(package, Text) == 2
      assert byte_size(Serializer.encode_manifest!(package)) < budget

      if columns == 5_000 do
        assert source_bytes > 200 * 1_024
        work = Fixture.work(package)
        assert byte_size(:erlang.term_to_binary(work, [:deterministic])) < 800 * 1_024
        assert {:ok, envelope, _hash} = PersistenceCodec.encode_payload(:asset_attempt, work)

        assert {:ok, ^work} =
                 PersistenceCodec.decode_payload(
                   :asset_attempt,
                   envelope,
                   %Favn.Manifest.Version{
                     manifest: %Favn.Manifest{
                       assets: [
                         %Favn.Manifest.Asset{
                           ref: package.asset_ref,
                           module: elem(package.asset_ref, 0),
                           name: elem(package.asset_ref, 1),
                           type: :sql,
                           execution_package_hash: package.content_hash
                         }
                       ]
                     }
                   },
                   [package]
                 )
      end
    end
  end

  test "whitespace adds SQL bytes rather than one object per space" do
    normal = Fixture.package(100)
    wide = Fixture.package(100, :ordinary, 40)
    assert Fixture.count(normal, Text) == Fixture.count(wide, Text)

    growth = byte_size(:erlang.term_to_binary(wide)) - byte_size(:erlang.term_to_binary(normal))
    assert growth <= 3 * (Fixture.source_bytes(wide) - Fixture.source_bytes(normal)) + 128
  end

  test "checks, nested helpers, and scopes remain compact through canonical round trips" do
    package = Fixture.package(100, :rich)
    assert byte_size(:erlang.term_to_binary(package)) < 40 * 1_024
    assert length(package.sql_execution.checks) >= 4
    assert Fixture.count(package, Call) == 2
    assert package.sql_execution.incremental_scope_template
    assert package.sql_execution.full_scope_template

    for template <- Fixture.templates(package), do: assert_compact_nodes(template.nodes)
    assert {:ok, ^package} = ExecutionPackage.verify(package)
    published = package |> Serializer.encode_manifest!() |> Jason.decode!()
    assert {:ok, ^package} = ExecutionPackage.from_published(published)
    assert Fixture.package(100, :rich).content_hash == package.content_hash
  end

  defp assert_compact_nodes(nodes) do
    refute Enum.any?(Enum.chunk_every(nodes, 2, 1, :discard), fn
             [%Text{}, %Text{}] -> true
             _ -> false
           end)

    for %Call{args: args} <- nodes, argument <- args, do: assert_compact_nodes(argument.nodes)
  end
end
