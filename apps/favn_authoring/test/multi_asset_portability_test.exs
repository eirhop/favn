defmodule Favn.MultiAssetPortabilityTest do
  use ExUnit.Case, async: false

  alias Favn.RunnerRelease.BeamDigest

  @module Favn.MultiAssetPortableConsumer

  test "generated runtime code is independent of the consumer workspace root" do
    source = """
    defmodule #{@module} do
      use Favn.MultiAsset

      runtime_config :shared, endpoint: env!("SHARED_ENDPOINT")

      asset :orders do
        runtime_config :child, token: secret_env!("CHILD_TOKEN")
      end

      def asset(_ctx), do: :ok
    end
    """

    root_a = temporary_root("workspace-a")
    root_b = temporary_root("workspace-b")

    on_exit(fn ->
      unload(@module)
      File.rm_rf(root_a)
      File.rm_rf(root_b)
    end)

    beam_a = compile_in(root_a, source)
    unload(@module)
    beam_b = compile_in(root_b, source)

    assert {:ok, digest_a} = BeamDigest.digest(beam_a)
    assert {:ok, digest_b} = BeamDigest.digest(beam_b)
    assert digest_a == digest_b

    assert {:ok, canonical} = BeamDigest.canonical_binary(beam_b)
    refute canonical =~ root_a
    refute canonical =~ root_b
  end

  defp compile_in(root, source) do
    source_path = Path.join(root, "lib/portable_consumer.ex")
    File.mkdir_p!(Path.dirname(source_path))
    File.write!(source_path, source)

    File.cd!(root, fn ->
      assert [{@module, beam}] = Code.compile_file(source_path)
      beam
    end)
  end

  defp temporary_root(name) do
    Path.join(
      System.tmp_dir!(),
      "favn_multi_asset_#{name}_#{System.unique_integer([:positive])}"
    )
  end

  defp unload(module) do
    :code.purge(module)
    :code.delete(module)
  end
end
