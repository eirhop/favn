defmodule Favn.RunnerRelease.BeamDigestTest do
  use ExUnit.Case, async: false

  alias Favn.RunnerRelease.BeamDigest

  test "digest excludes checkout paths, line tables, docs, and compiler metadata" do
    source = """
    defmodule FavnPathIndependentProbe do
      @moduledoc "path-independent probe"
      def add(left, right), do: left + right
    end
    """

    beam_a = compile_one(source, "/tmp/favn-release-a/lib/probe.ex")
    unload(FavnPathIndependentProbe)
    beam_b = compile_one(source, "/different/root/favn-release-b/lib/probe.ex")

    assert {:ok, digest_a} = BeamDigest.digest(beam_a)
    assert {:ok, digest_b} = BeamDigest.digest(beam_b)
    assert digest_a == digest_b

    assert {:ok, canonical_a} = BeamDigest.canonical_binary(beam_a)
    assert {:ok, canonical_b} = BeamDigest.canonical_binary(beam_b)
    assert canonical_a == canonical_b
    refute canonical_a =~ "/tmp/favn-release-a"
    refute canonical_a =~ "/different/root/favn-release-b"

    unload(FavnPathIndependentProbe)
  end

  test "digest excludes compile-generated external resource paths" do
    source = """
    defmodule FavnExternalResourceProbe do
      @external_resource __ENV__.file
      def value, do: :ok
    end
    """

    beam_a = compile_one(source, "/tmp/favn-release-a/lib/external.ex")
    unload(FavnExternalResourceProbe)
    beam_b = compile_one(source, "/different/root/favn-release-b/lib/external.ex")

    assert BeamDigest.digest(beam_a) == BeamDigest.digest(beam_b)

    unload(FavnExternalResourceProbe)
  end

  test "digest rejects compiler-derived and hard-coded absolute path literals" do
    source = """
    defmodule FavnSourceLiteralProbe do
      def file, do: __ENV__.file
      def directory, do: __DIR__
    end
    """

    beam_a = compile_one(source, "/tmp/favn-release-a/lib/source_literal.ex")
    unload(FavnSourceLiteralProbe)
    beam_b = compile_one(source, "/different/root/favn-release-b/lib/source_literal.ex")

    assert {:error, {:invalid_beam, {:absolute_path_literal, _index}}} =
             BeamDigest.digest(beam_a)

    assert {:error, {:invalid_beam, {:absolute_path_literal, _index}}} =
             BeamDigest.digest(beam_b)

    unload(FavnSourceLiteralProbe)

    hard_coded_source = """
    defmodule FavnHardCodedPathProbe do
      def fixture, do: "/tmp/favn-release-a/lib/fixture.json"
    end
    """

    hard_coded_a = compile_one(hard_coded_source, "/tmp/favn-release-a/lib/hard_coded.ex")
    unload(FavnHardCodedPathProbe)
    hard_coded_b = compile_one(hard_coded_source, "/different/root/hard_coded.ex")

    assert {:error, {:invalid_beam, {:absolute_path_literal, _index}}} =
             BeamDigest.digest(hard_coded_a)

    assert {:error, {:invalid_beam, {:absolute_path_literal, _index}}} =
             BeamDigest.digest(hard_coded_b)

    unload(FavnHardCodedPathProbe)
  end

  test "digest ignores strip-removable source paths in persisted attributes" do
    source = """
    defmodule FavnSourceAttributeProbe do
      Module.register_attribute(__MODULE__, :fixture_root, persist: true)
      @fixture_root __DIR__
      def value, do: :ok
    end
    """

    beam_a = compile_one(source, "/tmp/favn-release-a/lib/source_attribute.ex")
    unload(FavnSourceAttributeProbe)
    beam_b = compile_one(source, "/different/root/favn-release-b/lib/source_attribute.ex")

    assert BeamDigest.digest(beam_a) == BeamDigest.digest(beam_b)

    unload(FavnSourceAttributeProbe)
  end

  test "digest is identical before and after OTP release stripping" do
    source = """
    defmodule FavnStrippedReleaseProbe do
      Module.register_attribute(__MODULE__, :runtime_marker, persist: true)
      @runtime_marker :retained_before_strip
      def build(offset), do: fn value -> private_add(value, offset) end
      defp private_add(left, right), do: left + right
    end
    """

    beam = compile_one(source, "/tmp/favn-release/lib/stripped.ex")
    assert {:ok, {_module, stripped}} = :beam_lib.strip(beam)

    assert {:ok, digest} = BeamDigest.digest(beam)
    assert {:ok, ^digest} = BeamDigest.digest(stripped)
    assert BeamDigest.canonical_binary(beam) == BeamDigest.canonical_binary(stripped)

    assert {:ok, metadata} = BeamDigest.metadata(stripped)
    assert metadata.module == "Elixir.FavnStrippedReleaseProbe"
    assert metadata.protocol_implementation == nil

    unload(FavnStrippedReleaseProbe)
  end

  test "digest changes when executable code changes" do
    first =
      compile_one(
        "defmodule FavnExecutableProbe, do: def(value(number), do: number + 1)",
        "first.ex"
      )

    unload(FavnExecutableProbe)

    second =
      compile_one(
        "defmodule FavnExecutableProbe, do: def(value(number), do: number + 2)",
        "second.ex"
      )

    assert {:ok, first_digest} = BeamDigest.digest(first)
    assert {:ok, second_digest} = BeamDigest.digest(second)
    refute first_digest == second_digest

    unload(FavnExecutableProbe)
  end

  test "metadata returns stable module and unique sorted imports" do
    beam =
      compile_one(
        "defmodule FavnImportsProbe, do: def(run(values), do: Enum.map(values, &Kernel.to_string/1))",
        "imports.ex"
      )

    assert {:ok, metadata} = BeamDigest.metadata(beam)
    assert metadata.module == "Elixir.FavnImportsProbe"
    assert metadata.digest =~ ~r/\A[0-9a-f]{64}\z/
    assert metadata.imports == Enum.sort(Enum.uniq(metadata.imports))
    assert "Elixir.Enum" in metadata.imports
    assert metadata.protocol_implementation == nil

    unload(FavnImportsProbe)
  end

  test "metadata identifies protocol implementation modules" do
    modules =
      Code.compile_string("""
      defprotocol FavnMetadataProtocol do
        def value(input)
      end

      defimpl FavnMetadataProtocol, for: Integer do
        def value(input), do: input
      end
      """)

    beam = modules |> Map.new() |> Map.fetch!(FavnMetadataProtocol.Integer)

    assert {:ok, metadata} = BeamDigest.metadata(beam)

    assert metadata.protocol_implementation == %{
             protocol: "Elixir.FavnMetadataProtocol",
             for: "Elixir.Integer"
           }

    Enum.each(modules, fn {module, _beam} -> unload(module) end)
  end

  test "rejects malformed input with a bounded error" do
    assert {:error, {:invalid_beam, :malformed}} = BeamDigest.digest("not a BEAM")
    assert {:error, {:invalid_beam, :malformed}} = BeamDigest.metadata(<<0, 1, 2>>)
  end

  test "handles improper-list literals without crashing" do
    safe_beam =
      compile_one(
        "defmodule FavnImproperListProbe, do: def(value(), do: [1 | :tail])",
        "improper_list.ex"
      )

    assert {:ok, digest} = BeamDigest.digest(safe_beam)
    assert digest =~ ~r/\A[0-9a-f]{64}\z/
    unload(FavnImproperListProbe)

    path_beam =
      compile_one(
        """
        defmodule FavnImproperPathProbe do
          def value, do: [:head | "/tmp/secret"]
        end
        """,
        "improper_path.ex"
      )

    assert {:error, {:invalid_beam, {:absolute_path_literal, _index}}} =
             BeamDigest.digest(path_beam)

    unload(FavnImproperPathProbe)
  end

  test "handles struct literals without requiring Enumerable" do
    beam =
      compile_one(
        "defmodule FavnStructLiteralProbe, do: def(value(), do: %URI{scheme: \"https\", host: \"example.com\"})",
        "struct_literal.ex"
      )

    assert {:ok, digest} = BeamDigest.digest(beam)
    assert digest =~ ~r/\A[0-9a-f]{64}\z/
    unload(FavnStructLiteralProbe)
  end

  defp compile_one(source, file) do
    assert [{_module, beam}] = Code.compile_string(source, file)
    beam
  end

  defp unload(module) do
    :code.purge(module)
    :code.delete(module)
  end
end
