defmodule FavnAuthoring.DocReaderTest do
  use ExUnit.Case, async: false

  alias FavnAuthoring.DocReader

  test "reads module docs" do
    assert {:ok, result} = DocReader.read_module(Enum)
    assert result.module == Enum
    assert is_binary(result.format)
    assert is_map(result.moduledoc)
  end

  test "reads all public arities for a function name" do
    assert {:ok, result} = DocReader.read_function(Enum, "map")

    assert result.module == Enum
    assert result.function == "map"
    assert Enum.any?(result.entries, &(&1.name == :map and &1.arity == 2))
  end

  test "returns function_not_found for unknown function name" do
    assert {:error, :function_not_found} =
             DocReader.read_function(Enum, "definitely_missing_function_name")
  end

  test "returns module_not_found when module is unavailable" do
    assert {:error, {:fetch_failed, :module_not_found}} =
             DocReader.read_module(FavnAuthoring.NoSuchModule)
  end

  test "returns chunk_not_found when module docs chunk is missing" do
    module = String.to_atom("Elixir.FavnNoDocs#{System.unique_integer([:positive])}")
    temp_dir = Path.join(System.tmp_dir!(), "favn_no_docs_#{System.unique_integer([:positive])}")
    source_file = Path.join(temp_dir, "no_docs.ex")

    module_source = """
    defmodule #{inspect(module)} do
      def hello, do: :ok
    end
    """

    File.mkdir_p!(temp_dir)
    File.write!(source_file, module_source)

    previous_compiler_options = Code.compiler_options()

    try do
      Code.compiler_options(docs: false)
      Kernel.ParallelCompiler.compile_to_path([source_file], temp_dir, return_diagnostics: true)
      Code.prepend_path(temp_dir)

      assert {:error, {:fetch_failed, :chunk_not_found}} = DocReader.read_module(module)
    after
      _ = :code.del_path(String.to_charlist(temp_dir))
      Code.compiler_options(previous_compiler_options)
      File.rm_rf(temp_dir)
    end
  end
end
