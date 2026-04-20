defmodule Favn.Assets.CompilerParityTest do
  use ExUnit.Case, async: true

  alias Favn.Asset
  alias Favn.Assets.Compiler
  alias Favn.RelationRef
  alias Favn.Test.Fixtures.Assets.Basic.SampleAssets
  alias FavnTestSupport.Fixtures

  setup_all do
    Fixtures.compile_fixture!(:basic_assets)
    :ok
  end

  test "compile_module_assets/1 validates relation and metadata shape" do
    asset = %Asset{
      module: SampleAssets,
      name: :normalize_orders,
      entrypoint: :normalize_orders,
      ref: {SampleAssets, :normalize_orders},
      arity: 1,
      file: "lib/sample_assets.ex",
      line: 12
    }

    assert Asset.validate!(asset).meta == %{}

    assert_raise ArgumentError, ~r/asset relation must be a Favn.RelationRef or nil/, fn ->
      Asset.validate!(%{asset | relation: %{name: "bad"}})
    end

    assert_raise ArgumentError, ~r/asset meta must be a keyword list or map/, fn ->
      Asset.validate!(%{asset | meta: :invalid})
    end

    assert_raise ArgumentError, ~r/asset depends_on must be a list of Favn.Ref values/, fn ->
      Asset.validate!(%{asset | depends_on: [:not_a_ref]})
    end
  end

  test "single-asset module rejects module shorthand for multi-asset dependency modules" do
    module_name = Module.concat(__MODULE__, "BadDepends#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(module_name)} do
        use Favn.Asset

        @depends #{inspect(SampleAssets)}
        def asset(_ctx), do: :ok
      end
      """,
      "test/assets/compiler_parity_test.exs"
    )

    assert {:error, {:invalid_compiled_assets, message}} =
             Compiler.compile_module_assets(module_name)

    assert message =~ "module shorthand requires a single-asset module"
  end

  test "resolves namespace inheritance from separately compiled ancestor modules" do
    root = Module.concat(__MODULE__, "Root#{System.unique_integer([:positive])}")
    raw = Module.concat(root, Raw)
    sales = Module.concat(raw, Sales)
    assets = Module.concat(sales, Assets)

    Code.compile_string(
      "defmodule #{inspect(root)} do\n  use Favn.Namespace, relation: [connection: :warehouse]\nend",
      "test/assets/compiler_parity_test.exs"
    )

    Code.compile_string(
      "defmodule #{inspect(raw)} do\n  use Favn.Namespace, relation: [catalog: :raw]\nend",
      "test/assets/compiler_parity_test.exs"
    )

    Code.compile_string(
      "defmodule #{inspect(sales)} do\n  use Favn.Namespace, relation: [schema: :sales]\nend",
      "test/assets/compiler_parity_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(assets)} do
        use Favn.Assets

        @asset true
        @relation true
        def orders(_ctx), do: :ok
      end
      """,
      "test/assets/compiler_parity_test.exs"
    )

    assert {:ok, [%Asset{relation: relation}]} = Compiler.compile_module_assets(assets)

    assert relation == %RelationRef{
             connection: :warehouse,
             catalog: "raw",
             schema: "sales",
             name: "orders"
           }
  end

  test "relation inheritance is compile-order independent" do
    root = Module.concat(__MODULE__, "OrderRoot#{System.unique_integer([:positive])}")
    raw = Module.concat(root, Raw)
    sales = Module.concat(raw, Sales)
    assets = Module.concat(sales, Assets)

    Code.compile_string(
      """
      defmodule #{inspect(assets)} do
        use Favn.Assets

        @asset true
        @relation true
        def orders(_ctx), do: :ok
      end
      """,
      "test/assets/compiler_parity_test.exs"
    )

    Code.compile_string(
      "defmodule #{inspect(root)} do\n  use Favn.Namespace, relation: [connection: :warehouse]\nend",
      "test/assets/compiler_parity_test.exs"
    )

    Code.compile_string(
      "defmodule #{inspect(raw)} do\n  use Favn.Namespace, relation: [catalog: :raw]\nend",
      "test/assets/compiler_parity_test.exs"
    )

    Code.compile_string(
      "defmodule #{inspect(sales)} do\n  use Favn.Namespace, relation: [schema: :sales]\nend",
      "test/assets/compiler_parity_test.exs"
    )

    assert {:ok, [%Asset{relation: relation}]} = Compiler.compile_module_assets(assets)

    assert relation == %RelationRef{
             connection: :warehouse,
             catalog: "raw",
             schema: "sales",
             name: "orders"
           }
  end

  test "relation normalization rejects duplicate canonical keys" do
    module_name =
      Module.concat(__MODULE__, "DuplicateCatalog#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(module_name)} do
        use Favn.Assets

        @asset true
        @relation %{"database" => "raw", :catalog => "silver", "name" => "orders"}
        def duplicate_catalog_keys(_ctx), do: :ok
      end
      """,
      "test/assets/compiler_parity_test.exs"
    )

    assert {:error, {:invalid_compiled_assets, message}} =
             Compiler.compile_module_assets(module_name)

    assert message =~ "duplicate values for canonical key :catalog"
  end
end
