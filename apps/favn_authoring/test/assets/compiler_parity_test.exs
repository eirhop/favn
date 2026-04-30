defmodule FavnAuthoring.Assets.CompilerParityTest do
  use ExUnit.Case, async: false

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

  test "compile_module_assets/1 loads valid modules before export checks" do
    module = Module.concat(__MODULE__, "LoadableAsset#{System.unique_integer([:positive])}")

    compile_loadable_module!(
      module,
      """
      defmodule #{inspect(module)} do
        use Favn.Asset

        def asset(_ctx), do: :ok
      end
      """
    )

    with_unloaded_module(module, fn ->
      assert {:ok, [%Asset{ref: {^module, :asset}}]} = Compiler.compile_module_assets(module)
    end)
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

  test "sql asset namespace inheritance is compile-order independent" do
    root = Module.concat(__MODULE__, "SQLRoot#{System.unique_integer([:positive])}")
    gold = Module.concat(root, Gold)
    asset = Module.concat(gold, ExecutiveOverview)

    compile_modules_to_path!([
      {"sql_asset.ex",
       """
       defmodule #{inspect(asset)} do
         use Favn.Namespace
         use Favn.SQLAsset

         @materialized :view
         query do
           ~SQL\"\"\"
           select *
           from executive_overview
           \"\"\"
         end
       end
       """},
      {"root.ex",
       """
       defmodule #{inspect(root)} do
         Process.sleep(700)
         use Favn.Namespace, relation: [connection: :warehouse]
       end
       """},
      {"gold.ex",
       """
       defmodule #{inspect(gold)} do
         Process.sleep(700)
         use Favn.Namespace, relation: [schema: :gold]
       end
       """}
    ])

    assert {:ok, [%Asset{relation: relation, relation_inputs: relation_inputs}]} =
             Compiler.compile_module_assets(asset)

    assert relation == %RelationRef{
             connection: :warehouse,
             catalog: nil,
             schema: "gold",
             name: "executive_overview"
           }

    assert [%Favn.Asset.RelationInput{relation_ref: input_relation}] = relation_inputs

    assert input_relation == %RelationRef{
             connection: :warehouse,
             catalog: nil,
             schema: "gold",
             name: "executive_overview"
           }
  end

  test "sql asset missing connection is reported during asset compilation" do
    asset = Module.concat(__MODULE__, "SQLMissingConnection#{System.unique_integer([:positive])}")

    compile_modules_to_path!([
      {"sql_asset_missing_connection.ex",
       """
       defmodule #{inspect(asset)} do
         use Favn.SQLAsset

         @materialized :view
         query do
           ~SQL\"\"\"
           select * from orders
           \"\"\"
         end
       end
       """}
    ])

    assert {:error, {:invalid_compiled_assets, message}} = Compiler.compile_module_assets(asset)
    assert message =~ "SQL assets require a connection"
  end

  test "sql asset missing materialization is reported during asset compilation" do
    root =
      Module.concat(__MODULE__, "SQLMissingMaterializedRoot#{System.unique_integer([:positive])}")

    asset = Module.concat(root, MissingMaterialized)

    compile_modules_to_path!([
      {"sql_asset_missing_materialized.ex",
       """
       defmodule #{inspect(asset)} do
         use Favn.Namespace, relation: [connection: :warehouse]
         use Favn.SQLAsset

         query do
           ~SQL\"\"\"
           select 1 as id
           \"\"\"
         end
       end
       """}
    ])

    assert {:error, {:invalid_compiled_assets, message}} = Compiler.compile_module_assets(asset)
    assert message =~ "Favn.SQLAsset requires one @materialized attribute"
  end

  test "sql asset finalization resolves same-batch reusable SQL imports" do
    root = Module.concat(__MODULE__, "SQLImportRoot#{System.unique_integer([:positive])}")
    helpers = Module.concat(root, SQLHelpers)
    asset = Module.concat(root, ImportedSQLAsset)

    compile_modules_to_path!([
      {"sql_asset.ex",
       """
       defmodule #{inspect(asset)} do
         use Favn.Namespace, relation: [connection: :warehouse]
         use #{inspect(helpers)}
         use Favn.SQLAsset

         @materialized :view
         query do
           ~SQL\"\"\"
           select selected_id(id) as id
           from orders
           \"\"\"
         end
       end
       """},
      {"sql_helpers.ex",
       """
       defmodule #{inspect(helpers)} do
         Process.sleep(700)
         use Favn.SQL

         defsql selected_id(value) do
           ~SQL\"coalesce(@value, @value)\"
         end
       end
       """}
    ])

    assert {:ok, [%Asset{type: :sql}]} = Compiler.compile_module_assets(asset)
  end

  test "multi-asset namespace inheritance is compile-order independent" do
    root = Module.concat(__MODULE__, "MultiRoot#{System.unique_integer([:positive])}")
    raw = Module.concat(root, Raw)
    assets = Module.concat(raw, Extracts)

    compile_modules_to_path!([
      {"multi_asset.ex",
       """
       defmodule #{inspect(assets)} do
         use Favn.Namespace
         use Favn.MultiAsset

         @relation true
         asset :orders do
           rest do
             path "/orders"
             data_path "orders"
           end
         end

         def asset(_ctx), do: :ok
       end
       """},
      {"root.ex",
       "defmodule #{inspect(root)} do\n  use Favn.Namespace, relation: [connection: :warehouse]\nend"},
      {"raw.ex",
       "defmodule #{inspect(raw)} do\n  use Favn.Namespace, relation: [catalog: :raw, schema: :sales]\nend"}
    ])

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

  defp with_unloaded_module(module, fun) when is_atom(module) and is_function(fun, 0) do
    assert {:module, ^module} = Code.ensure_loaded(module)

    :code.purge(module)
    :code.delete(module)

    try do
      fun.()
    after
      assert {:module, ^module} = Code.ensure_loaded(module)
    end
  end

  defp compile_loadable_module!(module, source) when is_atom(module) and is_binary(source) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "favn_core_loadable_modules_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)

    file_path = Path.join(dir, "#{Macro.underscore(Atom.to_string(module))}.ex")
    File.mkdir_p!(Path.dirname(file_path))
    File.write!(file_path, source)

    Code.prepend_path(dir)

    assert {:ok, modules, _diagnostics} =
             Kernel.ParallelCompiler.compile_to_path([file_path], dir, return_diagnostics: true)

    assert module in modules
  end

  defp compile_modules_to_path!(entries) when is_list(entries) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "favn_core_parallel_modules_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)

    files =
      Enum.map(entries, fn {name, source} ->
        file_path = Path.join(dir, name)
        File.write!(file_path, source)
        file_path
      end)

    Code.prepend_path(dir)

    assert {:ok, _modules, _diagnostics} =
             Kernel.ParallelCompiler.compile_to_path(files, dir, return_diagnostics: true)

    :ok
  end
end
