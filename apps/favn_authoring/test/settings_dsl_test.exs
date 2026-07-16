defmodule Favn.SettingsDSLTest do
  use ExUnit.Case, async: false

  test "Asset composes repeated settings and meta declarations" do
    module = unique_module("Asset")

    compile!(module, """
    defmodule #{inspect(module)} do
      use Favn.Asset

      settings source: :orders, request: %{path: "/v1"}
      settings request: %{path: "/v2"}, optional: nil
      meta owner: "data-platform"
      meta category: :sales

      @doc "Extract orders."
      def asset(_ctx), do: :ok
    end
    """)

    assert [asset] = module.__favn_assets__()

    assert asset.settings == %{
             source: "orders",
             request: %{"path" => "/v2"},
             optional: nil
           }

    assert asset.meta == %{owner: "data-platform", category: "sales"}
    assert asset.doc == "Extract orders."
  end

  test "MultiAsset inherits shared declarations and shallowly overrides per child" do
    module = unique_module("Multi")

    compile!(module, """
    defmodule #{inspect(module)} do
      use Favn.MultiAsset

      settings method: "GET", request: %{timeout: 5}
      meta owner: "data-platform"
      freshness :always
      execution_pool :source_api

      asset :orders do
        description "Extract orders."
        settings path: "/orders", request: %{timeout: 10}
        meta category: :sales
      end

      asset :events do
        settings path: "/events"
        freshness nil
        execution_pool nil
      end

      @doc "Execute one generated extraction."
      def asset(_ctx), do: :ok
    end
    """)

    assert [orders, events] = module.__favn_assets__()

    assert orders.settings == %{
             method: "GET",
             path: "/orders",
             request: %{"timeout" => 10}
           }

    assert orders.meta == %{owner: "data-platform", category: "sales"}
    assert orders.doc == "Extract orders."
    assert orders.freshness.mode == :always
    assert orders.execution_pool == :source_api

    assert events.settings.path == "/events"
    assert events.settings.request == %{"timeout" => 5}
    assert events.freshness == nil
    assert events.execution_pool == nil
  end

  test "Source uses moduledoc as its manifest description" do
    module = unique_module("Source")

    compile!(module, """
    defmodule #{inspect(module)} do
      @moduledoc "External orders table."
      use Favn.Source

      meta owner: "source-team"
      relation connection: :warehouse, schema: "raw", name: "orders"
    end
    """)

    assert [asset] = module.__favn_assets__()
    assert asset.doc == "External orders table."
    assert Favn.Manifest.Asset.from_asset(asset).description == "External orders table."
  end

  test "SQLAsset carries settings and transfers query docs to generated asset/1" do
    module = unique_module("SQL")
    previous_docs = Code.get_compiler_option(:docs)
    Code.put_compiler_option(:docs, true)
    on_exit(fn -> Code.put_compiler_option(:docs, previous_docs) end)

    binary =
      compile!(module, """
      defmodule #{inspect(module)} do
        use Favn.Namespace, relation: [connection: :warehouse, schema: "mart"]
        use Favn.SQLAsset

        settings source: "orders"
        materialized :table

        @doc "Build the orders mart."
        query do
          ~SQL"select @source as source"
        end
      end
      """)

    definition = module.__favn_sql_asset_definition__()
    assert definition.asset.settings == %{source: "orders"}
    assert definition.asset.doc == "Build the orders mart."

    beam_path = Path.join(System.tmp_dir!(), "#{module}.beam")
    File.write!(beam_path, binary)
    on_exit(fn -> File.rm(beam_path) end)

    {:docs_v1, _, _, _, _, _, docs} = Code.fetch_docs(beam_path)

    assert Enum.any?(docs, fn
             {{:function, :asset, 1}, _, _, %{"en" => "Build the orders mart."}, _} -> true
             _ -> false
           end)
  end

  test "Pipeline composes repeated settings and meta declarations" do
    module = unique_module("Pipeline")

    compile!(module, """
    defmodule #{inspect(module)} do
      use Favn.Pipeline

      pipeline :daily do
        asset Some.Asset
        settings source: :orders, request: %{path: "/v1"}
        settings request: %{path: "/v2"}, optional: nil
        meta owner: "data-platform"
        meta category: :sales
      end
    end
    """)

    definition = module.__favn_pipeline__()

    assert definition.settings == %{
             source: "orders",
             request: %{"path" => "/v2"},
             optional: nil
           }

    assert definition.meta == %{"owner" => "data-platform", "category" => "sales"}

    string_meta = unique_module("PipelineStringMeta")

    compile!(string_meta, """
    defmodule #{inspect(string_meta)} do
      use Favn.Pipeline

      pipeline :daily do
        asset Some.Asset
        meta %{"owner" => "data-platform"}
        meta category: :sales
      end
    end
    """)

    assert string_meta.__favn_pipeline__().meta == %{
             "owner" => "data-platform",
             "category" => "sales"
           }

    oversized_meta = unique_module("PipelineOversizedMeta")
    first = String.duplicate("x", 40_000)
    second = String.duplicate("y", 40_000)

    assert_raise ArgumentError, ~r/at most 65536 bytes/, fn ->
      compile!(oversized_meta, """
      defmodule #{inspect(oversized_meta)} do
        use Favn.Pipeline

        pipeline :daily do
          asset Some.Asset
          meta first: #{inspect(first, limit: :infinity, printable_limit: :infinity)}
          meta second: #{inspect(second, limit: :infinity, printable_limit: :infinity)}
        end
      end
      """)
    end
  end

  test "late shared declarations and child @doc fail clearly" do
    late = unique_module("Late")

    assert_raise CompileError, ~r/shared settings must be declared before the first/, fn ->
      compile!(late, """
      defmodule #{inspect(late)} do
        use Favn.MultiAsset
        asset :orders do
        end
        settings method: "GET"
        def asset(_ctx), do: :ok
      end
      """)
    end

    child_doc = unique_module("ChildDoc")

    assert_raise CompileError, ~r/use description inside the asset block/, fn ->
      compile!(child_doc, """
      defmodule #{inspect(child_doc)} do
        use Favn.MultiAsset
        @doc "Not a function."
        asset :orders do
        end
        def asset(_ctx), do: :ok
      end
      """)
    end
  end

  test "legacy custom @ attributes are rejected" do
    declarations = [
      "@asset true",
      "@config %{}",
      "@custom %{}",
      "@defaults %{}",
      "@depends Other.Asset",
      "@description \"legacy\"",
      "@execution_pool :legacy",
      "@extra %{}",
      "@freshness :always",
      "@materialized :table",
      "@meta owner: \"legacy\"",
      "@relation true",
      "@resources []",
      "@rest %{}",
      "@retry [max_attempts: 2]",
      "@runtime_config []",
      "@runtime_inputs String",
      "@settings source: \"orders\"",
      "@title \"legacy\"",
      "@window nil"
    ]

    for declaration <- declarations do
      module = unique_module("Legacy")

      assert_raise CompileError, ~r/is not supported; use the .* DSL macro without @/, fn ->
        compile!(module, """
        defmodule #{inspect(module)} do
          use Favn.Asset
          #{declaration}
          def asset(_ctx), do: :ok
        end
        """)
      end
    end
  end

  defp compile!(module, source) do
    assert [{^module, binary}] = Code.compile_string(source, "test/settings_dsl_test.exs")
    binary
  end

  defp unique_module(label) do
    Module.concat(__MODULE__, "#{label}#{System.unique_integer([:positive])}")
  end
end
