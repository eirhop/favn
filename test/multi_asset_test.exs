defmodule Favn.MultiAssetTest do
  use ExUnit.Case, async: false

  alias Favn.Asset
  alias Favn.RelationRef
  alias Favn.Run.Context
  alias Favn.Runtime.Executor.Local

  test "compiles generated canonical assets with merged defaults and rest config" do
    module_name = Module.concat(__MODULE__, "Shopify#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module_name)} do
      use Favn.MultiAsset

      defaults do
        meta owner: "data-platform", category: :shopify, tags: [:raw]
        window Favn.Window.daily(lookback: 1)

        rest do
          primary_key "id"
          paginator :cursor, cursor_path: "links.next"
          incremental cursor: "updated_at", start_param: "updated_at_min"
          extra page_size: 250
        end
      end

      @doc "Extract orders"
      @meta tags: [:orders]
      asset :orders do
        rest do
          path "/orders.json"
          data_path "orders"
          params status: "any"
          extra include_metafields: true
        end
      end

      @doc "Extract customers"
      asset :customers do
        rest do
          path "/customers.json"
          data_path "customers"
        end
      end

      def asset(ctx), do: {:ok, %{rest: ctx.asset.config[:rest]}}
    end
    """

    assert Enum.any?(Code.compile_string(source, "test/dynamic_multi_asset_test.exs"), fn {mod, _} ->
             mod == module_name
           end)

    [orders, customers] = module_name.__favn_assets__()

    assert %Asset{name: :orders, entrypoint: :asset, ref: {^module_name, :orders}} = orders

    assert %Asset{name: :customers, entrypoint: :asset, ref: {^module_name, :customers}} =
             customers

    assert orders.meta == %{owner: "data-platform", category: :shopify, tags: [:orders]}
    assert customers.meta == %{owner: "data-platform", category: :shopify, tags: [:raw]}

    assert orders.window_spec == %Favn.Window.Spec{kind: :day, lookback: 1, timezone: "Etc/UTC"}

    assert customers.window_spec == %Favn.Window.Spec{
             kind: :day,
             lookback: 1,
             timezone: "Etc/UTC"
           }

    assert orders.config == %{
             rest: %{
               primary_key: "id",
               paginator: %{kind: :cursor, cursor_path: "links.next"},
               incremental: %{kind: :cursor, cursor: "updated_at", start_param: "updated_at_min"},
               extra: %{page_size: 250, include_metafields: true},
               path: "/orders.json",
               data_path: "orders",
               params: %{status: "any"}
             }
           }

    assert customers.config == %{
             rest: %{
               primary_key: "id",
               paginator: %{kind: :cursor, cursor_path: "links.next"},
               incremental: %{kind: :cursor, cursor: "updated_at", start_param: "updated_at_min"},
               extra: %{page_size: 250},
               path: "/customers.json",
               data_path: "customers"
             }
           }
  end

  test "relation defaults and @relation true infer generated asset names" do
    module_name = Module.concat(__MODULE__, "Produces#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module_name)} do
      use Favn.Namespace, relation: [connection: :warehouse, catalog: :raw, schema: :shopify]
      use Favn.MultiAsset

      @relation true
      asset :orders do
        rest do
          path "/orders.json"
          data_path "orders"
        end
      end

      @relation true
      asset :customers do
        rest do
          path "/customers.json"
          data_path "customers"
        end
      end

      def asset(_ctx), do: :ok
    end
    """

    assert Enum.any?(Code.compile_string(source, "test/dynamic_multi_asset_test.exs"), fn {mod, _} ->
             mod == module_name
           end)

    [orders, customers] = module_name.__favn_assets__()

    assert orders.relation == %RelationRef{
             connection: :warehouse,
             catalog: "raw",
             schema: "shopify",
             name: "orders"
           }

    assert customers.relation == %RelationRef{
             connection: :warehouse,
             catalog: "raw",
             schema: "shopify",
             name: "customers"
           }
  end

  test "runtime executor invokes shared entrypoint and keeps generated ref" do
    module_name = Module.concat(__MODULE__, "Runtime#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module_name)} do
      use Favn.MultiAsset

      asset :orders do
        rest do
          path "/orders.json"
          data_path "orders"
        end
      end

      def asset(ctx), do: {:ok, %{seen_ref: ctx.asset.ref, seen_config: ctx.asset.config}}
    end
    """

    assert Enum.any?(Code.compile_string(source, "test/dynamic_multi_asset_test.exs"), fn {mod, _} ->
             mod == module_name
           end)

    [asset] = module_name.__favn_assets__()

    ctx = %Context{
      run_id: "run",
      target_refs: [asset.ref],
      current_ref: asset.ref,
      asset: %{ref: asset.ref, relation: nil, config: asset.config},
      params: %{},
      window: nil,
      run_started_at: DateTime.utc_now(),
      stage: 0,
      attempt: 1,
      max_attempts: 1
    }

    step_ref = {asset.ref, nil}

    assert {:ok, %{exec_ref: exec_ref}} = Local.start_step(asset, ctx, self(), step_ref)

    assert_receive {:executor_step_result, ^exec_ref, ^step_ref, {:ok, result_meta}}
    assert result_meta.seen_ref == asset.ref
    assert result_meta.seen_config == asset.config
  end

  test "supports local and tuple @depends refs" do
    module_name = Module.concat(__MODULE__, "Depends#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module_name)} do
      use Favn.MultiAsset

      asset :upstream do
        rest do
          path "/upstream.json"
          data_path "upstream"
        end
      end

      @depends :upstream
      @depends {#{inspect(Favn.AssetsTest.Upstream)}, :source_rows}
      asset :orders do
        rest do
          path "/orders.json"
          data_path "orders"
        end
      end

      def asset(_ctx), do: :ok
    end
    """

    assert Enum.any?(Code.compile_string(source, "test/dynamic_multi_asset_test.exs"), fn {mod, _} ->
             mod == module_name
           end)

    [_upstream, orders] = module_name.__favn_assets__()

    assert orders.depends_on == [
             {module_name, :upstream},
             {Favn.AssetsTest.Upstream, :source_rows}
           ]
  end

  test "rejects invalid declarations and stray attributes" do
    assert_raise CompileError, ~r/must define exactly one public asset\/1 function/, fn ->
      compile_test_module("""
      use Favn.MultiAsset

      asset :orders do
        rest do
          path "/orders.json"
          data_path "orders"
        end
      end
      """)
    end

    assert_raise CompileError, ~r/requires exactly one public asset\/1 function/, fn ->
      compile_test_module("""
      use Favn.MultiAsset

      asset :orders do
        rest do
          path "/orders.json"
          data_path "orders"
        end
      end

      def asset(_ctx, _opts), do: :ok
      """)
    end

    assert_raise CompileError, ~r/requires a public def asset\(ctx\)/, fn ->
      compile_test_module("""
      use Favn.MultiAsset

      asset :orders do
        rest do
          path "/orders.json"
          data_path "orders"
        end
      end

      defp asset(_ctx), do: :ok
      """)
    end

    assert_raise CompileError, ~r/duplicate asset name/, fn ->
      compile_test_module("""
      use Favn.MultiAsset

      asset :orders do
        rest do
          path "/orders.json"
          data_path "orders"
        end
      end

      asset :orders do
        rest do
          path "/orders-2.json"
          data_path "orders"
        end
      end

      def asset(_ctx), do: :ok
      """)
    end

    assert_raise CompileError, ~r/multiple defaults blocks are not allowed/, fn ->
      compile_test_module("""
      use Favn.MultiAsset

      defaults do
        meta owner: "a"
      end

      defaults do
        meta owner: "b"
      end

      asset :orders do
        rest do
          path "/orders.json"
          data_path "orders"
        end
      end

      def asset(_ctx), do: :ok
      """)
    end

    assert_raise CompileError,
                 ~r/requires asset :name do immediately below/,
                 fn ->
                   compile_test_module("""
                   use Favn.MultiAsset

                   @meta owner: "data"
                   def helper(_ctx), do: :ok

                   asset :orders do
                     rest do
                       path "/orders.json"
                       data_path "orders"
                     end
                   end

                   def asset(_ctx), do: :ok
                   """)
                 end

    assert_raise CompileError, ~r/rest.extra must be a keyword list or map/, fn ->
      compile_test_module("""
      use Favn.MultiAsset

      asset :orders do
        rest do
          path "/orders.json"
          data_path "orders"
          extra :bad
        end
      end

      def asset(_ctx), do: :ok
      """)
    end

    assert_raise CompileError,
                 ~r/module shorthand is not supported in Favn.MultiAsset/,
                 fn ->
                   compile_test_module("""
                   use Favn.MultiAsset

                   @depends #{inspect(Favn.AssetsTest.Upstream)}
                   asset :orders do
                     rest do
                       path "/orders.json"
                       data_path "orders"
                     end
                   end

                   def asset(_ctx), do: :ok
                   """)
                 end

    assert_raise CompileError,
                 ~r/requires asset :name do immediately below/,
                 fn ->
                   compile_test_module("""
                   use Favn.MultiAsset

                   @doc "should-not-attach"
                   defaults do
                     meta owner: "x"
                   end

                   asset :orders do
                     rest do
                       path "/orders.json"
                       data_path "orders"
                     end
                   end

                   def asset(_ctx), do: :ok
                   """)
                 end

    assert_raise CompileError,
                 ~r/requires asset :name do immediately below/,
                 fn ->
                   compile_test_module("""
                   use Favn.MultiAsset

                   asset :orders do
                     rest do
                       path "/orders.json"
                       data_path "orders"
                     end
                   end

                   @meta owner: "late"

                   def asset(_ctx), do: :ok
                   """)
                 end
  end

  defp compile_test_module(module_body) do
    module_name = Module.concat(__MODULE__, "Invalid#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module_name)} do
      #{module_body}
    end
    """

    Code.compile_string(source, "test/dynamic_multi_asset_test.exs")
  end
end
