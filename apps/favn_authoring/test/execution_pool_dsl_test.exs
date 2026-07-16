defmodule Favn.ExecutionPoolDSLTest do
  use ExUnit.Case, async: false

  test "single asset captures execution pool" do
    module = unique_module("Single")

    compile_module!(module, """
    defmodule #{inspect(module)} do
      use Favn.Asset

      execution_pool :github_api
      def asset(_ctx), do: :ok
    end
    """)

    assert [%Favn.Asset{execution_pool: :github_api}] = module.__favn_assets__()
  end

  test "multi asset captures per-asset execution pool" do
    module = unique_module("Multi")

    compile_module!(module, """
    defmodule #{inspect(module)} do
      use Favn.MultiAsset

      execution_pool :shopify_api
      asset :orders do
        settings path: "/orders.json"
      end

      def asset(_ctx), do: :ok
    end
    """)

    assert [%Favn.Asset{name: :orders, execution_pool: :shopify_api}] = module.__favn_assets__()
  end

  test "invalid asset execution pool raises a compile error" do
    module = unique_module("Invalid")

    assert_raise CompileError, ~r/invalid execution_pool value/, fn ->
      compile_module!(module, """
      defmodule #{inspect(module)} do
        use Favn.Asset

        execution_pool "github_api"
        def asset(_ctx), do: :ok
      end
      """)
    end
  end

  defp compile_module!(module, source) do
    assert [{^module, _binary}] = Code.compile_string(source, "test/execution_pool_dsl_test.exs")
  end

  defp unique_module(prefix) do
    Module.concat(__MODULE__, "#{prefix}#{System.unique_integer([:positive])}")
  end
end
