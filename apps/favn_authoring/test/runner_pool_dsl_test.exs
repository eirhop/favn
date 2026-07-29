defmodule Favn.RunnerPoolDSLTest do
  use ExUnit.Case, async: false

  test "single and generated assets capture arbitrary runner pools" do
    single = unique_module("Single")

    compile_module!(single, """
    defmodule #{inspect(single)} do
      use Favn.Asset

      runner_pool :pure_elixir
      def asset(_ctx), do: :ok
    end
    """)

    assert [%Favn.Asset{runner_pool: :pure_elixir}] = single.__favn_assets__()

    multi = unique_module("Multi")

    compile_module!(multi, """
    defmodule #{inspect(multi)} do
      use Favn.MultiAsset

      runner_pool :duckdb

      asset :orders do
        settings path: "/orders"
      end

      def asset(_ctx), do: :ok
    end
    """)

    assert [%Favn.Asset{name: :orders, runner_pool: :duckdb}] = multi.__favn_assets__()
  end

  test "asset override and pipeline default remain distinct" do
    asset = unique_module("GpuAsset")

    compile_module!(asset, """
    defmodule #{inspect(asset)} do
      use Favn.Asset

      runner_pool :gpu
      def asset(_ctx), do: :ok
    end
    """)

    pipeline = unique_module("Pipeline")

    compile_module!(pipeline, """
    defmodule #{inspect(pipeline)} do
      use Favn.Pipeline

      pipeline :daily do
        asset #{inspect(asset)}
        runner_pool :duckdb
      end
    end
    """)

    assert pipeline.__favn_pipeline__().runner_pool == :duckdb
    assert [%Favn.Asset{runner_pool: :gpu}] = asset.__favn_assets__()
  end

  test "invalid runner pools fail at authoring time" do
    module = unique_module("Invalid")

    assert_raise ArgumentError, ~r/runner_pool/, fn ->
      compile_module!(module, """
      defmodule #{inspect(module)} do
        use Favn.Asset

        runner_pool :"has space"
        def asset(_ctx), do: :ok
      end
      """)
    end
  end

  defp compile_module!(module, source) do
    assert [{^module, _binary}] = Code.compile_string(source, "test/runner_pool_dsl_test.exs")
  end

  defp unique_module(prefix) do
    Module.concat(__MODULE__, "#{prefix}#{System.unique_integer([:positive])}")
  end
end
