defmodule Favn.FreshnessDSLTest do
  use ExUnit.Case, async: false

  alias Favn.Freshness.Policy
  alias Favn.SQLAsset.Definition

  test "single asset defaults windowed assets to window-success freshness" do
    module = unique_module("WindowedSingle")

    compile_module!(module, """
    defmodule #{inspect(module)} do
      use Favn.Asset

      @window Favn.Window.daily()
      def asset(_ctx), do: :ok
    end
    """)

    assert [%Favn.Asset{freshness: %Policy{mode: :window_success}}] = module.__favn_assets__()
  end

  test "single asset leaves non-windowed freshness unset" do
    module = unique_module("NonWindowedSingle")

    compile_module!(module, """
    defmodule #{inspect(module)} do
      use Favn.Asset

      def asset(_ctx), do: :ok
    end
    """)

    assert [%Favn.Asset{freshness: nil}] = module.__favn_assets__()
  end

  test "explicit always freshness overrides window default" do
    module = unique_module("AlwaysSingle")

    compile_module!(module, """
    defmodule #{inspect(module)} do
      use Favn.Asset

      @window Favn.Window.daily()
      @freshness :always
      def asset(_ctx), do: :ok
    end
    """)

    assert [%Favn.Asset{freshness: %Policy{mode: :always}}] = module.__favn_assets__()
  end

  test "freshness normalizes daily calendar policies with timezone" do
    module = unique_module("DailyOsloSingle")

    compile_module!(module, """
    defmodule #{inspect(module)} do
      use Favn.Asset

      @freshness {:daily, timezone: "Europe/Oslo"}
      def asset(_ctx), do: :ok
    end
    """)

    assert [asset] = module.__favn_assets__()

    assert asset.freshness == %Policy{
             mode: :calendar_period,
             kind: :day,
             timezone: "Europe/Oslo"
           }
  end

  test "function asset DSL supports max-age and window-success freshness forms" do
    module = unique_module("FunctionAssets")

    compile_module!(module, """
    defmodule #{inspect(module)} do
      use Favn.Assets

      @asset true
      @freshness max_age: {:hours, 24}
      def max_age_asset(_ctx), do: :ok

      @asset true
      @freshness window_success: true
      def window_success_asset(_ctx), do: :ok
    end
    """)

    assert [max_age_asset, window_success_asset] = module.__favn_assets__()
    assert max_age_asset.freshness == %Policy{mode: :max_age, amount: 24, unit: :hour}
    assert window_success_asset.freshness == %Policy{mode: :window_success}
  end

  test "SQL asset captures explicit freshness" do
    module = unique_module("SQLDailyOslo")

    compile_module!(module, """
    defmodule #{inspect(module)} do
      use Favn.Namespace, relation: [connection: :warehouse]
      use Favn.SQLAsset

      @materialized :table
      @freshness {:daily, timezone: "Europe/Oslo"}
      query do
        ~SQL"select 1 as id"
      end
    end
    """)

    assert %Definition{asset: asset} = module.__favn_sql_asset_definition__()

    assert asset.freshness == %Policy{
             mode: :calendar_period,
             kind: :day,
             timezone: "Europe/Oslo"
           }
  end

  test "SQL asset defaults windowed assets to window-success freshness" do
    module = unique_module("SQLWindowed")

    compile_module!(module, """
    defmodule #{inspect(module)} do
      use Favn.Namespace, relation: [connection: :warehouse]
      use Favn.SQLAsset

      @materialized {:incremental, strategy: :delete_insert, window_column: :id}
      @window Favn.Window.daily()
      query do
        ~SQL"select 1 as id"
      end
    end
    """)

    assert %Definition{asset: %Favn.Asset{freshness: %Policy{mode: :window_success}}} =
             module.__favn_sql_asset_definition__()
  end

  test "invalid freshness raises a compile error" do
    module = unique_module("InvalidFreshness")

    assert_raise CompileError, ~r/invalid freshness policy/, fn ->
      compile_module!(module, """
      defmodule #{inspect(module)} do
        use Favn.Asset

        @freshness :hourly
        def asset(_ctx), do: :ok
      end
      """)
    end
  end

  test "multiple freshness attributes raise a compile error" do
    module = unique_module("MultipleFreshness")

    assert_raise CompileError, ~r/multiple @freshness attributes are not allowed/, fn ->
      compile_module!(module, """
      defmodule #{inspect(module)} do
        use Favn.Asset

        @freshness :daily
        @freshness :always
        def asset(_ctx), do: :ok
      end
      """)
    end
  end

  test "stray freshness attributes must be attached to an asset declaration" do
    module = unique_module("StrayFreshness")

    assert_raise CompileError,
                 ~r/@depends\/@freshness\/@meta\/@window\/@relation on def helper\/1 requires def asset\(ctx\)/,
                 fn ->
                   compile_module!(module, """
                   defmodule #{inspect(module)} do
                     use Favn.Asset

                     @freshness :daily
                     def helper(_ctx), do: :ok

                     def asset(_ctx), do: :ok
                   end
                   """)
                 end
  end

  defp compile_module!(module, source) do
    assert [{^module, _binary}] = Code.compile_string(source, "test/freshness_dsl_test.exs")
  end

  defp unique_module(prefix) do
    Module.concat(__MODULE__, "#{prefix}#{System.unique_integer([:positive])}")
  end
end
