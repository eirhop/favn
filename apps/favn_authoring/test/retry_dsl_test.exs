defmodule Favn.RetryDSLTest do
  use ExUnit.Case, async: false

  alias Favn.Retry.Backoff
  alias Favn.Retry.Policy

  test "pipeline retry compiles to the shared policy" do
    module = unique_module("Pipeline")

    compile_module!(module, """
    defmodule #{inspect(module)} do
      use Favn.Pipeline

      pipeline :daily do
        asset {#{inspect(module)}, :asset}
        retry max_attempts: 3,
              backoff: {:exponential, initial: 100, max: 1_000, jitter: 0.1}
      end
    end
    """)

    assert %Policy{max_attempts: 3, backoff: %Backoff{strategy: :exponential}} =
             module.__favn_pipeline__().retry_policy
  end

  test "Elixir and SQL assets share retry" do
    asset_module = unique_module("Asset")
    sql_module = unique_module("SQL")

    compile_module!(asset_module, """
    defmodule #{inspect(asset_module)} do
      use Favn.Asset
      retry max_attempts: 2, backoff: 10
      def asset(_ctx), do: :ok
    end
    """)

    compile_module!(sql_module, """
    defmodule #{inspect(sql_module)} do
      use Favn.Namespace, relation: [connection: :warehouse]
      use Favn.SQLAsset
      retry max_attempts: 4, backoff: 20
      materialized :table
      query do
        ~SQL"select 1 as id"
      end
    end
    """)

    assert [%Favn.Asset{retry_policy: %Policy{max_attempts: 2}}] =
             asset_module.__favn_assets__()

    assert %{asset: %Favn.Asset{retry_policy: %Policy{max_attempts: 4}}} =
             sql_module.__favn_sql_asset_definition__()
  end

  test "invalid policies fail while compiling the declaration" do
    module = unique_module("Invalid")

    assert_raise ArgumentError, ~r/invalid retry policy/, fn ->
      compile_module!(module, """
      defmodule #{inspect(module)} do
        use Favn.Asset
        retry max_attempts: 0
        def asset(_ctx), do: :ok
      end
      """)
    end
  end

  defp compile_module!(module, source) do
    assert [{^module, _binary}] = Code.compile_string(source, "test/retry_dsl_test.exs")
  end

  defp unique_module(prefix) do
    Module.concat(__MODULE__, "#{prefix}#{System.unique_integer([:positive])}")
  end
end
