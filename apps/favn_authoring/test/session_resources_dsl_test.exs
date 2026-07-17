defmodule FavnAuthoring.SessionResourcesDSLTest do
  use ExUnit.Case, async: true

  alias Favn.SQL.SessionRequirements
  alias Favn.SQLAsset.Compiler

  defmodule Lakehouse do
    use Favn.Namespace

    resources([:azure_extension, :common_session])
  end

  defmodule Lakehouse.Raw do
    use Favn.Namespace

    relation(connection: :warehouse, schema: :raw)
    resources([:landing_storage, :common_session])
  end

  defmodule Lakehouse.Raw.Orders do
    use Favn.SQLAsset

    resources([:orders_api, :landing_storage])
    materialized(:table)

    query do
      ~SQL"select 1 as order_id"
    end
  end

  test "SQL assets inherit additive namespace resources and add local resources" do
    assert {:ok, definition} = Compiler.fetch_definition(Lakehouse.Raw.Orders)

    assert definition.session_requirements == %SessionRequirements{
             version: 1,
             resources: [
               "azure_extension",
               "common_session",
               "landing_storage",
               "orders_api"
             ]
           }

    assert definition.asset.session_requirements == definition.session_requirements
  end

  test "resource declarations must be lists placed before query" do
    module = Module.concat(__MODULE__, "Invalid#{System.unique_integer([:positive])}")

    assert_raise CompileError, ~r/resources must be a list/, fn ->
      Code.compile_string("""
      defmodule #{inspect(module)} do
        use Favn.SQLAsset
        resources :landing_storage
        materialized :table
        query do
          ~SQL"select 1"
        end
      end
      """)
    end

    late_module = Module.concat(__MODULE__, "Late#{System.unique_integer([:positive])}")

    assert_raise CompileError, ~r/SQL asset declarations must appear before query/, fn ->
      Code.compile_string("""
      defmodule #{inspect(late_module)} do
        use Favn.SQLAsset
        materialized :table
        query do
          ~SQL"select 1"
        end
        resources [:landing_storage]
      end
      """)
    end
  end

  test "namespace resources reject unstable names" do
    module = Module.concat(__MODULE__, "InvalidNamespace#{System.unique_integer([:positive])}")

    assert_raise CompileError, ~r/lowercase snake_case/, fn ->
      Code.compile_string("""
      defmodule #{inspect(module)} do
        use Favn.Namespace
        resources ["Landing-Storage"]
      end
      """)
    end
  end
end
