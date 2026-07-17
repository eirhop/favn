defmodule Favn.NamespaceDefaultsDSLTest do
  use ExUnit.Case, async: false

  alias Favn.Freshness.Policy
  alias Favn.Assets.Compiler
  alias Favn.RuntimeConfig.Ref
  alias Favn.SQL.SessionRequirements
  alias Favn.SQLAsset.RuntimeInputs
  alias Favn.Window.Spec

  defmodule Inputs do
    @behaviour RuntimeInputs

    alias RuntimeInputs.Result

    @impl true
    def resolve(_context), do: {:ok, %Result{params: %{}, identity: "namespace-defaults"}}
  end

  defmodule Platform do
    use Favn.Namespace

    relation(connection: :warehouse, catalog: "lakehouse")
    resources([:azure_extension])
    settings(environment: "production", request: %{timeout: 5})
    meta(owner: "platform", category: :shared)
    runtime_config(:platform, environment: env!("PLATFORM_ENVIRONMENT"))
    runtime_inputs(Inputs)
    freshness(:always)
  end

  defmodule Platform.Raw do
    use Favn.Namespace

    relation(schema: "raw")
    resources([:landing_storage])
    settings(request: %{timeout: 10}, layer: :raw)
    meta(category: :raw, tags: [:landing])
    window(Favn.Window.daily())
    materialized(:table)
  end

  defmodule Platform.Raw.Orders do
    use Favn.SQLAsset

    settings(dataset: "orders")
    meta(category: :orders)
    runtime_config(:source, token: secret_env!("ORDERS_TOKEN"))
    resources([:orders_api])
    relation(true)

    query do
      ~SQL"select 1 as order_id"
    end
  end

  defmodule Platform.Raw.Customers do
    use Favn.Asset

    settings(dataset: "customers")
    relation(true)
    def asset(_ctx), do: :ok
  end

  defmodule Platform.Raw.OrderPreview do
    use Favn.SQLAsset

    materialized(:view)

    query do
      ~SQL"select 1 as order_id"
    end
  end

  defmodule Platform.Raw.Extracts do
    use Favn.MultiAsset

    settings(method: "GET", request: %{timeout: 20})
    meta(category: :extract)

    asset :orders do
      settings(path: "/orders")
      meta(tags: [:orders])
    end

    asset :customers do
      settings(path: "/customers", request: %{timeout: 30})
      freshness(nil)
    end

    def asset(_ctx), do: :ok
  end

  defmodule Platform.Raw.Payments do
    @moduledoc "Externally managed payments."
    use Favn.Source

    meta(category: :payments)
    relation(name: "payments")
  end

  defmodule Clearing do
    use Favn.Namespace

    relation(connection: :warehouse)
    runtime_inputs(Inputs)
    freshness(:always)
    window(Favn.Window.monthly())
  end

  defmodule Clearing.Optional do
    use Favn.Namespace

    runtime_inputs(nil)
    freshness(nil)
    window(nil)
    materialized(:table)
  end

  defmodule Clearing.Optional.Snapshot do
    use Favn.SQLAsset

    query do
      ~SQL"select 1 as id"
    end
  end

  test "SQL assets merge structural namespace defaults with leaf declarations" do
    definition = Platform.Raw.Orders.__favn_sql_asset_definition__()
    asset = definition.asset

    assert asset.relation.connection == :warehouse
    assert asset.relation.catalog == "lakehouse"
    assert asset.relation.schema == "raw"
    assert asset.relation.name == "orders"

    assert asset.settings == %{
             environment: "production",
             request: %{"timeout" => 10},
             layer: "raw",
             dataset: "orders"
           }

    assert asset.meta == %{owner: "platform", category: "orders", tags: ["landing"]}

    assert asset.runtime_config == %{
             platform: %{environment: Ref.env!("PLATFORM_ENVIRONMENT")},
             source: %{token: Ref.secret_env!("ORDERS_TOKEN")}
           }

    assert definition.runtime_inputs.module == Inputs
    assert asset.window_spec == %Spec{kind: :day}
    assert asset.freshness == %Policy{mode: :always}
    assert definition.materialization == :table

    assert definition.session_requirements == %SessionRequirements{
             version: 1,
             resources: ["azure_extension", "landing_storage", "orders_api"]
           }
  end

  test "Asset, MultiAsset, and Source consume only their compatible inherited declarations" do
    assert {:ok, [customers]} = Compiler.compile_module_assets(Platform.Raw.Customers)
    assert customers.settings.request == %{"timeout" => 10}
    assert customers.settings.dataset == "customers"
    assert customers.meta == %{owner: "platform", category: "raw", tags: ["landing"]}
    assert customers.window_spec == %Spec{kind: :day}
    assert customers.freshness == %Policy{mode: :always}
    assert Map.has_key?(customers.runtime_config, :platform)

    assert {:ok, [orders, extracts_customers]} =
             Compiler.compile_module_assets(Platform.Raw.Extracts)

    assert orders.settings.request == %{"timeout" => 20}
    assert orders.settings.path == "/orders"
    assert orders.meta == %{owner: "platform", category: "extract", tags: ["orders"]}
    assert orders.freshness == %Policy{mode: :always}

    assert extracts_customers.settings.request == %{"timeout" => 30}
    assert extracts_customers.freshness == nil

    assert {:ok, [source]} = Compiler.compile_module_assets(Platform.Raw.Payments)
    assert source.meta == %{owner: "platform", category: "payments", tags: ["landing"]}
    assert source.settings == %{}
    assert source.runtime_config == %{}
  end

  test "nil in a closer namespace clears optional scalar defaults" do
    definition = Clearing.Optional.Snapshot.__favn_sql_asset_definition__()

    assert definition.runtime_inputs == nil
    assert definition.asset.window_spec == nil
    assert definition.asset.freshness == nil
    assert definition.materialization == :table
  end

  test "leaf scalar declarations override namespace defaults" do
    definition = Platform.Raw.OrderPreview.__favn_sql_asset_definition__()

    assert definition.materialization == :view
    assert definition.runtime_inputs.module == Inputs
  end

  test "clearing an inherited resolver still validates effective runtime requirements" do
    module =
      Module.concat(
        Platform.Raw,
        "MissingInputs#{System.unique_integer([:positive])}"
      )

    assert_raise CompileError,
                 ~r/runtime_config requires runtime_inputs so the resolved values have an explicit consumer/,
                 fn ->
                   Code.compile_string("""
                   defmodule #{inspect(module)} do
                     use Favn.SQLAsset

                     runtime_inputs nil

                     query do
                       ~SQL"select 1 as id"
                     end
                   end
                   """)

                   module.__favn_sql_asset_definition__()
                 end
  end

  test "a module cannot combine structural and executable DSL roles" do
    module = Module.concat(__MODULE__, "Mixed#{System.unique_integer([:positive])}")

    assert_raise CompileError, ~r/cannot combine Favn.Namespace with Favn.SQLAsset/, fn ->
      Code.compile_string("""
      defmodule #{inspect(module)} do
        use Favn.Namespace
        use Favn.SQLAsset
      end
      """)
    end
  end

  test "leaves compiled before their namespace resolve the later namespace deterministically" do
    root = Module.concat(__MODULE__, "LateNamespace#{System.unique_integer([:positive])}")
    single = Module.concat(root, Single)
    multi = Module.concat(root, Multi)
    source = Module.concat(root, External)
    sql = Module.concat(root, SQL)

    Code.compile_string("""
    defmodule #{inspect(single)} do
      use Favn.Asset
      settings leaf: :single
      relation true
      def asset(_ctx), do: :ok
    end

    defmodule #{inspect(multi)} do
      use Favn.MultiAsset

      asset :orders do
        settings leaf: :multi
      end

      def asset(_ctx), do: :ok
    end

    defmodule #{inspect(source)} do
      use Favn.Source
      relation true
    end

    defmodule #{inspect(sql)} do
      use Favn.SQLAsset

      materialized :table
      query do
        ~SQL"select 1 as id"
      end
    end
    """)

    Code.compile_string("""
    defmodule #{inspect(root)} do
      use Favn.Namespace

      relation connection: :warehouse
      settings inherited: true
      meta owner: "late-namespace"
      runtime_config :shared, token: secret_env!("LATE_NAMESPACE_TOKEN")
      runtime_inputs Favn.NamespaceDefaultsDSLTest.Inputs
      freshness :always
      window Favn.Window.daily()
    end
    """)

    assert [single_asset] = single.__favn_assets__()
    assert single_asset.settings == %{inherited: true, leaf: "single"}
    assert single_asset.meta.owner == "late-namespace"
    assert single_asset.window_spec.kind == :day
    assert single_asset.freshness.mode == :always
    assert single_asset.runtime_config.shared.token.secret?

    assert [multi_asset] = multi.__favn_assets__()
    assert multi_asset.settings == %{inherited: true, leaf: "multi"}
    assert multi_asset.meta.owner == "late-namespace"
    assert multi_asset.runtime_config.shared.token.secret?

    assert [source_asset] = source.__favn_assets__()
    assert source_asset.meta.owner == "late-namespace"

    sql_definition = sql.__favn_sql_asset_definition__()
    assert sql_definition.asset.settings == %{inherited: true}
    assert sql_definition.asset.meta.owner == "late-namespace"
    assert sql_definition.runtime_inputs.module == Inputs
    assert sql_definition.asset.runtime_config.shared.token.secret?
  end
end
