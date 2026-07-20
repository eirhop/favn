defmodule Favn.Local.CanonicalSampleProject do
  @moduledoc false

  @repo_root Path.expand("../../..", __DIR__)

  def create!(prefix \\ "favn_issue262_canonical") do
    project_dir =
      Path.join(
        native_tmp_dir(),
        "#{prefix}_#{System.unique_integer([:positive])}"
      )

    write!(project_dir, "mix.exs", mix_exs())
    write!(project_dir, "mix.lock", "%{}\n")
    write!(project_dir, "config/config.exs", config_exs())
    write!(project_dir, "lib/favn_issue262_sample/connections/warehouse.ex", connection_ex())
    write!(project_dir, "lib/favn_issue262_sample/runtime_configs.ex", runtime_configs_ex())
    write!(project_dir, "lib/favn_issue262_sample/assets/source_check.ex", source_check_ex())
    write!(project_dir, "lib/favn_issue262_sample/assets/missing_secret.ex", missing_secret_ex())
    write!(project_dir, "lib/favn_issue262_sample/lakehouse.ex", lakehouse_ex())
    write!(project_dir, "lib/favn_issue262_sample/lakehouse/raw.ex", raw_ex())
    write!(project_dir, "lib/favn_issue262_sample/lakehouse/raw/sales.ex", raw_sales_ex())
    write!(project_dir, "lib/favn_issue262_sample/lakehouse/raw/sales/orders.ex", orders_ex())
    write!(project_dir, "lib/favn_issue262_sample/lakehouse/mart.ex", mart_ex())
    write!(project_dir, "lib/favn_issue262_sample/lakehouse/mart/sales.ex", mart_sales_ex())

    write!(
      project_dir,
      "lib/favn_issue262_sample/lakehouse/mart/sales/order_summary.ex",
      order_summary_ex()
    )

    write!(project_dir, "lib/favn_issue262_sample/pipelines/production_smoke.ex", pipeline_ex())

    project_dir
  end

  defp write!(project_dir, relative, contents) do
    path = Path.join(project_dir, relative)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, contents)
  end

  defp native_tmp_dir do
    if match?({:unix, _}, :os.type()) and File.dir?("/tmp"), do: "/tmp", else: System.tmp_dir!()
  end

  defp mix_exs do
    """
    defmodule FavnIssue262Sample.MixProject do
      use Mix.Project

      def project do
        [
          app: :favn_issue262_sample,
          version: "0.1.0",
          elixir: "~> 1.20",
          start_permanent: Mix.env() == :prod,
          deps: deps()
        ]
      end

      def application do
        [extra_applications: [:logger]]
      end

      defp deps do
        [
          {:favn, path: #{inspect(Path.join(@repo_root, "apps/favn"))}},
          {:favn_duckdb, path: #{inspect(Path.join(@repo_root, "apps/favn_duckdb"))}}
        ]
      end
    end
    """
  end

  defp config_exs do
    """
    import Config

    config :favn,
      asset_modules: [
        FavnIssue262Sample.Assets.SourceCheck,
        FavnIssue262Sample.Assets.MissingSecret,
        FavnIssue262Sample.Lakehouse.Raw.Sales.Orders,
        FavnIssue262Sample.Lakehouse.Mart.Sales.OrderSummary
      ],
      pipeline_modules: [FavnIssue262Sample.Pipelines.ProductionSmoke],
      schedule_modules: [],
      connection_modules: [FavnIssue262Sample.Connections.Warehouse],
      connections: [
        issue262_warehouse: [
          open: [
            database: %{
              __struct__: Favn.RuntimeConfig.Ref,
              provider: :env,
              key: "FAVN_CANONICAL_DUCKDB_PATH",
              secret?: false,
              required?: true
            }
          ],
          duckdb: []
        ]
      ],
      runner_plugins: [
        {FavnDuckdb, execution_mode: :in_process}
      ]
    """
  end

  defp connection_ex do
    """
    defmodule FavnIssue262Sample.Connections.Warehouse do
      @moduledoc false

      @behaviour Favn.Connection

      @impl true
      def definition do
        %Favn.Connection.Definition{
          name: :issue262_warehouse,
          adapter: Favn.SQL.Adapter.DuckDB,
          doc: "Canonical DuckDB warehouse",
          metadata: %{scope: :issue262_acceptance},
          config_schema: Favn.SQL.Adapter.DuckDB.config_schema_fields()
        }
      end
    end
    """
  end

  defp source_check_ex do
    """
    defmodule FavnIssue262Sample.Assets.SourceCheck do
      @moduledoc false

      use Favn.Asset

      meta owner: "acceptance", category: :source, tags: [:issue262]
      runtime_config FavnIssue262Sample.RuntimeConfigs.source_system()

      def asset(ctx) do
        true = ctx.runtime_config.source_system.name != ""
        true = ctx.runtime_config.source_system.token != ""
        :ok
      end
    end
    """
  end

  defp missing_secret_ex do
    """
    defmodule FavnIssue262Sample.Assets.MissingSecret do
      @moduledoc false

      use Favn.Asset

      meta owner: "acceptance", category: :failure_path, tags: [:issue262]
      runtime_config FavnIssue262Sample.RuntimeConfigs.missing_source()

      def asset(_ctx), do: :ok
    end
    """
  end

  defp runtime_configs_ex do
    """
    defmodule FavnIssue262Sample.RuntimeConfigs do
      @moduledoc false

      use Favn.RuntimeConfig

      bundle :source_system,
        name: env!("FAVN_CANONICAL_SOURCE_NAME"),
        token: secret_env!("FAVN_CANONICAL_SOURCE_TOKEN")

      bundle :missing_source,
        token: secret_env!("FAVN_CANONICAL_MISSING_SECRET")
    end
    """
  end

  defp lakehouse_ex do
    """
    defmodule FavnIssue262Sample.Lakehouse do
      @moduledoc false

      use Favn.Namespace
      relation connection: :issue262_warehouse
    end
    """
  end

  defp raw_ex do
    """
    defmodule FavnIssue262Sample.Lakehouse.Raw do
      @moduledoc false

      use Favn.Namespace
    end
    """
  end

  defp raw_sales_ex do
    """
    defmodule FavnIssue262Sample.Lakehouse.Raw.Sales do
      @moduledoc false

      use Favn.Namespace
      relation schema: "raw"
    end
    """
  end

  defp orders_ex do
    ~S'''
    defmodule FavnIssue262Sample.Lakehouse.Raw.Sales.Orders do
      @moduledoc false

      use Favn.Asset

      alias Favn.SQLClient

      meta owner: "acceptance", category: :orders, tags: [:issue262, :raw]
      depends FavnIssue262Sample.Assets.SourceCheck
      relation true

      def asset(ctx) do
        relation = ctx.asset.relation

        SQLClient.with_connection(relation.connection, [], fn session ->
          with {:ok, _} <- SQLClient.execute(session, create_schema_sql(relation)),
               {:ok, _} <- SQLClient.execute(session, orders_sql(relation)) do
            :ok
          end
        end)
      end

      defp create_schema_sql(relation) do
        ["create schema if not exists ", quote_ident(relation.schema)]
      end

      defp orders_sql(relation) do
        """
        create or replace table #{qualified_relation(relation)} as
        select *
        from (
          values
            (1, 'Ada Labs', date '2026-01-01', 12000),
            (2, 'Beam Goods', date '2026-01-01', 8500),
            (3, 'Query Co', date '2026-01-02', 1575)
        ) as orders(order_id, customer_name, order_date, amount_cents)
        """
      end

      defp qualified_relation(relation) do
        [quote_ident(relation.schema), ".", quote_ident(relation.name)]
      end

      defp quote_ident(value) do
        [~s("), String.replace(to_string(value), ~s("), ~s("")), ~s(")]
      end
    end
    '''
  end

  defp mart_ex do
    """
    defmodule FavnIssue262Sample.Lakehouse.Mart do
      @moduledoc false

      use Favn.Namespace
    end
    """
  end

  defp mart_sales_ex do
    """
    defmodule FavnIssue262Sample.Lakehouse.Mart.Sales do
      @moduledoc false

      use Favn.Namespace
      relation schema: "mart"
    end
    """
  end

  defp order_summary_ex do
    """
    defmodule FavnIssue262Sample.Lakehouse.Mart.Sales.OrderSummary do
      @moduledoc false

      use Favn.SQLAsset

      meta owner: "acceptance", category: :orders, tags: [:issue262, :mart]
      depends FavnIssue262Sample.Lakehouse.Raw.Sales.Orders
      materialized :table
      relation true

      query do
        ~SQL\"\"\"
        select
          first_order.order_date,
          2 as order_count,
          first_order.amount_cents + second_order.amount_cents as revenue_cents
        from raw.orders as first_order
        join raw.orders as second_order
          on second_order.order_id = 2
        where first_order.order_id = 1
        union all
        select
          orders.order_date,
          1 as order_count,
          orders.amount_cents as revenue_cents
        from raw.orders as orders
        where orders.order_id = 3
        order by order_date
        \"\"\"
      end
    end
    """
  end

  defp pipeline_ex do
    """
    defmodule FavnIssue262Sample.Pipelines.ProductionSmoke do
      @moduledoc false

      use Favn.Pipeline

      pipeline :production_smoke do
        asset FavnIssue262Sample.Lakehouse.Mart.Sales.OrderSummary
        deps :all
        settings requested_by: "issue-262-acceptance"
        meta owner: "acceptance", purpose: :single_node_production_readiness
        schedule cron: "0 2 * * *", timezone: "Etc/UTC", active: true, missed: :skip
      end
    end
    """
  end
end
