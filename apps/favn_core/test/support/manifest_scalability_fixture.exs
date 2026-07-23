defmodule FavnTestSupport.ManifestScalabilityFixture do
  @moduledoc """
  Builds deterministic SQL-heavy manifests for repeatable scalability measurements.

  The fixture models the current manifest schema: compact SQL asset metadata
  and content hashes while generated SQL/template IR lives in immutable packages.
  """

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Graph
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.TargetDescriptor
  alias Favn.RelationRef
  alias Favn.SQL.Check
  alias Favn.SQL.Contract
  alias Favn.SQL.Contract.Column
  alias Favn.SQL.SessionRequirements
  alias Favn.SQL.Template

  # Calibrated to issue #483's observed ~4.5 MB canonical manifest at 66 assets.
  @default_sql_columns 14
  @default_contract_columns 14
  @maximum_assets 10_000
  @maximum_sql_columns 1_000
  @allowed_opts [:sql_columns, :contract_columns]

  @type option ::
          {:sql_columns, pos_integer()}
          | {:contract_columns, pos_integer()}

  @doc "Builds one deterministic SQL-heavy manifest with `asset_count` assets."
  @spec build(pos_integer(), [option()]) :: Manifest.t()
  def build(asset_count, opts \\ [])

  def build(asset_count, opts) when is_integer(asset_count) and is_list(opts) do
    {manifest, _packages} = build_with_packages(asset_count, opts)
    manifest
  end

  def build(asset_count, _opts) do
    raise ArgumentError,
          "manifest scalability asset_count must be an integer in 1..#{@maximum_assets}, got: #{inspect(asset_count)}"
  end

  @doc "Builds a compact manifest and its exact execution package set."
  @spec build_with_packages(pos_integer(), [option()]) :: {Manifest.t(), [ExecutionPackage.t()]}
  def build_with_packages(asset_count, opts \\ [])

  def build_with_packages(asset_count, opts) when is_integer(asset_count) and is_list(opts) do
    config = validate_options!(asset_count, opts)
    contract = contract(config.contract_columns)

    asset_packages =
      Enum.map(1..asset_count, fn index ->
        asset(index, config, contract)
      end)

    assets = Enum.map(asset_packages, &elem(&1, 0))
    packages = Enum.map(asset_packages, &elem(&1, 1))

    {:ok, graph} = Graph.build(assets)

    manifest =
      %{
        assets: assets,
        graph: graph,
        metadata: %{
          fixture: "sql_heavy_manifest_scalability",
          fixture_version: 2,
          sql_columns_per_asset: config.sql_columns,
          contract_columns_per_asset: config.contract_columns
        }
      }
      |> FavnTestSupport.with_manifest_contract()
      |> then(&struct!(Manifest, &1))

    {manifest, packages}
  end

  def build_with_packages(asset_count, _opts) do
    raise ArgumentError,
          "manifest scalability asset_count must be an integer in 1..#{@maximum_assets}, got: #{inspect(asset_count)}"
  end

  @doc "Returns the default number of generated SQL projection columns per asset."
  @spec default_sql_columns() :: pos_integer()
  def default_sql_columns, do: @default_sql_columns

  @doc "Returns the default number of manifest contract columns per asset."
  @spec default_contract_columns() :: pos_integer()
  def default_contract_columns, do: @default_contract_columns

  defp asset(index, config, contract) do
    module = asset_module(index)
    ref = {module, :asset}
    sql = sql(index, config.sql_columns)
    file = "scalability/asset_#{padded(index, 5)}.sql"

    template = compile_template(sql, file, module)

    execution = %SQLExecution{
      sql: sql,
      template: template,
      runtime_inputs: nil,
      contract: contract,
      sql_definitions: [],
      checks: checks(file, module)
    }

    {:ok, package} = ExecutionPackage.new(ref, execution)

    asset = %Asset{
      ref: ref,
      module: module,
      name: :asset,
      type: :sql,
      depends_on: dependencies(index),
      execution: %{entrypoint: :asset, arity: 1},
      description:
        "Synthetic SQL-heavy analytics asset #{padded(index, 5)} used for manifest scalability measurement.",
      relation:
        RelationRef.new!(
          connection: :warehouse,
          catalog: "analytics",
          schema: "manifest_scale",
          name: "asset_#{padded(index, 5)}"
        ),
      materialization:
        {:incremental,
         strategy: :delete_insert, unique_key: [:metric_001], window_column: :event_date},
      session_requirements: SessionRequirements.new!([:analytics_catalog, :quality_macros]),
      execution_package_hash: package.content_hash,
      assurance: %{
        contract: contract,
        checks:
          Enum.map(execution.checks, fn check ->
            Map.take(check, [:name, :origin, :claim_id, :at, :when, :on_violation, :message])
          end)
      },
      metadata: %{
        owner: "analytics-platform",
        domain: "manifest-scalability",
        category: "gold",
        tags: ["sql", "synthetic", "scalability"]
      }
    }

    descriptor =
      TargetDescriptor.from_asset(Map.from_struct(asset),
        connection_definitions: %{
          warehouse: %{
            adapter: FavnTestSupport.ManifestScale.Adapter,
            module: FavnTestSupport.ManifestScale.Connection
          }
        },
        manifest_schema_version: 12,
        runner_contract_version: 12
      )

    {%{asset | target_descriptor: descriptor}, package}
  end

  defp sql(index, column_count) do
    projections =
      Enum.map_join(1..column_count, ",\n", fn column ->
        name = column_name(column)

        "  coalesce(source.#{name}, 0) + #{index} * #{column} AS #{name}"
      end)

    """
    SELECT
    #{projections},
      cast(source.event_at AS date) AS event_date,
      @window_start AS favn_window_start,
      @window_end AS favn_window_end
    FROM raw.analytics.source_events AS source
    WHERE source.event_at >= @window_start
      AND source.event_at < @window_end
      AND source.tenant_id IS NOT NULL
    """
  end

  defp checks(file, module) do
    [
      check(
        :has_rows,
        :before_materialize,
        :fail,
        "SELECT count(*) > 0 AS passed FROM query()",
        file,
        module
      ),
      check(
        :non_negative_metrics,
        :after_materialize,
        :warn,
        "SELECT count(*) = 0 AS passed FROM query() WHERE metric_001 < 0 OR metric_002 < 0",
        file,
        module
      )
    ]
  end

  defp check(name, at, on_violation, sql, file, module) do
    Check.new!(%{
      name: name,
      at: at,
      on_violation: on_violation,
      message: "Synthetic scalability check #{name}",
      sql: sql,
      template: compile_template(sql, file, module),
      file: file,
      line: 1,
      uses_query?: true,
      uses_target?: false
    })
  end

  defp compile_template(sql, file, module) do
    Template.compile!(sql,
      file: file,
      line: 1,
      module: module,
      scope: :query,
      enforce_query_root: true
    )
  end

  defp contract(column_count) do
    columns =
      Enum.map(1..column_count, fn index ->
        Column.new!(column_atom(index), logical_type(index),
          null: true,
          description:
            "Synthetic metric #{padded(index, 3)} retained in the runtime output contract.",
          tags: ["metric", "manifest-scale"]
        )
      end)

    Contract.new!(columns: columns)
  end

  defp dependencies(1), do: []
  defp dependencies(index), do: [{asset_module(index - 1), :asset}]

  defp asset_module(index) do
    Module.concat(FavnTestSupport.ManifestScale, "Asset#{padded(index, 5)}")
  end

  defp column_atom(index), do: index |> column_name() |> String.to_atom()
  defp column_name(index), do: "metric_#{padded(index, 3)}"

  defp logical_type(index) do
    case rem(index, 4) do
      0 -> :decimal
      1 -> :integer
      2 -> :float
      3 -> :string
    end
  end

  defp validate_options!(asset_count, opts) do
    unless asset_count in 1..@maximum_assets do
      raise ArgumentError,
            "manifest scalability asset_count must be in 1..#{@maximum_assets}, got: #{inspect(asset_count)}"
    end

    unless Keyword.keyword?(opts) do
      raise ArgumentError, "manifest scalability options must be a keyword list"
    end

    case Enum.find(Keyword.keys(opts), &(&1 not in @allowed_opts)) do
      nil -> :ok
      key -> raise ArgumentError, "unknown manifest scalability option #{inspect(key)}"
    end

    sql_columns =
      positive_bounded!(opts, :sql_columns, @default_sql_columns, @maximum_sql_columns)

    contract_columns =
      positive_bounded!(
        opts,
        :contract_columns,
        @default_contract_columns,
        min(sql_columns, @maximum_sql_columns)
      )

    %{sql_columns: sql_columns, contract_columns: contract_columns}
  end

  defp positive_bounded!(opts, key, default, maximum) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value in 1..maximum//1 ->
        value

      value ->
        raise ArgumentError,
              "manifest scalability #{key} must be in 1..#{maximum}, got: #{inspect(value)}"
    end
  end

  defp padded(value, width), do: value |> Integer.to_string() |> String.pad_leading(width, "0")
end
