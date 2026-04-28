defmodule Favn.Dev.Init do
  @moduledoc """
  Idempotent local-project bootstrap scaffolding for Favn dogfooding.
  """

  alias Favn.Dev.Paths
  alias Favn.Dev.RuntimeSource

  @type result :: %{
          created: [Path.t()],
          existing: [Path.t()],
          updated: [Path.t()],
          skipped: [Path.t()],
          warnings: [String.t()],
          pipeline_module: String.t()
        }

  @spec run(keyword()) :: {:ok, result()} | {:error, term()}
  def run(opts) when is_list(opts) do
    with :ok <- require_flags(opts),
         {:ok, project} <- project(opts),
         {:ok, files_result} <- write_sample_files(project),
         {:ok, config_result} <- append_config(project),
         {:ok, deps_result} <- ensure_duckdb_dependency(project) do
      result =
        merge_results([files_result, config_result, deps_result])
        |> Map.put(:pipeline_module, module_name(project, ["Pipelines", "LocalSmoke"]))

      {:ok, result}
    end
  end

  defp require_flags(opts) do
    missing =
      [:duckdb, :sample]
      |> Enum.reject(&Keyword.get(opts, &1, false))

    case missing do
      [] -> :ok
      flags -> {:error, {:missing_required_flags, flags}}
    end
  end

  defp project(opts) do
    root_dir = Paths.root_dir(opts) |> Path.expand()
    mix_exs = Path.join(root_dir, "mix.exs")

    if File.exists?(mix_exs) do
      app = Keyword.get_lazy(opts, :app, fn -> Mix.Project.config()[:app] || :my_app end)
      app_string = app |> to_string() |> String.trim()
      base_module = Keyword.get_lazy(opts, :base_module, fn -> Macro.camelize(app_string) end)
      lib_root = Path.join([root_dir, "lib", Macro.underscore(base_module)])

      {:ok,
       %{
         root_dir: root_dir,
         mix_exs: mix_exs,
         app: app,
         app_string: app_string,
         base_module: base_module,
         lib_root: lib_root
       }}
    else
      {:error, {:missing_mix_project, root_dir}}
    end
  end

  defp write_sample_files(project) do
    files = sample_files(project) ++ env_files(project)

    files
    |> Enum.reduce_while({:ok, empty_result()}, fn {path, content}, {:ok, acc} ->
      case write_file(path, content, project.root_dir) do
        {:ok, status, relative} -> {:cont, {:ok, add_path(acc, status, relative)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp sample_files(project) do
    [
      {Path.join([project.lib_root, "connections", "warehouse.ex"]), connection_module(project)},
      {Path.join([project.lib_root, "warehouse.ex"]), warehouse_namespace(project)},
      {Path.join([project.lib_root, "warehouse", "raw.ex"]), raw_namespace(project)},
      {Path.join([project.lib_root, "warehouse", "raw", "orders.ex"]), raw_orders_asset(project)},
      {Path.join([project.lib_root, "warehouse", "gold.ex"]), gold_namespace(project)},
      {Path.join([project.lib_root, "warehouse", "gold", "order_summary.ex"]),
       order_summary_asset(project)},
      {Path.join([project.lib_root, "pipelines", "local_smoke.ex"]),
       local_smoke_pipeline(project)}
    ]
  end

  defp env_files(project) do
    [
      {Path.join(project.root_dir, ".env.example"), env_example()}
    ]
  end

  defp append_config(project) do
    config_path = Path.join([project.root_dir, "config", "config.exs"])
    block = config_block(project)

    with :ok <- File.mkdir_p(Path.dirname(config_path)) do
      cond do
        File.exists?(config_path) and config_marker_present?(config_path) ->
          {:ok, add_path(empty_result(), :existing, relative(project.root_dir, config_path))}

        File.exists?(config_path) ->
          File.write(config_path, "\n" <> block, [:append])
          {:ok, add_path(empty_result(), :updated, relative(project.root_dir, config_path))}

        true ->
          File.write(config_path, "import Config\n\n" <> block)
          {:ok, add_path(empty_result(), :created, relative(project.root_dir, config_path))}
      end
    end
  end

  defp ensure_duckdb_dependency(project) do
    case File.read(project.mix_exs) do
      {:ok, content} ->
        if String.contains?(content, ":favn_duckdb") do
          {:ok, add_path(empty_result(), :existing, relative(project.root_dir, project.mix_exs))}
        else
          case duckdb_dependency_line(project) do
            {:ok, dep_line} -> insert_duckdb_dependency(project, content, dep_line)
            {:error, reason} -> {:ok, add_warning(empty_result(), dependency_warning(reason))}
          end
        end

      {:error, reason} ->
        {:error, {:read_failed, project.mix_exs, reason}}
    end
  end

  defp duckdb_dependency_line(project) do
    with {:ok, source} <- RuntimeSource.resolve([]) do
      duckdb_path = Path.join(source.root, "apps/favn_duckdb")
      relative_path = Path.relative_to(duckdb_path, project.root_dir)
      {:ok, ~s({:favn_duckdb, path: "#{relative_path}"})}
    end
  end

  defp insert_duckdb_dependency(project, content, dep_line) do
    pattern = ~r/(defp deps do\s*\n(\s*)\[)/

    if Regex.match?(pattern, content) do
      updated =
        Regex.replace(
          pattern,
          content,
          fn _full, prefix, indent ->
            prefix <> "\n" <> indent <> "  " <> dep_line <> ","
          end,
          global: false
        )

      :ok = File.write(project.mix_exs, updated)
      {:ok, add_path(empty_result(), :updated, relative(project.root_dir, project.mix_exs))}
    else
      warning =
        "could not add #{dep_line} automatically; add it to defp deps/0 before running the DuckDB sample"

      {:ok, add_warning(empty_result(), warning)}
    end
  end

  defp dependency_warning(reason) do
    "could not resolve local favn_duckdb path (#{inspect(reason)}); add the favn_duckdb dependency manually"
  end

  defp write_file(path, content, root_dir) do
    relative = relative(root_dir, path)

    cond do
      File.exists?(path) and File.read!(path) == content ->
        {:ok, :existing, relative}

      File.exists?(path) ->
        {:ok, :skipped, relative}

      true ->
        with :ok <- File.mkdir_p(Path.dirname(path)),
             :ok <- File.write(path, content) do
          {:ok, :created, relative}
        else
          {:error, reason} -> {:error, {:write_failed, path, reason}}
        end
    end
  end

  defp config_marker_present?(config_path) do
    config_path
    |> File.read!()
    |> String.contains?("# Favn local bootstrap generated by mix favn.init")
  end

  defp config_block(project) do
    ~s'''
    # Favn local bootstrap generated by mix favn.init --duckdb --sample
    config :favn,
      asset_modules: [
        #{module_name(project, ["Warehouse", "Raw", "Orders"])},
        #{module_name(project, ["Warehouse", "Gold", "OrderSummary"])}
      ],
      pipeline_modules: [
        #{module_name(project, ["Pipelines", "LocalSmoke"])}
      ],
      connection_modules: [
        #{module_name(project, ["Connections", "Warehouse"])}
      ],
      connections: [
        warehouse: [database: ".favn/data/local_smoke.duckdb", write_concurrency: 1]
      ],
      runner_plugins: [
        {FavnDuckdb, execution_mode: :in_process}
      ],
      local: [
        storage: :memory
      ]
    '''
  end

  defp connection_module(project) do
    ~s'''
    defmodule #{module_name(project, ["Connections", "Warehouse"])} do
      @moduledoc """
      DuckDB warehouse connection for the generated local Favn smoke path.
      """

      @behaviour Favn.Connection

      @impl true
      def definition do
        %Favn.Connection.Definition{
          name: :warehouse,
          adapter: Favn.SQL.Adapter.DuckDB,
          doc: "Local DuckDB warehouse for Favn smoke runs",
          metadata: %{scope: :local_smoke},
          config_schema: [
            %{key: :database, required: true, type: :path}
          ]
        }
      end
    end
    '''
  end

  defp warehouse_namespace(project) do
    ~s'''
    defmodule #{module_name(project, ["Warehouse"])} do
      @moduledoc """
      Shared Favn namespace defaults for the generated local warehouse.
      """

      use Favn.Namespace, relation: [connection: :warehouse]
    end
    '''
  end

  defp raw_namespace(project) do
    ~s'''
    defmodule #{module_name(project, ["Warehouse", "Raw"])} do
      @moduledoc """
      Raw local DuckDB relations.
      """

      use Favn.Namespace, relation: [schema: "raw"]
    end
    '''
  end

  defp gold_namespace(project) do
    ~s'''
    defmodule #{module_name(project, ["Warehouse", "Gold"])} do
      @moduledoc """
      Business-facing local DuckDB outputs.
      """

      use Favn.Namespace, relation: [schema: "gold"]
    end
    '''
  end

  defp raw_orders_asset(project) do
    ~s'''
    defmodule #{module_name(project, ["Warehouse", "Raw", "Orders"])} do
      @moduledoc """
      Loads a tiny deterministic order dataset into DuckDB.
      """

      use Favn.Namespace
      use Favn.Asset

      alias Favn.SQLClient

      @meta owner: "local", category: :orders, tags: [:sample, :raw]
      @relation true
      def asset(_ctx) do
        SQLClient.with_connection(:warehouse, [], fn session ->
          with {:ok, _} <- SQLClient.execute(session, "create schema if not exists raw"),
               {:ok, _} <- SQLClient.execute(session, orders_sql()) do
            :ok
          end
        end)
      end

      defp orders_sql do
        """
        create or replace table raw.orders as
        select *
        from (
          values
            (1, 'Ada Labs', date '2026-01-01', 12000),
            (2, 'Beam Goods', date '2026-01-01', 8500),
            (3, 'Query Co', date '2026-01-02', 1575)
        ) as orders(order_id, customer_name, order_date, amount_cents)
        """
      end
    end
    '''
  end

  defp order_summary_asset(project) do
    ~s'''
    defmodule #{module_name(project, ["Warehouse", "Gold", "OrderSummary"])} do
      @moduledoc """
      Materializes a small business output from raw local orders.
      """

      use Favn.Namespace
      use Favn.SQLAsset

      @meta owner: "local", category: :orders, tags: [:sample, :gold]
      @materialized :table
      @relation true

      query do
        ~SQL"""
        select
          order_date,
          count(*) as order_count,
          sum(amount_cents) as revenue_cents
        from raw.orders
        group by order_date
        order by order_date
        """
      end
    end
    '''
  end

  defp local_smoke_pipeline(project) do
    ~s'''
    defmodule #{module_name(project, ["Pipelines", "LocalSmoke"])} do
      @moduledoc """
      First local Favn pipeline: load raw orders and materialize the summary.
      """

      use Favn.Pipeline

      pipeline :local_smoke do
        asset(#{module_name(project, ["Warehouse", "Gold", "OrderSummary"])})
        deps(:all)
        config(requested_by: "local-smoke")
        meta(owner: "local", purpose: :bootstrap_smoke)
      end
    end
    '''
  end

  defp env_example do
    ~s'''
    FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME=admin
    FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD=change-me-local-password
    FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME=Local Favn Admin
    FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES=admin,operator
    '''
  end

  defp module_name(project, segments), do: Enum.join([project.base_module | segments], ".")

  defp relative(root_dir, path), do: Path.relative_to(path, root_dir)

  defp empty_result, do: %{created: [], existing: [], updated: [], skipped: [], warnings: []}

  defp add_path(result, status, path) do
    Map.update!(result, status_bucket(status), &[path | &1])
  end

  defp add_warning(result, warning), do: Map.update!(result, :warnings, &[warning | &1])

  defp status_bucket(:created), do: :created
  defp status_bucket(:existing), do: :existing
  defp status_bucket(:updated), do: :updated
  defp status_bucket(:skipped), do: :skipped

  defp merge_results(results) do
    Enum.reduce(results, empty_result(), fn result, acc ->
      %{
        created: acc.created ++ Enum.reverse(result.created),
        existing: acc.existing ++ Enum.reverse(result.existing),
        updated: acc.updated ++ Enum.reverse(result.updated),
        skipped: acc.skipped ++ Enum.reverse(result.skipped),
        warnings: acc.warnings ++ Enum.reverse(result.warnings)
      }
    end)
  end
end
