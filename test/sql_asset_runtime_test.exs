defmodule Favn.SQLAssetRuntimeTest do
  use ExUnit.Case

  alias Favn.Connection.Definition
  alias Favn.Connection.Loader
  alias Favn.Connection.Registry
  alias Favn.SQL.{Capabilities, Params, Result, WritePlan}
  alias Favn.SQL.Error, as: SQLError
  alias Favn.SQLAsset.Error, as: SQLAssetError
  alias Favn.Window.{Key, Runtime}

  @events_table :sql_asset_runtime_test_events

  defmodule ConnectionProvider do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Definition{
        name: :sql_asset_runtime,
        adapter: Favn.SQLAssetRuntimeTest.Adapter,
        config_schema: [%{key: :database, required: true, type: :string}]
      }
    end
  end

  defmodule Adapter do
    @behaviour Favn.SQL.Adapter

    @events_table :sql_asset_runtime_test_events

    @impl true
    def connect(_resolved, _opts) do
      record({:connect})
      {:ok, :runtime_conn}
    end

    @impl true
    def disconnect(:runtime_conn, _opts) do
      record({:disconnect})
      :ok
    end

    @impl true
    def capabilities(_resolved, _opts) do
      {:ok, %Capabilities{replace_view: :supported, replace_table: :supported}}
    end

    @impl true
    def execute(:runtime_conn, statement, opts) do
      sql = IO.iodata_to_binary(statement)
      params = Keyword.get(opts, :params, [])
      record({:execute, sql, params})

      if "FAIL" in params do
        {:error,
         %SQLError{
           type: :execution_error,
           operation: :execute,
           connection: :sql_asset_runtime,
           message: "forced execute failure"
         }}
      else
        {:ok, %Result{kind: :execute, command: sql, rows_affected: 1, metadata: %{}}}
      end
    end

    @impl true
    def query(:runtime_conn, statement, opts) do
      sql = IO.iodata_to_binary(statement)
      params = Keyword.get(opts, :params, [])
      record({:query, sql, params})

      if "FAIL" in params do
        {:error,
         %SQLError{
           type: :execution_error,
           operation: :query,
           connection: :sql_asset_runtime,
           message: "forced query failure"
         }}
      else
        {:ok,
         %Result{kind: :query, command: sql, rows: [%{"ok" => true}], metadata: %{params: params}}}
      end
    end

    @impl true
    def introspection_query(_kind, _payload, _opts), do: {:ok, "SELECT 1"}

    @impl true
    def materialization_statements(%WritePlan{} = plan, %Capabilities{}, _opts) do
      target = plan.target.name
      {:ok, ["-- pre", "CREATE OR REPLACE #{target} AS #{plan.select_sql}", "-- post"]}
    end

    defp record(event) do
      if :ets.whereis(@events_table) != :undefined do
        :ets.insert(@events_table, {System.unique_integer([:positive]), event})
      end

      :ok
    end
  end

  setup do
    state = Favn.TestSetup.capture_state()

    on_exit(fn ->
      Favn.TestSetup.restore_state(state, reload_graph?: true)
      Registry.reload(%{})

      if :ets.whereis(@events_table) != :undefined do
        :ets.delete(@events_table)
      end
    end)

    :ets.new(@events_table, [:named_table, :ordered_set, :public])

    Application.put_env(:favn, :connection_modules, [ConnectionProvider])
    Application.put_env(:favn, :connections, sql_asset_runtime: [database: "local"])
    {:ok, resolved} = Loader.load()
    :ok = Registry.reload(resolved)
    :ok
  end

  test "render is backend-free and emits canonical positional params" do
    %{asset: asset_module} = compile_runtime_modules!()

    assert :ok = Favn.TestSetup.setup_asset_modules([asset_module], reload_graph?: true)

    window_start = ~U[2025-01-01 00:00:00Z]
    window_end = ~U[2025-01-02 00:00:00Z]

    assert {:ok, render} =
             Favn.render(asset_module,
               params: %{country: "NO"},
               runtime: %{window_start: window_start, window_end: window_end}
             )

    assert render.params.format == :positional
    assert Enum.map(render.params.bindings, & &1.source) == [:runtime, :runtime, :query_param]
    assert Params.to_adapter_params(render.params) == [window_start, window_end, "NO"]
    assert length(render.resolved_asset_refs) == 1

    refute Enum.any?(events(), fn event -> event == {:connect} end)
  end

  test "render accepts compiled SQL module input without global asset registration" do
    %{asset: asset_module} = compile_runtime_modules!()

    assert {:ok, render} =
             Favn.render(asset_module,
               params: %{country: "NO"},
               runtime: %{
                 window_start: ~U[2025-01-01 00:00:00Z],
                 window_end: ~U[2025-01-02 00:00:00Z]
               }
             )

    assert render.asset_ref == {asset_module, :asset}
  end

  test "preview returns executed preview statement and keeps canonical rendered SQL" do
    %{asset: asset_module} = compile_runtime_modules!()

    assert :ok = Favn.TestSetup.setup_asset_modules([asset_module], reload_graph?: true)

    assert {:ok, preview} =
             Favn.preview(asset_module,
               params: %{country: "NO"},
               runtime: %{
                 window_start: ~U[2025-01-01 00:00:00Z],
                 window_end: ~U[2025-01-02 00:00:00Z]
               },
               limit: 5
             )

    assert preview.statement =~ "LIMIT 5"
    refute preview.render.sql =~ "favn_preview"
    assert preview.result.command == preview.statement
  end

  test "explain executes explain statement" do
    %{asset: asset_module} = compile_runtime_modules!()

    assert :ok = Favn.TestSetup.setup_asset_modules([asset_module], reload_graph?: true)

    assert {:ok, explain} =
             Favn.explain(asset_module,
               params: %{country: "NO"},
               runtime: %{
                 window_start: ~U[2025-01-01 00:00:00Z],
                 window_end: ~U[2025-01-02 00:00:00Z]
               }
             )

    assert String.starts_with?(explain.statement, "EXPLAIN ")
    assert explain.result.command == explain.statement
  end

  test "render accepts runtime.window struct input" do
    %{asset: asset_module} = compile_runtime_modules!()

    assert :ok = Favn.TestSetup.setup_asset_modules([asset_module], reload_graph?: true)

    window_start = ~U[2025-01-01 00:00:00Z]
    window_end = ~U[2025-01-02 00:00:00Z]
    anchor_key = Key.new!(:day, window_start, "Etc/UTC")
    window = Runtime.new!(:day, window_start, window_end, anchor_key, timezone: "Etc/UTC")

    assert {:ok, render} =
             Favn.render(asset_module, params: %{country: "NO"}, runtime: %{window: window})

    assert render.runtime == window
    assert Params.to_adapter_params(render.params) == [window_start, window_end, "NO"]
  end

  test "materialize builds write plan with replace_existing semantics" do
    %{asset: asset_module} = compile_runtime_modules!()

    assert :ok = Favn.TestSetup.setup_asset_modules([asset_module], reload_graph?: true)

    assert {:ok, output} =
             Favn.materialize(asset_module,
               params: %{country: "NO"},
               runtime: %{
                 window_start: ~U[2025-01-01 00:00:00Z],
                 window_end: ~U[2025-01-02 00:00:00Z]
               }
             )

    assert output.write_plan.materialization == :table
    assert output.write_plan.replace_existing?
    assert output.write_plan.params.format == :positional
    assert output.result.kind == :materialize
  end

  test "render fails with missing params and missing runtime inputs" do
    %{asset: asset_module} = compile_runtime_modules!()

    assert :ok = Favn.TestSetup.setup_asset_modules([asset_module], reload_graph?: true)

    assert {:error, %SQLAssetError{type: :missing_query_param}} =
             Favn.render(asset_module,
               runtime: %{
                 window_start: ~U[2025-01-01 00:00:00Z],
                 window_end: ~U[2025-01-02 00:00:00Z]
               }
             )

    assert {:error, %SQLAssetError{type: :missing_runtime_input}} =
             Favn.render(asset_module, params: %{country: "NO"})
  end

  test "render fails hard for cross-connection direct asset refs" do
    %{cross: cross_asset} = compile_cross_connection_modules!()

    assert :ok = Favn.TestSetup.setup_asset_modules([cross_asset], reload_graph?: true)

    assert {:error, %SQLAssetError{type: :cross_connection_asset_ref}} = Favn.render(cross_asset)
  end

  test "preview, explain, and materialize normalize backend failures" do
    %{asset: asset_module} = compile_runtime_modules!()

    assert :ok = Favn.TestSetup.setup_asset_modules([asset_module], reload_graph?: true)

    opts = [
      params: %{country: "FAIL"},
      runtime: %{
        window_start: ~U[2025-01-01 00:00:00Z],
        window_end: ~U[2025-01-02 00:00:00Z]
      }
    ]

    assert {:error, %SQLAssetError{type: :backend_execution_failed, phase: :preview}} =
             Favn.preview(asset_module, opts)

    assert {:error, %SQLAssetError{type: :backend_execution_failed, phase: :explain}} =
             Favn.explain(asset_module, opts)

    assert {:error, %SQLAssetError{type: :backend_execution_failed, phase: :materialize}} =
             Favn.materialize(asset_module, opts)
  end

  test "nested defsql arguments support deferred asset refs resolved at render time" do
    %{asset: asset_module} = compile_nested_deferred_modules!()

    assert {:ok, render} = Favn.render(asset_module)

    assert length(render.resolved_asset_refs) == 1
    assert render.sql =~ "raw.sales.orders"
  end

  defp compile_runtime_modules! do
    root = Module.concat(__MODULE__, "Runtime#{System.unique_integer([:positive])}")
    raw_namespace = Module.concat([root, Raw, Sales])
    gold_namespace = Module.concat([root, Gold, Sales])
    raw_orders = Module.concat(raw_namespace, Orders)
    sql_provider = Module.concat(root, SQL)
    asset_module = Module.concat(gold_namespace, FctOrders)

    Code.compile_string(
      "defmodule #{inspect(raw_namespace)} do\n  use Favn.Namespace, connection: :sql_asset_runtime, catalog: :raw, schema: :sales\nend",
      "test/dynamic_sql_asset_runtime_test.exs"
    )

    Code.compile_string(
      "defmodule #{inspect(gold_namespace)} do\n  use Favn.Namespace, connection: :sql_asset_runtime, catalog: :gold, schema: :sales\nend",
      "test/dynamic_sql_asset_runtime_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(raw_orders)} do
        use Favn.Namespace
        use Favn.Asset

        @produces true

        def asset(_ctx), do: :ok
      end
      """,
      "test/dynamic_sql_asset_runtime_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(sql_provider)} do
        use Favn.SQL

        defsql orders_in_window(start_at, end_at) do
          ~SQL[
          select order_id, customer_id, country
          from #{inspect(raw_orders)}
          where inserted_at >= @start_at and inserted_at < @end_at
          ]
        end
      end
      """,
      "test/dynamic_sql_asset_runtime_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(asset_module)} do
        use Favn.Namespace
        use Favn.SQLAsset
        use #{inspect(sql_provider)}

        @materialized :table

        query do
          ~SQL[
          select customer_id
          from orders_in_window(@window_start, @window_end)
          where country = @country
          ]
        end
      end
      """,
      "test/dynamic_sql_asset_runtime_test.exs"
    )

    %{asset: asset_module}
  end

  defp compile_cross_connection_modules! do
    root = Module.concat(__MODULE__, "Cross#{System.unique_integer([:positive])}")
    other_namespace = Module.concat([root, Other, Sales])
    gold_namespace = Module.concat([root, Gold, Sales])
    other_asset = Module.concat(other_namespace, Orders)
    cross_asset = Module.concat(gold_namespace, FctOrders)

    Code.compile_string(
      "defmodule #{inspect(other_namespace)} do\n  use Favn.Namespace, connection: :other_connection, catalog: :raw, schema: :sales\nend",
      "test/dynamic_sql_asset_runtime_test.exs"
    )

    Code.compile_string(
      "defmodule #{inspect(gold_namespace)} do\n  use Favn.Namespace, connection: :sql_asset_runtime, catalog: :gold, schema: :sales\nend",
      "test/dynamic_sql_asset_runtime_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(other_asset)} do
        use Favn.Namespace
        use Favn.Asset

        @produces true
        def asset(_ctx), do: :ok
      end
      """,
      "test/dynamic_sql_asset_runtime_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(cross_asset)} do
        use Favn.Namespace
        use Favn.SQLAsset

        @materialized :view

        query do
          ~SQL[
          select *
          from #{inspect(other_asset)}
          ]
        end
      end
      """,
      "test/dynamic_sql_asset_runtime_test.exs"
    )

    %{cross: cross_asset}
  end

  defp compile_nested_deferred_modules! do
    root = Module.concat(__MODULE__, "Deferred#{System.unique_integer([:positive])}")
    raw_namespace = Module.concat([root, Raw, Sales])
    gold_namespace = Module.concat([root, Gold, Sales])
    sql_provider = Module.concat(root, SQL)
    deferred_module = Module.concat(raw_namespace, Orders)
    asset_module = Module.concat(gold_namespace, FctOrders)

    Code.compile_string(
      "defmodule #{inspect(raw_namespace)} do\n  use Favn.Namespace, connection: :sql_asset_runtime, catalog: :raw, schema: :sales\nend",
      "test/dynamic_sql_asset_runtime_test.exs"
    )

    Code.compile_string(
      "defmodule #{inspect(gold_namespace)} do\n  use Favn.Namespace, connection: :sql_asset_runtime, catalog: :gold, schema: :sales\nend",
      "test/dynamic_sql_asset_runtime_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(sql_provider)} do
        use Favn.SQL

        defsql wrap_relation(source_relation) do
          ~SQL[
          select *
          from @source_relation
          ]
        end

        defsql nested_relation(source_relation) do
          ~SQL[
          select *
          from wrap_relation(@source_relation)
          ]
        end
      end
      """,
      "test/dynamic_sql_asset_runtime_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(asset_module)} do
        use Favn.Namespace
        use Favn.SQLAsset
        use #{inspect(sql_provider)}

        @materialized :view

        query do
          ~SQL[
          select *
          from nested_relation(select * from #{inspect(deferred_module)})
          ]
        end
      end
      """,
      "test/dynamic_sql_asset_runtime_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(deferred_module)} do
        use Favn.Namespace
        use Favn.Asset

        @produces true

        def asset(_ctx), do: :ok
      end
      """,
      "test/dynamic_sql_asset_runtime_test.exs"
    )

    %{asset: asset_module}
  end

  defp events do
    @events_table
    |> :ets.tab2list()
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
  end
end
