defmodule Favn.SQLAssetRuntimeTest do
  use ExUnit.Case

  alias Favn.Connection.Definition
  alias Favn.Connection.Loader
  alias Favn.Connection.Registry
  alias Favn.SQL.{Capabilities, Column, Params, Relation, Result, WritePlan}
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
      {:ok,
       %Capabilities{
         replace_view: :supported,
         replace_table: :supported,
         transactions: :supported
       }}
    end

    @impl true
    def relation(:runtime_conn, _ref, _opts) do
      if Application.get_env(:favn, :sql_asset_runtime_target_exists, false) do
        {:ok, %Relation{schema: "gold", name: "fct_orders", type: :table}}
      else
        {:ok, nil}
      end
    end

    @impl true
    def columns(:runtime_conn, _ref, _opts) do
      configured = Application.get_env(:favn, :sql_asset_runtime_target_columns, ["event_at"])
      {:ok, Enum.map(configured, &%Column{name: &1})}
    end

    @impl true
    def transaction(:runtime_conn, fun, _opts) do
      record({:tx_begin})

      case fun.(:runtime_conn) do
        {:ok, value} ->
          record({:tx_commit})
          {:ok, value}

        {:error, reason} ->
          record({:tx_rollback})
          {:error, reason}
      end
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
        columns =
          if String.contains?(sql, "favn_incremental_probe") do
            Application.get_env(:favn, :sql_asset_runtime_render_columns, [
              "event_at",
              "customer_id"
            ])
          else
            ["ok"]
          end

        {:ok,
         %Result{
           kind: :query,
           command: sql,
           rows: [%{"ok" => true}],
           columns: columns,
           metadata: %{params: params}
         }}
      end
    end

    @impl true
    def introspection_query(_kind, _payload, _opts), do: {:ok, "SELECT 1"}

    @impl true
    def materialization_statements(%WritePlan{} = plan, %Capabilities{}, _opts) do
      target = plan.target.name

      statements =
        case {plan.materialization, plan.strategy, plan.mode} do
          {:incremental, _strategy, :bootstrap} ->
            ["CREATE OR REPLACE #{target} AS #{plan.select_sql}"]

          {:incremental, :append, _mode} ->
            ["INSERT INTO #{target} #{plan.select_sql}"]

          {:incremental, :delete_insert, _mode} ->
            column = plan.options |> Map.fetch!(:window_column) |> to_string()

            [
              "DELETE FROM #{target} WHERE #{column} >= '#{DateTime.to_iso8601(plan.window.start_at)}' AND #{column} < '#{DateTime.to_iso8601(plan.window.end_at)}'",
              "INSERT INTO #{target} #{plan.select_sql}"
            ]

          _ ->
            ["CREATE OR REPLACE #{target} AS #{plan.select_sql}"]
        end

      {:ok, statements}
    end

    @impl true
    def materialize(:runtime_conn, %WritePlan{} = plan, opts) do
      run = fn conn ->
        with {:ok, statements} <- materialization_statements(plan, %Capabilities{}, opts),
             {:ok, _} <- execute_all(conn, statements, opts) do
          {:ok,
           %Result{
             kind: :materialize,
             command: "sql",
             rows_affected: nil,
             metadata: %{strategy: plan.strategy, mode: plan.mode}
           }}
        end
      end

      if plan.transactional? do
        transaction(:runtime_conn, run, opts)
      else
        run.(:runtime_conn)
      end
    end

    defp execute_all(conn, statements, opts) do
      Enum.reduce_while(statements, {:ok, :ok}, fn statement, _acc ->
        case execute(conn, statement, opts) do
          {:ok, _} -> {:cont, {:ok, :ok}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
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
      Application.delete_env(:favn, :sql_asset_runtime_target_exists)
      Application.delete_env(:favn, :sql_asset_runtime_target_columns)
      Application.delete_env(:favn, :sql_asset_runtime_render_columns)

      if :ets.whereis(@events_table) != :undefined do
        :ets.delete(@events_table)
      end
    end)

    :ets.new(@events_table, [:named_table, :ordered_set, :public])

    Application.put_env(:favn, :sql_asset_runtime_target_exists, false)
    Application.put_env(:favn, :sql_asset_runtime_target_columns, ["event_at"])
    Application.put_env(:favn, :sql_asset_runtime_render_columns, ["event_at", "customer_id"])

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

  test "incremental append bootstraps when target relation does not exist" do
    %{asset: asset_module} = compile_incremental_runtime_modules!(:append)

    assert :ok = Favn.TestSetup.setup_asset_modules([asset_module], reload_graph?: true)

    assert {:ok, output} =
             Favn.materialize(asset_module,
               params: %{country: "NO"},
               runtime: %{
                 window: runtime_window(~U[2025-01-01 00:00:00Z], ~U[2025-01-02 00:00:00Z])
               }
             )

    assert output.write_plan.materialization == :incremental
    assert output.write_plan.strategy == :append
    assert output.write_plan.mode == :bootstrap
    assert output.write_plan.bootstrap?

    assert Enum.any?(events(), fn
             {:execute, sql, _params} -> String.contains?(sql, "CREATE OR REPLACE")
             _ -> false
           end)
  end

  test "incremental delete_insert uses transaction and window delete scope" do
    %{asset: asset_module} = compile_incremental_runtime_modules!(:delete_insert)

    Application.put_env(:favn, :sql_asset_runtime_target_exists, true)

    assert :ok = Favn.TestSetup.setup_asset_modules([asset_module], reload_graph?: true)

    assert {:ok, output} =
             Favn.materialize(asset_module,
               params: %{country: "NO"},
               runtime: %{
                 window: runtime_window(~U[2025-01-03 00:00:00Z], ~U[2025-01-04 00:00:00Z])
               }
             )

    assert output.write_plan.strategy == :delete_insert
    assert output.write_plan.transactional?
    assert output.write_plan.mode == :incremental
    assert output.write_plan.window_column == "event_at"
    assert output.write_plan.effective_window.start_at == ~U[2025-01-02 00:00:00Z]
    assert output.write_plan.effective_window.end_at == ~U[2025-01-04 00:00:00Z]

    runtime_events = events()
    assert {:tx_begin} in runtime_events
    assert {:tx_commit} in runtime_events

    assert Enum.any?(runtime_events, fn
             {:execute, sql, _params} -> String.starts_with?(sql, "DELETE FROM")
             _ -> false
           end)

    assert Enum.any?(runtime_events, fn
             {:execute, sql, _params} -> String.starts_with?(sql, "INSERT INTO")
             _ -> false
           end)
  end

  test "incremental lookback widens query input window params" do
    %{asset: asset_module} = compile_incremental_runtime_modules!(:append)

    Application.put_env(:favn, :sql_asset_runtime_target_exists, true)

    assert :ok = Favn.TestSetup.setup_asset_modules([asset_module], reload_graph?: true)

    assert {:ok, output} =
             Favn.materialize(asset_module,
               params: %{country: "NO"},
               runtime: %{
                 window: runtime_window(~U[2025-01-03 00:00:00Z], ~U[2025-01-04 00:00:00Z])
               }
             )

    assert output.write_plan.effective_window.start_at == ~U[2025-01-02 00:00:00Z]
    assert output.write_plan.effective_window.end_at == ~U[2025-01-04 00:00:00Z]

    assert Enum.any?(events(), fn
             {:execute, sql, [window_start, window_end, "NO"]} ->
               String.starts_with?(sql, "INSERT INTO") and
                 window_start == ~U[2025-01-02 00:00:00Z] and
                 window_end == ~U[2025-01-04 00:00:00Z]

             _ ->
               false
           end)
  end

  test "incremental materialize requires runtime window struct" do
    %{asset: asset_module} = compile_incremental_runtime_modules!(:append)

    assert :ok = Favn.TestSetup.setup_asset_modules([asset_module], reload_graph?: true)

    assert {:error, %SQLAssetError{type: :materialization_planning_failed}} =
             Favn.materialize(asset_module,
               params: %{country: "NO"},
               runtime: %{
                 window_start: ~U[2025-01-01 00:00:00Z],
                 window_end: ~U[2025-01-02 00:00:00Z]
               }
             )
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

  defp compile_incremental_runtime_modules!(strategy) do
    root = Module.concat(__MODULE__, "Incremental#{System.unique_integer([:positive])}")
    gold_namespace = Module.concat([root, Gold, Sales])
    asset_module = Module.concat(gold_namespace, FctOrders)

    Code.compile_string(
      "defmodule #{inspect(gold_namespace)} do\n  use Favn.Namespace, connection: :sql_asset_runtime, catalog: :gold, schema: :sales\nend",
      "test/dynamic_sql_asset_runtime_test.exs"
    )

    materialized =
      case strategy do
        :append -> "{:incremental, strategy: :append}"
        :delete_insert -> "{:incremental, strategy: :delete_insert, window_column: :event_at}"
      end

    Code.compile_string(
      """
      defmodule #{inspect(asset_module)} do
        use Favn.Namespace
        use Favn.SQLAsset

        @window Favn.Window.daily(lookback: 1)
        @materialized #{materialized}

        query do
          ~SQL[
          select customer_id, inserted_at as event_at
          from raw.sales.orders
          where inserted_at >= @window_start and inserted_at < @window_end
            and country = @country
          ]
        end
      end
      """,
      "test/dynamic_sql_asset_runtime_test.exs"
    )

    %{asset: asset_module}
  end

  defp runtime_window(start_at, end_at) do
    anchor_key = Key.new!(:day, start_at, "Etc/UTC")
    Runtime.new!(:day, start_at, end_at, anchor_key, timezone: "Etc/UTC")
  end

  defp events do
    @events_table
    |> :ets.tab2list()
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
  end
end
