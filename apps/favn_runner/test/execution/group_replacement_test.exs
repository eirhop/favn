defmodule FavnRunner.GroupReplacementTest.Asset do
  use Favn.SQLAsset

  relation(connection: :group_runtime, schema: "main", name: "customer_rows")

  materialized(
    {:incremental, strategy: :replace_groups, replacement_key: [:tenant_id, :customer_id]}
  )

  replacement_scope :incremental do
    ~SQL"select tenant_id, customer_id from incremental_changed_customers"
  end

  replacement_scope :full do
    ~SQL"select tenant_id, customer_id from all_customers"
  end

  query do
    ~SQL"""
    select scope.tenant_id, scope.customer_id, 'current' as value
    from replacement_scope() as scope
    """
  end
end

defmodule FavnRunner.GroupReplacementTest.SkipAsset do
  use Favn.SQLAsset

  relation(connection: :group_runtime, schema: "main", name: "customer_rows")

  materialized(
    {:incremental, strategy: :replace_groups, replacement_key: [:tenant_id, :customer_id]}
  )

  replacement_scope :incremental do
    ~SQL"select tenant_id, customer_id from incremental_changed_customers"
  end

  replacement_scope :full do
    ~SQL"select tenant_id, customer_id from all_customers"
  end

  check :skip_group_write,
    at: :before_materialize,
    when: :target_exists,
    on_violation: :skip_materialization do
    ~SQL"select false as passed /* skip_group_write */"
  end

  query do
    ~SQL"select scope.tenant_id, scope.customer_id, 'current' as value from replacement_scope() scope"
  end
end

defmodule FavnRunner.GroupReplacementTest.AfterFailAsset do
  use Favn.SQLAsset

  relation(connection: :group_runtime, schema: "main", name: "customer_rows")

  materialized(
    {:incremental, strategy: :replace_groups, replacement_key: [:tenant_id, :customer_id]}
  )

  replacement_scope :incremental do
    ~SQL"select tenant_id, customer_id from incremental_changed_customers"
  end

  replacement_scope :full do
    ~SQL"select tenant_id, customer_id from all_customers"
  end

  check :reject_group_write, at: :after_materialize, on_violation: :fail do
    ~SQL"select false as passed /* reject_group_write */"
  end

  query do
    ~SQL"select scope.tenant_id, scope.customer_id, 'current' as value from replacement_scope() scope"
  end
end

defmodule FavnRunner.GroupReplacementTest.PassAsset do
  use Favn.SQLAsset

  relation(connection: :group_runtime, schema: "main", name: "customer_rows")

  materialized(
    {:incremental, strategy: :replace_groups, replacement_key: [:tenant_id, :customer_id]}
  )

  replacement_scope :incremental do
    ~SQL"select tenant_id, customer_id from incremental_changed_customers"
  end

  replacement_scope :full do
    ~SQL"select tenant_id, customer_id from all_customers"
  end

  check :accept_group_write, at: :before_materialize, on_violation: :fail do
    ~SQL"select true as passed /* accept_group_write */"
  end

  contract do
    column(:tenant_id, :integer, null: false)
    column(:customer_id, :integer, null: false)
    column(:value, :string)
  end

  query do
    ~SQL"select scope.tenant_id, scope.customer_id, 'current' as value from replacement_scope() scope"
  end
end

defmodule FavnRunner.GroupReplacementTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.{Registry, Resolved}
  alias Favn.SQL.{GroupReplacementResult, MaterializationResult, WritePlan}
  alias Favn.SQL.Error, as: SQLError
  alias Favn.SQLAsset.Error
  alias FavnRunner.GroupReplacementTest.AfterFailAsset
  alias FavnRunner.GroupReplacementTest.Asset, as: GroupAsset
  alias FavnRunner.GroupReplacementTest.PassAsset
  alias FavnRunner.GroupReplacementTest.SkipAsset
  alias FavnRunner.GroupReplacementTest.{CommitErrorAdapter, FakeAdapter, UnsupportedAdapter}

  setup do
    Application.put_env(:favn_runner, :group_replacement_test_pid, self())
    Application.put_env(:favn_runner, :group_target_exists, true)
    Application.put_env(:favn_runner, :group_scope_count, 2)
    Application.put_env(:favn_runner, :group_candidate_count, 4)
    Application.put_env(:favn_runner, :group_deleted_count, 3)
    Application.put_env(:favn_runner, :group_null_scope_count, 0)
    Application.put_env(:favn_runner, :group_null_candidate_count, 0)
    Application.put_env(:favn_runner, :group_duplicate_scope_count, 0)
    Application.put_env(:favn_runner, :group_outside_scope_count, 0)
    Application.delete_env(:favn_runner, :group_type_overrides)

    reload_adapter(FakeAdapter)

    on_exit(fn ->
      for key <- [
            :group_replacement_test_pid,
            :group_target_exists,
            :group_scope_count,
            :group_candidate_count,
            :group_deleted_count,
            :group_null_scope_count,
            :group_null_candidate_count,
            :group_duplicate_scope_count,
            :group_outside_scope_count,
            :group_type_overrides
          ] do
        Application.delete_env(:favn_runner, key)
      end

      Registry.reload(%{}, registry_name: FavnRunner.ConnectionRegistry)
    end)

    :ok
  end

  test "replaces multiple composite-key groups using only the incremental scope" do
    assert {:ok,
            %MaterializationResult{
              write_outcome: :written,
              group_replacement: %GroupReplacementResult{
                operation: :replaced,
                scope_group_count: 2,
                candidate_row_count: 4,
                deleted_row_count: 3,
                inserted_row_count: 4
              }
            }} = materialize()

    assert_received {:group_execute, incremental_scope}
    assert incremental_scope =~ "CREATE TEMP TABLE"
    assert incremental_scope =~ "incremental_changed_customers"
    refute incremental_scope =~ "all_customers"

    assert_received {:group_materialize,
                     %WritePlan{
                       strategy: :replace_groups,
                       mode: :incremental,
                       replacement_key: ["tenant_id", "customer_id"]
                     }}

    assert_received :group_commit
  end

  test "missing target uses the full scope and can bootstrap a typed empty table" do
    Application.put_env(:favn_runner, :group_target_exists, false)
    Application.put_env(:favn_runner, :group_scope_count, 0)
    Application.put_env(:favn_runner, :group_candidate_count, 0)
    Application.put_env(:favn_runner, :group_deleted_count, 0)

    assert {:ok,
            %MaterializationResult{
              write_outcome: :written,
              group_replacement: %GroupReplacementResult{
                operation: :bootstrap_empty,
                scope_group_count: 0,
                candidate_row_count: 0,
                deleted_row_count: 0,
                inserted_row_count: 0
              }
            }} = materialize()

    assert_received {:group_execute, full_scope}
    assert full_scope =~ "all_customers"

    assert_received {:group_materialize,
                     %WritePlan{mode: :bootstrap, bootstrap?: true, strategy: :replace_groups}}
  end

  test "explicit rebuild generation uses full scope even when its relation already exists" do
    assert {:ok,
            %MaterializationResult{
              group_replacement: %GroupReplacementResult{operation: :replaced}
            }} = materialize(target_operation: :rebuild_candidate)

    assert_received {:group_execute, full_scope}
    assert full_scope =~ "all_customers"
    refute full_scope =~ "incremental_changed_customers"
  end

  test "empty incremental scope is a committed no-op and never calls the adapter write" do
    Application.put_env(:favn_runner, :group_scope_count, 0)
    Application.put_env(:favn_runner, :group_candidate_count, 0)
    Application.put_env(:favn_runner, :group_deleted_count, 0)

    assert {:ok,
            %MaterializationResult{
              write_outcome: :no_op,
              write_plan: nil,
              group_replacement: %GroupReplacementResult{
                operation: :empty_scope_no_op,
                deleted_row_count: 0,
                inserted_row_count: 0
              }
            }} = materialize()

    refute_received {:group_materialize, _plan}
    assert_received :group_commit
  end

  test "non-empty scope and empty candidate is delete-only" do
    Application.put_env(:favn_runner, :group_scope_count, 2)
    Application.put_env(:favn_runner, :group_candidate_count, 0)
    Application.put_env(:favn_runner, :group_deleted_count, 5)

    assert {:ok,
            %MaterializationResult{
              write_outcome: :written,
              group_replacement: %GroupReplacementResult{
                operation: :delete_only,
                deleted_row_count: 5,
                inserted_row_count: 0
              }
            }} = materialize()

    assert_received {:group_materialize, %WritePlan{strategy: :replace_groups}}
  end

  test "before-check skip keeps the existing target and reports separate group metrics" do
    assert {:ok,
            %MaterializationResult{
              write_outcome: :no_op,
              group_replacement: %GroupReplacementResult{
                operation: :before_check_skipped,
                scope_group_count: 2,
                candidate_row_count: 4,
                deleted_row_count: 0,
                inserted_row_count: 0
              }
            }} = materialize_asset(SkipAsset)

    refute_received {:group_materialize, _plan}
    assert_received :group_commit
  end

  test "after-check failure rolls the replacement back" do
    assert {:error, %Error{type: :check_failed, phase: :after_materialize}} =
             materialize_asset(AfterFailAsset)

    assert_received {:group_materialize, %WritePlan{strategy: :replace_groups}}
    assert_received :group_rollback
    refute_received :group_commit
  end

  test "target-exists skip check is condition-skipped during bootstrap" do
    Application.put_env(:favn_runner, :group_target_exists, false)

    assert {:ok,
            %MaterializationResult{
              write_outcome: :written,
              group_replacement: %GroupReplacementResult{operation: :bootstrap_created}
            }} = materialize_asset(SkipAsset)

    refute_received :group_skip_check_query
    assert_received {:group_materialize, %WritePlan{mode: :bootstrap}}
  end

  test "candidate outside scope fails and rolls back before adapter materialization" do
    Application.put_env(:favn_runner, :group_outside_scope_count, 1)

    assert {:error,
            %Error{
              type: :materialization_planning_failed,
              phase: :before_materialize
            }} = materialize()

    assert_received :group_rollback
    refute_received {:group_materialize, _plan}
  end

  test "null or duplicate scope keys fail before adapter materialization" do
    for {setting, expected_reason} <- [
          {:group_null_scope_count, :null_key},
          {:group_duplicate_scope_count, :duplicate_key}
        ] do
      Application.put_env(:favn_runner, setting, 1)

      assert {:error,
              %Error{
                type: :materialization_planning_failed,
                cause: %SQLError{
                  details: %{source: :scope, reason: ^expected_reason}
                }
              }} = materialize()

      assert_received :group_rollback
      refute_received {:group_materialize, _plan}
      Application.put_env(:favn_runner, setting, 0)
    end
  end

  test "null candidate keys fail before adapter materialization" do
    Application.put_env(:favn_runner, :group_null_candidate_count, 1)

    assert {:error,
            %Error{
              type: :materialization_planning_failed,
              cause: %SQLError{
                details: %{source: :candidate, reason: :null_key, row_count: 1}
              }
            }} = materialize()

    assert_received :group_rollback
    refute_received {:group_materialize, _plan}
  end

  test "scope, candidate, and target key types must match exactly" do
    Application.put_env(:favn_runner, :group_type_overrides, %{target: "BIGINT"})

    assert {:error, %Error{type: :materialization_planning_failed}} = materialize()
    assert_received :group_rollback
    refute_received {:group_materialize, _plan}
  end

  test "unsupported adapters fail before the transaction and mutation" do
    reload_adapter(UnsupportedAdapter)

    assert {:error,
            %Error{
              type: :unsupported_materialization,
              details: %{missing_capability: :group_replacement}
            }} = materialize()

    refute_received :group_begin
    refute_received {:group_execute, _statement}
  end

  test "unknown commit outcome is surfaced after exactly one write attempt" do
    reload_adapter(CommitErrorAdapter)
    handler_id = "group-unknown-commit-#{System.unique_integer([:positive])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:favn, :sql_asset, :check],
        fn _event, _measurements, metadata, _config ->
          send(test_pid, {:group_check_telemetry, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:error, %Error{cause: %SQLError{details: %{classification: :unknown_commit_state}}},
            %{contract_validation: %Favn.SQL.ContractValidation{status: :passed}}} =
             run_manifest(PassAsset)

    assert_received {:group_materialize, %WritePlan{strategy: :replace_groups}}
    refute_received {:group_materialize, _second_plan}

    assert_received {:group_check_telemetry,
                     %{outcome: :passed, transaction_outcome: :unknown, write_outcome: :unknown}}
  end

  test "render, preview, and explain reject the staged strategy explicitly" do
    asset = %{type: :sql, module: GroupAsset}

    for operation <- [:render, :preview, :explain] do
      assert {:error,
              %Error{
                type: :unsupported_materialization,
                message: message
              }} = apply(Favn.SQLAsset.Runtime, operation, [asset])

      assert message =~ "staged replacement scope"
    end

    refute_received :group_connect
  end

  test "ordinary runtime execution does not require a window" do
    now = ~U[2026-08-24 12:00:00Z]

    context = %Favn.Run.Context{
      run_id: "run_group_replacement",
      target_refs: [{GroupAsset, :asset}],
      asset: %Favn.Run.AssetContext{ref: {GroupAsset, :asset}},
      runtime_config: %{},
      params: %{},
      window: nil,
      run_started_at: now,
      stage: 0,
      attempt: 1,
      max_attempts: 1
    }

    assert {:ok,
            %{
              group_replacement: %GroupReplacementResult{operation: :replaced}
            }} = Favn.SQLAsset.Runtime.run(GroupAsset, context)

    assert_received :group_commit
  end

  defp materialize(opts \\ []) do
    materialize_asset(GroupAsset, opts)
  end

  defp materialize_asset(module, opts \\ []),
    do: Favn.SQLAsset.Runtime.materialize(%{type: :sql, module: module}, opts)

  defp run_manifest(module) do
    definition = module.__favn_sql_asset_definition__()
    ref = definition.asset.ref
    execution = Favn.Manifest.SQLExecution.from_definition(definition, [])
    {:ok, package} = Favn.Manifest.ExecutionPackage.new(ref, execution)

    asset = %Favn.Manifest.Asset{
      ref: ref,
      module: module,
      name: elem(ref, 1),
      type: :sql,
      relation: definition.asset.relation,
      materialization: definition.materialization,
      relation_inputs: [],
      execution_package_hash: package.content_hash
    }

    version = %Favn.Manifest.Version{
      manifest_version_id: "mv_group_test",
      content_hash: String.duplicate("a", 64),
      schema_version: 19,
      runner_contract_version: 14,
      runner_releases: %{}
    }

    work = %Favn.Contracts.RunnerWork{metadata: %{}}

    context = %Favn.Run.Context{
      run_id: "run_group_unknown_commit",
      target_refs: [ref],
      asset: %Favn.Run.AssetContext{ref: ref},
      runtime_config: %{},
      params: %{},
      window: nil,
      run_started_at: ~U[2026-08-24 12:00:00Z],
      stage: 0,
      attempt: 1,
      max_attempts: 1
    }

    Favn.SQLAsset.Runtime.run_manifest(asset, package, version, %{}, work, context)
  end

  defp reload_adapter(adapter) do
    Registry.reload(
      %{
        group_runtime: %Resolved{
          name: :group_runtime,
          adapter: adapter,
          module: __MODULE__,
          config: %{}
        }
      },
      registry_name: FavnRunner.ConnectionRegistry
    )
  end
end

defmodule FavnRunner.GroupReplacementTest.FakeAdapter do
  alias Favn.Connection.Resolved
  alias Favn.SQL.{Capabilities, Column, Error, Relation, Result}

  def connect(%Resolved{}, _opts) do
    notify(:group_connect)
    {:ok, :group_conn}
  end

  def disconnect(:group_conn, _opts), do: :ok

  def capabilities(%Resolved{}, _opts) do
    {:ok,
     %Capabilities{
       transactions: :supported,
       replace_table: :supported,
       group_replacement: :supported
     }}
  end

  def relation(:group_conn, ref, _opts) do
    if env(:group_target_exists, true) do
      {:ok,
       %Relation{
         catalog: ref.catalog,
         schema: ref.schema,
         name: ref.name,
         type: :table,
         metadata: %{}
       }}
    else
      {:ok, nil}
    end
  end

  def columns(:group_conn, ref, _opts) do
    source = source(ref.name)
    overrides = env(:group_type_overrides, %{})
    key_type = Map.get(overrides, source, "INTEGER")

    keys = [
      %Column{name: "tenant_id", position: 1, data_type: key_type, nullable?: false},
      %Column{name: "customer_id", position: 2, data_type: key_type, nullable?: false}
    ]

    columns =
      if source == :scope do
        keys
      else
        keys ++ [%Column{name: "value", position: 3, data_type: "VARCHAR", nullable?: true}]
      end

    {:ok, columns}
  end

  def execute(:group_conn, statement, _opts) do
    statement = IO.iodata_to_binary(statement)
    notify({:group_execute, statement})
    {:ok, %Result{kind: :execute, command: "EXECUTE", rows_affected: 0}}
  end

  def query(:group_conn, statement, _opts) do
    statement = IO.iodata_to_binary(statement)
    notify({:group_query, statement})

    if String.contains?(statement, "accept_group_write") do
      passed_result(true)
    else
      check_or_count_result(statement)
    end
  end

  defp check_or_count_result(statement) do
    if String.contains?(statement, "skip_group_write") or
         String.contains?(statement, "reject_group_write") do
      if String.contains?(statement, "skip_group_write"), do: notify(:group_skip_check_query)

      passed_result(false)
    else
      if String.contains?(String.downcase(statement), " as passed") do
        passed_result(true)
      else
        group_count_result(statement)
      end
    end
  end

  defp passed_result(passed?) do
    {:ok,
     %Result{
       kind: :query,
       command: "SELECT",
       columns: ["passed"],
       rows: [%{"passed" => passed?}]
     }}
  end

  defp group_count_result(statement) do
    count =
      cond do
        String.contains?(statement, "favn_duplicate_scope") ->
          env(:group_duplicate_scope_count, 0)

        String.contains?(statement, "WHERE NOT EXISTS") ->
          env(:group_outside_scope_count, 0)

        String.contains?(statement, "AS favn_target WHERE EXISTS") ->
          env(:group_deleted_count, 0)

        String.contains?(statement, "IS NULL") and String.contains?(statement, "_scope_") ->
          env(:group_null_scope_count, 0)

        String.contains?(statement, "IS NULL") and String.contains?(statement, "_candidate_") ->
          env(:group_null_candidate_count, 0)

        String.contains?(statement, "_scope_") ->
          env(:group_scope_count, 0)

        String.contains?(statement, "_candidate_") ->
          env(:group_candidate_count, 0)

        true ->
          0
      end

    {:ok,
     %Result{
       kind: :query,
       command: "SELECT",
       columns: ["favn_count"],
       rows: [%{"favn_count" => count}]
     }}
  end

  def materialize_in_transaction(:group_conn, write_plan, _opts) do
    notify({:group_materialize, write_plan})
    {:ok, %Result{kind: :materialize, command: "INSERT", rows_affected: nil}}
  end

  def transaction(:group_conn, fun, _opts) do
    notify(:group_begin)

    case fun.(:group_conn) do
      {:ok, value} ->
        notify(:group_commit)
        {:ok, value}

      {:error, %Error{} = error} ->
        notify(:group_rollback)
        {:error, error}
    end
  end

  defp source(name) do
    cond do
      String.contains?(name, "_scope_") -> :scope
      String.contains?(name, "_candidate_") -> :candidate
      true -> :target
    end
  end

  defp env(key, default), do: Application.get_env(:favn_runner, key, default)

  defp notify(message) do
    if pid = Application.get_env(:favn_runner, :group_replacement_test_pid),
      do: send(pid, message)
  end
end

defmodule FavnRunner.GroupReplacementTest.CommitErrorAdapter do
  alias Favn.SQL.Error

  defdelegate connect(resolved, opts), to: FavnRunner.GroupReplacementTest.FakeAdapter
  defdelegate disconnect(conn, opts), to: FavnRunner.GroupReplacementTest.FakeAdapter
  defdelegate capabilities(resolved, opts), to: FavnRunner.GroupReplacementTest.FakeAdapter
  defdelegate relation(conn, ref, opts), to: FavnRunner.GroupReplacementTest.FakeAdapter
  defdelegate columns(conn, ref, opts), to: FavnRunner.GroupReplacementTest.FakeAdapter
  defdelegate execute(conn, statement, opts), to: FavnRunner.GroupReplacementTest.FakeAdapter
  defdelegate query(conn, statement, opts), to: FavnRunner.GroupReplacementTest.FakeAdapter

  defdelegate materialize_in_transaction(conn, write_plan, opts),
    to: FavnRunner.GroupReplacementTest.FakeAdapter

  def transaction(:group_conn, fun, _opts) do
    case fun.(:group_conn) do
      {:ok, value} ->
        {:error,
         %Error{
           type: :execution_error,
           message: "commit outcome is unknown",
           operation: :transaction,
           details: %{
             classification: :unknown_commit_state,
             transaction_body_result: value
           }
         }}

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end
end

defmodule FavnRunner.GroupReplacementTest.UnsupportedAdapter do
  alias Favn.Connection.Resolved
  alias Favn.SQL.Capabilities

  defdelegate connect(resolved, opts), to: FavnRunner.GroupReplacementTest.FakeAdapter
  defdelegate disconnect(conn, opts), to: FavnRunner.GroupReplacementTest.FakeAdapter

  def capabilities(%Resolved{}, _opts),
    do: {:ok, %Capabilities{transactions: :supported, group_replacement: :unsupported}}
end
