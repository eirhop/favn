defmodule FavnRunner.SQLRuntimePreflightTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Registry
  alias Favn.Connection.Resolved
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.RuntimeConfig.Ref
  alias Favn.SQL.Template

  @missing_secret_env "FAVN_PREFLIGHT_MISSING_SECRET"
  @optional_secret_env "FAVN_PREFLIGHT_OPTIONAL_SECRET"

  setup do
    previous_modules = Application.get_env(:favn, :connection_modules)
    previous_connections = Application.get_env(:favn, :connections)
    previous_pid = Application.get_env(:favn_runner, :preflight_test_pid)

    Application.put_env(:favn_runner, :preflight_test_pid, self())
    System.delete_env(@missing_secret_env)
    System.delete_env(@optional_secret_env)
    Registry.reload(%{}, registry_name: FavnRunner.ConnectionRegistry)

    on_exit(fn ->
      restore_app_env(:favn, :connection_modules, previous_modules)
      restore_app_env(:favn, :connections, previous_connections)
      restore_app_env(:favn_runner, :preflight_test_pid, previous_pid)
      System.delete_env(@missing_secret_env)
      System.delete_env(@optional_secret_env)
      Registry.reload(%{}, registry_name: FavnRunner.ConnectionRegistry)
    end)

    :ok
  end

  test "Elixir-only planned run executes without SQL preflight failure" do
    elixir_ref = {__MODULE__.CountingAsset, :asset}
    version = register_manifest!([elixir_asset(elixir_ref)])
    work = work(version, elixir_ref, [elixir_ref])

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :ok
    assert [%{ref: ^elixir_ref, status: :ok}] = result.asset_results
    assert_receive :preflight_elixir_executed, 500
  end

  test "SQL-only planned run with valid registered config proceeds" do
    reload_fake_connection(:preflight_sql)

    sql_ref = {__MODULE__.SQLAsset, :asset}
    version = register_manifest!([sql_asset(sql_ref, :preflight_sql)])

    assert {:ok, result} = FavnRunner.run(work(version, sql_ref, [sql_ref]))
    assert result.status == :ok
    assert [%{ref: ^sql_ref, status: :ok}] = result.asset_results
  end

  test "SQL-only planned run with missing required secret env fails before execution" do
    configure_missing_secret_connection()

    sql_ref = {__MODULE__.SQLAsset, :asset}
    version = register_manifest!([sql_asset(sql_ref, :preflight_sql)])

    assert {:ok, result} = FavnRunner.run(work(version, sql_ref, [sql_ref]))
    assert result.status == :error
    assert result.asset_results == []
    assert result.error.type == :missing_runtime_config
    assert result.error.phase == :sql_preflight
    assert result.error.details.connections == [:preflight_sql]

    assert [%{type: :missing_env, env: @missing_secret_env, secret?: true}] =
             result.error.details.errors

    refute inspect(result) =~ "raw-secret"
  end

  test "mixed planned run fails before an earlier Elixir asset starts" do
    configure_missing_secret_connection()

    elixir_ref = {__MODULE__.CountingAsset, :asset}
    sql_ref = {__MODULE__.SQLAsset, :asset}
    version = register_manifest!([elixir_asset(elixir_ref), sql_asset(sql_ref, :preflight_sql)])

    assert {:ok, result} = FavnRunner.run(work(version, elixir_ref, [elixir_ref, sql_ref]))
    assert result.status == :error
    assert result.asset_results == []
    assert result.error.type == :missing_runtime_config
    refute_receive :preflight_elixir_executed, 200
  end

  test "SQL asset outside the selected planned set does not fail Elixir execution" do
    configure_missing_secret_connection()

    elixir_ref = {__MODULE__.CountingAsset, :asset}
    sql_ref = {__MODULE__.SQLAsset, :asset}
    version = register_manifest!([elixir_asset(elixir_ref), sql_asset(sql_ref, :preflight_sql)])

    assert {:ok, result} = FavnRunner.run(work(version, elixir_ref, [elixir_ref]))
    assert result.status == :ok
    assert [%{ref: ^elixir_ref, status: :ok}] = result.asset_results
    assert_receive :preflight_elixir_executed, 500
  end

  test "shared SQL connection emits one missing-runtime-config diagnostic" do
    configure_missing_secret_connection()

    left_ref = {__MODULE__.SQLAsset, :left}
    right_ref = {__MODULE__.SQLAsset, :right}

    version =
      register_manifest!([
        sql_asset(left_ref, :preflight_sql),
        sql_asset(right_ref, :preflight_sql)
      ])

    assert {:ok, result} = FavnRunner.run(work(version, left_ref, [left_ref, right_ref]))
    assert result.status == :error
    assert result.error.details.connections == [:preflight_sql]
    assert [_one_error] = result.error.details.errors
  end

  test "missing optional SQL runtime ref does not fail preflight" do
    Application.put_env(:favn, :connection_modules, [__MODULE__.OptionalSecretConnection])

    Application.put_env(:favn, :connections,
      preflight_sql: [
        database: ":memory:",
        token: Ref.env!(@optional_secret_env, secret?: true, required?: false)
      ]
    )

    reload_fake_connection(:preflight_sql)

    sql_ref = {__MODULE__.SQLAsset, :asset}
    version = register_manifest!([sql_asset(sql_ref, :preflight_sql)])

    assert {:ok, result} = FavnRunner.run(work(version, sql_ref, [sql_ref]))
    assert result.status == :ok
  end

  test "preflight diagnostics do not echo raw invalid runtime config values" do
    Application.put_env(:favn, :connection_modules, [__MODULE__.MissingSecretConnection])
    Application.put_env(:favn, :connections, preflight_sql: "raw-secret-like-value")

    sql_ref = {__MODULE__.SQLAsset, :asset}
    version = register_manifest!([sql_asset(sql_ref, :preflight_sql)])

    assert {:ok, result} = FavnRunner.run(work(version, sql_ref, [sql_ref]))
    assert result.status == :error
    assert result.error.type == :missing_runtime_config
    refute inspect(result.error) =~ "raw-secret-like-value"
  end

  defp configure_missing_secret_connection do
    Application.put_env(:favn, :connection_modules, [__MODULE__.MissingSecretConnection])

    Application.put_env(:favn, :connections,
      preflight_sql: [database: ":memory:", password: Ref.secret_env!(@missing_secret_env)]
    )
  end

  defp elixir_asset(ref) do
    %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :elixir,
      execution: %{entrypoint: elem(ref, 1), arity: 1}
    }
  end

  defp sql_asset(ref, connection) do
    relation = RelationRef.new!(%{connection: connection, name: Atom.to_string(elem(ref, 1))})

    template =
      Template.compile!("SELECT 1 AS id",
        file: "test/sql_runtime_preflight.sql",
        line: 1,
        module: __MODULE__,
        scope: elem(ref, 1),
        enforce_query_root: true
      )

    %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :sql,
      execution: %{entrypoint: elem(ref, 1), arity: 1},
      relation: relation,
      materialization: :table,
      sql_execution: %SQLExecution{sql: "SELECT 1 AS id", template: template, sql_definitions: []}
    }
  end

  defp register_manifest!(assets) do
    refs = Enum.map(assets, & &1.ref)

    manifest = %Manifest{
      schema_version: 1,
      runner_contract_version: 1,
      assets: assets,
      pipelines: [],
      schedules: [],
      graph: %Graph{nodes: refs, edges: [], topo_order: refs},
      metadata: %{}
    }

    {:ok, version} =
      Version.new(manifest,
        manifest_version_id:
          "mv_preflight_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
      )

    :ok = FavnRunner.register_manifest(version)
    version
  end

  defp work(%Version{} = version, asset_ref, planned_refs) do
    %RunnerWork{
      run_id: "run_preflight_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower),
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: asset_ref,
      asset_refs: [asset_ref],
      metadata: %{planned_asset_refs: planned_refs}
    }
  end

  defp reload_fake_connection(name) do
    Registry.reload(
      %{
        name => %Resolved{
          name: name,
          adapter: __MODULE__.FakeExecutionAdapter,
          module: __MODULE__,
          config: %{}
        }
      },
      registry_name: FavnRunner.ConnectionRegistry
    )
  end

  defp restore_app_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_app_env(app, key, value), do: Application.put_env(app, key, value)
end

defmodule FavnRunner.SQLRuntimePreflightTest.CountingAsset do
  def asset(_ctx) do
    if pid = Application.get_env(:favn_runner, :preflight_test_pid) do
      send(pid, :preflight_elixir_executed)
    end

    :ok
  end
end

defmodule FavnRunner.SQLRuntimePreflightTest.SQLAsset do
end

defmodule FavnRunner.SQLRuntimePreflightTest.MissingSecretConnection do
  @behaviour Favn.Connection

  alias Favn.Connection.Definition

  @impl true
  def definition do
    %Definition{
      name: :preflight_sql,
      adapter: FavnRunner.SQLRuntimePreflightTest.FakeExecutionAdapter,
      config_schema: [
        %{key: :database, required: true, type: :path},
        %{key: :password, required: true, secret: true, type: :string}
      ]
    }
  end
end

defmodule FavnRunner.SQLRuntimePreflightTest.OptionalSecretConnection do
  @behaviour Favn.Connection

  alias Favn.Connection.Definition

  @impl true
  def definition do
    %Definition{
      name: :preflight_sql,
      adapter: FavnRunner.SQLRuntimePreflightTest.FakeExecutionAdapter,
      config_schema: [
        %{key: :database, required: true, type: :path},
        %{key: :token, secret: true}
      ]
    }
  end
end

defmodule FavnRunner.SQLRuntimePreflightTest.FakeExecutionAdapter do
  alias Favn.Connection.Resolved
  alias Favn.SQL.Capabilities
  alias Favn.SQL.Result

  def connect(%Resolved{}, _opts), do: {:ok, :conn}
  def disconnect(:conn, _opts), do: :ok
  def capabilities(%Resolved{}, _opts), do: {:ok, %Capabilities{}}

  def materialize(:conn, _write_plan, _opts),
    do: {:ok, %Result{command: :insert, rows_affected: 1}}
end
