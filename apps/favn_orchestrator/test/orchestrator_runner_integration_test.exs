defmodule FavnOrchestrator.RunnerIntegrationTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.RuntimeConfig.Ref
  alias Favn.SQL.Template
  alias FavnOrchestrator
  alias FavnOrchestrator.Storage.Adapter.Memory

  @missing_secret_env "FAVN_ORCH_PREFLIGHT_MISSING_SECRET"

  setup do
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)
    previous_modules = Application.get_env(:favn, :connection_modules)
    previous_connections = Application.get_env(:favn, :connections)
    previous_pid = Application.get_env(:favn_orchestrator, :preflight_test_pid)

    Application.put_env(:favn_orchestrator, :runner_client, FavnRunner)
    Application.put_env(:favn_orchestrator, :runner_client_opts, [])
    Application.put_env(:favn_orchestrator, :preflight_test_pid, self())
    System.delete_env(@missing_secret_env)
    Memory.reset()
    {:ok, _} = Application.ensure_all_started(:favn_runner)
    Favn.Connection.Registry.reload(%{}, registry_name: FavnRunner.ConnectionRegistry)

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)
      restore_app_env(:favn, :connection_modules, previous_modules)
      restore_app_env(:favn, :connections, previous_connections)
      restore_app_env(:favn_orchestrator, :preflight_test_pid, previous_pid)
      System.delete_env(@missing_secret_env)
      Favn.Connection.Registry.reload(%{}, registry_name: FavnRunner.ConnectionRegistry)
      Memory.reset()
    end)

    :ok
  end

  test "same-node orchestrator run stays pinned when active manifest changes mid-flight" do
    version_a = manifest_version("mv_runner_a")
    version_b = manifest_version("mv_runner_b")

    assert :ok = FavnOrchestrator.register_manifest(version_a)
    assert :ok = FavnOrchestrator.register_manifest(version_b)
    assert :ok = FavnOrchestrator.activate_manifest(version_a.manifest_version_id)

    assert {:ok, run_id} = FavnOrchestrator.submit_asset_run({__MODULE__.SleepAsset, :asset})
    assert :ok = FavnOrchestrator.activate_manifest(version_b.manifest_version_id)

    assert {:ok, run} = await_terminal_run(run_id)
    assert run.status == :ok
    assert run.manifest_version_id == version_a.manifest_version_id
  end

  test "manual pipeline run resolves from persisted manifest pipeline descriptor" do
    version = manifest_version("mv_runner_pipeline")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    assert {:ok, run_id} = FavnOrchestrator.submit_pipeline_run(__MODULE__.DailyPipeline)
    assert {:ok, run} = await_terminal_run(run_id)
    assert run.status == :ok
    assert run.submit_kind == :pipeline
    assert run.target_refs == [{__MODULE__.SleepAsset, :asset}]
  end

  test "pipeline SQL preflight fails before an earlier Elixir dependency stage starts" do
    configure_missing_secret_connection()

    version = preflight_manifest_version("mv_runner_sql_preflight")
    sql_ref = {__MODULE__.PreflightSQLAsset, :asset}

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    assert {:ok, run_id} = FavnOrchestrator.submit_pipeline_run([sql_ref])
    assert {:ok, run} = await_terminal_run(run_id)

    assert run.status == :error
    assert run.error.type == :missing_runtime_config
    assert run.error.phase == :sql_preflight
    assert run.error.details.sql_asset_refs == [sql_ref]
    assert [%{env: @missing_secret_env, secret?: true}] = run.error.details.errors
    refute_receive :orchestrator_preflight_elixir_executed, 200
  end

  test "unplanned SQL asset missing config does not block selected Elixir run" do
    configure_missing_secret_connection()

    version = preflight_manifest_version("mv_runner_sql_preflight_unplanned")
    elixir_ref = {__MODULE__.PreflightElixirAsset, :asset}

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    assert {:ok, run_id} = FavnOrchestrator.submit_asset_run(elixir_ref)
    assert {:ok, run} = await_terminal_run(run_id)

    assert run.status == :ok
    assert_receive :orchestrator_preflight_elixir_executed, 500
  end

  defp await_terminal_run(run_id, attempts \\ 60)

  defp await_terminal_run(run_id, attempts) when attempts > 0 do
    case FavnOrchestrator.get_run(run_id) do
      {:ok, run} when run.status in [:ok, :error, :cancelled, :timed_out] ->
        {:ok, run}

      {:ok, _run} ->
        Process.sleep(20)
        await_terminal_run(run_id, attempts - 1)

      error ->
        error
    end
  end

  defp await_terminal_run(_run_id, 0), do: {:error, :timeout_waiting_for_terminal_state}

  defp manifest_version(manifest_version_id) do
    assets = [
      %Asset{
        ref: {__MODULE__.SleepAsset, :asset},
        module: __MODULE__.SleepAsset,
        name: :asset,
        type: :elixir,
        execution: %{entrypoint: :asset, arity: 1},
        depends_on: [],
        config: %{manifest_version_id: manifest_version_id}
      }
    ]

    refs = Enum.map(assets, & &1.ref)

    manifest = %Manifest{
      schema_version: 1,
      runner_contract_version: 1,
      assets: assets,
      pipelines: [
        %Pipeline{
          module: __MODULE__.DailyPipeline,
          name: :daily,
          selectors: [{:asset, {__MODULE__.SleepAsset, :asset}}],
          deps: :all,
          schedule: nil,
          metadata: %{owner: :integration}
        }
      ],
      schedules: [],
      graph: %Graph{nodes: refs, edges: [], topo_order: refs},
      metadata: %{}
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp preflight_manifest_version(manifest_version_id) do
    elixir_ref = {__MODULE__.PreflightElixirAsset, :asset}
    sql_ref = {__MODULE__.PreflightSQLAsset, :asset}

    assets = [
      %Asset{
        ref: elixir_ref,
        module: elem(elixir_ref, 0),
        name: :asset,
        type: :elixir,
        execution: %{entrypoint: :asset, arity: 1},
        depends_on: []
      },
      sql_asset(sql_ref, depends_on: [elixir_ref])
    ]

    manifest = %Manifest{
      schema_version: 1,
      runner_contract_version: 1,
      assets: assets,
      pipelines: [],
      schedules: [],
      graph: %Graph{
        nodes: [elixir_ref, sql_ref],
        edges: [%{from: elixir_ref, to: sql_ref}],
        topo_order: [elixir_ref, sql_ref]
      },
      metadata: %{}
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp sql_asset(ref, opts) do
    relation = RelationRef.new!(%{connection: :preflight_sql, name: "preflight_sql_asset"})

    template =
      Template.compile!("SELECT 1 AS id",
        file: "test/orchestrator_sql_preflight.sql",
        line: 1,
        module: __MODULE__,
        scope: :asset,
        enforce_query_root: true
      )

    %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: :asset,
      type: :sql,
      execution: %{entrypoint: :asset, arity: 1},
      depends_on: Keyword.get(opts, :depends_on, []),
      relation: relation,
      materialization: :table,
      sql_execution: %SQLExecution{sql: "SELECT 1 AS id", template: template, sql_definitions: []}
    }
  end

  defp configure_missing_secret_connection do
    Application.put_env(:favn, :connection_modules, [__MODULE__.MissingSecretConnection])

    Application.put_env(:favn, :connections,
      preflight_sql: [database: ":memory:", password: Ref.secret_env!(@missing_secret_env)]
    )
  end

  defp restore_app_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_app_env(app, key, value), do: Application.put_env(app, key, value)
end

defmodule FavnOrchestrator.RunnerIntegrationTest.SleepAsset do
  alias Favn.Run.Context

  def asset(%Context{} = _ctx) do
    Process.sleep(150)
    :ok
  end
end

defmodule FavnOrchestrator.RunnerIntegrationTest.DailyPipeline do
end

defmodule FavnOrchestrator.RunnerIntegrationTest.PreflightElixirAsset do
  def asset(_ctx) do
    if pid = Application.get_env(:favn_orchestrator, :preflight_test_pid) do
      send(pid, :orchestrator_preflight_elixir_executed)
    end

    :ok
  end
end

defmodule FavnOrchestrator.RunnerIntegrationTest.PreflightSQLAsset do
end

defmodule FavnOrchestrator.RunnerIntegrationTest.MissingSecretConnection do
  @behaviour Favn.Connection

  alias Favn.Connection.Definition

  @impl true
  def definition do
    %Definition{
      name: :preflight_sql,
      adapter: FavnOrchestrator.RunnerIntegrationTest.FakeExecutionAdapter,
      config_schema: [
        %{key: :database, required: true, type: :path},
        %{key: :password, required: true, secret: true, type: :string}
      ]
    }
  end
end

defmodule FavnOrchestrator.RunnerIntegrationTest.FakeExecutionAdapter do
  alias Favn.Connection.Resolved
  alias Favn.SQL.Capabilities
  alias Favn.SQL.Result

  def connect(%Resolved{}, _opts), do: {:ok, :conn}
  def disconnect(:conn, _opts), do: :ok
  def capabilities(%Resolved{}, _opts), do: {:ok, %Capabilities{}}

  def materialize(:conn, _write_plan, _opts),
    do: {:ok, %Result{command: :insert, rows_affected: 1}}
end
