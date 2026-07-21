defmodule FavnRunnerTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Registry, as: ConnectionRegistry
  alias Favn.Connection.Resolved
  alias Favn.Contracts.RunnerCancellation
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version

  setup do
    manifest_version = "mv_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

    manifest =
      build_manifest([
        %Asset{
          ref: {FavnRunnerTest.ElixirAsset, :asset},
          module: FavnRunnerTest.ElixirAsset,
          name: :asset,
          type: :elixir,
          execution: %{entrypoint: :asset, arity: 1},
          settings: %{hello: "world"}
        },
        %Asset{
          ref: {FavnRunnerTest.SourceAsset, :asset},
          module: FavnRunnerTest.SourceAsset,
          name: :asset,
          type: :source,
          execution: %{entrypoint: nil, arity: nil},
          relation: %{name: "external_source"}
        }
      ])

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version)
    :ok = FavnRunner.register_manifest(version)

    %{version: version}
  end

  test "readiness returns ok when the runner server is available" do
    assert :ok = FavnRunner.readiness()
  end

  test "readiness reports unavailable when the runner server is stopped" do
    assert :ok = Supervisor.terminate_child(FavnRunner.Supervisor, FavnRunner.Server)

    on_exit(fn ->
      case Supervisor.restart_child(FavnRunner.Supervisor, FavnRunner.Server) do
        {:ok, _pid} -> :ok
        {:ok, _pid, _info} -> :ok
        {:error, :running} -> :ok
      end
    end)

    assert {:error, :runner_not_available} = FavnRunner.readiness()
  end

  test "diagnostics reports runner and redacted data-plane connection details" do
    connection = %Resolved{
      name: :warehouse,
      adapter: FavnRunnerTest.DiagnosticsAdapter,
      module: FavnRunnerTest.ConnectionModule,
      config: %{
        database: "/tmp/secret/path.duckdb",
        token: "connection-secret",
        production?: true,
        duckdb_storage: :local_file
      },
      required_keys: [:database],
      secret_fields: [:token],
      schema_keys: [:database, :token]
    }

    :ok =
      ConnectionRegistry.reload(%{warehouse: connection},
        registry_name: FavnRunner.ConnectionRegistry
      )

    on_exit(fn ->
      :ok = ConnectionRegistry.reload(%{}, registry_name: FavnRunner.ConnectionRegistry)
    end)

    assert {:ok, diagnostics} = FavnRunner.diagnostics()
    assert diagnostics.available? == true
    assert diagnostics.ready? == true
    assert diagnostics.status == :ready
    assert diagnostics.runner_release_id == FavnTestSupport.runner_release_id()
    assert diagnostics.favn_version == Favn.RunnerRelease.current_favn_version()
    assert diagnostics.runner_contract_version == 10
    assert is_binary(diagnostics.node_name)
    assert diagnostics.data_plane.connection_count == 1

    assert [entry] = diagnostics.data_plane.connections
    assert entry.status == :ok
    assert entry.config.database_path == :redacted
    assert entry.details.token == "[REDACTED]"
    refute inspect(diagnostics) =~ "connection-secret"
    refute inspect(diagnostics) =~ "/tmp/secret/path.duckdb"
  end

  test "rejects a different release before manifest or work lookup", %{version: version} do
    alternate = FavnTestSupport.runner_release_id(:alternate)
    incompatible_version = %{version | required_runner_release_id: alternate}

    assert {:error, %RunnerError{type: :runner_release_mismatch, retryable?: false}} =
             FavnRunner.register_manifest(incompatible_version)

    assert {:error, %RunnerError{type: :runner_release_mismatch, retryable?: false}} =
             FavnRunner.ensure_manifest(incompatible_version)

    work = %RunnerWork{
      required_runner_release_id: alternate,
      run_id: "run_wrong_release",
      manifest_version_id: "mv_not_registered",
      manifest_content_hash: "not-registered",
      asset_ref: {FavnRunnerTest.ElixirAsset, :asset}
    }

    assert {:error,
            %RunnerError{
              type: :runner_release_mismatch,
              retryable?: false,
              outcome: :safe_failure,
              details: %{
                required_runner_release_id: ^alternate,
                runner_release_id: required
              }
            }} = FavnRunner.submit_work(work)

    assert required == version.required_runner_release_id

    request = %RelationInspectionRequest{
      manifest_version_id: "mv_not_registered",
      required_runner_release_id: alternate,
      asset_ref: {FavnRunnerTest.ElixirAsset, :asset}
    }

    assert {:error, %RunnerError{type: :runner_release_mismatch, retryable?: false}} =
             FavnRunner.inspect_relation(request)
  end

  test "runs a local plain Elixir asset through runner execution boundary", %{version: version} do
    fixture_ref = {FavnRunnerTest.PlainElixirAsset, :asset}

    fixture_manifest =
      build_manifest([
        %Asset{
          ref: fixture_ref,
          module: elem(fixture_ref, 0),
          name: :asset,
          type: :elixir,
          execution: %{entrypoint: :asset, arity: 1}
        }
      ])

    {:ok, fixture_version} =
      Version.new(fixture_manifest,
        manifest_version_id:
          "mv_fixture_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
      )

    assert :ok = FavnRunner.register_manifest(fixture_version)

    work =
      %RunnerWork{
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        run_id: "run_fixture",
        manifest_version_id: fixture_version.manifest_version_id,
        manifest_content_hash: fixture_version.content_hash,
        asset_ref: fixture_ref,
        params: %{partition: "2026-03-25"}
      }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :ok
    assert [%{ref: ^fixture_ref, status: :ok}] = result.asset_results

    assert [%{meta: meta}] = result.asset_results
    assert meta == %{partition: "2026-03-25"}

    assert version.manifest_version_id != fixture_version.manifest_version_id
  end

  test "runs one elixir asset through runner boundary", %{version: version} do
    work =
      %RunnerWork{
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        run_id: "run_elixir",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {FavnRunnerTest.ElixirAsset, :asset},
        attempt: 2,
        max_attempts: 3,
        params: %{value: 42},
        metadata: %{attempt: 1}
      }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :ok
    assert result.manifest_version_id == version.manifest_version_id
    assert result.required_runner_release_id == version.required_runner_release_id
    assert [asset_result] = result.asset_results
    assert asset_result.ref == {FavnRunnerTest.ElixirAsset, :asset}
    assert asset_result.status == :ok
    assert asset_result.attempt_count == 2
    assert asset_result.max_attempts == 3
    assert [%{attempt: 2}] = asset_result.attempts
  end

  test "runs one source asset as observe/no-op", %{version: version} do
    work =
      %RunnerWork{
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        run_id: "run_source",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {FavnRunnerTest.SourceAsset, :asset}
      }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :ok

    assert [asset_result] = result.asset_results
    assert asset_result.ref == {FavnRunnerTest.SourceAsset, :asset}
    assert asset_result.meta[:observed] == true
  end

  test "server forwards subscribed execution logs" do
    fixture_ref = {FavnRunnerTest.SleepLogAsset, :asset}

    fixture_manifest =
      build_manifest([
        %Asset{
          ref: fixture_ref,
          module: elem(fixture_ref, 0),
          name: :asset,
          type: :elixir,
          execution: %{entrypoint: :asset, arity: 1}
        }
      ])

    {:ok, fixture_version} =
      Version.new(fixture_manifest,
        manifest_version_id:
          "mv_log_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
      )

    assert :ok = FavnRunner.register_manifest(fixture_version)

    work =
      leased_work(
        %RunnerWork{
          required_runner_release_id: FavnTestSupport.runner_release_id(),
          run_id: "run_log_forward",
          manifest_version_id: fixture_version.manifest_version_id,
          manifest_content_hash: fixture_version.content_hash,
          asset_ref: fixture_ref,
          metadata: %{attempt: 1}
        },
        fixture_version
      )

    assert {:ok, execution_id} = FavnRunner.submit_work(work)
    assert :ok = FavnRunner.subscribe_execution_logs(execution_id, self())
    assert {:ok, entry} = receive_runner_log(execution_id)

    assert entry.run_id == "run_log_forward"
    assert entry.source == :runner
    assert entry.runner_execution_id == execution_id
    assert entry.producer_id == "runner:" <> execution_id
    assert is_integer(entry.producer_sequence)

    assert {:ok, _result} = FavnRunner.await_result(execution_id, 1_000)
  end

  test "normalizes invalid asset return into a non-retryable runner error" do
    fixture_ref = {FavnRunnerTest.InvalidReturnAsset, :asset}

    fixture_manifest =
      build_manifest([
        %Asset{
          ref: fixture_ref,
          module: elem(fixture_ref, 0),
          name: :asset,
          type: :elixir,
          execution: %{entrypoint: :asset, arity: 1}
        }
      ])

    {:ok, fixture_version} =
      Version.new(fixture_manifest,
        manifest_version_id:
          "mv_invalid_return_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
      )

    assert :ok = FavnRunner.register_manifest(fixture_version)

    work = %RunnerWork{
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      run_id: "run_invalid_return",
      manifest_version_id: fixture_version.manifest_version_id,
      manifest_content_hash: fixture_version.content_hash,
      asset_ref: fixture_ref
    }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :error
    assert %RunnerError{type: :invalid_return_shape, retryable?: false} = result.error
    assert [%{error: %RunnerError{type: :invalid_return_shape}}] = result.asset_results
  end

  test "cancellation reports explicit runner outcome" do
    fixture_ref = {FavnRunnerTest.SleepLogAsset, :asset}

    fixture_manifest =
      build_manifest([
        %Asset{
          ref: fixture_ref,
          module: elem(fixture_ref, 0),
          name: :asset,
          type: :elixir,
          execution: %{entrypoint: :asset, arity: 1}
        }
      ])

    {:ok, fixture_version} =
      Version.new(fixture_manifest,
        manifest_version_id:
          "mv_cancel_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
      )

    assert :ok = FavnRunner.register_manifest(fixture_version)

    work =
      leased_work(
        %RunnerWork{
          required_runner_release_id: FavnTestSupport.runner_release_id(),
          run_id: "run_cancel",
          manifest_version_id: fixture_version.manifest_version_id,
          manifest_content_hash: fixture_version.content_hash,
          asset_ref: fixture_ref
        },
        fixture_version
      )

    assert {:ok, execution_id} = FavnRunner.submit_work(work)

    assert {:ok, %{status: :acknowledged, execution_id: ^execution_id}} =
             FavnRunner.cancel_work(execution_id, RunnerCancellation.request("run_cancel", :test))

    assert {:ok, result} = FavnRunner.await_result(execution_id, 1_000)
    assert result.status == :cancelled
    assert %RunnerError{kind: :cancelled, retryable?: false} = result.error
  end

  test "rejects direct submission without an active manifest lease" do
    work =
      %RunnerWork{
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        run_id: "run_missing",
        manifest_version_id: "mv_missing",
        manifest_content_hash: "hash_missing",
        asset_ref: {FavnRunnerTest.ElixirAsset, :asset}
      }

    assert {:error, :manifest_lease_not_found} = FavnRunner.submit_work(work)
  end

  defp receive_runner_log(execution_id, timeout \\ 1_000) do
    receive do
      {:runner_log_entry, ^execution_id, entry} ->
        {:ok, entry}

      {:runner_log_entry, _other_execution_id, _entry} ->
        receive_runner_log(execution_id, timeout)
    after
      timeout -> {:error, :timeout}
    end
  end

  defp build_manifest(assets) do
    refs = Enum.map(assets, & &1.ref)

    %Manifest{
      schema_version: 10,
      runner_contract_version: 10,
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      assets: assets,
      pipelines: [],
      schedules: [],
      graph: %Graph{nodes: refs, edges: [], topo_order: refs},
      metadata: %{}
    }
  end

  defp leased_work(%RunnerWork{} = work, %Version{} = version) do
    lease_id = "test:" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
    expires_at = DateTime.add(DateTime.utc_now(), 60, :second)
    planned_asset_refs = Enum.map(version.manifest.assets, & &1.ref)
    assert :ok = FavnRunner.acquire_manifest(version, lease_id, expires_at, planned_asset_refs)
    on_exit(fn -> FavnRunner.release_manifest(lease_id) end)
    %{work | manifest_lease_id: lease_id}
  end
end

defmodule FavnRunnerTest.PlainElixirAsset do
  alias Favn.Run.Context

  @spec asset(Context.t()) :: {:ok, map()}
  def asset(%Context{} = ctx), do: {:ok, %{partition: ctx.params[:partition]}}
end

defmodule FavnRunnerTest.ElixirAsset do
  alias Favn.Run.Context

  @spec asset(Context.t()) :: :ok | {:ok, map()}
  def asset(%Context{} = ctx) do
    {:ok, %{asset_ref: ctx.asset.ref, params: ctx.params}}
  end
end

defmodule FavnRunnerTest.SourceAsset do
end

defmodule FavnRunnerTest.SleepLogAsset do
  alias Favn.Run.Context

  @spec asset(Context.t()) :: :ok
  def asset(%Context{}) do
    Process.sleep(100)
    :ok
  end
end

defmodule FavnRunnerTest.InvalidReturnAsset do
  alias Favn.Run.Context

  @spec asset(Context.t()) :: atom()
  def asset(%Context{}), do: :not_a_valid_asset_return
end

defmodule FavnRunnerTest.ConnectionModule do
end

defmodule FavnRunnerTest.DiagnosticsAdapter do
  def diagnostics(_resolved, _opts) do
    {:ok, %{status: :ok, token: "connection-secret", database: "/tmp/secret/path.duckdb"}}
  end
end
