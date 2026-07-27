defmodule FavnRunnerTest do
  use ExUnit.Case, async: false

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

  test "readiness returns ok when the runner runtime is available" do
    assert :ok = FavnRunner.readiness()
  end

  test "diagnostics reports bounded runner identity and cache state" do
    assert {:ok, diagnostics} = FavnRunner.diagnostics()
    assert diagnostics.ready? == true
    assert diagnostics.status == :ready
    assert diagnostics.release.runner_release_id == FavnTestSupport.runner_release_id()
    assert diagnostics.release.runner_contract_version == 13
    assert diagnostics.manifest_cache.count >= 1
  end

  test "rejects a different release before manifest or work lookup", %{version: version} do
    alternate = FavnTestSupport.runner_release_id(:alternate)

    incompatible_version = %{
      version
      | runner_releases: %{"default" => alternate}
    }

    assert {:error, :manifest_runner_release_mismatch} =
             FavnRunner.register_manifest(incompatible_version)

    assert {:error, :manifest_runner_release_mismatch} =
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
            }} = FavnRunner.TestExecution.run(work)

    assert required == Map.fetch!(version.runner_releases, "default")

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

    assert {:ok, result} = FavnRunner.TestExecution.run(work)
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

    assert {:ok, result} = FavnRunner.TestExecution.run(work)
    assert result.status == :ok
    assert result.manifest_version_id == version.manifest_version_id
    assert result.required_runner_release_id == Map.fetch!(version.runner_releases, "default")
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

    assert {:ok, result} = FavnRunner.TestExecution.run(work)
    assert result.status == :ok

    assert [asset_result] = result.asset_results
    assert asset_result.ref == {FavnRunnerTest.SourceAsset, :asset}
    assert asset_result.meta[:observed] == true
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

    assert {:ok, result} = FavnRunner.TestExecution.run(work)
    assert result.status == :error
    assert %RunnerError{type: :invalid_return_shape, retryable?: false} = result.error
    assert [%{error: %RunnerError{type: :invalid_return_shape}}] = result.asset_results
  end

  defp build_manifest(assets) do
    refs = Enum.map(assets, & &1.ref)

    %Manifest{
      schema_version: 14,
      runner_contract_version: 13,
      runner_releases: %{"default" => FavnTestSupport.runner_release_id()},
      assets: assets,
      pipelines: [],
      schedules: [],
      graph: %Graph{nodes: refs, edges: [], topo_order: refs},
      metadata: %{}
    }
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

defmodule FavnRunnerTest.BlockingDiagnosticsAdapter do
  def diagnostics(_resolved, _opts) do
    Process.sleep(:infinity)
  end
end
