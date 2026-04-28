defmodule FavnRunner.WorkerTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version
  alias Favn.RuntimeConfig.Ref

  test "worker sends runner result for crashing asset invocation" do
    asset =
      %Asset{
        ref: {FavnRunner.WorkerTest.CrashingAsset, :asset},
        module: FavnRunner.WorkerTest.CrashingAsset,
        name: :asset,
        type: :elixir,
        execution: %{entrypoint: :asset, arity: 1}
      }

    manifest =
      %Manifest{
        schema_version: 1,
        runner_contract_version: 1,
        assets: [asset],
        pipelines: [],
        schedules: [],
        graph: %Graph{nodes: [asset.ref], edges: [], topo_order: [asset.ref]},
        metadata: %{}
      }

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv_worker_test")

    work =
      %RunnerWork{
        run_id: "run_worker_test",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: asset.ref,
        metadata: %{}
      }

    assert {:ok, _pid} =
             FavnRunner.Worker.start_link(%{
               server: self(),
               execution_id: "rx_worker_test",
               work: work,
               version: version,
               asset: asset
             })

    assert_receive {:runner_result, "rx_worker_test", %RunnerResult{} = result}, 2_000
    assert result.status == :error
    assert [%{status: :error}] = result.asset_results
  end

  test "worker normalizes throw and exit failure kinds" do
    assert_throw_exit_result(FavnRunner.WorkerTest.ThrowingAsset, :throw)
    assert_throw_exit_result(FavnRunner.WorkerTest.ExitingAsset, :exit)
  end

  test "worker normalizes invalid return shape" do
    result = run_single_asset(FavnRunner.WorkerTest.BadReturnAsset)

    assert result.status == :error
    assert [asset_result] = result.asset_results
    assert asset_result.status == :error

    assert asset_result.error.reason ==
             {:invalid_return_shape, {:ok, :bad_shape},
              expected: ":ok | {:ok, map()} | {:error, reason}"}
  end

  test "worker rejects unsupported entrypoint arity" do
    result =
      run_single_asset(FavnRunner.WorkerTest.UnsupportedArityAsset,
        execution: %{entrypoint: :asset, arity: 2}
      )

    assert result.status == :error
    assert [asset_result] = result.asset_results

    assert asset_result.error.reason ==
             {:unsupported_entrypoint_arity, 2, expected: 1}
  end

  test "worker resolves runtime config into context before asset invocation" do
    previous_segment = System.get_env("FAVN_TEST_SEGMENT_ID")
    previous_token = System.get_env("FAVN_TEST_TOKEN")

    try do
      System.put_env("FAVN_TEST_SEGMENT_ID", "segment-123")
      System.put_env("FAVN_TEST_TOKEN", "secret-token")

      result =
        run_single_asset(FavnRunner.WorkerTest.ConfigAsset,
          runtime_config: %{
            source_system: %{
              segment_id: Ref.env!("FAVN_TEST_SEGMENT_ID"),
              token: Ref.secret_env!("FAVN_TEST_TOKEN")
            }
          }
        )

      assert result.status == :ok
      assert [%{meta: %{segment_id: "segment-123", token_seen?: true}}] = result.asset_results
    after
      restore_env("FAVN_TEST_SEGMENT_ID", previous_segment)
      restore_env("FAVN_TEST_TOKEN", previous_token)
    end
  end

  test "worker fails before invocation when required runtime config is missing" do
    previous = System.get_env("FAVN_TEST_MISSING_REQUIRED")

    try do
      System.delete_env("FAVN_TEST_MISSING_REQUIRED")

      result =
        run_single_asset(FavnRunner.WorkerTest.ConfigAsset,
          runtime_config: %{
            source_system: %{segment_id: Ref.env!("FAVN_TEST_MISSING_REQUIRED")}
          }
        )

      assert result.status == :error
      assert [%{error: error}] = result.asset_results
      assert error.type == :missing_env
      assert error.message == "missing_env FAVN_TEST_MISSING_REQUIRED"
    after
      restore_env("FAVN_TEST_MISSING_REQUIRED", previous)
    end
  end

  test "worker redacts declared secret runtime config from returned metadata" do
    previous_segment = System.get_env("FAVN_TEST_LEAK_SEGMENT_ID")
    previous_token = System.get_env("FAVN_TEST_LEAK_TOKEN")

    try do
      System.put_env("FAVN_TEST_LEAK_SEGMENT_ID", "segment-456")
      System.put_env("FAVN_TEST_LEAK_TOKEN", "leaked-secret-token")

      result =
        run_single_asset(FavnRunner.WorkerTest.ConfigLeakAsset,
          runtime_config: %{
            source_system: %{
              segment_id: Ref.env!("FAVN_TEST_LEAK_SEGMENT_ID"),
              token: Ref.secret_env!("FAVN_TEST_LEAK_TOKEN")
            }
          }
        )

      assert result.status == :ok
      assert [%{meta: meta, attempts: [%{meta: attempt_meta}]}] = result.asset_results
      assert meta.config.source_system.segment_id == "segment-456"
      assert meta.config.source_system.token == :redacted
      assert meta.nested.source_system.token == :redacted
      assert attempt_meta.config.source_system.token == :redacted
      refute inspect(result) =~ "leaked-secret-token"
    after
      restore_env("FAVN_TEST_LEAK_SEGMENT_ID", previous_segment)
      restore_env("FAVN_TEST_LEAK_TOKEN", previous_token)
    end
  end

  defp assert_throw_exit_result(module, expected_kind) do
    result = run_single_asset(module)
    assert result.status == :error
    assert [asset_result] = result.asset_results
    assert asset_result.status == :error
    assert asset_result.error.kind == expected_kind
  end

  defp run_single_asset(module, opts \\ []) do
    asset =
      %Asset{
        ref: {module, :asset},
        module: module,
        name: :asset,
        type: :elixir,
        execution: Keyword.get(opts, :execution, %{entrypoint: :asset, arity: 1}),
        runtime_config: Keyword.get(opts, :runtime_config, %{})
      }

    manifest =
      %Manifest{
        schema_version: 1,
        runner_contract_version: 1,
        assets: [asset],
        pipelines: [],
        schedules: [],
        graph: %Graph{nodes: [asset.ref], edges: [], topo_order: [asset.ref]},
        metadata: %{}
      }

    manifest_version_id =
      "mv_worker_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)

    work =
      %RunnerWork{
        run_id: "run_worker_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower),
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: asset.ref,
        metadata: %{}
      }

    execution_id = "rx_worker_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

    assert {:ok, _pid} =
             FavnRunner.Worker.start_link(%{
               server: self(),
               execution_id: execution_id,
               work: work,
               version: version,
               asset: asset
             })

    assert_receive {:runner_result, ^execution_id, %RunnerResult{} = result}, 2_000
    result
  end

  defp restore_env(key, nil), do: System.delete_env(key)
  defp restore_env(key, value), do: System.put_env(key, value)
end

defmodule FavnRunner.WorkerTest.CrashingAsset do
  @spec asset(Favn.Run.Context.t()) :: no_return()
  def asset(_ctx), do: raise("boom")
end

defmodule FavnRunner.WorkerTest.ThrowingAsset do
  @spec asset(Favn.Run.Context.t()) :: no_return()
  def asset(_ctx), do: throw(:boom)
end

defmodule FavnRunner.WorkerTest.ExitingAsset do
  @spec asset(Favn.Run.Context.t()) :: no_return()
  def asset(_ctx), do: exit(:boom)
end

defmodule FavnRunner.WorkerTest.BadReturnAsset do
  @spec asset(Favn.Run.Context.t()) :: {:ok, atom()}
  def asset(_ctx), do: {:ok, :bad_shape}
end

defmodule FavnRunner.WorkerTest.UnsupportedArityAsset do
  @spec asset(Favn.Run.Context.t(), term()) :: :ok
  def asset(_ctx, _value), do: :ok
end

defmodule FavnRunner.WorkerTest.ConfigAsset do
  @spec asset(Favn.Run.Context.t()) :: {:ok, map()}
  def asset(ctx) do
    {:ok,
     %{
       segment_id: ctx.config.source_system.segment_id,
       token_seen?: Map.get(ctx.config.source_system, :token) == "secret-token"
     }}
  end
end

defmodule FavnRunner.WorkerTest.ConfigLeakAsset do
  @spec asset(Favn.Run.Context.t()) :: {:ok, map()}
  def asset(ctx) do
    {:ok, %{config: ctx.config, nested: %{source_system: ctx.config.source_system}}}
  end
end
