defmodule FavnRunner.ServerTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version

  test "cancel_work marks running execution as cancelled" do
    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version)

    work =
      %RunnerWork{
        run_id: unique_id("run"),
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {FavnRunner.ServerTest.SlowAsset, :asset}
      }

    assert {:ok, execution_id} = FavnRunner.submit_work(work)

    assert {:ok, %{status: :acknowledged}} =
             FavnRunner.cancel_work(execution_id, %{reason: :operator_cancel})

    assert {:ok, result} = FavnRunner.await_result(execution_id, 7_000)
    assert result.status == :cancelled
  end

  test "late runner_result does not overwrite a cancelled terminal result" do
    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version)

    work =
      %RunnerWork{
        run_id: unique_id("run"),
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {FavnRunner.ServerTest.SlowAsset, :asset}
      }

    assert {:ok, execution_id} = FavnRunner.submit_work(work)

    assert {:ok, %{status: :acknowledged}} =
             FavnRunner.cancel_work(execution_id, %{reason: :operator_cancel})

    send(FavnRunner.Server, {
      :runner_result,
      execution_id,
      %RunnerResult{
        run_id: work.run_id,
        manifest_version_id: work.manifest_version_id,
        manifest_content_hash: work.manifest_content_hash,
        status: :ok,
        asset_results: [],
        error: nil,
        metadata: %{}
      }
    })

    assert {:ok, result} = FavnRunner.await_result(execution_id, 7_000)
    assert result.status == :cancelled
  end

  test "server returns worker crash result when worker dies unexpectedly" do
    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version)

    work =
      %RunnerWork{
        run_id: unique_id("run"),
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {FavnRunner.ServerTest.SlowAsset, :asset}
      }

    assert {:ok, execution_id} = FavnRunner.submit_work(work)

    state = :sys.get_state(FavnRunner.Server)
    worker_pid = state.lifecycle.executions[execution_id].pid
    assert is_pid(worker_pid)
    Process.exit(worker_pid, :kill)

    assert {:ok, result} = FavnRunner.await_result(execution_id, 2_000)
    assert result.status == :error
    assert %RunnerError{type: :worker_crash, kind: :exit} = result.error
    assert result.error.reason == ":killed"
  end

  test "await_result returns timeout while execution is still running" do
    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version)

    work =
      %RunnerWork{
        run_id: unique_id("run"),
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {FavnRunner.ServerTest.SlowAsset, :asset}
      }

    assert {:ok, execution_id} = FavnRunner.submit_work(work)
    assert {:error, :timeout} = FavnRunner.await_result(execution_id, 10)
    assert {:ok, result} = FavnRunner.await_result(execution_id, 7_000)
    assert result.status == :ok
  end

  test "cancel_work and await_result return execution_not_found for unknown execution id" do
    assert {:ok, %{status: :not_found}} = FavnRunner.cancel_work("rx_missing")
    assert {:error, :execution_not_found} = FavnRunner.await_result("rx_missing", 50)
  end

  test "completed execution retention evicts the oldest retained result" do
    server = start_runner_server(max_completed_executions: 1)

    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.FastAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version, server: server)

    first_work = build_work(version, FavnRunner.ServerTest.FastAsset)
    second_work = build_work(version, FavnRunner.ServerTest.FastAsset)

    assert {:ok, first_execution_id} = FavnRunner.submit_work(first_work, server: server)

    assert {:ok, first_result} =
             FavnRunner.await_result(first_execution_id, 1_000, server: server)

    assert first_result.status == :ok

    assert {:ok, second_execution_id} = FavnRunner.submit_work(second_work, server: server)

    assert {:ok, second_result} =
             FavnRunner.await_result(second_execution_id, 1_000, server: server)

    assert second_result.status == :ok

    assert {:error, :execution_not_found} =
             FavnRunner.await_result(first_execution_id, 50, server: server)

    assert {:ok, diagnostics} = FavnRunner.diagnostics(server: server)
    assert diagnostics.completed_executions == 1
    assert diagnostics.retention.evicted_completed_executions == 1
  end

  test "submit_work rejects overload when active worker capacity is exhausted" do
    server = start_runner_server(admission: [max_active_workers: 1, max_queue_size: 0])

    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version, server: server)

    assert {:ok, execution_id} =
             FavnRunner.submit_work(build_work(version, FavnRunner.ServerTest.SlowAsset),
               server: server
             )

    assert {:error, %RunnerError{type: :runner_overloaded, kind: :boundary, retryable?: true}} =
             FavnRunner.submit_work(build_work(version, FavnRunner.ServerTest.SlowAsset),
               server: server
             )

    assert {:ok, diagnostics} = FavnRunner.diagnostics(server: server)
    assert diagnostics.admission.active_worker_count == 1
    assert diagnostics.admission.queued_worker_count == 0
    assert diagnostics.admission.rejected_overload_count == 1
    assert diagnostics.admission.max_active_workers == 1
    assert diagnostics.admission.max_queue_size == 0

    assert {:ok, %{status: :acknowledged}} =
             FavnRunner.cancel_work(execution_id, %{}, server: server)
  end

  test "submit_work rejects instead of blocking when bounded capacity is exhausted" do
    server =
      start_runner_server(
        admission: [max_active_workers: 1, max_queue_size: 1, queue_timeout_ms: 1_000]
      )

    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version, server: server)

    assert {:ok, first_execution_id} =
             FavnRunner.submit_work(build_work(version, FavnRunner.ServerTest.SlowAsset),
               server: server
             )

    assert {:error, %RunnerError{type: :runner_overloaded, kind: :boundary, retryable?: true}} =
             FavnRunner.submit_work(build_work(version, FavnRunner.ServerTest.SlowAsset),
               server: server
             )

    assert {:ok, diagnostics} = FavnRunner.diagnostics(server: server)
    assert diagnostics.admission.queued_worker_count == 0

    assert {:ok, %{status: :acknowledged}} =
             FavnRunner.cancel_work(first_execution_id, %{}, server: server)
  end

  test "submit_work accepts new work after a worker completes" do
    server = start_runner_server(admission: [max_active_workers: 1, max_queue_size: 0])

    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.FastAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version, server: server)

    assert {:ok, first_execution_id} =
             FavnRunner.submit_work(build_work(version, FavnRunner.ServerTest.FastAsset),
               server: server
             )

    assert {:ok, %{status: :ok}} =
             FavnRunner.await_result(first_execution_id, 1_000, server: server)

    assert {:ok, second_execution_id} =
             FavnRunner.submit_work(build_work(version, FavnRunner.ServerTest.FastAsset),
               server: server
             )

    assert {:ok, %{status: :ok}} =
             FavnRunner.await_result(second_execution_id, 1_000, server: server)
  end

  test "invalid admission config normalizes to safe defaults" do
    server =
      start_runner_server(
        admission: [max_active_workers: 0, max_queue_size: -1, queue_timeout_ms: :bad]
      )

    assert {:ok, diagnostics} = FavnRunner.diagnostics(server: server)
    assert diagnostics.admission.max_active_workers == System.schedulers_online()
    assert diagnostics.admission.max_queue_size == System.schedulers_online() * 2
    assert diagnostics.admission.queue_timeout_ms == 30_000
  end

  test "log replay is bounded to newest entries in chronological order" do
    server = start_runner_server(max_logs_per_execution: 2)

    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version, server: server)

    work = build_work(version, FavnRunner.ServerTest.SlowAsset)
    assert {:ok, execution_id} = FavnRunner.submit_work(work, server: server)

    wait_until(fn ->
      state = :sys.get_state(server)
      execution = state.lifecycle.executions[execution_id]
      execution && length(execution.logs) > 0
    end)

    send(server, {:runner_log_entry, execution_id, %{sequence: 1}})
    send(server, {:runner_log_entry, execution_id, %{sequence: 2}})
    send(server, {:runner_log_entry, execution_id, %{sequence: 3}})

    wait_until(fn ->
      state = :sys.get_state(server)
      execution = state.lifecycle.executions[execution_id]
      length(execution.logs) == 2 and execution.dropped_log_count >= 1
    end)

    assert :ok = FavnRunner.subscribe_execution_logs(execution_id, self(), server: server)
    assert_receive {:runner_log_entry, ^execution_id, %{sequence: 2}}
    assert_receive {:runner_log_entry, ^execution_id, %{sequence: 3}}
    refute_receive {:runner_log_entry, ^execution_id, %{sequence: 1}}, 50

    assert {:ok, %{status: :acknowledged}} =
             FavnRunner.cancel_work(execution_id, %{}, server: server)
  end

  test "dead log subscribers are removed from diagnostics" do
    server = start_runner_server()

    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version, server: server)

    work = build_work(version, FavnRunner.ServerTest.SlowAsset)
    assert {:ok, execution_id} = FavnRunner.submit_work(work, server: server)

    subscriber =
      spawn(fn ->
        :ok = FavnRunner.subscribe_execution_logs(execution_id, self(), server: server)

        receive do
          :stop -> :ok
        end
      end)

    wait_until(fn ->
      {:ok, diagnostics} = FavnRunner.diagnostics(server: server)
      diagnostics.log_subscribers == 1
    end)

    Process.exit(subscriber, :kill)

    wait_until(fn ->
      {:ok, diagnostics} = FavnRunner.diagnostics(server: server)
      diagnostics.log_subscribers == 0 and diagnostics.log_subscriptions == 0
    end)

    assert {:ok, %{status: :acknowledged}} =
             FavnRunner.cancel_work(execution_id, %{}, server: server)
  end

  test "abandoned await callers are removed from diagnostics" do
    server = start_runner_server()

    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version, server: server)

    work = build_work(version, FavnRunner.ServerTest.SlowAsset)
    assert {:ok, execution_id} = FavnRunner.submit_work(work, server: server)

    waiter =
      spawn(fn ->
        _ = FavnRunner.await_result(execution_id, 5_000, server: server)
      end)

    wait_until(fn ->
      {:ok, diagnostics} = FavnRunner.diagnostics(server: server)
      diagnostics.waiters == 1
    end)

    Process.exit(waiter, :kill)

    wait_until(fn ->
      {:ok, diagnostics} = FavnRunner.diagnostics(server: server)
      diagnostics.waiters == 0
    end)

    assert {:ok, %{status: :acknowledged}} =
             FavnRunner.cancel_work(execution_id, %{}, server: server)
  end

  test "favn runner facade validates await and cancel arguments" do
    assert {:error, :invalid_await_args} = FavnRunner.await_result(:bad, 10)
    assert {:error, :invalid_await_args} = FavnRunner.await_result("rx", -1)
    assert {:error, %RunnerError{type: :invalid_cancel_args}} = FavnRunner.cancel_work(:bad, %{})
    assert {:error, %RunnerError{type: :invalid_cancel_args}} = FavnRunner.cancel_work("rx", :bad)
  end

  defp build_manifest(asset_module) do
    ref = {asset_module, :asset}

    %Manifest{
      schema_version: 4,
      runner_contract_version: 4,
      assets: [
        %Asset{
          ref: ref,
          module: asset_module,
          name: :asset,
          type: :elixir,
          execution: %{entrypoint: :asset, arity: 1}
        }
      ],
      pipelines: [],
      schedules: [],
      graph: %Graph{nodes: [ref], edges: [], topo_order: [ref]},
      metadata: %{}
    }
  end

  defp unique_id(prefix),
    do: prefix <> "_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

  defp build_work(%Version{} = version, asset_module) do
    %RunnerWork{
      run_id: unique_id("run"),
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: {asset_module, :asset}
    }
  end

  defp start_runner_server(opts \\ []) do
    server = String.to_atom(unique_id("runner_server"))

    server_opts =
      if Keyword.has_key?(opts, :admission) or Keyword.has_key?(opts, :retention) do
        opts
      else
        [retention: opts]
      end

    start_supervised!({FavnRunner.Server, Keyword.merge([name: server], server_opts)})
    server
  end

  defp wait_until(fun, attempts \\ 50)

  defp wait_until(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, attempts - 1)
    end
  end

  defp wait_until(fun, 0), do: assert(fun.())
end

defmodule FavnRunner.ServerTest.FastAsset do
  @spec asset(Favn.Run.Context.t()) :: :ok
  def asset(_ctx), do: :ok
end

defmodule FavnRunner.ServerTest.SlowAsset do
  @spec asset(Favn.Run.Context.t()) :: :ok
  def asset(_ctx) do
    Process.sleep(5_000)
    :ok
  end
end
