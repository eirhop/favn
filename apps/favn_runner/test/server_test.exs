defmodule FavnRunner.ServerTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerEvent
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version

  test "replays an orchestrator execution id only for the exact same work" do
    server = start_runner_server()

    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version, server: server)

    execution_id = unique_id("rx_control_plane")

    work =
      version
      |> build_work(FavnRunner.ServerTest.SlowAsset)
      |> Map.put(:execution_id, execution_id)

    assert {:ok, ^execution_id} = FavnRunner.submit_work(work, server: server)
    assert {:ok, ^execution_id} = FavnRunner.submit_work(work, server: server)

    changed_work = %{work | params: %{different: true}}

    assert {:error,
            %RunnerError{
              type: :runner_execution_id_in_use,
              retryable?: false,
              outcome: :safe_failure
            }} = FavnRunner.submit_work(changed_work, server: server)

    assert {:ok, %{status: :acknowledged}} =
             FavnRunner.cancel_work(execution_id, %{reason: :test_cleanup}, server: server)

    assert {:ok, _result} = FavnRunner.await_result(execution_id, 1_000, server: server)
    assert {:ok, ^execution_id} = FavnRunner.submit_work(work, server: server)

    assert {:error, %RunnerError{type: :runner_execution_id_in_use}} =
             FavnRunner.submit_work(changed_work, server: server)
  end

  test "exact replay remains available after its manifest cache entry is evicted" do
    {:ok, manifest_store} =
      start_supervised({FavnRunner.ManifestStore, name: nil, max_entries: 1})

    server = start_runner_server(manifest_store: manifest_store)

    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv_replay")
      )

    assert :ok =
             FavnRunner.register_manifest(version,
               server: server,
               manifest_store: manifest_store
             )

    execution_id = unique_id("rx_evicted_replay")

    work =
      version
      |> build_work(FavnRunner.ServerTest.SlowAsset, manifest_store: manifest_store)
      |> Map.put(:execution_id, execution_id)

    submit_opts = [server: server, manifest_store: manifest_store]
    assert {:ok, ^execution_id} = FavnRunner.submit_work(work, submit_opts)

    assert :ok =
             FavnRunner.release_manifest(work.manifest_lease_id, manifest_store: manifest_store)

    {:ok, replacement} =
      Version.new(build_manifest(FavnRunner.ServerTest.FastAsset),
        manifest_version_id: unique_id("mv_replacement")
      )

    assert :ok = FavnRunner.register_manifest(replacement, manifest_store: manifest_store)

    assert {:error, :manifest_not_found} =
             FavnRunner.ManifestStore.fetch(
               version.manifest_version_id,
               version.content_hash,
               server: manifest_store
             )

    assert {:ok, ^execution_id} = FavnRunner.submit_work(work, submit_opts)

    assert {:ok, %{status: :acknowledged}} =
             FavnRunner.cancel_work(execution_id, %{reason: :test_cleanup}, server: server)

    assert {:ok, _result} = FavnRunner.await_result(execution_id, 1_000, server: server)
    assert {:ok, ^execution_id} = FavnRunner.submit_work(work, submit_opts)
  end

  test "runtime input resolution honors a configured manifest store" do
    {:ok, manifest_store} = start_supervised({FavnRunner.ManifestStore, name: nil})

    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.FastAsset),
        manifest_version_id: unique_id("mv_custom_resolver_store")
      )

    assert :ok = FavnRunner.register_manifest(version, manifest_store: manifest_store)

    work = build_work(version, FavnRunner.ServerTest.FastAsset, manifest_store: manifest_store)

    assert {:ok, nil} =
             FavnRunner.resolve_runtime_inputs(work, manifest_store: manifest_store)
  end

  test "rejects an invalid orchestrator-allocated execution id" do
    server = start_runner_server()

    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.FastAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version, server: server)

    work =
      version
      |> build_work(FavnRunner.ServerTest.FastAsset)
      |> Map.put(:execution_id, "")

    assert {:error,
            %RunnerError{
              type: :invalid_runner_execution_id,
              retryable?: false,
              outcome: :safe_failure
            }} = FavnRunner.submit_work(work, server: server)
  end

  test "cancel_work marks running execution as cancelled" do
    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version)

    work = build_work(version, FavnRunner.ServerTest.SlowAsset)

    assert {:ok, execution_id} = FavnRunner.submit_work(work)

    assert {:ok, %{status: :acknowledged}} =
             FavnRunner.cancel_work(execution_id, %{reason: :operator_cancel})

    assert {:ok, result} = FavnRunner.await_result(execution_id, 7_000)
    assert result.status == :cancelled
  end

  test "discards runner events that do not match their stored work identity" do
    server = start_runner_server()

    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv_event_binding")
      )

    assert :ok = FavnRunner.register_manifest(version, server: server)
    work = build_work(version, FavnRunner.ServerTest.SlowAsset)
    assert {:ok, execution_id} = FavnRunner.submit_work(work, server: server)

    forged = %RunnerEvent{
      run_id: work.run_id,
      manifest_version_id: work.manifest_version_id,
      manifest_content_hash: String.duplicate("f", 64),
      required_runner_release_id: work.required_runner_release_id,
      event_type: :asset_started,
      occurred_at: DateTime.utc_now(),
      payload: %{}
    }

    send(server, {:runner_event, execution_id, forged})
    state = :sys.get_state(server)
    execution = state.lifecycle.executions[execution_id]
    refute forged in execution.events

    assert {:ok, %{status: :acknowledged}} =
             FavnRunner.cancel_work(execution_id, %{reason: :test_cleanup}, server: server)

    assert {:ok, _result} = FavnRunner.await_result(execution_id, 1_000, server: server)
  end

  test "replaces worker results that do not match their stored work identity" do
    server = start_runner_server()

    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv_result_binding")
      )

    assert :ok = FavnRunner.register_manifest(version, server: server)
    work = build_work(version, FavnRunner.ServerTest.SlowAsset)
    assert {:ok, execution_id} = FavnRunner.submit_work(work, server: server)

    state = :sys.get_state(server)
    worker_pid = state.lifecycle.executions[execution_id].pid

    forged = %RunnerResult{
      run_id: "run_forged",
      manifest_version_id: work.manifest_version_id,
      manifest_content_hash: work.manifest_content_hash,
      required_runner_release_id: work.required_runner_release_id,
      status: :ok,
      asset_results: [],
      metadata: %{}
    }

    send(server, {:runner_result, execution_id, forged})

    assert {:ok, result} = FavnRunner.await_result(execution_id, 1_000, server: server)
    assert result.status == :error
    assert result.run_id == work.run_id
    assert %RunnerError{type: :runner_result_identity_mismatch, retryable?: false} = result.error

    Process.exit(worker_pid, :kill)
  end

  test "late runner_result does not overwrite a cancelled terminal result" do
    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version)

    work = build_work(version, FavnRunner.ServerTest.SlowAsset)

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

    work = build_work(version, FavnRunner.ServerTest.SlowAsset)

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

    work = build_work(version, FavnRunner.ServerTest.SlowAsset)

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
    server = start_runner_server(admission: [max_active_workers: 1])

    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version, server: server)

    assert {:ok, execution_id} =
             FavnRunner.submit_work(build_work(version, FavnRunner.ServerTest.SlowAsset),
               server: server
             )

    assert {:error,
            %RunnerError{
              type: :runner_overloaded,
              kind: :boundary,
              retryable?: true,
              outcome: :safe_failure
            }} =
             FavnRunner.submit_work(build_work(version, FavnRunner.ServerTest.SlowAsset),
               server: server
             )

    assert {:ok, diagnostics} = FavnRunner.diagnostics(server: server)
    assert diagnostics.admission.active_worker_count == 1
    assert diagnostics.admission.rejected_overload_count == 1
    assert diagnostics.admission.max_active_workers == 1

    assert {:ok, %{status: :acknowledged}} =
             FavnRunner.cancel_work(execution_id, %{}, server: server)
  end

  test "submit_work rejects instead of blocking when bounded capacity is exhausted" do
    server = start_runner_server(admission: [max_active_workers: 1])

    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version, server: server)

    assert {:ok, first_execution_id} =
             FavnRunner.submit_work(build_work(version, FavnRunner.ServerTest.SlowAsset),
               server: server
             )

    assert {:error,
            %RunnerError{
              type: :runner_overloaded,
              kind: :boundary,
              retryable?: true,
              outcome: :safe_failure
            }} =
             FavnRunner.submit_work(build_work(version, FavnRunner.ServerTest.SlowAsset),
               server: server
             )

    assert {:ok, %{status: :acknowledged}} =
             FavnRunner.cancel_work(first_execution_id, %{}, server: server)
  end

  test "shutdown deadline cancellation stops active workers without another drain-length wait" do
    server = start_runner_server(admission: [max_active_workers: 1])

    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version, server: server)

    assert {:ok, execution_id} =
             FavnRunner.submit_work(build_work(version, FavnRunner.ServerTest.SlowAsset),
               server: server
             )

    started_at = System.monotonic_time(:millisecond)

    assert {:ok, 1} =
             FavnRunner.Server.cancel_active(%{kind: :runner_shutdown_deadline}, server: server)

    assert System.monotonic_time(:millisecond) - started_at < 500

    assert {:ok,
            %{
              status: :error,
              error: %RunnerError{
                type: :runner_shutdown_interrupted,
                outcome: :unknown,
                details: %{native_status: :native_cancel_unknown}
              },
              metadata: %{
                shutdown_interruption: %{native_status: :native_cancel_unknown}
              }
            }} = FavnRunner.await_result(execution_id, 1_000, server: server)
  end

  test "submit_work accepts new work after a worker completes" do
    server = start_runner_server(admission: [max_active_workers: 1])

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

  test "worker compacts oversized metadata before sending it to the central server" do
    server =
      start_runner_server(
        retention: [
          max_completed_execution_bytes: 20_000,
          max_completed_bytes: 100_000
        ]
      )

    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.LargeMetadataAsset),
        manifest_version_id: unique_id("mv")
      )

    work = build_work(version, FavnRunner.ServerTest.LargeMetadataAsset)

    assert {:ok, execution_id} = FavnRunner.submit_work(work, server: server)
    assert {:ok, result} = FavnRunner.await_result(execution_id, 1_000, server: server)

    assert :erlang.external_size(result) <= 15_000
    assert result.metadata.retention.truncated
    assert result.metadata.retention.original_bytes > 1_000_000
  end

  test "invalid admission config normalizes to safe defaults" do
    server =
      start_runner_server(admission: [max_active_workers: 0])

    assert {:ok, diagnostics} = FavnRunner.diagnostics(server: server)
    assert diagnostics.admission.max_active_workers == System.schedulers_online()
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
      execution && execution.logs != []
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
      schema_version: 11,
      runner_contract_version: 11,
      required_runner_release_id: FavnTestSupport.runner_release_id(),
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

  defp build_work(%Version{} = version, asset_module, opts \\ []) do
    lease_id = unique_id("manifest_lease")
    expires_at = DateTime.add(DateTime.utc_now(), 3_600, :second)
    manifest_opts = Keyword.take(opts, [:manifest_store])

    planned_asset_refs = Enum.map(version.manifest.assets, & &1.ref)

    assert :ok =
             FavnRunner.acquire_manifest(
               version,
               lease_id,
               expires_at,
               planned_asset_refs,
               manifest_opts
             )

    on_exit(fn ->
      try do
        FavnRunner.release_manifest(lease_id, manifest_opts)
      catch
        :exit, _reason -> :ok
      end
    end)

    %RunnerWork{
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      run_id: unique_id("run"),
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      manifest_lease_id: lease_id,
      asset_ref: {asset_module, :asset}
    }
  end

  defp start_runner_server(opts \\ []) do
    server = String.to_atom(unique_id("runner_server"))

    server_opts =
      if Enum.any?([:admission, :retention, :manifest_store], &Keyword.has_key?(opts, &1)) do
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

defmodule FavnRunner.ServerTest.LargeMetadataAsset do
  @spec asset(Favn.Run.Context.t()) :: {:ok, map()}
  def asset(_ctx), do: {:ok, %{payload: String.duplicate("x", 2 * 1_024 * 1_024)}}
end
