defmodule FavnOrchestrator.LogsTest do
  use ExUnit.Case, async: false

  alias Favn.Log.Entry
  alias Favn.Log.Filter
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.RunnerLogBridge
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory
  alias FavnOrchestrator.TransitionWriter

  defmodule RunnerClientLogStub do
    def subscribe_execution_logs(execution_id, subscriber, opts) do
      opts
      |> Keyword.fetch!(:runner_log_entries)
      |> Map.fetch!(execution_id)
      |> Enum.each(fn entry -> send(subscriber, {:runner_log_entry, execution_id, entry}) end)

      :ok
    end

    def unsubscribe_execution_logs(_execution_id, _subscriber, _opts), do: :ok
  end

  setup do
    previous_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_opts = Application.get_env(:favn_orchestrator, :storage_adapter_opts)

    Application.put_env(:favn_orchestrator, :storage_adapter, Memory)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, [])

    Memory.reset()

    on_exit(fn ->
      restore_env(:favn_orchestrator, :storage_adapter, previous_adapter)
      restore_env(:favn_orchestrator, :storage_adapter_opts, previous_opts)
      Memory.reset()
    end)

    :ok
  end

  test "log writer broadcasts persisted entries" do
    assert {:ok, subscription} = FavnOrchestrator.subscribe_logs(%Filter{run_id: "run_log_live"})

    assert {:ok, [persisted]} =
             FavnOrchestrator.emit_log(%Entry{
               run_id: "run_log_live",
               source: :runner,
               level: :info,
               message: "asset execution started",
               producer_id: "runner:exec-log-live",
               producer_sequence: 1
             })

    assert persisted.global_sequence == 1
    assert {:ok, received} = receive_log_entry("run_log_live")
    assert received.global_sequence == persisted.global_sequence

    assert :ok = FavnOrchestrator.unsubscribe_logs(subscription)
  end

  test "list and replay APIs delegate to storage" do
    assert {:ok, [_first, second]} =
             FavnOrchestrator.emit_logs([
               %Entry{
                 run_id: "run_log_read",
                 message: "first",
                 producer_id: "p",
                 producer_sequence: 1
               },
               %Entry{
                 run_id: "run_log_read",
                 message: "second",
                 producer_id: "p",
                 producer_sequence: 2
               }
             ])

    assert {:ok, page} = FavnOrchestrator.list_logs(%Filter{run_id: "run_log_read"})
    assert Enum.map(page.items, & &1.message) == ["first", "second"]

    assert {:ok, replayed} =
             FavnOrchestrator.replay_logs(second.global_sequence - 1, %Filter{
               run_id: "run_log_read"
             })

    assert Enum.map(replayed, & &1.message) == ["second"]
  end

  test "list logs supports descending order for latest pages" do
    entries =
      for sequence <- 1..5 do
        %Entry{
          run_id: "run_log_latest",
          message: "log #{sequence}",
          producer_id: "latest",
          producer_sequence: sequence
        }
      end

    assert {:ok, _persisted} = FavnOrchestrator.emit_logs(entries)

    assert {:ok, page} =
             FavnOrchestrator.list_logs(%Filter{run_id: "run_log_latest"},
               limit: 2,
               order: :desc
             )

    assert Enum.map(page.items, & &1.message) == ["log 5", "log 4"]
    assert page.has_more?
  end

  test "asset step log context prefers exact asset step logs" do
    run_id = "run_asset_step_exact"
    asset_ref = {__MODULE__.ExactAsset, :asset}
    node_key = {asset_ref, nil}
    asset_step_id = persisted_step_id(node_key)

    persist_run_with_node_result(run_id, asset_ref, node_key, :running)

    assert {:ok, _} =
             FavnOrchestrator.emit_logs([
               %Entry{run_id: run_id, asset_ref: asset_ref, message: "fallback candidate"},
               %Entry{
                 run_id: run_id,
                 asset_step_id: asset_step_id,
                 asset_ref: asset_ref,
                 message: "exact step"
               }
             ])

    assert {:ok, context} = FavnOrchestrator.get_asset_step_log_context(run_id, asset_step_id)

    assert context.title == "#{inspect(__MODULE__.ExactAsset)}.asset"
    assert context.status == :running
    assert context.fallback? == false
    assert context.note == nil
    assert context.log_filter == %Filter{run_id: run_id, asset_step_id: asset_step_id}

    assert {:ok, page} = FavnOrchestrator.list_logs(context.log_filter)
    assert Enum.map(page.items, & &1.message) == ["exact step"]
  end

  test "asset step log context falls back to asset ref logs when exact step logs are absent" do
    run_id = "run_asset_step_fallback"
    asset_ref = {__MODULE__.FallbackAsset, :asset}
    node_key = {asset_ref, nil}
    asset_step_id = persisted_step_id(node_key)

    persist_run_with_node_result(run_id, asset_ref, node_key, :ok)

    assert {:ok, _} =
             FavnOrchestrator.emit_logs([
               %Entry{run_id: run_id, asset_ref: asset_ref, message: "legacy asset log"},
               %Entry{
                 run_id: run_id,
                 asset_ref: {__MODULE__.OtherAsset, :asset},
                 message: "other"
               }
             ])

    assert {:ok, context} = FavnOrchestrator.get_asset_step_log_context(run_id, asset_step_id)

    assert context.status == :ok
    assert context.fallback? == true
    assert context.note =~ "Exact asset-step logs were not found"
    assert context.log_filter == %Filter{run_id: run_id, asset_ref: asset_ref}

    assert {:ok, page} = FavnOrchestrator.list_logs(context.log_filter)
    assert Enum.map(page.items, & &1.message) == ["legacy asset log"]
  end

  test "runner bridge merges step context so runner logs filter by asset step" do
    runner_opts = [
      runner_log_entries: %{
        "exec_step_a" => [
          %{
            level: :info,
            source: :runner,
            message: "from runner step a",
            producer_id: "runner:exec_step_a",
            producer_sequence: 1,
            metadata: %{password: "secret", keep: "visible"}
          }
        ],
        "exec_step_b" => [
          %{
            level: :info,
            source: :runner,
            message: "from runner step b",
            producer_id: "runner:exec_step_b",
            producer_sequence: 1
          }
        ]
      }
    ]

    assert {:ok, bridge_a} =
             RunnerLogBridge.start(RunnerClientLogStub, "exec_step_a", runner_opts, %{
               run_id: "run_bridge_filter",
               asset_step_id: "step_a",
               node_key: {:node, :a},
               asset_ref: {__MODULE__, :asset_a},
               runner_execution_id: "exec_step_a",
               attempt: 2
             })

    assert {:ok, bridge_b} =
             RunnerLogBridge.start(RunnerClientLogStub, "exec_step_b", runner_opts, %{
               run_id: "run_bridge_filter",
               asset_step_id: "step_b",
               node_key: {:node, :b},
               asset_ref: {__MODULE__, :asset_b},
               runner_execution_id: "exec_step_b",
               attempt: 1
             })

    eventually(fn ->
      assert {:ok, page} =
               FavnOrchestrator.list_logs(%Filter{
                 run_id: "run_bridge_filter",
                 asset_step_id: "step_a"
               })

      assert [%Entry{} = entry] = page.items
      assert entry.message == "from runner step a"
      assert entry.run_id == "run_bridge_filter"
      assert entry.asset_step_id == "step_a"
      assert entry.node_key == {:node, :a}
      assert entry.asset_ref == {__MODULE__, :asset_a}
      assert entry.runner_execution_id == "exec_step_a"
      assert entry.attempt == 2
      assert entry.metadata["password"] == "[REDACTED]"
    end)

    assert {:ok, replayed} =
             FavnOrchestrator.replay_logs(0, %Filter{
               run_id: "run_bridge_filter",
               asset_step_id: "step_a"
             })

    assert Enum.map(replayed, & &1.message) == ["from runner step a"]

    RunnerLogBridge.stop(bridge_a, RunnerClientLogStub, "exec_step_a", runner_opts)
    RunnerLogBridge.stop(bridge_b, RunnerClientLogStub, "exec_step_b", runner_opts)
  end

  test "runner bridge ignores malformed runner log entries without crashing" do
    runner_opts = [
      runner_log_entries: %{
        "exec_bad_log" => [
          %{
            level: :fatal,
            source: :alien,
            message: "bad runner log",
            producer_id: "runner:exec_bad_log",
            producer_sequence: 1
          }
        ]
      }
    ]

    assert {:ok, bridge} =
             RunnerLogBridge.start(RunnerClientLogStub, "exec_bad_log", runner_opts, %{
               run_id: "run_bad_log",
               asset_step_id: "step_bad"
             })

    Process.sleep(50)
    assert Process.alive?(bridge)

    assert {:ok, page} = FavnOrchestrator.list_logs(%Filter{run_id: "run_bad_log"})
    assert page.items == []

    RunnerLogBridge.stop(bridge, RunnerClientLogStub, "exec_bad_log", runner_opts)
  end

  test "step transition logs are durable and idempotent" do
    run = transition_run("run_transition_log_once", :running)
    data = transition_data("step_once", :step_started)

    assert :ok = TransitionWriter.persist_transition(run, :step_started, data)
    assert :ok = TransitionWriter.persist_transition(run, :step_started, data)

    assert {:ok, page} = FavnOrchestrator.list_logs(%Filter{run_id: run.id})
    assert [%Entry{} = entry] = page.items
    assert entry.message == "asset execution started"
    assert entry.producer_id == "orchestrator:#{run.id}"
    assert entry.producer_sequence == run.event_seq
  end

  test "step transition logs use lifecycle levels and sanitized metadata" do
    failed = transition_run("run_transition_log_failed", :error)
    retrying = transition_run("run_transition_log_retrying", :running)

    assert :ok =
             TransitionWriter.persist_transition(
               failed,
               :step_failed,
               transition_data("step_failed", :step_failed,
                 error: %{kind: :error, reason: {:db_password, "secret"}},
                 reason: {:db_password, "secret"},
                 result_status: :error
               )
             )

    assert :ok =
             TransitionWriter.persist_transition(
               retrying,
               :step_retry_scheduled,
               transition_data("step_retrying", :step_retry_scheduled,
                 reason: :retryable,
                 attempt: 2,
                 max_attempts: 3
               )
             )

    assert {:ok, failed_page} = FavnOrchestrator.list_logs(%Filter{run_id: failed.id})
    assert [%Entry{} = failed_entry] = failed_page.items
    assert failed_entry.level == :error
    assert failed_entry.message == "asset execution failed"
    assert failed_entry.metadata["event_type"] == "step_failed"
    assert failed_entry.metadata["result_status"] == "error"
    assert failed_entry.metadata["error"]["redacted"] == true
    refute Map.has_key?(failed_entry.metadata, "data")

    assert {:ok, retry_page} = FavnOrchestrator.list_logs(%Filter{run_id: retrying.id})
    assert [%Entry{} = retry_entry] = retry_page.items
    assert retry_entry.level == :warning
    assert retry_entry.message == "asset execution retry scheduled"
    assert retry_entry.metadata["attempt"] == 2
    assert retry_entry.metadata["max_attempts"] == 3
  end

  test "live run subscriptions deliver only matching run entries" do
    assert {:ok, subscription} = FavnOrchestrator.subscribe_logs(%Filter{run_id: "run_live_a"})

    assert {:ok, _} =
             FavnOrchestrator.emit_log(%Entry{run_id: "run_live_b", message: "other run"})

    assert {:ok, [persisted]} =
             FavnOrchestrator.emit_log(%Entry{run_id: "run_live_a", message: "wanted run"})

    assert {:ok, received} = receive_log_entry("run_live_a")
    assert received.global_sequence == persisted.global_sequence
    refute_receive {:favn_log_entry, %Entry{run_id: "run_live_b"}}, 100

    assert :ok = FavnOrchestrator.unsubscribe_logs(subscription)
  end

  test "unsubscribe requires the returned subscription handle" do
    assert {:error, :invalid_log_subscription} =
             FavnOrchestrator.unsubscribe_logs(%Filter{run_id: "run_unsubscribe_filter"})
  end

  test "live asset-step subscriptions use asset topics and honor remaining filters" do
    assert {:ok, subscription} =
             FavnOrchestrator.subscribe_logs(%Filter{
               run_id: "run_live_asset",
               asset_step_id: "step_a",
               levels: [:error]
             })

    assert {:ok, _} =
             FavnOrchestrator.emit_log(%Entry{
               run_id: "run_live_asset",
               asset_step_id: "step_b",
               level: :error,
               message: "other step"
             })

    assert {:ok, _} =
             FavnOrchestrator.emit_log(%Entry{
               run_id: "run_live_asset",
               asset_step_id: "step_a",
               level: :info,
               message: "filtered level"
             })

    assert {:ok, [persisted]} =
             FavnOrchestrator.emit_log(%Entry{
               run_id: "run_live_asset",
               asset_step_id: "step_a",
               level: :error,
               message: "wanted step"
             })

    assert {:ok, received} = receive_log_entry("run_live_asset")
    assert received.global_sequence == persisted.global_sequence
    assert received.asset_step_id == "step_a"
    assert received.level == :error
    refute_receive {:favn_log_entry, %Entry{}}, 100

    assert :ok = FavnOrchestrator.unsubscribe_logs(subscription)
  end

  defp receive_log_entry(run_id, timeout \\ 1_000) do
    receive do
      {:favn_log_entry, %Entry{run_id: ^run_id} = entry} -> {:ok, entry}
      {:favn_log_entry, %Entry{}} -> receive_log_entry(run_id, timeout)
    after
      timeout -> {:error, :timeout}
    end
  end

  defp eventually(fun, attempts \\ 20)

  defp eventually(fun, attempts) when attempts > 0 do
    fun.()
  rescue
    ExUnit.AssertionError ->
      Process.sleep(25)
      eventually(fun, attempts - 1)
  end

  defp eventually(fun, 0), do: fun.()

  defp persist_run_with_node_result(run_id, asset_ref, node_key, status) do
    run =
      RunState.new(
        id: run_id,
        manifest_version_id: "manifest_logs_test",
        manifest_content_hash: "hash_logs_test",
        asset_ref: asset_ref,
        target_refs: [asset_ref]
      )
      |> RunState.transition(
        status: status,
        result: %{
          node_results: [
            %{
              node_key: node_key,
              ref: asset_ref,
              stage: 1,
              status: status,
              attempt_count: 1,
              duration_ms: 120
            }
          ]
        }
      )

    assert :ok = Storage.put_run(run)
  end

  defp persisted_step_id(key) do
    key
    |> :erlang.term_to_binary()
    |> Base.url_encode64(padding: false)
    |> String.replace(~r/[^a-zA-Z0-9_-]+/, "-")
  end

  defp transition_run(run_id, status) do
    RunState.new(
      id: run_id,
      manifest_version_id: "manifest_logs_test",
      manifest_content_hash: "hash_logs_test",
      asset_ref: {__MODULE__.TransitionAsset, :asset},
      target_refs: [{__MODULE__.TransitionAsset, :asset}]
    )
    |> RunState.transition(status: status)
  end

  defp transition_data(asset_step_id, event_type, opts \\ []) do
    asset_ref = {__MODULE__.TransitionAsset, :asset}

    %{
      asset_ref: asset_ref,
      asset_step_id: asset_step_id,
      node_key: {asset_ref, nil},
      stage: 0,
      attempt: Keyword.get(opts, :attempt, 1),
      max_attempts: Keyword.get(opts, :max_attempts, 1),
      event_type: event_type,
      freshness_key: Keyword.get(opts, :freshness_key, Favn.Freshness.Key.latest()),
      result_status: Keyword.get(opts, :result_status),
      error: Keyword.get(opts, :error),
      reason: Keyword.get(opts, :reason),
      unsafe_extra: %{credentials: "secret"}
    }
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
