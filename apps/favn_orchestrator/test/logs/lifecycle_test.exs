defmodule FavnOrchestrator.Logs.LifecycleTest do
  use ExUnit.Case, async: true

  alias Favn.Log.Identity
  alias FavnOrchestrator.Logs.Lifecycle
  alias FavnOrchestrator.Storage.RunEventCodec

  test "every lifecycle transition retains its message and severity after persistence" do
    for {type, level, message} <- [
          {:step_queued, :info, "step queued"},
          {:step_started, :info, "asset execution submitted"},
          {:step_retry_started, :info, "asset execution retry submitted"},
          {:step_running, :info, "asset execution started on a runner"},
          {:step_finished, :info, "asset execution finished"},
          {:step_skipped_fresh, :info, "asset skipped because it is fresh"},
          {:step_retry_scheduled, :warning, "asset execution retry scheduled"},
          {:step_failed, :error, "asset execution failed"},
          {:step_timed_out, :error, "asset execution timed out"},
          {:step_cancelled, :error, "asset execution cancelled"},
          {:step_blocked, :error, "asset execution blocked"},
          {:step_advisory, :info, "step advisory"}
        ] do
      payload = payload(type)
      assert {:ok, entry} = Lifecycle.render("workspace", 12, 8, payload)
      assert entry.level == level
      assert entry.message == message
      assert entry.source == :orchestrator
      assert entry.stream == :system
      assert entry.global_sequence == 7_001
      assert entry.id == "workspace:event:run:3"
      assert entry.attempt == 2
      assert entry.runner_task_id == "task"
      assert entry.asset_step_id == "step"
      assert {:ok, entry.node_key} == Identity.node_key({{MyApp.Asset, :asset}, %{window: 1}})
      assert {:ok, entry.asset_ref} == Identity.asset_ref({MyApp.Asset, :asset})
    end
  end

  test "generic persisted messages do not depend on the VM atom table" do
    type = "step_custom_#{System.unique_integer([:positive])}"
    persisted = Map.put(payload(:step_running), "event_type", type)
    assert_raise ArgumentError, fn -> String.to_existing_atom(type) end
    assert {:ok, before} = Lifecycle.render("workspace", 12, 8, persisted)
    _ = String.to_atom(type)
    assert {:ok, after_load} = Lifecycle.render("workspace", 12, 8, persisted)
    assert before.message == String.replace(type, "_", " ")
    assert after_load.message == before.message
  end

  test "canonical identities survive repeated codec normalization and bounded metadata" do
    event = event(:step_running)
    data = Map.merge(event.data, Map.new(1..100, &{"field_#{&1}", &1}))
    assert {:ok, first} = RunEventCodec.normalize("run", %{event | data: data})
    assert {:ok, second} = RunEventCodec.normalize("run", first)
    assert first.data["log_node_key"] == second.data["log_node_key"]
    assert {:ok, json} = RunEventCodec.encode(second)
    assert {:ok, decoded} = RunEventCodec.decode(json)
    assert decoded.data["log_node_key"] == first.data["log_node_key"]
    assert decoded.data["log_asset_ref"] == first.data["log_asset_ref"]
  end

  test "errors are bounded and do not expose malformed payloads" do
    assert {:error, {:invalid_lifecycle_event, "workspace", 12}} =
             Lifecycle.render("workspace", 12, 8, %{"secret" => "private"})

    bad = put_in(payload(:step_failed), ["data", "log_asset_ref"], %{"secret" => "private"})

    assert {:error, {:invalid_lifecycle_event, "workspace", 12}} =
             Lifecycle.render("workspace", 12, 8, bad)
  end

  test "redacts errors, ignores unrelated payload fields, and rejects oversized metadata" do
    event = event(:step_failed)

    event = %{
      event
      | data:
          Map.merge(event.data, %{
            error: %{message: "token=private-value"},
            unrelated: "not log metadata"
          })
    }

    {:ok, encoded} = RunEventCodec.encode(event)
    assert {:ok, entry} = Lifecycle.render("workspace", 12, 8, Jason.decode!(encoded))
    refute Jason.encode!(entry.metadata) =~ "private-value"
    refute Jason.encode!(entry.metadata) =~ "not log metadata"

    oversized =
      Enum.reduce(
        ~w(status attempt max_attempts freshness_key result_status),
        payload(:step_failed),
        fn field, data ->
          put_in(data, ["data", field], String.duplicate("x", 8_192))
        end
      )

    assert {:error, {:invalid_lifecycle_event, "workspace", 12}} =
             Lifecycle.render("workspace", 12, 8, oversized)

    missing_asset = update_in(payload(:step_failed), ["data"], &Map.delete(&1, "log_asset_ref"))

    assert {:error, {:invalid_lifecycle_event, "workspace", 12}} =
             Lifecycle.render("workspace", 12, 8, missing_asset)

    invalid_node =
      put_in(payload(:step_failed), ["data", "log_node_key"], %{"unexpected" => "value"})

    assert {:error, {:invalid_lifecycle_event, "workspace", 12}} =
             Lifecycle.render("workspace", 12, 8, invalid_node)
  end

  test "independent event sequences produce distinct identities" do
    first = payload(:step_running)
    assert {:ok, a} = Lifecycle.render("workspace", 12, 8, first)
    assert {:ok, b} = Lifecycle.render("workspace", 13, 9, %{first | "sequence" => 4})
    refute a.id == b.id
  end

  defp payload(type) do
    {:ok, json} = RunEventCodec.encode(event(type))
    Jason.decode!(json)
  end

  defp event(type) do
    %{
      run_id: "run",
      sequence: 3,
      event_type: type,
      entity: :step,
      occurred_at: ~U[2026-09-15 10:00:00Z],
      asset_ref: {MyApp.Asset, :asset},
      data: %{
        node_key: {{MyApp.Asset, :asset}, %{window: 1}},
        attempt: 2,
        asset_step_id: "step",
        runner_task_id: "task"
      }
    }
  end
end
