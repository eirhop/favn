defmodule FavnOrchestrator.HTTPContract.SchemaTest do
  use ExUnit.Case, async: true

  @contract_dir Path.expand("../../priv/http_contract/v1", __DIR__)

  test "v1 contract schemas exist and are valid json schema documents" do
    schema_paths =
      @contract_dir
      |> Path.join("*.schema.json")
      |> Path.wildcard()

    assert schema_paths != []

    Enum.each(schema_paths, fn schema_path ->
      assert {:ok, body} = File.read(schema_path)
      assert {:ok, schema} = Jason.decode(body)

      assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
      assert is_binary(schema["$id"]) and schema["$id"] != ""
      assert is_binary(schema["title"]) and schema["title"] != ""
      assert schema["type"] == "object"
      assert is_list(schema["required"])
      assert is_map(schema["properties"])
    end)
  end

  test "run summary schema locks expected keys" do
    schema = load_schema!("run-summary.schema.json")

    assert_required_keys(schema, %{
      "id" => "run_123",
      "status" => "running",
      "submit_kind" => "asset",
      "manifest_version_id" => "mv_123",
      "event_seq" => 1,
      "started_at" => "2026-01-01T00:00:00Z",
      "finished_at" => nil,
      "target_refs" => [],
      "asset_results" => [],
      "error" => nil
    })
  end

  test "sse run event envelope schema locks expected keys" do
    schema = load_schema!("sse-run-event-envelope.schema.json")

    assert_required_keys(schema, %{
      "schema_version" => 1,
      "event_id" => "evt:run_1:1",
      "stream" => "runs",
      "topic" => %{"type" => "run", "id" => "run_1"},
      "event_type" => "run_updated",
      "occurred_at" => "2026-01-01T00:00:00Z",
      "actor" => %{"type" => "system", "id" => "orchestrator"},
      "resource" => %{"type" => "run", "id" => "run_1"},
      "sequence" => 1,
      "cursor" => "runs:run_1:1",
      "data" => %{}
    })
  end

  test "backfill window schema locks expected keys" do
    schema = load_schema!("backfill-window.schema.json")

    assert_required_keys(schema, %{
      "backfill_run_id" => "run_backfill",
      "pipeline_module" => "Elixir.MyApp.Pipelines.Daily",
      "manifest_version_id" => "mv_123",
      "window_kind" => "day",
      "window_start_at" => "2026-01-01T00:00:00Z",
      "window_end_at" => "2026-01-02T00:00:00Z",
      "timezone" => "Etc/UTC",
      "window_key" => "day:Etc/UTC:2026-01-01T00:00:00Z",
      "status" => "error",
      "attempt_count" => 1,
      "latest_attempt_run_id" => "run_child",
      "last_success_run_id" => nil,
      "updated_at" => "2026-01-01T00:00:00Z"
    })
  end

  test "coverage baseline schema locks expected keys" do
    schema = load_schema!("coverage-baseline.schema.json")

    assert_required_keys(schema, %{
      "baseline_id" => "baseline_123",
      "pipeline_module" => "Elixir.MyApp.Pipelines.Daily",
      "source_key" => "orders",
      "segment_key_hash" => "hash",
      "window_kind" => "day",
      "timezone" => "Etc/UTC",
      "coverage_until" => "2026-01-01T00:00:00Z",
      "created_by_run_id" => "run_baseline",
      "manifest_version_id" => "mv_123",
      "status" => "ok",
      "created_at" => "2026-01-01T00:00:00Z",
      "updated_at" => "2026-01-01T00:00:00Z"
    })
  end

  test "asset window state schema locks expected keys" do
    schema = load_schema!("asset-window-state.schema.json")

    assert_required_keys(schema, %{
      "asset_ref_module" => "Elixir.MyApp.Assets.Orders",
      "asset_ref_name" => "asset",
      "pipeline_module" => "Elixir.MyApp.Pipelines.Daily",
      "manifest_version_id" => "mv_123",
      "window_kind" => "day",
      "window_start_at" => "2026-01-01T00:00:00Z",
      "window_end_at" => "2026-01-02T00:00:00Z",
      "timezone" => "Etc/UTC",
      "window_key" => "day:Etc/UTC:2026-01-01T00:00:00Z",
      "status" => "ok",
      "latest_run_id" => "run_child",
      "updated_at" => "2026-01-01T00:00:00Z"
    })
  end

  test "pagination schema locks expected keys" do
    schema = load_schema!("pagination.schema.json")

    assert_required_keys(schema, %{
      "limit" => 100,
      "offset" => 0,
      "has_more" => false,
      "next_offset" => nil
    })
  end

  defp load_schema!(file_name) do
    path = Path.join(@contract_dir, file_name)
    {:ok, body} = File.read(path)
    {:ok, schema} = Jason.decode(body)
    schema
  end

  defp assert_required_keys(schema, sample_payload)
       when is_map(schema) and is_map(sample_payload) do
    Enum.each(schema["required"], fn key ->
      assert Map.has_key?(sample_payload, key), "missing required key #{inspect(key)}"
    end)
  end
end
