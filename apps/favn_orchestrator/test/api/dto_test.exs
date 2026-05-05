defmodule FavnOrchestrator.API.DTOTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Run.AssetResult
  alias Favn.Window.Policy
  alias FavnOrchestrator.API.DTO
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.SchedulerEntry

  @now ~U[2026-01-02 03:04:05Z]
  @later ~U[2026-01-02 04:04:05Z]

  test "actor and session DTOs expose only schema-owned fields" do
    actor = %{
      id: "act_1",
      username: "ada",
      display_name: "Ada",
      roles: [:admin, :viewer],
      status: :active,
      inserted_at: @now,
      updated_at: @later,
      password_hash: "secret"
    }

    session = %{
      id: "ses_1",
      actor_id: "act_1",
      provider: "password_local",
      issued_at: @now,
      expires_at: @later,
      revoked_at: nil,
      token: "raw-token"
    }

    assert DTO.actor(actor) == %{
             id: "act_1",
             username: "ada",
             display_name: "Ada",
             roles: ["admin", "viewer"],
             status: "active",
             inserted_at: "2026-01-02T03:04:05Z",
             updated_at: "2026-01-02T04:04:05Z"
           }

    assert DTO.session(session) == %{
             id: "ses_1",
             actor_id: "act_1",
             provider: "password_local",
             issued_at: "2026-01-02T03:04:05Z",
             expires_at: "2026-01-02T04:04:05Z",
             revoked_at: nil
           }
  end

  test "schedule and manifest target DTOs keep stable fields explicit" do
    schedule = %SchedulerEntry{
      pipeline_module: SamplePipeline,
      schedule_id: :daily,
      cron: "0 0 * * *",
      timezone: "UTC",
      overlap: :skip,
      missed: :catch_up,
      active: true,
      window: %Policy{kind: :calendar, anchor: :scheduled_at, timezone: "UTC"},
      schedule_fingerprint: "sch_fp",
      manifest_version_id: "manifest_1",
      manifest_content_hash: "hash_1",
      last_evaluated_at: @now,
      last_due_at: @now,
      last_submitted_due_at: @later,
      in_flight_run_id: "run_1",
      queued_due_at: nil,
      updated_at: @later
    }

    assert DTO.schedule(schedule) == %{
             id: "schedule:Elixir.SamplePipeline:daily",
             pipeline_module: "Elixir.SamplePipeline",
             schedule_id: "daily",
             cron: "0 0 * * *",
             timezone: "UTC",
             overlap: "skip",
             missed: "catch_up",
             active: true,
             window: %{
               kind: "calendar",
               anchor: "scheduled_at",
               timezone: "UTC",
               allow_full_load: false
             },
             schedule_fingerprint: "sch_fp",
             manifest_version_id: "manifest_1",
             manifest_content_hash: "hash_1",
             last_evaluated_at: "2026-01-02T03:04:05Z",
             last_due_at: "2026-01-02T03:04:05Z",
             last_submitted_due_at: "2026-01-02T04:04:05Z",
             in_flight_run_id: "run_1",
             queued_due_at: nil,
             updated_at: "2026-01-02T04:04:05Z"
           }

    assert DTO.manifest_targets(%{
             manifest_version_id: "manifest_1",
             assets: [
               %{
                 target_id: "asset_1",
                 label: "SampleAsset",
                 asset_ref: "Elixir.SampleAsset:orders",
                 type: "table",
                 relation: %{connection: :warehouse, name: "orders"},
                 metadata: %{owner: :finance, api_token: "hidden"},
                 runtime_config: %{secret_key: "hidden"},
                 depends_on: ["Elixir.Upstream:orders"],
                 materialization: %{mode: :replace},
                 window: %{kind: :calendar}
               }
             ],
             pipelines: [%{target_id: "pipe_1", label: "Pipe", window: %{kind: :calendar}}]
           }) == %{
             manifest_version_id: "manifest_1",
             assets: [
               %{
                 target_id: "asset_1",
                 label: "Elixir.SampleAsset:orders",
                 asset_ref: "Elixir.SampleAsset:orders",
                 type: "table",
                 relation: %{"connection" => "warehouse", "name" => "orders"},
                 metadata: %{"api_token" => "[REDACTED]", "owner" => "finance"},
                 runtime_config: %{"secret_key" => "[REDACTED]"},
                 depends_on: ["Elixir.Upstream:orders"],
                 materialization: %{"mode" => "replace"},
                 window: %{"kind" => "calendar"}
               }
             ],
             pipelines: [%{target_id: "pipe_1", label: "Pipe", window: %{"kind" => "calendar"}}]
           }
  end

  test "run DTOs normalize runtime payloads and errors through JsonSafe" do
    asset_result = %AssetResult{
      ref: {SampleAsset, :orders},
      stage: 1,
      status: :error,
      started_at: @now,
      finished_at: @later,
      duration_ms: 100,
      meta: %{rows: 10, api_token: "hidden"},
      error: %{kind: :error, reason: :boom},
      attempt_count: 1,
      max_attempts: 3,
      attempts: [%{attempt: 1, error: %{kind: :error, reason: :boom}}],
      next_retry_at: @later
    }

    run = %{
      id: "run_1",
      status: :error,
      submit_kind: :manual,
      manifest_version_id: "manifest_1",
      manifest_content_hash: "hash_1",
      event_seq: 3,
      started_at: @now,
      finished_at: @later,
      timeout_ms: 5_000,
      retry_backoff_ms: 100,
      rerun_of_run_id: nil,
      parent_run_id: nil,
      root_run_id: "run_1",
      target_refs: [{SampleAsset, :orders}],
      params: %{api_token: "hidden", limit: 5},
      trigger: %{kind: :manual},
      metadata: %{source: :test},
      result: %{rows: 10},
      pipeline: %{module: SamplePipeline},
      pipeline_context: %{attempt: 1},
      asset_results: %{{SampleAsset, :orders} => asset_result},
      node_results: %{{:asset, {SampleAsset, :orders}} => asset_result},
      error: %{kind: :error, reason: :boom}
    }

    summary = DTO.run_summary(run)
    detail = DTO.run_detail(run)

    assert summary.asset_results == [DTO.asset_result(asset_result)]
    assert summary.error["kind"] == "error"
    assert detail.params == %{"api_token" => "[REDACTED]", "limit" => 5}
    assert detail.asset_results == [DTO.asset_result(asset_result)]

    assert [%{node_key: ["asset", %{"module" => "Elixir.SampleAsset", "name" => "orders"}]}] =
             detail.node_results

    assert hd(detail.asset_results).meta == %{"api_token" => "[REDACTED]", "rows" => 10}
    assert hd(detail.asset_results).error["type"] == "boom"
  end

  test "asset results sort plain normalized maps with string keys" do
    results = %{
      second: %{
        "asset_ref" => "Elixir.SampleAsset:second",
        "stage" => 2,
        "meta" => %{token: "hidden"}
      },
      first: %{asset_ref: "Elixir.SampleAsset:first", stage: 1, meta: %{rows: 1}}
    }

    assert [first, second] = DTO.asset_results(results)
    assert first["asset_ref"] == "Elixir.SampleAsset:first"
    assert first["stage"] == 1
    assert second["asset_ref"] == "Elixir.SampleAsset:second"
    assert second["meta"] == %{"token" => "[REDACTED]"}
  end

  test "run event and inspection result DTOs normalize event data and inspection runtime fields" do
    event = %RunEvent{
      run_id: "run_1",
      sequence: 2,
      event_type: :asset_failed,
      entity: :step,
      occurred_at: @now,
      status: :error,
      manifest_version_id: "manifest_1",
      manifest_content_hash: "hash_1",
      asset_ref: {SampleAsset, :orders},
      stage: 1,
      data: %{api_token: "hidden", rows: 10}
    }

    assert DTO.run_event(event) == %{
             schema_version: 1,
             run_id: "run_1",
             sequence: 2,
             event_type: "asset_failed",
             entity: "step",
             occurred_at: "2026-01-02T03:04:05Z",
             status: "error",
             manifest_version_id: "manifest_1",
             manifest_content_hash: "hash_1",
             asset_ref: "Elixir.SampleAsset:orders",
             stage: 1,
             data: %{"api_token" => "[REDACTED]", "rows" => 10}
           }

    result = %RelationInspectionResult{
      asset_ref: {SampleAsset, :orders},
      relation_ref:
        Favn.RelationRef.new!(%{connection: :warehouse, schema: "public", name: "orders"}),
      row_count: 10,
      sample: [%{id: 1, customer_token: "hidden"}],
      table_metadata: %{password: "hidden"},
      adapter: SampleAdapter,
      inspected_at: @now,
      warnings: [%{code: :sample_limited}],
      error: %{kind: :error, reason: :boom}
    }

    dto = DTO.inspection_result(result)

    assert dto.asset_ref == "Elixir.SampleAsset:orders"

    assert dto.relation_ref == %{
             connection: "warehouse",
             catalog: nil,
             schema: "public",
             name: "orders"
           }

    assert dto.sample == [%{"customer_token" => "[REDACTED]", "id" => 1}]
    assert dto.table_metadata == %{"password" => "[REDACTED]"}
    assert dto.error["kind"] == "error"
  end

  test "backfill and audit DTOs normalize runtime metadata and errors" do
    window = %BackfillWindow{
      backfill_run_id: "backfill_1",
      child_run_id: "run_2",
      pipeline_module: SamplePipeline,
      manifest_version_id: "manifest_1",
      coverage_baseline_id: "baseline_1",
      window_kind: :calendar,
      window_start_at: @now,
      window_end_at: @later,
      timezone: "UTC",
      window_key: "2026-01-02",
      status: :error,
      attempt_count: 1,
      latest_attempt_run_id: "run_2",
      last_success_run_id: nil,
      last_error: %{kind: :error, reason: :boom},
      errors: [%{kind: :error, reason: :boom}],
      metadata: %{api_token: "hidden"},
      started_at: @now,
      finished_at: @later,
      created_at: @now,
      updated_at: @later
    }

    baseline = %CoverageBaseline{
      baseline_id: "baseline_1",
      pipeline_module: SamplePipeline,
      source_key: "source",
      segment_key_hash: "hash",
      segment_key_redacted: "seg***",
      window_kind: :calendar,
      timezone: "UTC",
      coverage_start_at: @now,
      coverage_until: @later,
      created_by_run_id: "run_1",
      manifest_version_id: "manifest_1",
      status: :ok,
      errors: [],
      metadata: %{password: "hidden"},
      created_at: @now,
      updated_at: @later
    }

    state = %AssetWindowState{
      asset_ref_module: SampleAsset,
      asset_ref_name: :orders,
      pipeline_module: SamplePipeline,
      manifest_version_id: "manifest_1",
      window_kind: :calendar,
      window_start_at: @now,
      window_end_at: @later,
      timezone: "UTC",
      window_key: "2026-01-02",
      status: :error,
      latest_run_id: "run_2",
      latest_error: %{kind: :error, reason: :boom},
      errors: [%{kind: :error, reason: :boom}],
      rows_written: 10,
      metadata: %{secret: "hidden"},
      updated_at: @later
    }

    assert DTO.backfill_window(window).last_error["kind"] == "error"
    assert DTO.backfill_window(window).metadata == %{"api_token" => "[REDACTED]"}
    assert DTO.coverage_baseline(baseline).metadata == %{"password" => "[REDACTED]"}
    assert DTO.asset_window_state(state).latest_error["kind"] == "error"
    assert DTO.asset_window_state(state).metadata == %{"secret" => "[REDACTED]"}

    assert DTO.audit_entry(%{
             id: "aud_1",
             action: "run.submit",
             occurred_at: @now,
             api_token: "hidden"
           }) == %{
             "id" => "aud_1",
             "action" => "run.submit",
             "occurred_at" => "2026-01-02T03:04:05Z",
             "api_token" => "[REDACTED]"
           }
  end

  test "page helper maps items and pagination shape" do
    page = %Page{items: [1, 2], limit: 2, offset: 4, has_more?: true, next_offset: 6}

    assert DTO.page(page, &%{value: &1}) == %{
             items: [%{value: 1}, %{value: 2}],
             pagination: %{limit: 2, offset: 4, has_more: true, next_offset: 6}
           }
  end
end
