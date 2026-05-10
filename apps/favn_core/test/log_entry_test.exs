defmodule Favn.Log.EntryTest do
  use ExUnit.Case, async: true

  alias Favn.Log
  alias Favn.Log.Entry
  alias Favn.Run.Context

  test "normalizes entry attrs" do
    now = ~U[2026-05-01 12:00:00Z]

    entry =
      Entry.normalize(%{
        "run_id" => "run_1",
        "global_sequence" => 10,
        "level" => "warning",
        "source" => "runner",
        "stream" => "stderr",
        "message" => "hello",
        "metadata" => %{rows: 3},
        "occurred_at" => now
      })

    assert entry.schema_version == 1
    assert entry.global_sequence == 10
    assert entry.run_id == "run_1"
    assert entry.level == :warning
    assert entry.source == :runner
    assert entry.stream == :stderr
    assert entry.message == "hello"
    assert entry.metadata == %{rows: 3}
    assert entry.occurred_at == now
    refute entry.truncated
  end

  test "user-code helpers infer run context fields" do
    context = %Context{run_id: "run_1", current_ref: {MyApp.Asset, :daily}, attempt: 2}

    entry = Log.info(context, "asset started", %{batch: 1})

    assert %Entry{} = entry
    assert entry.run_id == "run_1"
    assert entry.asset_ref == {MyApp.Asset, :daily}
    assert entry.attempt == 2
    assert entry.level == :info
    assert entry.source == :user_code
    assert entry.stream == :system
    assert entry.message == "asset started"
    assert entry.metadata == %{batch: 1}
    assert %DateTime{} = entry.occurred_at
  end

  test "user-code helpers accept attrs without runner or orchestrator dependencies" do
    entry = Log.error([run_id: "run_2", producer_id: "runner-a"], "failed", %{reason: :boom})

    assert entry.run_id == "run_2"
    assert entry.producer_id == "runner-a"
    assert entry.level == :error
    assert entry.message == "failed"
    assert entry.metadata == %{reason: :boom}
  end
end
