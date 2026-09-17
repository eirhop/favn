defmodule Favn.RuntimeCatalog.PublicationTest do
  use ExUnit.Case, async: true
  alias Favn.RuntimeCatalog.Publication
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.{Asset, TargetDescriptor}
  alias Favn.Freshness.{Key, Policy}

  defp work do
    %RunnerWork{
      run_id: "run",
      asset_step_id: "step",
      attempt: 1,
      manifest_version_id: "mv_1",
      manifest_content_hash: String.duplicate("a", 64),
      required_runner_release_id: "release",
      logical_target_id: "target",
      target_generation_id: "generation"
    }
  end

  defp asset(policy) do
    %Asset{
      ref: {__MODULE__, :daily},
      type: :sql,
      freshness: policy,
      target_descriptor: struct(TargetDescriptor, adapter: "Elixir.Favn.SQL.Adapter.DuckDB.ADBC")
    }
  end

  test "every supported asset gets a bounded deterministic intent without selection configuration" do
    for n <- 1..300 do
      w = %{work() | asset_step_id: "step#{n}"}
      assert {:ok, p} = Publication.new(asset(nil), w, "workspace", "latest")
      assert {:ok, ^p} = Publication.new(asset(nil), w, "workspace", "latest")
      assert :ok = Publication.validate(p)
      assert {:ok, {"unknown", nil, false}} = Publication.expiry(p, ~U[2026-09-17 12:00:00Z])
    end

    assert {:ok, nil} = Publication.new(%Asset{}, work(), "workspace", "latest")
  end

  test "age uses publication time and inclusive deadline; calendar uses the pinned period across DST" do
    {:ok, age} = Policy.max_age(6, :hour)
    {:ok, p} = Publication.new(asset(age), work(), "workspace", "latest")

    assert {:ok, {"deadline", ~U[2026-09-17 08:00:00Z], true}} =
             Publication.expiry(p, ~U[2026-09-17 02:00:00Z])

    {:ok, daily} = Policy.calendar(:day, timezone: "Europe/Oslo")
    key = Key.calendar!(:day, "Europe/Oslo", ~D[2026-03-29])
    {:ok, p} = Publication.new(asset(daily), work(), "workspace", key)
    assert {:ok, {"deadline", deadline, false}} = Publication.expiry(p, ~U[2026-04-01 02:00:00Z])
    assert DateTime.compare(deadline, ~U[2026-03-29 22:00:00Z]) == :eq
  end

  test "window-success policy without a runtime window has no time deadline" do
    {:ok, policy} = Policy.from_value(window_success: true)
    {:ok, p} = Publication.new(asset(policy), work(), "workspace", "latest")
    assert {:ok, {"none", nil, false}} = Publication.expiry(p, ~U[2026-09-17 12:00:00Z])
  end

  test "coverage accepts exact logical windows and rejects partial or oversized ranges" do
    key = Favn.Window.Key.new!(:day, ~U[2026-01-01 00:00:00Z], "Etc/UTC")

    window =
      Favn.Window.Runtime.new!(:day, ~U[2026-01-01 00:00:00Z], ~U[2026-01-02 00:00:00Z], key)

    assert {:ok, p} =
             Publication.new(
               asset(nil),
               %{work() | metadata: %{window: window}},
               "workspace",
               "latest"
             )

    assert :ok = Publication.validate_window(p, window)

    for invalid <- [
          %{window | start_at: ~U[2026-01-01 01:00:00Z]},
          %{window | end_at: ~U[2026-01-01 23:00:00Z]},
          %{window | end_at: ~U[2036-01-01 00:00:00Z]},
          %{window | logical_window_count: 1001}
        ] do
      assert {:error, :invalid_runtime_coverage_scope} =
               Publication.new(
                 asset(nil),
                 %{work() | metadata: %{window: invalid}},
                 "workspace",
                 "latest"
               )
    end
  end

  test "missing identity and invalid key are rejected before dispatch" do
    assert {:error, :invalid_runtime_publication} =
             Publication.new(asset(nil), work(), nil, "latest")

    assert {:error, :invalid_runtime_publication} =
             Publication.new(asset(nil), work(), "workspace", "bad")
  end
end
