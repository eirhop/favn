defmodule CrmDemo.Landing.Crm.ExtractorTest do
  use ExUnit.Case, async: false

  alias CrmDemo.Landing.Crm.{Daily, Snapshots}
  alias CrmDemo.RunContext
  alias CrmDemo.Support.Landing.{Manifest, Storage}

  setup do
    File.rm_rf!(Storage.root())
    :ok
  end

  test "a full-refresh extraction writes every page as its own part" do
    ctx = RunContext.new({Snapshots, :accounts})

    assert {:ok, result} = Snapshots.asset(ctx)
    assert result.dataset == "accounts"
    assert result.mode == "full_refresh"
    assert result.rows_landed == 4
    assert result.part_count == 2

    dir = Storage.run_dir("accounts", result.landing_run_id)
    assert File.exists?(Path.join(dir, "part-0001.jsonl"))
    assert File.exists?(Path.join(dir, "part-0002.jsonl"))
  end

  test "the manifest lists exactly the parts written for the window" do
    window = RunContext.day(~D[2026-07-23])
    ctx = RunContext.new({Daily, :deals}, window: window)

    assert {:ok, result} = Daily.asset(ctx)
    assert result.rows_landed == 3

    manifest = Storage.latest_manifest!("deals", window)
    assert manifest.landing_run_id == result.landing_run_id
    assert manifest.window_start == window.start_at
    assert manifest.window_end == window.end_at
    assert manifest.row_count == 3
    assert Enum.all?(manifest.files, &File.exists?/1)
  end

  test "a window with no source activity lands an explicit empty part" do
    window = RunContext.day(~D[2026-07-25])

    assert {:ok, result} = Daily.asset(RunContext.new({Daily, :activities}, window: window))
    assert result.rows_landed == 0
    assert result.part_count == 1

    manifest = Storage.latest_manifest!("activities", window)
    assert manifest.row_count == 0
    assert [path] = manifest.files
    assert File.read!(path) == ""
  end

  test "two windows of one run land side by side without overwriting" do
    first_window = RunContext.day(~D[2026-07-22])
    second_window = RunContext.day(~D[2026-07-23])
    ctx = RunContext.new({Daily, :deals}, window: first_window)

    assert {:ok, first} = Daily.asset(ctx)
    assert {:ok, second} = Daily.asset(%{ctx | window: second_window})

    refute first.landing_run_id == second.landing_run_id

    assert Storage.latest_manifest!("deals", first_window).landing_run_id ==
             first.landing_run_id

    assert Storage.latest_manifest!("deals", second_window).landing_run_id ==
             second.landing_run_id
  end

  test "a retry lands a new run instead of modifying the failed one" do
    ctx = RunContext.new({Snapshots, :contacts})

    assert {:ok, first} = Snapshots.asset(ctx)
    assert {:ok, second} = Snapshots.asset(%{ctx | attempt: 2})

    refute first.landing_run_id == second.landing_run_id
    assert Storage.latest_manifest!("contacts", nil).landing_run_id == second.landing_run_id
  end

  test "manifests survive an encode and decode round trip" do
    assert {:ok, result} = Snapshots.asset(RunContext.new({Snapshots, :accounts}))
    manifest = Storage.latest_manifest!("accounts", nil)

    assert manifest |> Manifest.encode!() |> Manifest.decode!() == manifest
    assert manifest.landing_run_id == result.landing_run_id
  end
end
