defmodule FavnView.PresentersTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunState
  alias FavnView.Presenters.ManifestPresenter
  alias FavnView.Presenters.RunPresenter
  alias FavnView.Presenters.SchedulerPresenter
  alias FavnView.TestFixtures

  test "run presenter returns stable run summary keys" do
    run =
      RunState.new(
        id: "run_presenter",
        manifest_version_id: "mv_presenter",
        manifest_content_hash: "hash_presenter",
        asset_ref: {MyApp.Assets.Gold, :asset}
      )

    summary = RunPresenter.summary(run)

    assert summary.id == "run_presenter"
    assert summary.status == :pending
    assert summary.cancel_enabled == false
    assert summary.rerun_enabled == false
  end

  test "run presenter timeline event has stable keys" do
    event =
      %RunEvent{
        run_id: "run_presenter",
        sequence: 2,
        event_type: :run_started,
        occurred_at: DateTime.utc_now(),
        status: :running,
        entity: :run
      }

    timeline = RunPresenter.timeline_event(event)

    assert timeline.sequence == 2
    assert timeline.event_type == :run_started
    assert timeline.entity == :run
    assert timeline.stage == nil
    assert timeline.status == :running
    assert timeline.label == "Run started"
  end

  test "manifest presenter summary and detail are stable" do
    version = TestFixtures.manifest_version("mv_presenter")

    summary = ManifestPresenter.summary(version, "mv_presenter")
    detail = ManifestPresenter.detail(version)

    assert summary.manifest_version_id == "mv_presenter"
    assert summary.active == true
    assert detail.asset_count >= 1
    assert detail.pipeline_count >= 1
    assert detail.schedule_count == 0
  end

  test "manifest presenter term encoding roundtrip for form options" do
    version = TestFixtures.manifest_version("mv_presenter")
    [{_label, encoded} | _rest] = ManifestPresenter.asset_options([version])

    assert {:ok, decoded} = ManifestPresenter.decode_term(encoded)
    assert decoded == {MyApp.Assets.Gold, :asset} or decoded == {MyApp.Assets.Raw, :asset}
  end

  test "manifest presenter returns stable error tuple for invalid term payload" do
    assert {:error, :invalid_term_payload} = ManifestPresenter.decode_term("not-base64")
  end

  test "scheduler presenter returns stable entry keys" do
    entry =
      SchedulerPresenter.entry(%{
        pipeline_module: MyApp.Pipelines.Daily,
        schedule_id: :daily,
        cron: "0 * * * *",
        timezone: "UTC"
      })

    assert entry.pipeline_module == "MyApp.Pipelines.Daily"
    assert entry.schedule_id == ":daily"
    assert entry.cron == "0 * * * *"
    assert entry.timezone == "UTC"
  end
end
