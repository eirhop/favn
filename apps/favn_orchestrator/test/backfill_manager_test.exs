defmodule FavnOrchestrator.BackfillManagerTest do
  use ExUnit.Case, async: false

  @moduletag capture_log: true

  alias Favn.Contracts.RunnerResult
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Version
  alias Favn.Window.Key, as: WindowKey
  alias Favn.Window.Policy
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  defmodule RunnerClientStub do
    @behaviour Favn.Contracts.RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(work, opts) do
      submit_log = Keyword.fetch!(opts, :submit_log)
      Agent.update(submit_log, fn submissions -> [work | submissions] end)
      {:ok, "exec_#{work.run_id}"}
    end

    @impl true
    def await_result(_execution_id, _timeout, _opts) do
      {:ok, %RunnerResult{status: :ok, asset_results: [], metadata: %{stub: true}}}
    end

    @impl true
    def cancel_work(_execution_id, _reason, _opts), do: :ok

    @impl true
    def inspect_relation(_request, _opts), do: {:error, :invalid_inspection_target}
  end

  setup do
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)
    {:ok, submit_log} = Agent.start_link(fn -> [] end)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, submit_log: submit_log)

    Memory.reset()

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)
      Memory.reset()

      if Process.alive?(submit_log) do
        Agent.stop(submit_log)
      end
    end)

    {:ok, submit_log: submit_log}
  end

  test "submits parent pipeline backfill, ledger rows, and child runs", %{submit_log: submit_log} do
    version = manifest_version("mv_backfill_submit")

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    assert {:ok, parent_run_id} =
             FavnOrchestrator.submit_pipeline_backfill(MyApp.Pipelines.Daily,
               run_id: "run_backfill_parent",
               range_request: %{
                 kind: :day,
                 from: "2026-04-26",
                 to: "2026-04-27",
                 timezone: "Etc/UTC"
               },
               lookback: %{days: 7},
               timeout_ms: 1_000,
               max_attempts: 2,
               retry_backoff_ms: 10,
               coverage_baseline_id: "baseline_1"
             )

    assert parent_run_id == "run_backfill_parent"
    assert {:ok, parent} = Storage.get_run(parent_run_id)
    assert parent.submit_kind == :backfill_pipeline
    assert parent.status in [:running, :ok]
    assert parent.runner_execution_id == nil
    assert parent.metadata.backfill.lookback == %{days: 7}
    assert parent.metadata.backfill.coverage_baseline_id == "baseline_1"
    assert parent.metadata.backfill.requested_count == 2

    assert {:ok, parent_events} = Storage.list_run_events(parent_run_id)

    assert parent_events |> Enum.map(& &1.event_type) |> Enum.take(2) == [
             :run_created,
             :backfill_started
           ]

    refute Enum.any?(parent_events, &(&1.event_type == :run_started))

    eventually(fn ->
      assert {:ok, windows} = Storage.list_backfill_windows(backfill_run_id: parent_run_id)
      assert length(windows) == 2
      assert Enum.all?(windows, &(&1.status == :ok))
      assert {:ok, parent} = Storage.get_run(parent_run_id)
      assert parent.status == :ok
    end)

    assert {:ok, windows} = Storage.list_backfill_windows(backfill_run_id: parent_run_id)
    assert Enum.all?(windows, &(&1.attempt_count == 1))
    assert Enum.all?(windows, &(&1.pipeline_module == MyApp.Pipelines.Daily))
    assert Enum.all?(windows, &(&1.manifest_version_id == version.manifest_version_id))
    assert Enum.all?(windows, &(&1.coverage_baseline_id == "baseline_1"))
    assert Enum.all?(windows, &is_binary(&1.window_key))
    assert Enum.all?(windows, &(&1.child_run_id == &1.latest_attempt_run_id))

    child_run_ids = windows |> Enum.map(& &1.child_run_id) |> Enum.sort()
    refute Enum.member?(child_run_ids, nil)

    assert {:ok, all_runs} = Storage.list_runs()

    children =
      all_runs
      |> Enum.filter(&(&1.parent_run_id == parent_run_id))
      |> Enum.sort_by(& &1.id)

    assert Enum.map(children, & &1.id) == child_run_ids
    assert length(children) == 2
    assert Enum.all?(children, &(&1.root_run_id == parent_run_id))
    assert Enum.all?(children, &(&1.lineage_depth == 1))
    assert Enum.all?(children, &(&1.trigger.kind == :backfill))
    assert Enum.all?(children, &(&1.trigger.backfill_run_id == parent_run_id))
    assert Enum.all?(children, &(&1.target_refs == [{MyApp.Assets.Gold, :asset}]))

    child_window_keys = Enum.map(children, & &1.trigger.window_key) |> Enum.sort()
    ledger_window_keys = Enum.map(windows, & &1.window_key) |> Enum.sort()
    assert child_window_keys == ledger_window_keys

    child_anchor_keys =
      children
      |> Enum.map(& &1.metadata.pipeline_context.anchor_window.key)
      |> Enum.map(&WindowKey.encode/1)
      |> Enum.sort()

    assert child_anchor_keys == ledger_window_keys

    eventually(fn ->
      submissions = Agent.get(submit_log, & &1)
      assert length(submissions) == 4
      refute Enum.any?(submissions, &(&1.run_id == parent_run_id))
      assert submissions |> Enum.map(& &1.run_id) |> Enum.uniq() |> Enum.sort() == child_run_ids
    end)
  end

  test "rejects oversized ranges before parent or windows are persisted" do
    version = manifest_version("mv_backfill_max_windows")

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    assert {:error, {:too_many_backfill_windows, 2, 1}} =
             FavnOrchestrator.submit_pipeline_backfill(MyApp.Pipelines.Daily,
               run_id: "run_backfill_too_large",
               range_request: %{kind: :day, from: "2026-04-26", to: "2026-04-27"},
               max_windows: 1
             )

    assert {:error, :not_found} = Storage.get_run("run_backfill_too_large")
    assert {:ok, []} = Storage.list_backfill_windows(backfill_run_id: "run_backfill_too_large")
  end

  test "generic cancel and rerun reject backfill parent runs" do
    version = manifest_version("mv_backfill_parent_safety")

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    assert {:ok, parent_run_id} =
             FavnOrchestrator.submit_pipeline_backfill(MyApp.Pipelines.Daily,
               run_id: "run_backfill_parent_safety",
               range_request: %{kind: :day, from: "2026-04-26", to: "2026-04-26"}
             )

    assert {:error, :backfill_parent_cancel_not_supported} =
             FavnOrchestrator.cancel_run(parent_run_id, %{reason: "operator"})

    assert {:error, :backfill_parent_rerun_not_supported} = FavnOrchestrator.rerun(parent_run_id)

    assert {:ok, all_runs} = Storage.list_runs()

    refute Enum.any?(all_runs, fn run ->
             run.rerun_of_run_id == parent_run_id and run.submit_kind == :rerun
           end)
  end

  test "relative range can resolve from coverage baseline id" do
    version = manifest_version("mv_backfill_baseline_relative")
    now = DateTime.utc_now()

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    assert {:ok, baseline} =
             CoverageBaseline.new(%{
               baseline_id: "baseline_relative",
               pipeline_module: MyApp.Pipelines.Daily,
               source_key: "source",
               segment_key_hash: "hash",
               window_kind: :day,
               timezone: "Etc/UTC",
               coverage_until: ~U[2026-04-28 00:00:00Z],
               created_by_run_id: "run_baseline",
               manifest_version_id: version.manifest_version_id,
               status: :ok,
               created_at: now,
               updated_at: now
             })

    assert :ok = Storage.put_coverage_baseline(baseline)

    assert {:ok, parent_run_id} =
             FavnOrchestrator.submit_pipeline_backfill(MyApp.Pipelines.Daily,
               run_id: "run_backfill_relative_baseline",
               range_request: %{"last" => [2, "day"], "timezone" => "Etc/UTC"},
               coverage_baseline_id: "baseline_relative"
             )

    assert {:ok, windows} = Storage.list_backfill_windows(backfill_run_id: parent_run_id)

    assert Enum.map(windows, & &1.window_start_at) == [
             ~U[2026-04-26 00:00:00Z],
             ~U[2026-04-27 00:00:00Z]
           ]
  end

  test "coverage baseline id validates pipeline status kind and timezone scope" do
    version = manifest_version("mv_backfill_baseline_scope")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    assert :ok = put_baseline("wrong_pipeline", pipeline_module: Other.Pipeline)

    assert {:error, {:coverage_baseline_pipeline_mismatch, Other.Pipeline, MyApp.Pipelines.Daily}} =
             submit_relative_with_baseline("wrong_pipeline")

    assert :ok = put_baseline("not_ok", status: :error)
    assert {:error, {:coverage_baseline_not_ok, :error}} = submit_relative_with_baseline("not_ok")

    assert :ok = put_baseline("wrong_kind", window_kind: :month)

    assert {:error, {:coverage_baseline_window_kind_mismatch, :month, :day}} =
             submit_relative_with_baseline("wrong_kind")

    assert :ok = put_baseline("wrong_timezone", timezone: "Europe/Oslo")

    assert {:error, {:coverage_baseline_timezone_mismatch, "Europe/Oslo", "Etc/UTC"}} =
             submit_relative_with_baseline("wrong_timezone",
               range_request: %{"last" => [1, "day"], "timezone" => "Etc/UTC"}
             )
  end

  test "coverage baseline timezone is used when relative request omits timezone" do
    version = manifest_version("mv_backfill_baseline_timezone")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)
    assert :ok = put_baseline("baseline_oslo", timezone: "Europe/Oslo")

    assert {:ok, parent_run_id} =
             submit_relative_with_baseline("baseline_oslo",
               run_id: "run_backfill_baseline_timezone",
               range_request: %{"last" => [1, "day"]}
             )

    assert {:ok, [window]} = Storage.list_backfill_windows(backfill_run_id: parent_run_id)
    assert window.timezone == "Europe/Oslo"
  end

  test "empty relative references still allow coverage baseline id resolution" do
    version = manifest_version("mv_backfill_empty_reference")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)
    assert :ok = put_baseline("baseline_empty_reference")

    assert {:ok, parent_run_id} =
             submit_relative_with_baseline("baseline_empty_reference",
               run_id: "run_backfill_empty_reference",
               range_request: %{"last" => [1, "day"], "relative_to" => "", "baseline" => nil}
             )

    assert {:ok, [_window]} = Storage.list_backfill_windows(backfill_run_id: parent_run_id)
  end

  test "partial child submission failure projects parent partial when a prior child succeeded" do
    version = manifest_version("mv_backfill_partial_compensation")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    counter = :counters.new(1, [])

    submitter = fn pipeline_module, child_opts ->
      :counters.add(counter, 1, 1)
      count = :counters.get(counter, 1)

      if count == 1 do
        result = RunManager.submit_pipeline_module_run(pipeline_module, child_opts)
        trigger = Keyword.fetch!(child_opts, :trigger)

        wait_for_window_status(
          trigger.backfill_run_id,
          pipeline_module,
          trigger.window_key,
          :ok
        )

        result
      else
        {:error, :synthetic_child_submit_failed}
      end
    end

    assert {:error, :synthetic_child_submit_failed} =
             FavnOrchestrator.submit_pipeline_backfill(MyApp.Pipelines.Daily,
               run_id: "run_backfill_partial_compensation",
               range_request: %{kind: :day, from: "2026-04-26", to: "2026-04-27"},
               _child_submitter: submitter
             )

    assert {:ok, parent} = Storage.get_run("run_backfill_partial_compensation")
    assert parent.status == :partial
    assert {:ok, windows} = Storage.list_backfill_windows(backfill_run_id: parent.id)
    assert Enum.map(windows, & &1.status) |> Enum.sort() == [:error, :ok]
  end

  test "child submission failure before any success projects parent error" do
    version = manifest_version("mv_backfill_error_compensation")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    submitter = fn _pipeline_module, _child_opts -> {:error, :synthetic_first_submit_failed} end

    assert {:error, :synthetic_first_submit_failed} =
             FavnOrchestrator.submit_pipeline_backfill(MyApp.Pipelines.Daily,
               run_id: "run_backfill_error_compensation",
               range_request: %{kind: :day, from: "2026-04-26", to: "2026-04-27"},
               _child_submitter: submitter
             )

    assert {:ok, parent} = Storage.get_run("run_backfill_error_compensation")
    assert parent.status == :error
    assert {:ok, windows} = Storage.list_backfill_windows(backfill_run_id: parent.id)
    assert Enum.all?(windows, &(&1.status == :error))
  end

  test "coverage baseline rejects nested raw source identity" do
    now = DateTime.utc_now()

    assert {:error, {:raw_source_identity_not_allowed, :token}} =
             CoverageBaseline.new(%{
               baseline_id: "baseline_secret",
               pipeline_module: MyApp.Pipelines.Daily,
               source_key: "source",
               segment_key_hash: "hash",
               window_kind: :day,
               timezone: "Etc/UTC",
               coverage_until: ~U[2026-04-28 00:00:00Z],
               created_by_run_id: "run_baseline",
               manifest_version_id: "mv",
               status: :ok,
               metadata: %{nested: %{"token" => "raw"}},
               created_at: now,
               updated_at: now
             })
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

  defp submit_relative_with_baseline(baseline_id, opts \\ []) do
    FavnOrchestrator.submit_pipeline_backfill(MyApp.Pipelines.Daily,
      run_id: Keyword.get(opts, :run_id, "run_#{baseline_id}"),
      range_request: Keyword.get(opts, :range_request, %{"last" => [1, "day"]}),
      coverage_baseline_id: baseline_id
    )
  end

  defp put_baseline(baseline_id, overrides \\ []) do
    now = DateTime.utc_now()

    attrs =
      Keyword.merge(
        [
          baseline_id: baseline_id,
          pipeline_module: MyApp.Pipelines.Daily,
          source_key: "source",
          segment_key_hash: "hash",
          window_kind: :day,
          timezone: "Etc/UTC",
          coverage_until: ~U[2026-04-28 00:00:00Z],
          created_by_run_id: "run_baseline",
          manifest_version_id: "mv_baseline",
          status: :ok,
          created_at: now,
          updated_at: now
        ],
        overrides
      )

    with {:ok, baseline} <- CoverageBaseline.new(attrs),
         do: Storage.put_coverage_baseline(baseline)
  end

  defp wait_for_window_status(backfill_run_id, pipeline_module, window_key, status) do
    eventually(fn ->
      assert {:ok, window} =
               Storage.get_backfill_window(backfill_run_id, pipeline_module, window_key)

      assert window.status == status
    end)
  end

  defp manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Asset{
          ref: {MyApp.Assets.Raw, :asset},
          module: MyApp.Assets.Raw,
          name: :asset
        },
        %Asset{
          ref: {MyApp.Assets.Gold, :asset},
          module: MyApp.Assets.Gold,
          name: :asset,
          depends_on: [{MyApp.Assets.Raw, :asset}]
        }
      ],
      pipelines: [
        %Pipeline{
          module: MyApp.Pipelines.Daily,
          name: :daily,
          selectors: [{:asset, {MyApp.Assets.Gold, :asset}}],
          deps: :all,
          window: Policy.new!(:day),
          metadata: %{}
        }
      ]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end
end
