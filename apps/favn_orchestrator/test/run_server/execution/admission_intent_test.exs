Code.require_file("../../../../favn_test_support/fixtures/runner_task_persistence.exs", __DIR__)

defmodule FavnOrchestrator.RunServer.Execution.AdmissionIntentTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerWork
  alias Favn.Plan.NodeIdentity
  alias FavnOrchestrator.RunServer.Execution.AdmissionIntent
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.RunSnapshotCodec
  alias FavnOrchestrator.TestSupport.ManifestRecord
  alias FavnTestSupport.RunnerTaskPersistence, as: Fixture

  setup do
    version = Fixture.version()
    ref = hd(version.manifest.assets).ref

    run =
      RunState.new(
        submit_kind: :pipeline,
        workspace_id: "workspace",
        id: "intent-run",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        runner_releases: version.runner_releases,
        asset_ref: ref,
        target_refs: [ref]
      )

    work = %RunnerWork{
      run_id: run.id,
      manifest_version_id: run.manifest_version_id,
      manifest_content_hash: run.manifest_content_hash,
      node_identity: %NodeIdentity{node_key: {ref, nil}},
      asset_step_id: "step",
      stage: 0,
      attempt: 1,
      deadline_at: ~U[2026-09-17 10:05:00Z]
    }

    context = %{
      kind: :pipeline,
      materialization_claim: nil,
      resource_circuit_permits: [],
      freshness_key: "latest",
      decision: %{decision: :run, reason: :forced, node_key: {ref, nil}, freshness_key: "latest"},
      freshness_checkpoint: %{
        version: 1,
        revision: 1,
        sequence: 2,
        stage: 0,
        attempt: 1,
        payload_hash: :crypto.hash(:sha256, "checkpoint")
      }
    }

    %{run: run, work: work, version: version, context: context}
  end

  test "JSON persistence preserves the original deadline and frozen decision on refill", f do
    for context <- [f.context, %{kind: :sequential, materialization_claim: nil}] do
      run = %{f.run | submit_kind: if(context.kind == :pipeline, do: :pipeline, else: :manual)}

      assert {:ok, intent} =
               AdmissionIntent.new(run, f.work, context, ~U[2026-09-17 10:00:00Z])

      assert {:ok, metadata} = AdmissionIntent.put(f.run.metadata, intent)
      persisted = %{run | metadata: metadata |> Jason.encode!() |> Jason.decode!()}
      later_work = %{f.work | deadline_at: ~U[2026-09-17 12:05:00Z]}

      assert {:ok, ^intent} = AdmissionIntent.load(persisted, later_work, f.version)
      assert intent.deadline_at == f.work.deadline_at
      assert {:ok, ^metadata} = AdmissionIntent.put(metadata, intent)
      assert {:ok, %{}} = AdmissionIntent.clear(metadata, intent)
    end
  end

  test "an unresolved intent cannot be overwritten or cleared by another attempt", f do
    {:ok, intent} = AdmissionIntent.new(f.run, f.work, f.context, ~U[2026-09-17 10:00:00Z])
    {:ok, metadata} = AdmissionIntent.put(%{}, intent)
    next = %{intent | attempt: 2}

    assert {:error, :admission_intent_already_pending} = AdmissionIntent.put(metadata, next)
    assert {:error, :admission_intent_mismatch} = AdmissionIntent.clear(metadata, next)

    assert {:error, :admission_intent_already_pending} =
             AdmissionIntent.put(%{"execution_admission_intent" => nil}, intent)

    assert {:error, :invalid_admission_intent} =
             AdmissionIntent.load(
               %{f.run | metadata: metadata},
               %{f.work | attempt: 2},
               f.version
             )
  end

  test "the real snapshot codec preserves intent outside lossy display metadata", f do
    key = String.duplicate("long-window-key", 700)

    context = %{
      f.context
      | freshness_key: key,
        decision: %{f.context.decision | freshness_key: key}
    }

    {:ok, intent} = AdmissionIntent.new(f.run, f.work, context, ~U[2026-09-17 10:00:00Z])
    {:ok, metadata} = AdmissionIntent.put(f.run.metadata, intent)
    run = %{f.run | metadata: metadata} |> RunState.with_snapshot_hash()
    {:ok, snapshot} = RunSnapshotCodec.encode_run(run)
    {:ok, manifest} = ManifestRecord.to_record(f.version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: snapshot, manifest_version_id: f.version.manifest_version_id},
               manifest
             )

    assert {:ok, ^intent} = AdmissionIntent.load(restored, f.work, f.version)
  end

  test "missing deadline, acquired handles and different pinned identities are rejected", f do
    now = ~U[2026-09-17 10:00:00Z]

    for {work, context} <- [
          {%{f.work | deadline_at: nil}, f.context},
          {%{f.work | run_id: "foreign"}, f.context},
          {%{f.work | manifest_content_hash: String.duplicate("f", 64)}, f.context},
          {f.work, %{f.context | resource_circuit_permits: [%{}]}},
          {f.work, %{f.context | materialization_claim: %{}}},
          {f.work, %{kind: :sequential, materialization_claim: nil}},
          {f.work, %{f.context | freshness_key: "another-window"}},
          {f.work, put_in(f.context, [:freshness_checkpoint, :stage], 1)},
          {f.work, put_in(f.context, [:freshness_checkpoint, :attempt], 2)},
          {f.work, put_in(f.context, [:decision, :node_key], {{__MODULE__, :foreign}, nil})}
        ] do
      assert {:error, :invalid_admission_intent} = AdmissionIntent.new(f.run, work, context, now)
    end

    {:ok, intent} = AdmissionIntent.new(f.run, f.work, f.context, now)
    {:ok, metadata} = AdmissionIntent.put(%{}, intent)
    run = %{f.run | metadata: metadata}

    assert {:error, :invalid_admission_intent} =
             AdmissionIntent.load(run, f.work, %{f.version | manifest_version_id: "foreign"})

    assert {:error, :invalid_admission_intent} =
             AdmissionIntent.load(%{run | submit_kind: :manual}, f.work, f.version)

    for replacement <- [nil, 1, %{"version" => 99}] do
      damaged = put_in(metadata, ["execution_admission_intent", "deadline_at"], replacement)

      assert {:error, :invalid_admission_intent} =
               AdmissionIntent.load(%{run | metadata: damaged}, f.work, f.version)
    end
  end

  test "intent retains execution facts without duplicating the diagnostic tree", f do
    context = put_in(f.context, [:decision, :stale_reasons], [String.duplicate("x", 70_000)])

    assert {:ok, intent} =
             AdmissionIntent.new(f.run, f.work, context, ~U[2026-09-17 10:00:00Z])

    assert intent.context == f.context
    {:ok, encoded} = AdmissionIntent.encode(intent)
    assert byte_size(Jason.encode!(encoded)) < 4_096
  end

  test "a fresh BEAM restores intent using only the saved snapshot and manifest", f do
    module_name = "Elixir.AdmissionConsumer#{System.unique_integer([:positive])}"
    version = Fixture.version(module_name)
    ref = hd(version.manifest.assets).ref

    run =
      RunState.new(
        submit_kind: :pipeline,
        workspace_id: f.run.workspace_id,
        id: f.run.id,
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        runner_releases: version.runner_releases,
        asset_ref: ref,
        target_refs: [ref]
      )

    work = %{
      f.work
      | manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        node_identity: %NodeIdentity{node_key: {ref, nil}}
    }

    context = put_in(f.context, [:decision, :node_key], {ref, nil})
    {:ok, intent} = AdmissionIntent.new(run, work, context, ~U[2026-09-17 10:00:00Z])
    {:ok, metadata} = AdmissionIntent.put(%{}, intent)
    {:ok, snapshot} = RunSnapshotCodec.encode_run(%{run | metadata: metadata})
    {:ok, manifest} = ManifestRecord.to_record(version)
    dir = Path.join(System.tmp_dir!(), "favn-intent-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    file = Path.join(dir, "intent.json")

    File.write!(
      file,
      Jason.encode!(%{module: module_name, snapshot: snapshot, manifest: manifest})
    )

    script = Path.expand("../../support/admission_intent_process.exs", __DIR__)
    paths = :code.get_path() |> Enum.flat_map(fn path -> ["-pa", to_string(path)] end)

    for _restart <- 1..2 do
      {output, status} =
        System.cmd(System.find_executable("elixir"), paths ++ [script, file],
          stderr_to_stdout: true,
          env: [{"ERL_FLAGS", "+S 2:2"}]
        )

      assert status == 0, output
      assert output =~ "original intent restored"
    end
  end
end
