alias Favn.Contracts.RunnerWork
alias Favn.Manifest.Serializer
alias Favn.Manifest.Version
alias Favn.Plan.NodeIdentity
alias FavnOrchestrator.RunServer.Execution.AdmissionIntent
alias FavnOrchestrator.Storage.RunSnapshotCodec

[file] = System.argv()
data = file |> File.read!() |> Jason.decode!()

try do
  String.to_existing_atom(data["module"])
  raise "consumer module atom unexpectedly present"
rescue
  ArgumentError -> :ok
end

record = data["manifest"]
{:ok, manifest} = Serializer.decode_manifest(record["manifest_index_json"])

{:ok, version} =
  Version.from_published(manifest,
    manifest_version_id: record["manifest_version_id"],
    content_hash: record["content_hash"],
    runner_releases: record["runner_releases"]
  )

manifest_record = %{
  manifest_version_id: record["manifest_version_id"],
  content_hash: record["content_hash"],
  runner_releases: record["runner_releases"],
  manifest_index_json: record["manifest_index_json"]
}

{:ok, run} =
  RunSnapshotCodec.decode_run(
    %{run_blob: data["snapshot"], manifest_version_id: version.manifest_version_id},
    manifest_record
  )

work = %RunnerWork{
  run_id: run.id,
  manifest_version_id: run.manifest_version_id,
  manifest_content_hash: run.manifest_content_hash,
  node_identity: %NodeIdentity{node_key: {run.asset_ref, nil}},
  asset_step_id: "step",
  stage: 0,
  attempt: 1,
  deadline_at: ~U[2026-09-17 12:05:00Z]
}

{:ok, intent} = AdmissionIntent.load(run, work, version)
true = intent.deadline_at == ~U[2026-09-17 10:05:00Z]
true = intent.occurred_at == ~U[2026-09-17 10:00:00Z]
true = intent.context.decision.node_key == {run.asset_ref, nil}
IO.puts("original intent restored")
