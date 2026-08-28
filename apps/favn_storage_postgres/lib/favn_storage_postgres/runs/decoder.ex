defmodule FavnStoragePostgres.Runs.Decoder do
  @moduledoc false

  import Ecto.Query

  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Storage.RunSnapshotCodec
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.ManifestVersion
  alias FavnStoragePostgres.Schemas.Run
  alias FavnStoragePostgres.Schemas.RunPlan

  @spec decode(Run.t()) :: {:ok, FavnOrchestrator.RunState.t()} | {:error, Error.t()}
  def decode(%Run{} = row) do
    decode(
      row,
      load_manifest(row.manifest_version_id),
      Repo.get_by(RunPlan, workspace_id: row.workspace_id, run_id: row.run_id)
    )
  end

  defp load_manifest(manifest_version_id) do
    manifest =
      from(manifest in ManifestVersion,
        where: manifest.manifest_version_id == ^manifest_version_id,
        select: %ManifestVersion{
          manifest_version_id: manifest.manifest_version_id,
          content_hash: manifest.content_hash,
          runner_releases: manifest.runner_releases,
          atom_strings: manifest.atom_strings,
          manifest:
            fragment(
              "CASE WHEN ? IS NULL THEN ? ELSE NULL END",
              manifest.atom_strings,
              manifest.manifest
            )
        }
      )
      |> Repo.one()

    manifest
  end

  defp decode(
         %Run{manifest_version_id: manifest_version_id} = row,
         %ManifestVersion{manifest_version_id: manifest_version_id} = manifest,
         run_plan
       )
       when is_binary(manifest_version_id) do
    snapshot = attach_plan(row.snapshot, run_plan)

    run_record = %{
      run_blob: Jason.encode!(snapshot),
      manifest_version_id: manifest_version_id
    }

    case manifest.atom_strings do
      atom_strings when is_list(atom_strings) ->
        manifest_record = %{
          manifest_version_id: manifest_version_id,
          content_hash: Base.encode16(manifest.content_hash, case: :lower),
          runner_releases: manifest.runner_releases,
          atom_strings: atom_strings
        }

        case RunSnapshotCodec.decode_run(run_record, manifest_record) do
          {:ok, run} ->
            {:ok, %{run | workspace_id: row.workspace_id, deployment_id: row.deployment_id}}

          {:error, reason} ->
            invalid_snapshot(reason)
        end

      _missing_inventory ->
        manifest_record = %{
          manifest_version_id: manifest_version_id,
          content_hash: Base.encode16(manifest.content_hash, case: :lower),
          runner_releases: manifest.runner_releases,
          manifest_index_json: Jason.encode!(manifest.manifest)
        }

        case RunSnapshotCodec.decode_run(run_record, manifest_record) do
          {:ok, run} ->
            {:ok, %{run | workspace_id: row.workspace_id, deployment_id: row.deployment_id}}

          {:error, reason} ->
            invalid_snapshot(reason)
        end
    end
  end

  defp decode(%Run{}, nil, _run_plan),
    do: {:error, Error.new(:internal, "run references a missing manifest")}

  defp decode(%Run{}, %ManifestVersion{}, _run_plan),
    do: {:error, Error.new(:internal, "run references a different manifest")}

  defp attach_plan(snapshot, %RunPlan{} = plan) do
    expected = Base.encode16(plan.plan_hash, case: :lower)

    if Map.get(snapshot, "plan_hash") == expected do
      Map.put(snapshot, "plan", plan.plan)
    else
      Map.put(snapshot, "plan", %{"invalid_plan_hash" => expected})
    end
  end

  defp attach_plan(snapshot, nil), do: snapshot

  defp invalid_snapshot(reason) do
    {:error,
     Error.new(:internal, "persisted run snapshot is invalid",
       details: %{reason: inspect(reason)}
     )}
  end
end
