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

  @spec decode_many([Run.t()]) ::
          {:ok, [FavnOrchestrator.RunState.t()]} | {:error, Error.t()}
  def decode_many(rows) when is_list(rows) do
    manifests = load_manifests(rows)
    plans = load_plans(rows)

    rows
    |> Enum.reduce_while({:ok, []}, fn row, {:ok, acc} ->
      case decode(
             row,
             Map.get(manifests, row.manifest_version_id),
             Map.get(plans, {row.workspace_id, row.run_id})
           ) do
        {:ok, run} -> {:cont, {:ok, [run | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> then(fn
      {:ok, runs} -> {:ok, Enum.reverse(runs)}
      error -> error
    end)
  end

  defp load_manifests(rows) do
    ids = rows |> Enum.map(& &1.manifest_version_id) |> Enum.uniq()

    manifests =
      from(manifest in ManifestVersion,
        where: manifest.manifest_version_id in ^ids,
        select: %ManifestVersion{
          manifest_version_id: manifest.manifest_version_id,
          content_hash: manifest.content_hash,
          runner_releases: manifest.runner_releases,
          atom_strings: manifest.atom_strings
        }
      )
      |> Repo.all()

    Map.new(manifests, fn manifest ->
      {manifest.manifest_version_id, manifest}
    end)
  end

  defp load_manifest(manifest_version_id) do
    from(manifest in ManifestVersion,
      where: manifest.manifest_version_id == ^manifest_version_id,
      select: %ManifestVersion{
        manifest_version_id: manifest.manifest_version_id,
        content_hash: manifest.content_hash,
        runner_releases: manifest.runner_releases,
        atom_strings: manifest.atom_strings
      }
    )
    |> Repo.one()
  end

  defp load_plans(rows) do
    identities = rows |> Enum.map(&{&1.workspace_id, &1.run_id}) |> Enum.uniq()

    case identities do
      [] ->
        %{}

      identities ->
        predicate =
          Enum.reduce(identities, dynamic(false), fn {workspace_id, run_id}, predicate ->
            dynamic(
              [plan],
              ^predicate or
                (plan.workspace_id == ^workspace_id and plan.run_id == ^run_id)
            )
          end)

        RunPlan
        |> where(^predicate)
        |> Repo.all()
        |> Map.new(&{{&1.workspace_id, &1.run_id}, &1})
    end
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

    case RunSnapshotCodec.decode_run(run_record, manifest_record(manifest)) do
      {:ok, run} ->
        {:ok, %{run | workspace_id: row.workspace_id, deployment_id: row.deployment_id}}

      {:error, reason} ->
        {:error,
         Error.new(:internal, "persisted run snapshot is invalid",
           details: %{reason: inspect(reason)}
         )}
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

  defp manifest_record(%ManifestVersion{atom_strings: atom_strings} = manifest)
       when is_list(atom_strings) do
    %{
      manifest_version_id: manifest.manifest_version_id,
      content_hash: Base.encode16(manifest.content_hash, case: :lower),
      runner_releases: manifest.runner_releases,
      atom_strings: atom_strings
    }
  end

  defp manifest_record(%ManifestVersion{} = manifest) do
    document =
      from(row in ManifestVersion,
        where: row.manifest_version_id == ^manifest.manifest_version_id,
        select: row.manifest
      )
      |> Repo.one!()

    %{
      manifest_version_id: manifest.manifest_version_id,
      content_hash: Base.encode16(manifest.content_hash, case: :lower),
      runner_releases: manifest.runner_releases,
      manifest_index_json: Jason.encode!(document)
    }
  end
end
