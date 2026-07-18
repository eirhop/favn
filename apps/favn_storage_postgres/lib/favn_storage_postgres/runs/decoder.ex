defmodule FavnStoragePostgres.Runs.Decoder do
  @moduledoc false

  import Ecto.Query

  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Storage.RunSnapshotCodec
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.ManifestVersion
  alias FavnStoragePostgres.Schemas.Run

  @spec decode(Run.t()) :: {:ok, FavnOrchestrator.RunState.t()} | {:error, Error.t()}
  def decode(%Run{} = row), do: decode(row, Repo.get(ManifestVersion, row.manifest_version_id))

  @spec decode_many([Run.t()]) ::
          {:ok, [FavnOrchestrator.RunState.t()]} | {:error, Error.t()}
  def decode_many(rows) when is_list(rows) do
    manifests = load_manifests(rows)

    rows
    |> Enum.reduce_while({:ok, []}, fn row, {:ok, acc} ->
      case decode(row, Map.get(manifests, row.manifest_version_id)) do
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

    from(manifest in ManifestVersion, where: manifest.manifest_version_id in ^ids)
    |> Repo.all()
    |> Map.new(&{&1.manifest_version_id, &1})
  end

  defp decode(%Run{} = row, %ManifestVersion{} = manifest) do
    run_record = %{
      run_blob: Jason.encode!(row.snapshot),
      manifest_version_id: row.manifest_version_id
    }

    manifest_record = %{
      manifest_version_id: manifest.manifest_version_id,
      content_hash: Base.encode16(manifest.content_hash, case: :lower),
      manifest_json: Jason.encode!(manifest.manifest)
    }

    case RunSnapshotCodec.decode_run(run_record, manifest_record) do
      {:ok, run} ->
        {:ok, %{run | workspace_id: row.workspace_id, deployment_id: row.deployment_id}}

      {:error, reason} ->
        {:error,
         Error.new(:internal, "persisted run snapshot is invalid",
           details: %{reason: inspect(reason)}
         )}
    end
  end

  defp decode(%Run{}, nil),
    do: {:error, Error.new(:internal, "run references a missing manifest")}
end
