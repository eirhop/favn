defmodule Favn.Dev.Maintainer.RunnerContext do
  @moduledoc false

  alias Favn.Dev.Build.{Artifact, SourceInputSet}
  alias Favn.Dev.Paths

  @bundle_kind "favn_maintainer_runner_context"

  @spec ensure(SourceInputSet.t(), keyword()) :: {:ok, Path.t()} | {:error, term()}
  def ensure(%SourceInputSet{} = input_set, opts) when is_list(opts) do
    fingerprint = SourceInputSet.fingerprint(input_set)
    root_dir = opts |> Paths.root_dir() |> Path.expand()

    directory =
      Path.join([Paths.build_dir(root_dir), "maintainer-runner-context", fingerprint])

    metadata = %{"source_fingerprint" => fingerprint}

    case Artifact.verify_bundle(directory, @bundle_kind, metadata) do
      :ok ->
        {:ok, Path.join(directory, "context")}

      {:error, _reason} ->
        build(directory, input_set, metadata)
    end
  end

  defp build(directory, input_set, metadata) do
    case Artifact.atomic_directory(directory, fn temp_dir ->
           with :ok <- SourceInputSet.materialize(input_set, Path.join(temp_dir, "context")),
                :ok <- Artifact.write_bundle(temp_dir, @bundle_kind, metadata) do
             {:ok, :created}
           end
         end) do
      {:ok, :created} ->
        {:ok, Path.join(directory, "context")}

      {:error, :artifact_already_exists} ->
        if Artifact.verify_bundle(directory, @bundle_kind, metadata) == :ok,
          do: {:ok, Path.join(directory, "context")},
          else: {:error, :maintainer_runner_context_invalid}

      {:error, reason} ->
        {:error, {:maintainer_runner_context_failed, reason}}
    end
  end
end
