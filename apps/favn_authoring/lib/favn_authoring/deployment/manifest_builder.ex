defmodule FavnAuthoring.Deployment.ManifestBuilder do
  @moduledoc """
  Builds an immutable manifest release aligned with an exact pool-to-release map.

  The caller supplies every release ID: local lifecycle tooling generates them,
  while production CI chooses them explicitly. This module compiles authored
  definitions and binds each effective pool to its release; it does not inspect,
  package, or build customer runtime code.
  """

  alias FavnAuthoring.Deployment.Artifact
  alias FavnAuthoring.Deployment.ManifestArchive
  alias Favn.Manifest.{Publication, Serializer}
  alias Favn.RunnerPool

  @test_only_options [:allow_non_prod_build, :skip_compile]

  @type result :: %{
          manifest_version_id: String.t(),
          runner_releases: RunnerPool.releases(),
          dist_dir: Path.t(),
          manifest_path: Path.t(),
          archive_path: Path.t(),
          archive_sha256: String.t(),
          status: :built | :already_built
        }

  @doc "Builds a standalone manifest release for an exact pool-to-release map."
  @spec run(keyword()) :: {:ok, result()} | {:error, term()}
  def run(opts) when is_list(opts) do
    with :ok <- validate_test_only_options(opts),
         :ok <- ensure_production_build(opts),
         {:ok, runner_releases} <- required_runner_releases(opts),
         :ok <- compile_project(opts),
         {:ok, publication} <- build_publication(runner_releases),
         {:ok, result} <- write_release(root_dir(opts), publication) do
      {:ok, result}
    end
  end

  defp validate_test_only_options(opts) do
    invalid = Keyword.keys(opts) |> Enum.filter(&(&1 in @test_only_options)) |> Enum.uniq()

    if Mix.env() == :test or invalid == [],
      do: :ok,
      else: {:error, {:unsupported_build_options, Enum.sort(invalid)}}
  end

  defp ensure_production_build(opts) do
    if Mix.env() == :prod or
         (Mix.env() == :test and Keyword.get(opts, :allow_non_prod_build, false)) do
      :ok
    else
      {:error, {:production_build_required, Mix.env()}}
    end
  end

  @doc false
  @spec build_publication(RunnerPool.releases()) :: {:ok, Publication.t()} | {:error, term()}
  def build_publication(runner_releases) when is_map(runner_releases) do
    with :ok <- RunnerPool.validate_releases(runner_releases),
         {:ok, build} <- FavnAuthoring.build_manifest(runner_releases: runner_releases),
         {:ok, publication} <- FavnAuthoring.prepare_manifest_publication(build) do
      {:ok, publication}
    end
  end

  @doc false
  @spec write_bundle(Path.t(), Publication.t()) :: :ok | {:error, term()}
  def write_bundle(directory, %Publication{} = publication) do
    version = publication.version

    with :ok <- File.mkdir_p(directory),
         {:ok, manifest} <- FavnAuthoring.serialize_manifest(version.manifest),
         :ok <- File.write(Path.join(directory, "manifest-index.json"), manifest <> "\n"),
         :ok <- write_packages(directory, publication.execution_packages),
         :ok <- normalize_file_modes(directory),
         :ok <-
           Artifact.write_bundle(directory, "favn_manifest_release", %{
             "manifest" => %{
               "manifest_version_id" => version.manifest_version_id,
               "content_hash" => version.content_hash,
               "schema_version" => version.schema_version,
               "runner_contract_version" => version.runner_contract_version,
               "runner_releases" => version.runner_releases,
               "serialization_format" => version.serialization_format,
               "index_path" => "manifest-index.json",
               "execution_packages_path" => "execution-packages"
             }
           }) do
      :ok
    end
  end

  @doc false
  @spec compile_project(keyword()) :: :ok | {:error, term()}
  def compile_project(opts) do
    if Mix.env() == :test and Keyword.get(opts, :skip_compile, false) do
      :ok
    else
      Mix.Task.reenable("compile")

      case Mix.Task.run("compile", []) do
        {:error, reason} -> {:error, {:compile_failed, reason}}
        _result -> :ok
      end
    end
  end

  @doc false
  @spec write_release(Path.t(), Publication.t()) :: {:ok, result()} | {:error, term()}
  def write_release(root_dir, %Publication{} = publication) when is_binary(root_dir) do
    dist_dir =
      Path.join([
        root_dir,
        ".favn",
        "dist",
        "manifest",
        publication.version.manifest_version_id
      ])

    directory_result =
      Artifact.atomic_directory(dist_dir, fn temp_dir ->
        with :ok <- write_bundle(temp_dir, publication) do
          {:ok, :built}
        end
      end)

    with {:ok, directory_status} <- resolve_directory(directory_result, dist_dir, publication),
         archive_path = dist_dir <> ".tar.gz",
         {:ok, archive} <- ManifestArchive.write(dist_dir, archive_path) do
      status =
        if directory_status == :built or archive.status == :built,
          do: :built,
          else: :already_built

      {:ok, result(publication, dist_dir, archive, status)}
    end
  end

  defp resolve_directory({:ok, :built}, _dist_dir, _publication), do: {:ok, :built}

  defp resolve_directory({:error, :artifact_already_exists}, dist_dir, publication) do
    with :ok <- verify_existing(dist_dir, publication), do: {:ok, :already_built}
  end

  defp resolve_directory({:error, reason}, _dist_dir, _publication), do: {:error, reason}

  defp verify_existing(dist_dir, publication) do
    manifest_version_id = publication.version.manifest_version_id
    content_hash = publication.version.content_hash
    runner_releases = publication.version.runner_releases

    with :ok <-
           Artifact.verify_bundle(dist_dir, "favn_manifest_release", %{
             "manifest" => %{
               "manifest_version_id" => manifest_version_id,
               "content_hash" => content_hash,
               "schema_version" => publication.version.schema_version,
               "runner_contract_version" => publication.version.runner_contract_version,
               "runner_releases" => runner_releases,
               "serialization_format" => publication.version.serialization_format,
               "index_path" => "manifest-index.json",
               "execution_packages_path" => "execution-packages"
             }
           }) do
      :ok
    else
      _mismatch -> {:error, :manifest_artifact_conflict}
    end
  end

  defp result(publication, dist_dir, archive, status) do
    %{
      manifest_version_id: publication.version.manifest_version_id,
      runner_releases: publication.version.runner_releases,
      dist_dir: dist_dir,
      manifest_path: Path.join(dist_dir, "manifest-index.json"),
      archive_path: archive.path,
      archive_sha256: archive.sha256,
      status: status
    }
  end

  defp write_packages(directory, packages) do
    package_dir = Path.join(directory, "execution-packages")

    with :ok <- File.mkdir_p(package_dir) do
      Enum.reduce_while(packages, :ok, fn package, :ok ->
        with {:ok, encoded} <- Serializer.encode_manifest(package),
             :ok <-
               File.write(
                 Path.join(package_dir, package.content_hash <> ".json"),
                 encoded <> "\n"
               ) do
          {:cont, :ok}
        else
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  defp normalize_file_modes(directory) do
    directory
    |> Path.join("**/*")
    |> Path.wildcard()
    |> Enum.filter(&File.regular?/1)
    |> Enum.reduce_while(:ok, fn path, :ok ->
      case File.chmod(path, 0o644) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:manifest_file_mode_failed, path, reason}}}
      end
    end)
  end

  defp required_runner_releases(opts) do
    case Keyword.get(opts, :runner_releases) do
      value when is_map(value) ->
        case RunnerPool.validate_releases(value) do
          :ok -> {:ok, value}
          {:error, _reason} -> {:error, {:invalid_runner_releases, value}}
        end

      _missing ->
        {:error, {:missing_required_option, :runner_releases}}
    end
  end

  defp root_dir(opts), do: opts |> Keyword.get(:root_dir, File.cwd!()) |> Path.expand()
end
