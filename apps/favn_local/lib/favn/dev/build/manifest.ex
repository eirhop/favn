defmodule Favn.Dev.Build.Manifest do
  @moduledoc """
  Builds an immutable manifest release aligned with one runner descriptor.

  A descriptor is verified before compilation output can become publishable.
  The current runtime fingerprint is recomputed after compilation; executable
  code, dependency, plugin, or runtime-toolchain drift requires a new runner.
  """

  alias Favn.Dev.Build.{Artifact, RunnerInputs}
  alias Favn.Dev.Paths
  alias Favn.Manifest.{Publication, Serializer}
  alias Favn.RunnerRelease

  @test_only_options [
    :allow_non_prod_build,
    :allow_unpinned_favn,
    :build_dependency_sources,
    :current_app,
    :current_app_source,
    :dependency_sources,
    :extra_applications,
    :extra_modules,
    :host_toolchain,
    :lock,
    :module_inventory,
    :runner_build,
    :runner_plugins,
    :skip_compile
  ]

  @type result :: %{
          manifest_version_id: String.t(),
          required_runner_release_id: String.t(),
          dist_dir: Path.t(),
          manifest_path: Path.t(),
          status: :built | :already_built
        }

  @doc "Builds a standalone manifest release from an explicit descriptor path."
  @spec run(keyword()) :: {:ok, result()} | {:error, term()}
  def run(opts) when is_list(opts) do
    with :ok <- validate_test_only_options(opts),
         :ok <- ensure_production_build(opts),
         {:ok, descriptor_path} <- required_path(opts, :runner_release),
         {:ok, descriptor} <- read_descriptor(descriptor_path),
         :ok <- compile_project(opts),
         {:ok, publication, _inputs} <- build_publication(descriptor, opts) do
      root_dir = Paths.root_dir(opts)
      write_release(root_dir, publication)
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
  @spec build_publication(RunnerRelease.t(), keyword()) ::
          {:ok, Publication.t(), RunnerInputs.t()} | {:error, term()}
  def build_publication(%RunnerRelease{} = descriptor, opts) when is_list(opts) do
    with {:ok, build} <- FavnAuthoring.build_manifest(runner_release: descriptor),
         {:ok, inputs} <- RunnerInputs.collect(build, opts),
         :ok <- RunnerInputs.compare(descriptor, inputs.descriptor),
         {:ok, publication} <- FavnAuthoring.prepare_manifest_publication(build) do
      {:ok, publication, inputs}
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
         :ok <-
           Artifact.write_bundle(directory, "favn_manifest_release", %{
             "manifest" => %{
               "manifest_version_id" => version.manifest_version_id,
               "content_hash" => version.content_hash,
               "schema_version" => version.schema_version,
               "runner_contract_version" => version.runner_contract_version,
               "required_runner_release_id" => version.required_runner_release_id,
               "serialization_format" => version.serialization_format,
               "index_path" => "manifest-index.json",
               "execution_packages_path" => "execution-packages"
             }
           }) do
      :ok
    end
  end

  @doc false
  @spec read_descriptor(Path.t()) :: {:ok, RunnerRelease.t()} | {:error, term()}
  def read_descriptor(path) when is_binary(path) do
    with {:ok, bytes} <- File.read(path),
         {:ok, descriptor} <- RunnerRelease.decode(bytes) do
      {:ok, descriptor}
    else
      {:error, :enoent} ->
        {:error, :runner_release_descriptor_missing}

      {:error, reason} ->
        {:error, {:runner_release_descriptor_invalid, descriptor_reason(reason)}}
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
    dist_dir = Paths.dist_manifest_dir(root_dir, publication.version.manifest_version_id)

    case Artifact.atomic_directory(dist_dir, fn temp_dir ->
           with :ok <- write_bundle(temp_dir, publication) do
             {:ok, :built}
           end
         end) do
      {:ok, :built} -> {:ok, result(publication, dist_dir, :built)}
      {:error, :artifact_already_exists} -> verify_existing(dist_dir, publication)
      {:error, reason} -> {:error, reason}
    end
  end

  defp verify_existing(dist_dir, publication) do
    manifest_version_id = publication.version.manifest_version_id
    content_hash = publication.version.content_hash
    required_runner_release_id = publication.version.required_runner_release_id

    with :ok <-
           Artifact.verify_bundle(dist_dir, "favn_manifest_release", %{
             "manifest" => %{
               "manifest_version_id" => manifest_version_id,
               "content_hash" => content_hash,
               "schema_version" => publication.version.schema_version,
               "runner_contract_version" => publication.version.runner_contract_version,
               "required_runner_release_id" => required_runner_release_id,
               "serialization_format" => publication.version.serialization_format,
               "index_path" => "manifest-index.json",
               "execution_packages_path" => "execution-packages"
             }
           }) do
      {:ok, result(publication, dist_dir, :already_built)}
    else
      _mismatch -> {:error, :manifest_artifact_conflict}
    end
  end

  defp result(publication, dist_dir, status) do
    %{
      manifest_version_id: publication.version.manifest_version_id,
      required_runner_release_id: publication.version.required_runner_release_id,
      dist_dir: dist_dir,
      manifest_path: Path.join(dist_dir, "manifest-index.json"),
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

  defp required_path(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, {:missing_required_option, key}}
    end
  end

  defp descriptor_reason({:unsupported_runner_release_schema, _actual, _expected}),
    do: :unsupported_schema

  defp descriptor_reason({:unsupported_runner_contract, _actual, _expected}),
    do: :unsupported_runner_contract

  defp descriptor_reason({:unsupported_favn_version, _actual, _expected}),
    do: :unsupported_favn_version

  defp descriptor_reason({:invalid_runner_release_json, _reason}), do: :invalid_json
  defp descriptor_reason(_reason), do: :invalid_descriptor
end
