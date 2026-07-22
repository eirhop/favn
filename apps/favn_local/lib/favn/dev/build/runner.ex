defmodule Favn.Dev.Build.Runner do
  @moduledoc """
  Builds the immutable, customer-owned runner OCI context.

  The output directory is named by `runner_release_id`. It contains the
  self-verifying descriptor, an aligned manifest release, vendored release
  inputs, a digest-pinned multi-stage Dockerfile, and hashed bundle metadata.
  This operation never invokes Docker or pushes an image.
  """

  alias Favn.Dev.Build.{Artifact, Manifest, RunnerInputs, RunnerReleaseInput}
  alias Favn.Dev.{Install, Paths, State}
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
    :lock,
    :module_inventory,
    :runner_build,
    :runner_plugins,
    :skip_compile,
    :skip_project_root_check
  ]

  @type result :: %{
          runner_release_id: String.t(),
          build_id: String.t(),
          dist_dir: Path.t(),
          descriptor_path: Path.t(),
          manifest_dir: Path.t(),
          embedded_manifest_dir: Path.t(),
          manifest_status: :built | :already_built,
          status: :built | :already_built
        }

  @spec run(keyword()) :: {:ok, result()} | {:error, term()}
  def run(opts \\ []) when is_list(opts) do
    with :ok <- validate_test_only_options(opts),
         :ok <- ensure_production_build(opts),
         :ok <- ensure_project_root(opts),
         :ok <- Install.ensure_ready(opts),
         :ok <- State.ensure_layout(opts),
         :ok <- Manifest.compile_project(opts),
         {:ok, seed} <- seed_descriptor(),
         {:ok, seed_build} <- FavnAuthoring.build_manifest(runner_release: seed),
         {:ok, inputs} <- RunnerInputs.collect(seed_build, opts),
         {:ok, publication, aligned_inputs} <-
           Manifest.build_publication(inputs.descriptor, Keyword.put(opts, :skip_compile, true)) do
      root_dir = Paths.root_dir(opts)
      release_id = inputs.descriptor.runner_release_id
      dist_dir = Paths.dist_runner_dir(root_dir, release_id)

      with {:ok, runner_result} <- write_artifact(dist_dir, aligned_inputs, publication, opts),
           {:ok, manifest_result} <- Manifest.write_release(root_dir, publication),
           result <-
             runner_result
             |> Map.put(:manifest_dir, manifest_result.dist_dir)
             |> Map.put(:manifest_status, manifest_result.status),
           :ok <- write_latest(result, publication, opts) do
        {:ok, result}
      end
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

  defp write_artifact(dist_dir, inputs, publication, opts) do
    descriptor = inputs.descriptor

    case Artifact.atomic_directory(dist_dir, fn temp_dir ->
           with {:ok, encoded_descriptor} <- RunnerRelease.encode(descriptor),
                :ok <-
                  File.write(
                    Path.join(temp_dir, "runner-release.json"),
                    encoded_descriptor <> "\n"
                  ),
                :ok <- Manifest.write_bundle(Path.join(temp_dir, "manifest"), publication),
                :ok <- RunnerReleaseInput.write(temp_dir, inputs, opts),
                :ok <- write_operator_notes(temp_dir, descriptor, publication),
                :ok <-
                  Artifact.write_bundle(temp_dir, "favn_runner_build_context", %{
                    "runner_release_id" => descriptor.runner_release_id,
                    "favn_version" => descriptor.favn_version,
                    "runner_contract_version" => descriptor.runner_contract_version,
                    "target" => descriptor.target,
                    "manifest_version_id" => publication.version.manifest_version_id
                  }) do
             {:ok, :built}
           end
         end) do
      {:ok, :built} -> {:ok, result(dist_dir, descriptor, :built)}
      {:error, :artifact_already_exists} -> verify_existing(dist_dir, descriptor)
      {:error, reason} -> {:error, reason}
    end
  end

  defp verify_existing(dist_dir, descriptor) do
    manifest_version_id = aligned_manifest_version_id(dist_dir)

    with {:ok, existing} <- Manifest.read_descriptor(Path.join(dist_dir, "runner-release.json")),
         true <- existing.runner_release_id == descriptor.runner_release_id,
         value when is_binary(value) <- manifest_version_id,
         :ok <-
           Artifact.verify_bundle(dist_dir, "favn_runner_build_context", %{
             "runner_release_id" => descriptor.runner_release_id,
             "favn_version" => descriptor.favn_version,
             "runner_contract_version" => descriptor.runner_contract_version,
             "target" => descriptor.target,
             "manifest_version_id" => value
           }) do
      {:ok, result(dist_dir, descriptor, :already_built)}
    else
      _mismatch -> {:error, :runner_artifact_conflict}
    end
  end

  defp aligned_manifest_version_id(dist_dir) do
    with {:ok, bytes} <- File.read(Path.join([dist_dir, "manifest", "bundle.json"])),
         {:ok, bundle} <- JSON.decode(bytes) do
      get_in(bundle, ["manifest", "manifest_version_id"])
    else
      _invalid -> nil
    end
  end

  defp result(dist_dir, descriptor, status) do
    %{
      runner_release_id: descriptor.runner_release_id,
      build_id: descriptor.runner_release_id,
      dist_dir: dist_dir,
      descriptor_path: Path.join(dist_dir, "runner-release.json"),
      manifest_dir: nil,
      embedded_manifest_dir: Path.join(dist_dir, "manifest"),
      manifest_status: nil,
      status: status
    }
  end

  defp seed_descriptor do
    RunnerRelease.new(%{
      schema_version: RunnerRelease.current_schema_version(),
      favn_version: RunnerRelease.current_favn_version(),
      runner_contract_version: Favn.Manifest.Compatibility.current_runner_contract_version(),
      elixir_version: System.version(),
      otp_release: to_string(:erlang.system_info(:otp_release)),
      target: RunnerRelease.current_target(),
      runtime_modules: [],
      runtime_applications: [],
      plugins: [],
      build_profile: "prod"
    })
  end

  defp ensure_project_root(opts) do
    requested_root = opts |> Paths.root_dir() |> Path.expand()
    current_root = File.cwd!() |> Path.expand()

    if requested_root == current_root or
         (Mix.env() == :test and Keyword.get(opts, :skip_project_root_check, false)) do
      :ok
    else
      {:error, {:unsupported_root_dir, requested_root, current_root}}
    end
  end

  defp write_operator_notes(directory, descriptor, publication) do
    notes = """
    # Favn customer runner

    Runner release: `#{descriptor.runner_release_id}`
    Manifest version: `#{publication.version.manifest_version_id}`

    Build this context with an OCI-compatible builder, tag it immutably, and
    push it to the registry you operate. Favn does not receive registry
    credentials and does not publish customer runner images.

    The container runs as UID/GID 10001. Deploy it with a read-only root
    filesystem and keep only `/tmp/favn` writable. Expose EPMD and the single
    fixed BEAM distribution port only on the private application network.
    """

    File.write(Path.join(directory, "operator-notes.md"), notes)
  end

  defp write_latest(result, publication, opts) do
    State.write_runner_latest(
      %{
        "schema_version" => 1,
        "runner_release_id" => result.runner_release_id,
        "dist_dir" => result.dist_dir,
        "descriptor_path" => result.descriptor_path,
        "manifest_dir" => result.manifest_dir,
        "manifest_version_id" => publication.version.manifest_version_id
      },
      opts
    )
  end
end
