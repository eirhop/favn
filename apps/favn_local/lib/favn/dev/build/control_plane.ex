defmodule Favn.Dev.Build.ControlPlane do
  @moduledoc """
  Repository-maintainer builder for the production control-plane image.

  The builder writes a relocatable, integrity-checked context containing only
  the four control-plane applications and their production inputs. With
  `load: true`, it builds that context for Linux amd64 and loads the unpublished
  `favn-control-plane-candidate:<control_plane_build_id>` image locally.
  """

  alias Favn.Dev.Build.Artifact
  alias Favn.Dev.Build.ControlPlaneInputs
  alias Favn.Dev.Paths
  alias Favn.Manifest.Serializer

  @image_repository "ghcr.io/eirhop/favn-control-plane"
  @candidate_repository "favn-control-plane-candidate"
  @image_id ~r/\Asha256:[0-9a-f]{64}\z/
  @digest ~r/\A[0-9a-f]{64}\z/

  @type result :: %{
          required(:control_plane_build_id) => String.t(),
          required(:status) => :built | :already_built,
          required(:build_dir) => Path.t(),
          required(:context_dir) => Path.t(),
          required(:descriptor_path) => Path.t(),
          required(:image_repository) => String.t(),
          optional(:image_status) => :loaded | :reused,
          optional(:image_tag) => String.t(),
          optional(:image_id) => String.t(),
          optional(:static_asset_digest) => String.t(),
          optional(:candidate_path) => Path.t()
        }

  @doc "Assembles the deterministic context and optionally builds/loads its candidate image."
  @spec run(keyword()) :: {:ok, result()} | {:error, term()}
  def run(opts \\ []) when is_list(opts) do
    root_dir = opts |> Keyword.get(:root_dir, File.cwd!()) |> Path.expand()

    with :ok <- validate_options(opts, root_dir),
         {:ok, result} <- do_run(root_dir, opts) do
      {:ok, result}
    end
  end

  @doc "Builds from an explicitly selected local checkout for maintainer development."
  @spec run_from_checkout(Path.t(), keyword()) :: {:ok, result()} | {:error, term()}
  def run_from_checkout(checkout, opts \\ []) when is_binary(checkout) and is_list(opts) do
    root_dir = Path.expand(checkout)

    with :ok <- validate_checkout_options(opts, root_dir),
         {:ok, result} <- do_run(root_dir, opts) do
      {:ok, result}
    end
  end

  defp do_run(root_dir, opts) do
    with :ok <- validate_checkout_root(root_dir),
         {:ok, collected} <- ControlPlaneInputs.collect(root_dir),
         {:ok, artifact} <- build_context(root_dir, collected, opts),
         {:ok, result} <- maybe_load_image(artifact, collected.descriptor, root_dir, opts) do
      {:ok, result}
    end
  end

  defp validate_options(opts, root_dir) do
    allowed = [:root_dir, :load, :build_root]

    cond do
      Enum.any?(Keyword.keys(opts), &(&1 not in allowed)) ->
        {:error, :unsupported_control_plane_build_option}

      Keyword.has_key?(opts, :build_root) and Mix.env() != :test ->
        {:error, :control_plane_build_root_is_test_only}

      not is_boolean(Keyword.get(opts, :load, false)) ->
        {:error, :invalid_control_plane_load_option}

      Path.expand(root_dir) != Path.expand(File.cwd!()) and
          not (Mix.env() == :test and Keyword.has_key?(opts, :build_root)) ->
        {:error, {:control_plane_build_must_run_at_repository_root, root_dir}}

      true ->
        :ok
    end
  end

  defp validate_checkout_options(opts, root_dir) do
    allowed = [:load, :build_root]

    cond do
      Enum.any?(Keyword.keys(opts), &(&1 not in allowed)) ->
        {:error, :unsupported_control_plane_build_option}

      Keyword.has_key?(opts, :build_root) and Mix.env() != :test ->
        {:error, :control_plane_build_root_is_test_only}

      not is_boolean(Keyword.get(opts, :load, false)) ->
        {:error, :invalid_control_plane_load_option}

      Path.expand(root_dir) == Path.expand(File.cwd!()) ->
        :ok

      true ->
        validate_checkout_root(root_dir)
    end
  end

  defp validate_checkout_root(root_dir) do
    case File.lstat(root_dir) do
      {:ok, %{type: :directory}} -> :ok
      {:ok, %{type: :symlink}} -> {:error, {:control_plane_checkout_symlink, root_dir}}
      _invalid -> {:error, {:invalid_control_plane_checkout, root_dir}}
    end
  end

  defp build_context(
         root_dir,
         %{descriptor: descriptor, source_paths: source_paths, dependency_lock: dependency_lock},
         opts
       ) do
    build_id = descriptor.control_plane_build_id

    build_root =
      Keyword.get_lazy(opts, :build_root, fn ->
        Paths.build_target_dir(root_dir, "control-plane")
      end)

    final_dir = Path.join(build_root, build_id)

    case verify_existing(final_dir, descriptor) do
      :ok ->
        {:ok, artifact_result(final_dir, descriptor, :already_built)}

      {:error, :not_found} ->
        create_context(final_dir, root_dir, source_paths, dependency_lock, descriptor)

      {:error, reason} ->
        {:error, {:control_plane_artifact_conflict, reason}}
    end
  end

  defp create_context(final_dir, root_dir, source_paths, dependency_lock, descriptor) do
    case Artifact.atomic_directory(final_dir, fn temp_dir ->
           context_dir = Path.join(temp_dir, "context")

           with :ok <- File.mkdir_p(context_dir),
                :ok <- copy_source_paths(root_dir, context_dir, source_paths),
                :ok <- write_dependency_lock(context_dir, dependency_lock),
                :ok <-
                  copy_required_file(
                    root_dir,
                    context_dir,
                    "rel/control_plane/context.mix.exs",
                    "mix.exs"
                  ),
                :ok <- write_descriptor(context_dir, descriptor),
                :ok <- canonicalize_tree(context_dir),
                :ok <-
                  Artifact.write_bundle(context_dir, "favn_control_plane_context", %{
                    "control_plane_build_id" => descriptor.control_plane_build_id
                  }),
                :ok <- write_operator_notes(temp_dir, descriptor),
                :ok <-
                  Artifact.write_bundle(temp_dir, "favn_control_plane_build", %{
                    "control_plane_build_id" => descriptor.control_plane_build_id
                  }),
                :ok <- canonicalize_tree(temp_dir) do
             {:ok, :created}
           end
         end) do
      {:ok, :created} ->
        case verify_canonical_modes(final_dir) do
          :ok -> {:ok, artifact_result(final_dir, descriptor, :built)}
          {:error, reason} -> {:error, {:control_plane_artifact_conflict, reason}}
        end

      {:error, :artifact_already_exists} ->
        case verify_existing(final_dir, descriptor) do
          :ok -> {:ok, artifact_result(final_dir, descriptor, :already_built)}
          {:error, reason} -> {:error, {:control_plane_artifact_conflict, reason}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp write_dependency_lock(context_dir, dependency_lock) do
    entries =
      dependency_lock
      |> Enum.sort_by(fn {app, _entry} -> app end)
      |> Enum.map(fn {app, entry} ->
        encoded =
          inspect(entry,
            pretty: false,
            limit: :infinity,
            printable_limit: :infinity,
            width: :infinity,
            charlists: :as_lists
          )

        ~s(  "#{app}": #{encoded},\n)
      end)

    File.write(Path.join(context_dir, "mix.lock"), ["%{\n", entries, "}\n"])
  end

  defp copy_source_paths(root_dir, context_dir, source_paths) do
    Enum.reduce_while(source_paths, :ok, fn relative, :ok ->
      case copy_required_file(root_dir, context_dir, relative, relative) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp copy_required_file(root_dir, context_dir, source_relative, target_relative) do
    source = Path.join(root_dir, source_relative)
    target = Path.join(context_dir, target_relative)

    with {:ok, %{type: :regular}} <- File.lstat(source),
         :ok <- File.mkdir_p(Path.dirname(target)),
         :ok <- File.cp(source, target) do
      :ok
    else
      {:ok, %{type: :symlink}} -> {:error, {:control_plane_input_symlink, source_relative}}
      {:ok, _other} -> {:error, {:control_plane_input_not_regular, source_relative}}
      {:error, reason} -> {:error, {:control_plane_input_copy_failed, source_relative, reason}}
    end
  end

  @doc false
  @spec canonicalize_tree(Path.t()) :: :ok | {:error, term()}
  def canonicalize_tree(root) when is_binary(root) do
    paths = [root | Path.wildcard(Path.join(root, "**/*"), match_dot: true)]

    Enum.reduce_while(paths, :ok, fn path, :ok ->
      case File.lstat(path) do
        {:ok, %{type: :directory}} -> canonical_mode(File.chmod(path, 0o755), path)
        {:ok, %{type: :regular}} -> canonical_mode(File.chmod(path, 0o644), path)
        {:ok, %{type: :symlink}} -> {:halt, {:error, {:artifact_symlink, path}}}
        {:ok, _other} -> {:halt, {:error, {:artifact_special_file, path}}}
        {:error, reason} -> {:halt, {:error, {:artifact_mode_failed, path, reason}}}
      end
    end)
  end

  defp canonical_mode(:ok, _path), do: {:cont, :ok}

  defp canonical_mode({:error, reason}, path),
    do: {:halt, {:error, {:artifact_mode_failed, path, reason}}}

  defp write_descriptor(context_dir, descriptor) do
    Artifact.write_json(
      Path.join(context_dir, "control-plane-build.json"),
      descriptor_payload(descriptor)
    )
  end

  defp write_operator_notes(directory, descriptor) do
    build_id = descriptor.control_plane_build_id

    File.write(
      Path.join(directory, "operator-notes.md"),
      """
      # Favn control-plane candidate

      Control-plane build ID: `#{build_id}`

      The relocatable OCI context is in `context/`. Repository maintainers load
      it with `mix favn.build.control_plane --load`. Official images are
      published only by the protected GitHub Actions workflow and deployed by
      immutable registry digest.
      """
    )
  end

  defp verify_existing(final_dir, descriptor) do
    if File.dir?(final_dir) do
      with :ok <- verify_canonical_modes(final_dir),
           :ok <-
             Artifact.verify_bundle(final_dir, "favn_control_plane_build", %{
               "control_plane_build_id" => descriptor.control_plane_build_id
             }),
           :ok <-
             Artifact.verify_bundle(
               Path.join(final_dir, "context"),
               "favn_control_plane_context",
               %{"control_plane_build_id" => descriptor.control_plane_build_id}
             ),
           {:ok, bytes} <-
             File.read(Path.join(final_dir, "context/control-plane-build.json")),
           {:ok, decoded} <- JSON.decode(bytes),
           true <- decoded == json_value(descriptor_payload(descriptor)) do
        :ok
      else
        _invalid -> {:error, :invalid_control_plane_artifact}
      end
    else
      {:error, :not_found}
    end
  end

  defp verify_canonical_modes(root) do
    paths = [root | Path.wildcard(Path.join(root, "**/*"), match_dot: true)]

    Enum.reduce_while(paths, :ok, fn path, :ok ->
      case File.lstat(path) do
        {:ok, %{type: type, mode: mode}} when type in [:directory, :regular] ->
          expected = if type == :directory, do: 0o755, else: 0o644

          if Bitwise.band(mode, 0o777) == expected,
            do: {:cont, :ok},
            else:
              {:halt,
               {:error,
                {:noncanonical_artifact_mode, Path.relative_to(path, root),
                 Bitwise.band(mode, 0o777), expected}}}

        _invalid ->
          {:halt, {:error, {:noncanonical_artifact_mode, Path.relative_to(path, root)}}}
      end
    end)
  end

  defp maybe_load_image(artifact, descriptor, root_dir, opts) do
    if Keyword.get(opts, :load, false) do
      case reuse_loaded_image(artifact, descriptor) do
        {:ok, result} -> {:ok, result}
        {:error, _not_reusable} -> load_image(artifact, descriptor, root_dir)
      end
    else
      {:ok, artifact}
    end
  end

  defp reuse_loaded_image(artifact, descriptor) do
    image_tag = "#{@candidate_repository}:#{descriptor.control_plane_build_id}"
    candidate_path = candidate_path(artifact, descriptor)

    with {:ok, docker} <- docker_executable(),
         {:ok, %{type: :regular}} <- File.lstat(candidate_path),
         {:ok, encoded} <- File.read(candidate_path),
         {:ok, candidate} <- JSON.decode(encoded),
         {:ok, metadata} <- reusable_candidate(candidate, descriptor, image_tag),
         image_id = metadata.image_id,
         static_asset_digest = metadata.static_asset_digest,
         {:ok, ^image_id} <- inspect_image_id(docker, image_tag),
         :ok <-
           verify_loaded_image(
             docker,
             image_tag,
             descriptor,
             metadata.timestamp,
             %{revision: metadata.revision, dirty: metadata.dirty}
           ),
         {:ok, ^static_asset_digest} <- static_asset_digest(docker, image_tag) do
      {:ok,
       Map.merge(artifact, %{
         image_status: :reused,
         image_tag: image_tag,
         image_id: metadata.image_id,
         static_asset_digest: metadata.static_asset_digest,
         candidate_path: candidate_path
       })}
    else
      _not_reusable -> {:error, :candidate_not_reusable}
    end
  end

  defp reusable_candidate(
         %{
           "schema_version" => 1,
           "control_plane_build_id" => build_id,
           "image_repository" => @image_repository,
           "candidate_tag" => image_tag,
           "image_id" => image_id,
           "static_asset_digest" => static_asset_digest,
           "built_at" => timestamp,
           "source_revision" => revision,
           "source_dirty" => dirty,
           "target" => target
         },
         %{control_plane_build_id: build_id},
         image_tag
       )
       when is_binary(image_id) and is_binary(static_asset_digest) and is_binary(target) and
              is_binary(timestamp) and is_binary(revision) and is_boolean(dirty) do
    if target == ControlPlaneInputs.target() and Regex.match?(@image_id, image_id) and
         Regex.match?(@digest, static_asset_digest) and timestamp != "" and revision != "" do
      {:ok,
       %{
         image_id: image_id,
         static_asset_digest: static_asset_digest,
         timestamp: timestamp,
         revision: revision,
         dirty: dirty
       }}
    else
      {:error, :invalid_candidate_metadata}
    end
  end

  defp reusable_candidate(_candidate, _descriptor, _image_tag),
    do: {:error, :invalid_candidate_metadata}

  defp load_image(artifact, descriptor, root_dir) do
    image_tag = "#{@candidate_repository}:#{descriptor.control_plane_build_id}"
    timestamp = DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()
    provenance = source_provenance(root_dir)

    with {:ok, docker} <- docker_executable(),
         :ok <- docker_buildx_available(docker),
         :ok <-
           build_image(docker, artifact.context_dir, descriptor, image_tag, timestamp, provenance),
         {:ok, image_id} <- inspect_image_id(docker, image_tag),
         :ok <- verify_loaded_image(docker, image_tag, descriptor, timestamp, provenance),
         {:ok, static_asset_digest} <- static_asset_digest(docker, image_tag),
         {:ok, candidate_path} <-
           write_candidate(
             artifact,
             descriptor,
             image_tag,
             image_id,
             static_asset_digest,
             timestamp,
             provenance
           ) do
      {:ok,
       Map.merge(artifact, %{
         image_status: :loaded,
         image_tag: image_tag,
         image_id: image_id,
         static_asset_digest: static_asset_digest,
         candidate_path: candidate_path
       })}
    end
  end

  defp docker_executable do
    case System.find_executable("docker") do
      nil -> {:error, {:missing_tool, "docker"}}
      executable -> {:ok, executable}
    end
  end

  defp docker_buildx_available(docker) do
    case System.cmd(docker, ["buildx", "version"], stderr_to_stdout: true) do
      {_output, 0} -> :ok
      {_output, _status} -> {:error, :docker_buildx_unavailable}
    end
  end

  defp build_image(docker, context_dir, descriptor, image_tag, timestamp, provenance) do
    identity = descriptor.identity

    args = [
      "buildx",
      "build",
      "--load",
      "--platform",
      ControlPlaneInputs.target(),
      "--provenance=false",
      "--sbom=false",
      "--file",
      Path.join(context_dir, "rel/control_plane/Dockerfile"),
      "--tag",
      image_tag,
      "--build-arg",
      "FAVN_CONTROL_PLANE_BUILD_ID=#{descriptor.control_plane_build_id}",
      "--build-arg",
      "FAVN_SOURCE_REVISION=#{provenance.revision}",
      "--build-arg",
      "FAVN_BUILD_TIMESTAMP=#{timestamp}",
      "--build-arg",
      "FAVN_CONTROL_PLANE_VERSION=#{identity["control_plane_version"]}",
      "--build-arg",
      "FAVN_MANIFEST_SCHEMA_VERSION=#{identity["manifest_schema_version"]}",
      "--build-arg",
      "FAVN_RUNNER_CONTRACT_VERSION=#{identity["runner_contract_version"]}",
      context_dir
    ]

    case System.cmd(docker, args,
           into: IO.stream(:stdio, :line),
           stderr_to_stdout: true
         ) do
      {_stream, 0} -> :ok
      {_stream, status} -> {:error, {:control_plane_image_build_failed, status}}
    end
  end

  defp inspect_image_id(docker, image_tag) do
    case System.cmd(docker, ["image", "inspect", "--format", "{{.Id}}", image_tag],
           stderr_to_stdout: true
         ) do
      {output, 0} ->
        image_id = String.trim(output)

        if Regex.match?(@image_id, image_id),
          do: {:ok, image_id},
          else: {:error, :invalid_image_id}

      {_output, status} ->
        {:error, {:control_plane_image_inspection_failed, status}}
    end
  end

  defp verify_loaded_image(docker, image_tag, descriptor, timestamp, provenance) do
    identity = descriptor.identity

    expected = [
      {"{{.Os}}/{{.Architecture}}", ControlPlaneInputs.target()},
      {"{{ index .Config.Labels \"org.opencontainers.image.source\" }}",
       "https://github.com/eirhop/favn"},
      {"{{ index .Config.Labels \"org.opencontainers.image.revision\" }}", provenance.revision},
      {"{{ index .Config.Labels \"org.opencontainers.image.created\" }}", timestamp},
      {"{{ index .Config.Labels \"org.opencontainers.image.version\" }}",
       identity["control_plane_version"]},
      {"{{ index .Config.Labels \"io.favn.control-plane.build-id\" }}",
       descriptor.control_plane_build_id},
      {"{{ index .Config.Labels \"io.favn.manifest-schema-version\" }}",
       to_string(identity["manifest_schema_version"])},
      {"{{ index .Config.Labels \"io.favn.runner-contract-version\" }}",
       to_string(identity["runner_contract_version"])},
      {"{{ index .Config.Labels \"io.favn.elixir-version\" }}", identity["elixir_version"]},
      {"{{ index .Config.Labels \"io.favn.otp-version\" }}", identity["otp_version"]},
      {"{{ index .Config.Labels \"io.favn.target\" }}", ControlPlaneInputs.target()}
    ]

    Enum.reduce_while(expected, :ok, fn {format, expected_value}, :ok ->
      case inspect_field(docker, image_tag, format) do
        {:ok, ^expected_value} -> {:cont, :ok}
        _missing_or_mismatched -> {:halt, {:error, :control_plane_image_contract_mismatch}}
      end
    end)
  end

  defp inspect_field(docker, image_tag, format) do
    case System.cmd(docker, ["image", "inspect", "--format", format, image_tag],
           stderr_to_stdout: true
         ) do
      {output, 0} -> {:ok, String.trim(output)}
      {_output, status} -> {:error, {:control_plane_image_inspection_failed, status}}
    end
  end

  defp static_asset_digest(docker, image_tag) do
    script =
      "set -eu; file=$(find /app/lib -path '*/favn_view-*/priv/static/cache_manifest.json' -type f -print -quit); test -n \"$file\"; sha256sum \"$file\" | cut -d ' ' -f 1"

    case System.cmd(
           docker,
           ["run", "--rm", "--entrypoint", "/bin/sh", image_tag, "-c", script],
           stderr_to_stdout: true
         ) do
      {output, 0} ->
        digest = String.trim(output)
        if Regex.match?(@digest, digest), do: {:ok, digest}, else: {:error, :invalid_asset_digest}

      {_output, status} ->
        {:error, {:control_plane_asset_inspection_failed, status}}
    end
  end

  defp write_candidate(
         artifact,
         descriptor,
         image_tag,
         image_id,
         static_asset_digest,
         timestamp,
         provenance
       ) do
    path = candidate_path(artifact, descriptor)

    payload = %{
      "schema_version" => 1,
      "control_plane_build_id" => descriptor.control_plane_build_id,
      "image_repository" => @image_repository,
      "candidate_tag" => image_tag,
      "image_id" => image_id,
      "static_asset_digest" => static_asset_digest,
      "built_at" => timestamp,
      "source_revision" => provenance.revision,
      "source_dirty" => provenance.dirty,
      "target" => ControlPlaneInputs.target()
    }

    with :ok <- write_json_atomic(path, payload) do
      {:ok, path}
    end
  end

  defp candidate_path(artifact, descriptor) do
    build_root = Path.dirname(artifact.build_dir)
    Path.join(build_root, "candidate-#{descriptor.control_plane_build_id}.json")
  end

  defp source_provenance(root_dir) do
    revision =
      case System.cmd("git", ["-C", root_dir, "rev-parse", "--verify", "HEAD"],
             stderr_to_stdout: true
           ) do
        {output, 0} -> String.trim(output)
        _failure -> "unknown"
      end

    dirty =
      case System.cmd(
             "git",
             ["-C", root_dir, "status", "--porcelain", "--untracked-files=normal"],
             stderr_to_stdout: true
           ) do
        {"", 0} -> false
        {_output, 0} -> true
        _failure -> true
      end

    %{revision: revision, dirty: dirty}
  end

  defp write_json_atomic(path, payload) do
    temp = path <> ".tmp-#{System.unique_integer([:positive])}"

    with :ok <- Artifact.write_json(temp, payload),
         :ok <- File.rename(temp, path) do
      :ok
    else
      {:error, reason} ->
        _ = File.rm(temp)
        {:error, {:candidate_metadata_write_failed, reason}}
    end
  end

  defp artifact_result(final_dir, descriptor, status) do
    %{
      control_plane_build_id: descriptor.control_plane_build_id,
      status: status,
      build_dir: final_dir,
      context_dir: Path.join(final_dir, "context"),
      descriptor_path: Path.join(final_dir, "context/control-plane-build.json"),
      image_repository: @image_repository
    }
  end

  defp descriptor_payload(descriptor) do
    %{
      "schema_version" => descriptor.schema_version,
      "control_plane_build_id" => descriptor.control_plane_build_id,
      "identity" => descriptor.identity,
      "inputs" => descriptor.inputs
    }
  end

  defp json_value(value) do
    value
    |> Serializer.encode_canonical!()
    |> JSON.decode!()
  end
end
