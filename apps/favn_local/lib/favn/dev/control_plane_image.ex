defmodule Favn.Dev.ControlPlaneImage do
  @moduledoc """
  Validation helpers for official control-plane registry references.

  Tags are lookup aliases only. Runtime state always records and consumes a
  repository-qualified SHA-256 digest reference.
  """

  @repository "ghcr.io/eirhop/favn-control-plane"
  @digest ~r/\Asha256:[0-9a-f]{64}\z/
  @build_id ~r/\A[0-9a-f]{64}\z/
  @version_tag ~r/\Av[0-9A-Za-z_.-]{1,127}\z/
  @image_id ~r/\Asha256:[0-9a-f]{64}\z/

  @doc "Returns the canonical official image repository."
  @spec repository() :: String.t()
  def repository, do: @repository

  @doc "Returns the immutable build lookup tag for one control-plane input ID."
  @spec build_tag(String.t()) :: {:ok, String.t()} | {:error, :invalid_control_plane_build_id}
  def build_tag(build_id) when is_binary(build_id) do
    if Regex.match?(@build_id, build_id) do
      {:ok, "build-" <> build_id}
    else
      {:error, :invalid_control_plane_build_id}
    end
  end

  def build_tag(_build_id), do: {:error, :invalid_control_plane_build_id}

  @doc "Returns the immutable Favn release lookup tag for a valid semantic version."
  @spec version_tag(String.t()) :: {:ok, String.t()} | {:error, :invalid_favn_version}
  def version_tag(version) when is_binary(version) do
    tag = "v" <> version

    case Version.parse(version) do
      {:ok, _parsed} when byte_size(tag) <= 128 ->
        if Regex.match?(@version_tag, tag),
          do: {:ok, tag},
          else: {:error, :invalid_favn_version}

      _invalid ->
        {:error, :invalid_favn_version}
    end
  end

  def version_tag(_version), do: {:error, :invalid_favn_version}

  @doc "Builds the only supported deployment reference shape."
  @spec immutable_reference(String.t()) :: {:ok, String.t()} | {:error, :invalid_image_digest}
  def immutable_reference(digest) when is_binary(digest) do
    if Regex.match?(@digest, digest) do
      {:ok, @repository <> "@" <> digest}
    else
      {:error, :invalid_image_digest}
    end
  end

  def immutable_reference(_digest), do: {:error, :invalid_image_digest}

  @doc "Selects the exact official RepoDigest from Docker inspection output."
  @spec repo_digest([String.t()]) :: {:ok, String.t()} | {:error, :repo_digest_unavailable}
  def repo_digest(repo_digests) when is_list(repo_digests) do
    matches =
      Enum.filter(repo_digests, fn value ->
        case String.split(value, "@", parts: 2) do
          [@repository, digest] -> Regex.match?(@digest, digest)
          _other -> false
        end
      end)

    case Enum.uniq(matches) do
      [reference] -> {:ok, reference}
      _missing_or_ambiguous -> {:error, :repo_digest_unavailable}
    end
  end

  def repo_digest(_repo_digests), do: {:error, :repo_digest_unavailable}

  @doc "Validates the installed image's platform, user, and Favn compatibility labels."
  @spec validate_install_image(map()) :: {:ok, map()} | {:error, term()}
  def validate_install_image(%{
        id: id,
        architecture: "amd64",
        os: "linux",
        user: "10001:10001",
        labels: labels
      })
      when is_binary(id) and is_map(labels) do
    manifest_schema =
      Favn.Manifest.Compatibility.current_schema_version() |> Integer.to_string()

    runner_contract =
      Favn.Manifest.Compatibility.current_runner_contract_version() |> Integer.to_string()

    build_id = labels["io.favn.control-plane.build-id"]
    version = labels["org.opencontainers.image.version"]

    with true <- Regex.match?(@image_id, id),
         true <- is_binary(build_id) and Regex.match?(@build_id, build_id),
         {:ok, _version} <- Version.parse(version),
         ^manifest_schema <- labels["io.favn.manifest-schema-version"],
         ^runner_contract <- labels["io.favn.runner-contract-version"],
         "linux/amd64" <- labels["io.favn.target"] do
      {:ok,
       %{
         image_id: id,
         control_plane_build_id: build_id,
         control_plane_version: version,
         manifest_schema_version: String.to_integer(manifest_schema),
         runner_contract_version: String.to_integer(runner_contract),
         target: "linux/amd64"
       }}
    else
      _invalid -> {:error, :incompatible_control_plane_image}
    end
  end

  def validate_install_image(_image), do: {:error, :incompatible_control_plane_image}
end
