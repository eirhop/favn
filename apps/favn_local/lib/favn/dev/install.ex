defmodule Favn.Dev.Install do
  @moduledoc """
  Installs the version-matched prebuilt control plane for local development.

  Installation verifies Docker Engine, resolves the official
  Favn version tag to one immutable GHCR RepoDigest, validates the image
  contract, and writes image-only project state. It never probes Compose,
  compiles the control plane, or accepts registry credentials.
  """

  alias Favn.Dev.{
    ControlPlaneImage,
    Docker,
    Lock,
    OutputRedactor,
    State
  }

  alias Favn.Dev.Maintainer.Candidate

  alias Favn.RunnerRelease

  @schema_version 5
  @legacy_schema_version 4

  @type opts :: keyword()

  @spec run(opts()) :: {:ok, :installed | :already_installed} | {:error, term()}
  def run(opts \\ []) when is_list(opts) do
    Lock.with_lock(opts, fn ->
      case do_run(opts) do
        {:ok, status} ->
          {:ok, status}

        {:error, reason} = error ->
          _ = record_failure(reason, opts)
          error
      end
    end)
  end

  @spec ensure_ready(opts()) :: :ok | {:error, term()}
  def ensure_ready(opts \\ []) when is_list(opts) do
    with {:ok, _probe} <- Docker.probe_engine(opts) do
      ensure_installed(opts)
    end
  end

  @doc false
  @spec select_maintainer(Candidate.t(), opts()) :: :ok | {:error, term()}
  def select_maintainer(%Candidate{} = candidate, opts \\ []) when is_list(opts) do
    with :ok <- maintainer_environment(opts),
         {:ok, _probe} <- Docker.probe_engine(opts),
         {:ok, image} <- Docker.inspect_image(candidate.image_id, opts),
         true <- image.id == candidate.image_id,
         {:ok, metadata} <- ControlPlaneImage.validate_install_image(image),
         true <- metadata.control_plane_build_id == candidate.control_plane_build_id,
         install <- maintainer_install_state(candidate, metadata, opts),
         :ok <- State.write_install(install, opts) do
      :ok
    else
      false -> {:error, :maintainer_control_plane_identity_mismatch}
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_maintainer_candidate}
    end
  end

  defp ensure_installed(opts) do
    case State.read_install(opts) do
      {:ok, install} ->
        case validate_existing(install, current_favn_version(opts), opts) do
          :ok -> :ok
          {:error, _reason} -> {:error, :install_stale}
        end

      {:error, :not_found} ->
        {:error, :install_required}

      {:error, _reason} ->
        {:error, :install_stale}
    end
  end

  defp do_run(opts) do
    with :ok <- State.ensure_layout(opts),
         {:ok, _probe} <- Docker.probe_engine(opts),
         version <- current_favn_version(opts),
         {:ok, decision} <- install_decision(version, opts) do
      case decision do
        :already_installed -> {:ok, :already_installed}
        {:install, previous} -> install(version, previous, opts)
      end
    end
  end

  defp install_decision(version, opts) do
    previous =
      case State.read_install(opts) do
        {:ok, install} -> install
        {:error, _reason} -> nil
      end

    if Keyword.get(opts, :force, false) do
      {:ok, {:install, previous}}
    else
      case previous do
        %{"schema_version" => @legacy_schema_version} ->
          {:ok, {:install, previous}}

        %{"source" => "maintainer"} ->
          {:ok, {:install, previous}}

        %{} = install ->
          case validate_existing(install, version, opts) do
            :ok -> {:ok, :already_installed}
            {:error, _reason} -> {:ok, {:install, previous}}
          end

        nil ->
          {:ok, {:install, nil}}
      end
    end
  end

  defp install(version, previous, opts) do
    with {:ok, resolution} <- resolve_image(version, previous, opts),
         install = install_state(version, resolution),
         :ok <- State.write_install(install, opts) do
      {:ok, :installed}
    end
  end

  defp resolve_image(version, previous, opts) do
    case Keyword.get(opts, :candidate_control_plane) do
      nil -> resolve_official(version, previous, opts)
      candidate -> resolve_candidate(candidate, opts)
    end
  end

  defp resolve_official(version, previous, opts) do
    with {:ok, version_tag} <- ControlPlaneImage.version_tag(version) do
      tagged_reference = ControlPlaneImage.repository() <> ":" <> version_tag

      case Docker.pull(tagged_reference, opts) do
        :ok ->
          inspect_official(tagged_reference, version_tag, opts)

        {:error, {:control_plane_pull_failed, _status, _output}} = error ->
          offline_reuse(previous, version, error, opts)

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp inspect_official(tagged_reference, version_tag, opts) do
    with {:ok, tagged_image} <- Docker.inspect_image(tagged_reference, opts),
         {:ok, metadata} <- ControlPlaneImage.validate_install_image(tagged_image),
         {:ok, immutable_reference} <- ControlPlaneImage.repo_digest(tagged_image.repo_digests),
         {:ok, immutable_image} <- Docker.inspect_image(immutable_reference, opts),
         true <- immutable_image.id == tagged_image.id,
         {:ok, ^metadata} <- ControlPlaneImage.validate_install_image(immutable_image) do
      {:ok,
       metadata
       |> stringify_metadata()
       |> Map.merge(%{
         "source" => "official",
         "lookup_reference" => tagged_reference,
         "version_tag" => version_tag,
         "image_reference" => immutable_reference,
         "image_id" => tagged_image.id
       })}
    else
      false -> {:error, :control_plane_digest_identity_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp offline_reuse(previous, version, pull_error, opts) do
    case previous do
      %{"source" => "official"} = install ->
        case validate_reusable(install, version, opts) do
          :ok ->
            {:ok,
             install
             |> Map.take([
               "source",
               "lookup_reference",
               "version_tag",
               "image_reference",
               "image_id",
               "control_plane_build_id",
               "control_plane_version",
               "manifest_schema_version",
               "runner_contract_version",
               "target"
             ])
             |> Map.put("offline_reuse", true)}

          {:error, _reason} ->
            pull_error
        end

      _missing_or_maintainer ->
        pull_error
    end
  end

  defp resolve_candidate(candidate, opts) do
    if Mix.env() == :test do
      with %{"reference" => reference, "image_id" => expected_id} <- candidate,
           true <- is_binary(reference) and is_binary(expected_id),
           {:ok, image} <- Docker.inspect_image(reference, opts),
           true <- image.id == expected_id,
           {:ok, metadata} <- ControlPlaneImage.validate_install_image(image) do
        {:ok,
         metadata
         |> stringify_metadata()
         |> Map.merge(%{
           "source" => "candidate",
           "lookup_reference" => reference,
           "image_reference" => expected_id,
           "image_id" => expected_id
         })}
      else
        false -> {:error, :candidate_control_plane_identity_mismatch}
        _invalid -> {:error, :invalid_candidate_control_plane}
      end
    else
      {:error, :candidate_control_plane_not_allowed}
    end
  end

  defp validate_existing(
         %{
           "schema_version" => @schema_version,
           "favn_version" => version,
           "image_reference" => reference,
           "image_id" => image_id
         } = install,
         version,
         opts
       )
       when is_binary(reference) and is_binary(image_id) do
    with :ok <- validate_source(install, opts),
         {:ok, image} <- Docker.inspect_image(reference, opts),
         true <- image.id == image_id,
         {:ok, metadata} <- ControlPlaneImage.validate_install_image(image),
         true <- compatible_metadata?(install, metadata) do
      :ok
    else
      _invalid -> {:error, :install_stale}
    end
  end

  defp validate_existing(_install, _version, _opts), do: {:error, :install_stale}

  defp validate_reusable(
         %{
           "schema_version" => schema_version,
           "favn_version" => version,
           "image_reference" => reference,
           "image_id" => image_id
         } = install,
         version,
         opts
       )
       when schema_version in [@legacy_schema_version, @schema_version] and
              is_binary(reference) and is_binary(image_id) do
    with :ok <- validate_source(install, opts),
         {:ok, image} <- Docker.inspect_image(reference, opts),
         true <- image.id == image_id,
         {:ok, metadata} <- ControlPlaneImage.validate_install_image(image),
         true <- compatible_metadata?(install, metadata) do
      :ok
    else
      _invalid -> {:error, :install_stale}
    end
  end

  defp validate_reusable(_install, _version, _opts), do: {:error, :install_stale}

  defp validate_source(%{"source" => "official", "image_reference" => reference}, _opts) do
    prefix = ControlPlaneImage.repository() <> "@sha256:"

    if String.starts_with?(reference, prefix),
      do: :ok,
      else: {:error, :invalid_official_reference}
  end

  defp validate_source(%{"source" => "candidate"}, _opts) do
    if Mix.env() == :test, do: :ok, else: {:error, :candidate_control_plane_not_allowed}
  end

  defp validate_source(
         %{
           "source" => "maintainer",
           "image_reference" => image_reference,
           "checkout" => checkout,
           "checkout_revision" => revision,
           "checkout_dirty" => dirty
         },
         opts
       )
       when is_binary(image_reference) and is_binary(checkout) and is_binary(revision) and
              is_boolean(dirty) do
    if Regex.match?(~r/\Asha256:[0-9a-f]{64}\z/, image_reference),
      do: maintainer_environment(opts),
      else: {:error, :invalid_maintainer_control_plane}
  end

  defp validate_source(_install, _opts), do: {:error, :invalid_control_plane_install_source}

  defp compatible_metadata?(install, metadata) do
    install["control_plane_build_id"] == metadata.control_plane_build_id and
      install["control_plane_version"] == metadata.control_plane_version and
      install["manifest_schema_version"] == metadata.manifest_schema_version and
      install["runner_contract_version"] == metadata.runner_contract_version and
      install["target"] == metadata.target
  end

  defp install_state(version, resolution) do
    resolution
    |> Map.merge(%{
      "schema_version" => @schema_version,
      "favn_version" => version,
      "installed_at" => DateTime.utc_now() |> DateTime.to_iso8601()
    })
  end

  defp maintainer_install_state(candidate, metadata, opts) do
    resolution =
      metadata
      |> stringify_metadata()
      |> Map.merge(%{
        "source" => "maintainer",
        "lookup_reference" => candidate.image_tag,
        "image_reference" => candidate.image_id,
        "image_id" => candidate.image_id,
        "candidate_path" => candidate.candidate_path,
        "image_source_revision" => candidate.image_source_revision,
        "image_source_dirty" => candidate.image_source_dirty,
        "checkout" => candidate.checkout,
        "checkout_revision" => candidate.checkout_revision,
        "checkout_dirty" => candidate.checkout_dirty
      })

    install_state(current_favn_version(opts), resolution)
  end

  defp maintainer_environment(opts) do
    cond do
      Mix.env() == :dev -> :ok
      Mix.env() == :test and Keyword.get(opts, :allow_maintainer_install, false) -> :ok
      true -> {:error, {:maintainer_environment_forbidden, Mix.env()}}
    end
  end

  defp stringify_metadata(metadata) do
    %{
      "control_plane_build_id" => metadata.control_plane_build_id,
      "control_plane_version" => metadata.control_plane_version,
      "manifest_schema_version" => metadata.manifest_schema_version,
      "runner_contract_version" => metadata.runner_contract_version,
      "target" => metadata.target
    }
  end

  defp current_favn_version(opts) do
    Keyword.get(opts, :favn_version, RunnerRelease.current_favn_version())
  end

  defp record_failure(reason, opts) do
    State.write_last_failure(
      %{
        "command" => "install",
        "error" => reason |> OutputRedactor.redact_term(opts) |> inspect(),
        "at" => DateTime.utc_now() |> DateTime.to_iso8601()
      },
      opts
    )
  end
end
