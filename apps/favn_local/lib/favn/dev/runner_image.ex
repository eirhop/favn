defmodule Favn.Dev.RunnerImage do
  @moduledoc """
  Builds or selects and validates a customer-owned runner image.

  Local development builds the generated customer Dockerfile when no image is
  selected. An explicit image reference always bypasses the local build.
  """

  alias Favn.Dev.{Docker, Paths, State}
  alias Favn.Manifest.Compatibility
  alias Favn.RunnerRelease

  @default_dockerfile "deploy/runner/Dockerfile"

  @type result :: %{
          runner_release_id: String.t(),
          selected_reference: String.t(),
          image_reference: String.t(),
          image_id: String.t(),
          favn_version: String.t(),
          runner_contract_version: pos_integer(),
          target: String.t(),
          status: :built | :selected
        }

  @doc "Builds the default local runner or validates an explicitly selected image."
  @spec ensure(map(), keyword()) :: {:ok, result()} | {:error, term()}
  def ensure(project, opts \\ []) when is_map(project) and is_list(opts) do
    with {:ok, reference, status, expected_release_id} <- prepare_reference(project, opts),
         {:ok, image} <- Docker.inspect_image(reference, opts),
         {:ok, metadata} <- validate_image(image),
         :ok <- validate_expected_release_id(metadata.runner_release_id, expected_release_id),
         result = %{
           runner_release_id: metadata.runner_release_id,
           selected_reference: reference,
           image_reference: image.id,
           image_id: image.id,
           favn_version: metadata.favn_version,
           runner_contract_version: metadata.runner_contract_version,
           target: metadata.target,
           status: status
         },
         :ok <- persist(result, opts) do
      {:ok, result}
    end
  end

  defp prepare_reference(%{"runner_image" => reference}, _opts)
       when is_binary(reference) and reference != "",
       do: {:ok, reference, :selected, nil}

  defp prepare_reference(%{"project_name" => project_name}, opts)
       when is_binary(project_name) and project_name != "" do
    root_dir = opts |> Paths.root_dir() |> Path.expand()
    context = opts |> Keyword.get(:runner_build_context, root_dir) |> Path.expand(root_dir)
    dockerfile = Path.join(root_dir, @default_dockerfile)
    reference = "favn-local/#{project_name}-runner:dev"
    release_id = release_id(opts)

    with :ok <- RunnerRelease.validate_id(release_id),
         {:ok, project_root} <- project_root_in_context(root_dir, context),
         :ok <- validate_dockerfile(dockerfile, root_dir),
         :ok <- progress(opts, "Building customer runner #{reference}"),
         :ok <-
           Docker.build_runner(
             reference,
             dockerfile,
             context,
             project_root,
             release_id,
             opts
           ) do
      {:ok, reference, :built, release_id}
    end
  end

  defp prepare_reference(_project, _opts), do: {:error, :invalid_compose_project}

  defp validate_dockerfile(dockerfile, root_dir) do
    case File.lstat(dockerfile) do
      {:ok, %{type: :regular}} ->
        validate_dockerfile_parents(Path.dirname(dockerfile), root_dir, dockerfile)

      {:ok, _other} ->
        {:error, {:runner_dockerfile_not_regular, dockerfile}}

      {:error, :enoent} ->
        {:error, {:runner_dockerfile_missing, dockerfile}}

      {:error, reason} ->
        {:error, {:runner_dockerfile_unreadable, dockerfile, reason}}
    end
  end

  defp validate_dockerfile_parents(path, root_dir, dockerfile) do
    path
    |> Stream.iterate(&Path.dirname/1)
    |> Enum.take_while(fn candidate ->
      candidate == root_dir or String.starts_with?(candidate, root_dir <> "/")
    end)
    |> Enum.reduce_while(:ok, fn candidate, :ok ->
      case File.lstat(candidate) do
        {:ok, %{type: :directory}} -> {:cont, :ok}
        _invalid -> {:halt, {:error, {:runner_dockerfile_unsafe, dockerfile}}}
      end
    end)
  end

  defp project_root_in_context(root_dir, context) do
    relative = Path.relative_to(root_dir, context)

    if relative == "." or
         (Path.type(relative) == :relative and relative != ".." and
            not String.starts_with?(relative, "../")) do
      {:ok, String.replace(relative, "\\", "/")}
    else
      {:error, {:runner_project_outside_build_context, root_dir, context}}
    end
  end

  defp release_id(opts) do
    generator =
      Keyword.get(opts, :runner_release_id_fun, fn ->
        "rr_" <> (:crypto.strong_rand_bytes(32) |> Base.encode16(case: :lower))
      end)

    generator.()
  end

  defp progress(opts, message) do
    case Keyword.get(opts, :progress_fun) do
      fun when is_function(fun, 1) -> fun.(message)
      _missing -> :ok
    end
  end

  defp validate_expected_release_id(_actual, nil), do: :ok
  defp validate_expected_release_id(expected, expected), do: :ok

  defp validate_expected_release_id(actual, expected),
    do: {:error, {:runner_image_release_id_mismatch, %{expected: expected, actual: actual}}}

  defp validate_image(image) do
    release_id = image.labels["io.favn.runner-release-id"]
    favn_version = image.labels["io.favn.version"]
    contract = image.labels["io.favn.runner-contract-version"]
    target = image.labels["io.favn.target"]
    expected_contract = Compatibility.current_runner_contract_version()

    with true <-
           Regex.match?(~r/\Asha256:[0-9a-f]{64}\z/, image.id) ||
             {:error, {:runner_image_id_invalid, image.id}},
         true <-
           (image.os == "linux" and image.architecture == "amd64") ||
             {:error, {:unsupported_runner_image_target, image.os, image.architecture}},
         :ok <- RunnerRelease.validate_id(release_id),
         true <-
           favn_version == RunnerRelease.current_favn_version() ||
             {:error,
              {:runner_image_favn_version_mismatch,
               %{expected: RunnerRelease.current_favn_version(), actual: favn_version}}},
         {parsed_contract, ""} <- Integer.parse(contract || ""),
         true <-
           parsed_contract == expected_contract ||
             {:error,
              {:runner_image_contract_mismatch,
               %{expected: expected_contract, actual: parsed_contract}}},
         true <-
           target == RunnerRelease.current_target() ||
             {:error,
              {:runner_image_target_mismatch,
               %{expected: RunnerRelease.current_target(), actual: target}}} do
      {:ok,
       %{
         runner_release_id: release_id,
         favn_version: favn_version,
         runner_contract_version: parsed_contract,
         target: target
       }}
    else
      {:error, {:invalid_runner_release_field, :runner_release_id, :invalid_id}} ->
        {:error, {:runner_image_release_id_invalid, release_id}}

      :error ->
        {:error, {:runner_image_contract_invalid, contract}}

      {:error, _reason} = error ->
        error
    end
  end

  defp persist(result, opts) do
    State.write_runner_latest(
      %{
        "schema_version" => 1,
        "runner_release_id" => result.runner_release_id,
        "selected_reference" => result.selected_reference,
        "image_reference" => result.image_reference,
        "image_id" => result.image_id,
        "favn_version" => result.favn_version,
        "runner_contract_version" => result.runner_contract_version,
        "target" => result.target
      },
      opts
    )
  end
end
