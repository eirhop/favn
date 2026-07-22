defmodule Favn.Dev.RunnerImage do
  @moduledoc """
  Builds and verifies the project-local customer runner image.

  The customer release context is always produced by `mix favn.build.runner`
  under `MIX_ENV=prod`. Docker images are cached by the logical runner release
  ID, then inspected before Compose is allowed to select them.
  """

  alias Favn.Dev.Build.Manifest
  alias Favn.Dev.{Command, ComposeProject, Docker, OutputRedactor, Paths, State}

  @release_id_pattern ~r/\Arr_[0-9a-f]{64}\z/

  @type result :: %{
          runner_release_id: String.t(),
          image_reference: String.t(),
          image_id: String.t(),
          dist_dir: Path.t(),
          descriptor_path: Path.t(),
          manifest_dir: Path.t(),
          manifest_version_id: String.t(),
          status: :built | :cached
        }

  @doc "Builds or reuses the exact local runner image and selects it in Compose."
  @spec ensure(map(), keyword()) :: {:ok, result()} | {:error, term()}
  def ensure(project, opts \\ []) when is_map(project) and is_list(opts) do
    with :ok <- build_release(opts),
         {:ok, latest} <- read_latest(opts),
         image_reference <- image_reference(project["project_name"], latest.runner_release_id),
         {:ok, image, status} <- ensure_image(image_reference, latest, opts),
         :ok <- ComposeProject.put_runner_image(project, image_reference),
         result <-
           Map.merge(latest, %{
             image_reference: image_reference,
             image_id: image.id,
             status: status
           }),
         :ok <- persist_image(result, opts) do
      {:ok, result}
    end
  end

  @doc "Returns the project-scoped immutable local tag for one runner release."
  @spec image_reference(String.t(), String.t()) :: String.t()
  def image_reference(project_name, release_id)
      when is_binary(project_name) and project_name != "" and is_binary(release_id),
      do: "favn-local-runner-#{project_name}:#{release_id}"

  defp build_release(opts) do
    case Keyword.get(opts, :runner_build_fun) do
      fun when is_function(fun, 1) ->
        if Mix.env() == :test do
          case fun.(opts) do
            :ok -> :ok
            {:ok, _result} -> :ok
            {:error, _reason} = error -> error
            _invalid -> {:error, :invalid_runner_build_result}
          end
        else
          {:error, :runner_build_injection_not_allowed}
        end

      _other ->
        with {:ok, command_runner} <- command_runner(opts),
             mix when is_binary(mix) <- System.find_executable("mix") do
          root_dir = opts |> Paths.root_dir() |> Path.expand()
          sink = Keyword.get(opts, :progress_fun, fn _chunk -> :ok end)
          {output_writer, flush_output} = OutputRedactor.stream_writer(opts, sink)

          {output, status} =
            try do
              command_runner.(
                mix,
                ["favn.build.runner", "--root-dir", root_dir],
                cd: root_dir,
                env: [{"MIX_ENV", "prod"}],
                stderr_to_stdout: true,
                timeout_ms: Keyword.get(opts, :runner_release_build_timeout_ms, 1_200_000),
                output_writer: output_writer
              )
            after
              flush_output.()
            end

          output = OutputRedactor.redact(output, opts)

          if status == 0,
            do: :ok,
            else: {:error, {:runner_release_build_failed, status, bounded(output)}}
        else
          {:error, _reason} = error -> error
          _missing -> {:error, {:missing_tool, "mix"}}
        end
    end
  end

  defp command_runner(opts) do
    case Keyword.get(opts, :runner_command_runner) do
      fun when is_function(fun, 3) ->
        if Mix.env() == :test,
          do: {:ok, fun},
          else: {:error, :runner_command_injection_not_allowed}

      _other ->
        {:ok, &Command.run/3}
    end
  end

  defp read_latest(opts) do
    root_dir = opts |> Paths.root_dir() |> Path.expand()

    with {:ok,
          %{
            "schema_version" => 1,
            "runner_release_id" => release_id,
            "dist_dir" => dist_dir,
            "descriptor_path" => descriptor_path,
            "manifest_dir" => manifest_dir,
            "manifest_version_id" => manifest_version_id
          }} <- State.read_runner_latest(opts),
         true <- Regex.match?(@release_id_pattern, release_id),
         true <- Path.expand(dist_dir) == Paths.dist_runner_dir(root_dir, release_id),
         true <- Path.expand(descriptor_path) == Path.join(dist_dir, "runner-release.json"),
         true <-
           Path.expand(manifest_dir) == Paths.dist_manifest_dir(root_dir, manifest_version_id),
         {:ok, descriptor} <- Manifest.read_descriptor(descriptor_path),
         true <- descriptor.runner_release_id == release_id,
         true <- File.regular?(Path.join(manifest_dir, "manifest-index.json")) do
      {:ok,
       %{
         runner_release_id: release_id,
         dist_dir: dist_dir,
         descriptor_path: descriptor_path,
         manifest_dir: manifest_dir,
         manifest_version_id: manifest_version_id,
         favn_version: descriptor.favn_version,
         runner_contract_version: descriptor.runner_contract_version,
         target: descriptor.target
       }}
    else
      _invalid -> {:error, :invalid_runner_latest_state}
    end
  end

  defp ensure_image(reference, latest, opts) do
    case Docker.inspect_image(reference, opts) do
      {:ok, image} ->
        with :ok <- validate_image(image, latest) do
          {:ok, image, :cached}
        end

      {:error, {:docker_image_unavailable, ^reference}} ->
        with :ok <- Docker.build_image(reference, latest.dist_dir, opts),
             {:ok, image} <- Docker.inspect_image(reference, opts),
             :ok <- validate_image(image, latest) do
          {:ok, image, :built}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp validate_image(image, latest) do
    cond do
      image.os != "linux" or image.architecture != "amd64" ->
        {:error, {:unsupported_runner_image_target, image.os, image.architecture}}

      image.user != "10001:10001" ->
        {:error, {:invalid_runner_image_user, image.user}}

      image.labels["io.favn.runner-release-id"] != latest.runner_release_id ->
        {:error, :runner_image_release_mismatch}

      image.labels["io.favn.version"] != latest.favn_version ->
        {:error, :runner_image_favn_version_mismatch}

      image.labels["io.favn.runner-contract-version"] !=
          Integer.to_string(latest.runner_contract_version) ->
        {:error, :runner_image_contract_mismatch}

      image.labels["io.favn.target"] != latest.target ->
        {:error, :runner_image_target_mismatch}

      true ->
        :ok
    end
  end

  defp persist_image(result, opts) do
    State.write_runner_latest(
      %{
        "schema_version" => 1,
        "runner_release_id" => result.runner_release_id,
        "dist_dir" => result.dist_dir,
        "descriptor_path" => result.descriptor_path,
        "manifest_dir" => result.manifest_dir,
        "manifest_version_id" => result.manifest_version_id,
        "image_reference" => result.image_reference,
        "image_id" => result.image_id
      },
      opts
    )
  end

  defp bounded(output) when is_binary(output),
    do: output |> String.trim() |> String.slice(-8_192, 8_192)

  defp bounded(output), do: inspect(output, limit: 20, printable_limit: 1_024)
end
