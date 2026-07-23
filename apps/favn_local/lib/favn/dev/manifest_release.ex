defmodule Favn.Dev.ManifestRelease do
  @moduledoc """
  Builds the local manifest for the prepared runner release ID.

  This operation compiles authored definitions only. Runner image preparation
  remains a separate customer-Dockerfile boundary.
  """

  alias Favn.Dev.{Command, OutputRedactor, Paths, State}
  alias Favn.RunnerRelease

  @type result :: %{
          manifest_version_id: String.t(),
          required_runner_release_id: String.t(),
          dist_dir: Path.t(),
          manifest_path: Path.t()
        }

  @spec ensure(String.t(), keyword()) :: {:ok, result()} | {:error, term()}
  def ensure(runner_release_id, opts \\ [])
      when is_binary(runner_release_id) and is_list(opts) do
    with :ok <- RunnerRelease.validate_id(runner_release_id),
         :ok <- build(runner_release_id, opts),
         {:ok, latest} <- State.read_manifest_latest(opts),
         {:ok, result} <- validate_latest(latest, runner_release_id, opts) do
      {:ok, result}
    end
  end

  defp build(runner_release_id, opts) do
    case Keyword.get(opts, :manifest_build_fun) do
      fun when is_function(fun, 2) ->
        if Mix.env() == :test do
          case fun.(runner_release_id, opts) do
            :ok -> :ok
            {:error, _reason} = error -> error
            _invalid -> {:error, :invalid_manifest_build_result}
          end
        else
          run_build_command(runner_release_id, opts)
        end

      _other ->
        run_build_command(runner_release_id, opts)
    end
  end

  defp run_build_command(runner_release_id, opts) do
    with mix when is_binary(mix) <- System.find_executable("mix") do
      root_dir = opts |> Paths.root_dir() |> Path.expand()
      sink = Keyword.get(opts, :progress_fun, fn _chunk -> :ok end)
      {output_writer, flush_output} = OutputRedactor.stream_writer(opts, sink)

      {output, status} =
        try do
          Command.run(
            mix,
            [
              "favn.build.manifest",
              "--root-dir",
              root_dir,
              "--runner-release-id",
              runner_release_id
            ],
            cd: root_dir,
            env: [{"MIX_ENV", "prod"}],
            stderr_to_stdout: true,
            timeout_ms: Keyword.get(opts, :manifest_build_timeout_ms, 1_200_000),
            output_writer: output_writer
          )
        after
          flush_output.()
        end

      if status == 0 do
        :ok
      else
        {:error,
         {:manifest_build_failed, status,
          output |> OutputRedactor.redact(opts) |> String.trim() |> String.slice(-8_192, 8_192)}}
      end
    else
      _missing -> {:error, {:missing_tool, "mix"}}
    end
  end

  defp validate_latest(
         %{
           "schema_version" => 1,
           "manifest_version_id" => manifest_version_id,
           "required_runner_release_id" => runner_release_id,
           "dist_dir" => dist_dir,
           "manifest_path" => manifest_path
         },
         runner_release_id,
         opts
       )
       when is_binary(manifest_version_id) and is_binary(dist_dir) and is_binary(manifest_path) do
    root_dir = opts |> Paths.root_dir() |> Path.expand()

    if Path.expand(dist_dir) == Paths.dist_manifest_dir(root_dir, manifest_version_id) and
         Path.expand(manifest_path) == Path.join(dist_dir, "manifest-index.json") and
         File.regular?(manifest_path) do
      {:ok,
       %{
         manifest_version_id: manifest_version_id,
         required_runner_release_id: runner_release_id,
         dist_dir: dist_dir,
         manifest_path: manifest_path
       }}
    else
      {:error, :invalid_manifest_latest_state}
    end
  end

  defp validate_latest(_latest, _runner_release_id, _opts),
    do: {:error, :invalid_manifest_latest_state}
end
