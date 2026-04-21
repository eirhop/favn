defmodule Favn.Dev.Build.Single do
  @moduledoc """
  Project-local single-node assembly target.
  """

  alias Favn.Dev.Build.Orchestrator
  alias Favn.Dev.Build.Runner
  alias Favn.Dev.Build.Web
  alias Favn.Dev.Install
  alias Favn.Dev.Paths
  alias Favn.Dev.State

  @schema_version 1
  @target "single"

  @type root_opt :: [root_dir: Path.t()]

  @spec run(root_opt()) :: {:ok, map()} | {:error, term()}
  def run(opts \\ []) when is_list(opts) do
    with :ok <- Install.ensure_ready(opts),
         :ok <- State.ensure_layout(opts),
         {:ok, web} <- Web.run(opts),
         {:ok, orchestrator} <- Orchestrator.run(opts),
         {:ok, runner} <- Runner.run(opts),
         storage <- storage_mode(opts),
         :ok <- validate_storage(storage),
         {build_id, root_dir} <- {build_id(), Paths.root_dir(opts)},
         build_dir <- Paths.build_single_dir(root_dir, build_id),
         dist_dir <- Paths.dist_single_dir(root_dir, build_id),
         :ok <- File.mkdir_p(build_dir),
         :ok <- File.mkdir_p(Path.join(dist_dir, "web")),
         :ok <- File.mkdir_p(Path.join(dist_dir, "orchestrator")),
         :ok <- File.mkdir_p(Path.join(dist_dir, "runner")),
         :ok <- File.mkdir_p(Path.join(dist_dir, "config")),
         :ok <- File.mkdir_p(Path.join(dist_dir, "env")),
         :ok <- File.mkdir_p(Path.join(dist_dir, "bin")),
         :ok <- copy_target_outputs(web.dist_dir, Path.join(dist_dir, "web")),
         :ok <- copy_target_outputs(orchestrator.dist_dir, Path.join(dist_dir, "orchestrator")),
         :ok <- copy_target_outputs(runner.dist_dir, Path.join(dist_dir, "runner")),
         assembly <- assembly_json(build_id, web, orchestrator, runner, storage),
         :ok <-
           write_json(Path.join(build_dir, "build.json"), build_json(build_id, assembly, opts)),
         :ok <-
           write_json(
             Path.join(dist_dir, "metadata.json"),
             metadata_json(build_id, assembly, opts)
           ),
         :ok <- write_json(Path.join(dist_dir, "config/assembly.json"), assembly),
         :ok <- write_env_files(dist_dir, storage),
         :ok <- write_scripts(dist_dir),
         :ok <- write_operator_notes(dist_dir) do
      {:ok, %{build_id: build_id, build_dir: build_dir, dist_dir: dist_dir}}
    end
  end

  defp storage_mode(opts) do
    case Keyword.get(opts, :storage, :sqlite) do
      value when is_binary(value) -> String.to_atom(value)
      value -> value
    end
  end

  defp validate_storage(:sqlite), do: :ok
  defp validate_storage(:postgres), do: :ok
  defp validate_storage(other), do: {:error, {:invalid_storage, other}}

  defp copy_target_outputs(source_dir, target_dir) do
    case File.ls(source_dir) do
      {:ok, entries} ->
        Enum.reduce_while(entries, :ok, fn entry, :ok ->
          source = Path.join(source_dir, entry)
          destination = Path.join(target_dir, entry)

          case copy_entry(source, destination) do
            :ok -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)

      {:error, reason} ->
        {:error, {:read_dist_failed, source_dir, reason}}
    end
  end

  defp copy_entry(source, destination) do
    case File.stat(source) do
      {:ok, %{type: :directory}} ->
        case File.cp_r(source, destination) do
          {:ok, _} -> :ok
          {:error, reason, _} -> {:error, {:copy_failed, source, reason}}
        end

      {:ok, _} ->
        File.cp(source, destination)

      {:error, reason} ->
        {:error, {:copy_failed, source, reason}}
    end
  end

  defp assembly_json(build_id, web, orchestrator, runner, storage) do
    %{
      "schema_version" => @schema_version,
      "target" => @target,
      "build_id" => build_id,
      "assembled_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "storage" => %{"mode" => Atom.to_string(storage)},
      "services" => %{
        "web" => %{"build_id" => web.build_id, "bundle_dir" => "web"},
        "orchestrator" => %{"build_id" => orchestrator.build_id, "bundle_dir" => "orchestrator"},
        "runner" => %{"build_id" => runner.build_id, "bundle_dir" => "runner"}
      }
    }
  end

  defp build_json(build_id, assembly, opts) do
    base(build_id, opts)
    |> Map.put("phase", "build")
    |> Map.put("target", @target)
    |> Map.put("assembly", assembly)
  end

  defp metadata_json(build_id, assembly, opts) do
    base(build_id, opts)
    |> Map.put("phase", "dist")
    |> Map.put("target", @target)
    |> Map.put("assembly", assembly)
    |> Map.put("artifact", %{
      "kind" => "assembly_bundle",
      "operational" => false,
      "truthfulness" => "topology_assembly_only"
    })
    |> Map.put("topology", %{"boundary" => "web+orchestrator+runner", "collapsed" => false})
    |> Map.put("compatibility", %{
      "topology" => "web+orchestrator+runner",
      "storage_modes" => ["sqlite", "postgres"]
    })
    |> Map.put("required_env", [
      "FAVN_ORCHESTRATOR_BASE_URL",
      "FAVN_ORCHESTRATOR_SERVICE_TOKEN",
      "FAVN_WEB_SESSION_SECRET",
      "FAVN_STORAGE",
      "FAVN_SQLITE_PATH",
      "FAVN_POSTGRES_HOST",
      "FAVN_POSTGRES_PORT",
      "FAVN_POSTGRES_USERNAME",
      "FAVN_POSTGRES_PASSWORD",
      "FAVN_POSTGRES_DATABASE",
      "FAVN_POSTGRES_SSL"
    ])
  end

  defp base(build_id, opts) do
    %{
      "schema_version" => @schema_version,
      "build_id" => build_id,
      "built_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "favn_version" => to_string(Application.spec(:favn, :vsn) || "unknown"),
      "install_fingerprint" => read_install_fingerprint(opts),
      "elixir_version" => System.version(),
      "otp_release" => :erlang.system_info(:otp_release) |> List.to_string()
    }
  end

  defp read_install_fingerprint(opts) do
    case State.read_install(opts) do
      {:ok, %{"fingerprint" => fingerprint}} when is_map(fingerprint) -> fingerprint
      _ -> %{}
    end
  end

  defp write_env_files(dist_dir, :sqlite) do
    orchestrator = ["FAVN_STORAGE=sqlite", "FAVN_SQLITE_PATH=.favn/data/orchestrator.sqlite3", ""]

    web = [
      "FAVN_ORCHESTRATOR_BASE_URL=http://127.0.0.1:4101",
      "FAVN_ORCHESTRATOR_SERVICE_TOKEN=replace-me",
      "FAVN_WEB_SESSION_SECRET=replace-me",
      ""
    ]

    runner = [
      "FAVN_ORCHESTRATOR_BASE_URL=http://127.0.0.1:4101",
      "FAVN_ORCHESTRATOR_SERVICE_TOKEN=replace-me",
      ""
    ]

    write_env_bundle(dist_dir, web, orchestrator, runner)
  end

  defp write_env_files(dist_dir, :postgres) do
    orchestrator = [
      "FAVN_STORAGE=postgres",
      "FAVN_POSTGRES_HOST=127.0.0.1",
      "FAVN_POSTGRES_PORT=5432",
      "FAVN_POSTGRES_USERNAME=postgres",
      "FAVN_POSTGRES_PASSWORD=postgres",
      "FAVN_POSTGRES_DATABASE=favn",
      "FAVN_POSTGRES_SSL=false",
      ""
    ]

    web = [
      "FAVN_ORCHESTRATOR_BASE_URL=http://127.0.0.1:4101",
      "FAVN_ORCHESTRATOR_SERVICE_TOKEN=replace-me",
      "FAVN_WEB_SESSION_SECRET=replace-me",
      ""
    ]

    runner = [
      "FAVN_ORCHESTRATOR_BASE_URL=http://127.0.0.1:4101",
      "FAVN_ORCHESTRATOR_SERVICE_TOKEN=replace-me",
      ""
    ]

    write_env_bundle(dist_dir, web, orchestrator, runner)
  end

  defp write_env_bundle(dist_dir, web, orchestrator, runner) do
    [
      File.write(Path.join(dist_dir, "env/web.env"), Enum.join(web, "\n")),
      File.write(Path.join(dist_dir, "env/orchestrator.env"), Enum.join(orchestrator, "\n")),
      File.write(Path.join(dist_dir, "env/runner.env"), Enum.join(runner, "\n"))
    ]
    |> run_steps()
  end

  defp write_scripts(dist_dir) do
    start_script = [
      "#!/usr/bin/env sh",
      "set -eu",
      "echo \"Favn single bundle in this Phase 9 build is assembly-only.\"",
      "echo \"No operational runtime launch wiring is bundled yet.\"",
      "echo \"See OPERATOR_NOTES.md and env/*.env for required deployment inputs.\"",
      "exit 1",
      ""
    ]

    stop_script = [
      "#!/usr/bin/env sh",
      "set -eu",
      "echo \"No managed processes were started by this assembly-only artifact.\"",
      "exit 1",
      ""
    ]

    [
      File.write(Path.join(dist_dir, "bin/start"), Enum.join(start_script, "\n")),
      File.write(Path.join(dist_dir, "bin/stop"), Enum.join(stop_script, "\n")),
      File.chmod(Path.join(dist_dir, "bin/start"), 0o755),
      File.chmod(Path.join(dist_dir, "bin/stop"), 0o755)
    ]
    |> run_steps()
  end

  defp run_steps(steps) when is_list(steps) do
    Enum.reduce_while(steps, :ok, fn
      :ok, :ok -> {:cont, :ok}
      {:error, reason}, :ok -> {:halt, {:error, reason}}
      _other, :ok -> {:halt, {:error, :unexpected_step_result}}
    end)
  end

  defp write_json(path, map) when is_map(map) do
    encoded = JSON.encode_to_iodata!(map)
    File.write(path, [encoded, "\n"])
  end

  defp build_id do
    stamp = DateTime.utc_now() |> Calendar.strftime("%Y%m%d%H%M%S")
    unique = System.unique_integer([:positive])
    "sb_#{stamp}_#{unique}"
  end

  defp write_operator_notes(dist_dir) do
    notes = [
      "# Favn Single Artifact Notes",
      "",
      "This output preserves the web + orchestrator + runner topology and env contracts.",
      "In this Phase 9 cut it is assembly metadata/output, not an operational launcher bundle.",
      "",
      "The generated bin/start and bin/stop scripts intentionally exit non-zero to avoid",
      "falsely implying full local deployment automation.",
      ""
    ]

    File.write(Path.join(dist_dir, "OPERATOR_NOTES.md"), Enum.join(notes, "\n"))
  end
end
