defmodule Favn.Dev.Build.Single do
  @moduledoc """
  Project-local single-node backend artifact target.
  """

  alias Favn.Dev.Build.Orchestrator
  alias Favn.Dev.Build.Runner
  alias Favn.Dev.Install
  alias Favn.Dev.Paths
  alias Favn.Dev.State

  @schema_version 1
  @target "single"

  @type root_opt :: [root_dir: Path.t()]

  @spec run(root_opt()) :: {:ok, map()} | {:error, term()}
  def run(opts \\ []) when is_list(opts) do
    with storage <- storage_mode(opts),
         :ok <- validate_storage(storage),
         :ok <- Install.ensure_ready(opts),
         :ok <- State.ensure_layout(opts),
         {:ok, orchestrator} <- Orchestrator.run(opts),
         {:ok, runner} <- Runner.run(opts),
         {build_id, root_dir} <- {build_id(), Paths.root_dir(opts)},
         build_dir <- Paths.build_single_dir(root_dir, build_id),
         dist_dir <- Paths.dist_single_dir(root_dir, build_id),
         :ok <- File.mkdir_p(build_dir),
         :ok <- File.mkdir_p(Path.join(dist_dir, "orchestrator")),
         :ok <- File.mkdir_p(Path.join(dist_dir, "runner")),
         :ok <- File.mkdir_p(Path.join(dist_dir, "config")),
         :ok <- File.mkdir_p(Path.join(dist_dir, "env")),
         :ok <- File.mkdir_p(Path.join(dist_dir, "bin")),
         :ok <- copy_target_outputs(orchestrator.dist_dir, Path.join(dist_dir, "orchestrator")),
         :ok <- copy_target_outputs(runner.dist_dir, Path.join(dist_dir, "runner")),
         assembly <- assembly_json(build_id, orchestrator, runner, storage),
         :ok <-
           write_json(Path.join(build_dir, "build.json"), build_json(build_id, assembly, opts)),
         :ok <-
           write_json(
             Path.join(dist_dir, "metadata.json"),
             metadata_json(build_id, assembly, opts)
           ),
         :ok <- write_json(Path.join(dist_dir, "config/assembly.json"), assembly),
         :ok <- write_env_files(dist_dir),
         :ok <- write_scripts(dist_dir, orchestrator),
         :ok <- write_operator_notes(dist_dir) do
      {:ok, %{build_id: build_id, build_dir: build_dir, dist_dir: dist_dir}}
    end
  end

  defp storage_mode(opts) do
    case Keyword.get(opts, :storage, :sqlite) do
      "sqlite" -> :sqlite
      "postgres" -> :postgres
      value -> value
    end
  end

  defp validate_storage(:sqlite), do: :ok
  defp validate_storage(:postgres), do: {:error, {:unsupported_storage, :postgres}}
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

  defp assembly_json(build_id, orchestrator, runner, storage) do
    %{
      "schema_version" => @schema_version,
      "target" => @target,
      "build_id" => build_id,
      "assembled_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "storage" => %{"mode" => Atom.to_string(storage)},
      "services" => %{
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
      "kind" => "project_local_backend_launcher",
      "operational" => false,
      "truthfulness" =>
        "project_local_launcher_requires_runtime_source_and_start_stop_verification"
    })
    |> Map.put("topology", %{
      "boundary" => "orchestrator+runner+scheduler",
      "boundary_preserved" => true,
      "process_model" => "one_backend_beam_runtime",
      "backend_only" => true,
      "backend_nodes" => 1,
      "runner_mode" => "local",
      "scheduler_instances" => 1
    })
    |> Map.put("compatibility", %{
      "scope" => "project-local backend-only SQLite launcher",
      "runtime_dependency" => "recorded_orchestrator_source_root",
      "storage_modes" => ["sqlite"],
      "unsupported" => [
        "self_contained_release_artifact",
        "postgres_production_mode",
        "distributed_execution",
        "shared_sqlite",
        "high_availability_orchestrators",
        "web_production_startup"
      ]
    })
    |> Map.put("required_env", [
      "FAVN_STORAGE",
      "FAVN_SQLITE_PATH",
      "FAVN_SQLITE_MIGRATION_MODE",
      "FAVN_SQLITE_BUSY_TIMEOUT_MS",
      "FAVN_SQLITE_POOL_SIZE",
      "FAVN_ORCHESTRATOR_API_BIND_HOST",
      "FAVN_ORCHESTRATOR_API_PORT",
      "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS",
      "FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN",
      "FAVN_SCHEDULER_ENABLED",
      "FAVN_SCHEDULER_TICK_MS",
      "FAVN_SCHEDULER_MAX_MISSED_ALL_OCCURRENCES",
      "FAVN_RUNNER_MODE"
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

  defp write_env_files(dist_dir) do
    backend = [
      "# Copy this file to env/backend.env or set FAVN_ENV_FILE before running bin/start.",
      "FAVN_STORAGE=sqlite",
      "FAVN_SQLITE_PATH=/var/lib/favn/control-plane.sqlite3",
      "FAVN_SQLITE_MIGRATION_MODE=manual",
      "FAVN_SQLITE_BUSY_TIMEOUT_MS=5000",
      "FAVN_SQLITE_POOL_SIZE=1",
      "FAVN_ORCHESTRATOR_API_BIND_HOST=127.0.0.1",
      "FAVN_ORCHESTRATOR_API_PORT=4101",
      "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS=replace-me",
      "FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN=replace-me",
      "FAVN_SCHEDULER_ENABLED=true",
      "FAVN_SCHEDULER_TICK_MS=15000",
      "FAVN_SCHEDULER_MAX_MISSED_ALL_OCCURRENCES=1000",
      "FAVN_RUNNER_MODE=local",
      ""
    ]

    File.write(Path.join(dist_dir, "env/backend.env.example"), Enum.join(backend, "\n"))
  end

  defp write_scripts(dist_dir, orchestrator) do
    with {:ok, source_root} <- bundled_source_root(orchestrator.dist_dir) do
      [
        File.write(Path.join(dist_dir, "bin/start"), start_script(source_root)),
        File.write(Path.join(dist_dir, "bin/stop"), stop_script()),
        File.chmod(Path.join(dist_dir, "bin/start"), 0o755),
        File.chmod(Path.join(dist_dir, "bin/stop"), 0o755)
      ]
      |> run_steps()
    end
  end

  defp bundled_source_root(orchestrator_dist_dir) do
    with {:ok, encoded} <- File.read(Path.join(orchestrator_dist_dir, "bundle.json")),
         {:ok, %{"source_root" => source_root}} when is_binary(source_root) <-
           JSON.decode(encoded) do
      {:ok, source_root}
    else
      {:error, reason} -> {:error, {:read_orchestrator_bundle_failed, reason}}
      _other -> {:error, :invalid_orchestrator_bundle}
    end
  end

  defp start_script(orchestrator_source_root) do
    ~S'''
    #!/usr/bin/env sh
    set -eu

    SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
    ARTIFACT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
    ORCHESTRATOR_SOURCE_ROOT="__ORCHESTRATOR_SOURCE_ROOT__"
    RUNTIME_HOME=${FAVN_SINGLE_NODE_HOME:-"$ARTIFACT_ROOT/var"}
    RUN_DIR="$RUNTIME_HOME/run"
    LOG_DIR="$RUNTIME_HOME/log"
    DATA_DIR="$RUNTIME_HOME/data"
    PID_FILE="$RUN_DIR/backend.pid"
    LOG_FILE="$LOG_DIR/backend.log"
    BOOT_FILE="$RUN_DIR/backend_boot.exs"
    STARTUP_TIMEOUT_SECONDS=${FAVN_STARTUP_TIMEOUT_SECONDS:-30}

    mkdir -p "$RUN_DIR" "$LOG_DIR" "$DATA_DIR"

    if [ -n "${FAVN_ENV_FILE:-}" ]; then
      if [ ! -f "$FAVN_ENV_FILE" ]; then
        echo "FAVN_ENV_FILE does not exist: $FAVN_ENV_FILE" >&2
        exit 1
      fi
      set -a
      . "$FAVN_ENV_FILE"
      set +a
    elif [ -f "$ARTIFACT_ROOT/env/backend.env" ]; then
      set -a
      . "$ARTIFACT_ROOT/env/backend.env"
      set +a
    fi

    if [ -f "$PID_FILE" ]; then
      old_pid=$(cat "$PID_FILE" 2>/dev/null || true)
      case "$old_pid" in
        ''|*[!0-9]*) rm -f "$PID_FILE" ;;
        *)
          if kill -0 "$old_pid" 2>/dev/null; then
            echo "Favn backend already running with PID $old_pid" >&2
            exit 1
          fi
          rm -f "$PID_FILE"
          ;;
      esac
    fi

    if ! command -v curl >/dev/null 2>&1; then
      echo "curl is required for readiness polling" >&2
      exit 1
    fi

    cat > "$BOOT_FILE" <<'EOF'
    artifact_root = System.fetch_env!("FAVN_ARTIFACT_ROOT")
    runner_ebin = Path.join([artifact_root, "runner", "ebin"])

    if File.dir?(runner_ebin) do
      Code.prepend_path(runner_ebin)
    end

    env = System.get_env()

    # FAVN_SCHEDULER_ENABLED is consumed only by FavnOrchestrator.ProductionRuntimeConfig.
    with {:ok, _runner} <- FavnRunner.ProductionRuntimeConfig.validate(env),
         {:ok, _orchestrator} <- FavnOrchestrator.ProductionRuntimeConfig.validate(env),
         {:ok, _} <- Application.ensure_all_started(:favn_runner),
         {:ok, _} <- Application.ensure_all_started(:favn_storage_sqlite),
         {:ok, _} <- Application.ensure_all_started(:favn_orchestrator) do
      manifest_path = Path.join([artifact_root, "runner", "manifest.json"])

      if File.regular?(manifest_path) do
        with {:ok, encoded} <- File.read(manifest_path),
             {:ok, manifest} <- Favn.Manifest.Serializer.decode_manifest(encoded),
             {:ok, version} <- Favn.Manifest.Version.new(manifest),
             :ok <- FavnRunner.register_manifest(version),
             :ok <- FavnOrchestrator.register_manifest(version) do
          :ok
        else
          {:error, reason} -> raise "failed to register packaged manifest: #{inspect(reason)}"
          other -> raise "failed to register packaged manifest: #{inspect(other)}"
        end
      end

      Process.sleep(:infinity)
    else
      {:error, reason} -> raise "invalid Favn backend production runtime config or startup: #{inspect(reason)}"
      other -> raise "invalid Favn backend production runtime config or startup: #{inspect(other)}"
    end
    EOF

    (
      cd "$ORCHESTRATOR_SOURCE_ROOT"
      FAVN_ARTIFACT_ROOT="$ARTIFACT_ROOT" MIX_ENV=${MIX_ENV:-prod} elixir -S mix run --no-start "$BOOT_FILE"
    ) >"$LOG_FILE" 2>&1 &

    pid=$!
    printf '%s\n' "$pid" > "$PID_FILE"

    host=${FAVN_ORCHESTRATOR_API_BIND_HOST:-127.0.0.1}
    port=${FAVN_ORCHESTRATOR_API_PORT:-4101}
    ready_url="http://$host:$port/api/orchestrator/v1/health/ready"
    elapsed=0

    while [ "$elapsed" -lt "$STARTUP_TIMEOUT_SECONDS" ]; do
      if ! kill -0 "$pid" 2>/dev/null; then
        rm -f "$PID_FILE"
        echo "Favn backend exited before readiness; see $LOG_FILE" >&2
        exit 1
      fi

      if curl -fsS "$ready_url" >/dev/null 2>&1; then
        echo "Favn backend started with PID $pid"
        echo "Readiness: $ready_url"
        exit 0
      fi

      sleep 1
      elapsed=$((elapsed + 1))
    done

    kill "$pid" 2>/dev/null || true
    rm -f "$PID_FILE"
    echo "Favn backend did not become ready within ${STARTUP_TIMEOUT_SECONDS}s; see $LOG_FILE" >&2
    exit 1
    '''
    |> script_body()
    |> String.replace(
      "__ORCHESTRATOR_SOURCE_ROOT__",
      shell_double_quote_escape(orchestrator_source_root)
    )
  end

  defp stop_script do
    ~S'''
    #!/usr/bin/env sh
    set -eu

    SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
    ARTIFACT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
    RUNTIME_HOME=${FAVN_SINGLE_NODE_HOME:-"$ARTIFACT_ROOT/var"}
    PID_FILE="$RUNTIME_HOME/run/backend.pid"
    STOP_TIMEOUT_SECONDS=${FAVN_STOP_TIMEOUT_SECONDS:-30}

    if [ ! -f "$PID_FILE" ]; then
      echo "Favn backend is not running"
      exit 0
    fi

    pid=$(cat "$PID_FILE" 2>/dev/null || true)
    case "$pid" in
      ''|*[!0-9]*)
        rm -f "$PID_FILE"
        echo "Removed stale Favn backend PID file"
        exit 0
        ;;
    esac

    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$PID_FILE"
      echo "Removed stale Favn backend PID file"
      exit 0
    fi

    kill "$pid"
    elapsed=0
    while [ "$elapsed" -lt "$STOP_TIMEOUT_SECONDS" ]; do
      if ! kill -0 "$pid" 2>/dev/null; then
        rm -f "$PID_FILE"
        echo "Favn backend stopped"
        exit 0
      fi
      sleep 1
      elapsed=$((elapsed + 1))
    done

    echo "Favn backend did not stop within ${STOP_TIMEOUT_SECONDS}s" >&2
    exit 1
    '''
    |> script_body()
  end

  defp script_body(contents) when is_binary(contents) do
    contents
    |> String.trim_leading()
    |> String.replace(~r/^    /m, "")
  end

  defp shell_double_quote_escape(value) when is_binary(value) do
    value
    |> String.replace("\\", "\\\\")
    |> String.replace("\"", "\\\"")
    |> String.replace("$", "\\$")
    |> String.replace("`", "\\`")
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
      "This output is a project-local backend-only SQLite launcher, not a",
      "self-contained operational production artifact yet. It depends on the",
      "recorded orchestrator source/runtime root used by the install step.",
      "The launcher starts one BEAM runtime containing the runner, SQLite storage",
      "adapter, orchestrator API, and scheduler when FAVN_SCHEDULER_ENABLED allows it.",
      "",
      "Copy env/backend.env.example to env/backend.env or set FAVN_ENV_FILE before",
      "running bin/start. The example service token is intentionally invalid and",
      "must be replaced with a real secret. Web production startup, Postgres",
      "production mode, distributed execution, shared SQLite, and HA orchestrators",
      "are not included.",
      ""
    ]

    File.write(Path.join(dist_dir, "OPERATOR_NOTES.md"), Enum.join(notes, "\n"))
  end
end
