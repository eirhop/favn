defmodule Favn.Dev.Init.Compose do
  @moduledoc """
  Scaffolds consumer-owned Docker Compose starting templates.

  Templates are written only when their target is absent or byte-identical.
  Favn never overwrites a consumer-modified deployment file.
  """

  alias Favn.Dev.{ComposeProject, Paths}

  @profiles [:local, :single_host]
  @extensions [".yml", ".yaml"]

  @type result :: %{
          created: [Path.t()],
          existing: [Path.t()],
          profile: :local | :single_host,
          output: Path.t(),
          env_example: Path.t()
        }

  @doc "Writes one versioned Compose template and its secret-free environment example."
  @spec run(keyword()) :: {:ok, result()} | {:error, term()}
  def run(opts) when is_list(opts) do
    with {:ok, profile} <- profile(opts),
         {:ok, root_dir} <- project_root(opts),
         {:ok, output} <- output_path(root_dir, profile, opts),
         env_example = Path.rootname(output) <> ".env.example",
         {:ok, compose} <- render(profile, root_dir, output),
         environment = environment_example(profile),
         targets = [{output, compose}, {env_example, environment}],
         {:ok, statuses} <- preflight(targets),
         :ok <- write_missing(targets, statuses) do
      {:ok,
       %{
         created: relative_paths(root_dir, targets, statuses, :missing),
         existing: relative_paths(root_dir, targets, statuses, :identical),
         profile: profile,
         output: Path.relative_to(output, root_dir),
         env_example: Path.relative_to(env_example, root_dir)
       }}
    end
  end

  @doc false
  @spec render(:local | :single_host, Path.t(), Path.t()) ::
          {:ok, String.t()} | {:error, term()}
  def render(:local, root_dir, output) do
    output_dir = Path.dirname(output)

    postgres_init =
      deployment_relative_path(
        Paths.compose_postgres_init_path(root_dir),
        root_dir,
        output_dir
      )

    runner_data =
      deployment_relative_path(
        Paths.local_data_dir(root_dir),
        root_dir,
        output_dir
      )

    {:ok, local_template(postgres_init, runner_data)}
  end

  def render(:single_host, _root_dir, _output), do: {:ok, single_host_template()}

  defp profile(opts) do
    value = Keyword.get(opts, :profile, :local)

    case normalize_profile(value) do
      profile when profile in @profiles -> {:ok, profile}
      _invalid -> {:error, {:unsupported_compose_profile, value}}
    end
  end

  defp normalize_profile(:local), do: :local
  defp normalize_profile("local"), do: :local
  defp normalize_profile(:single_host), do: :single_host
  defp normalize_profile("single-host"), do: :single_host
  defp normalize_profile(_value), do: :invalid

  defp project_root(opts) do
    root_dir = opts |> Paths.root_dir() |> Path.expand()

    if File.regular?(Path.join(root_dir, "mix.exs")),
      do: {:ok, root_dir},
      else: {:error, {:missing_mix_project, root_dir}}
  end

  defp output_path(root_dir, profile, opts) do
    default =
      case profile do
        :local -> "deploy/local/compose.yml"
        :single_host -> "deploy/single-host/compose.yml"
      end

    value = Keyword.get(opts, :output, default)

    with true <- is_binary(value) and String.trim(value) != "",
         output <- Path.expand(value, root_dir),
         true <- Path.extname(output) in @extensions,
         true <- inside_root?(output, root_dir),
         :ok <- safe_parent(output, root_dir) do
      {:ok, output}
    else
      _invalid -> {:error, {:unsafe_compose_output, value}}
    end
  end

  defp safe_parent(output, root_dir) do
    output
    |> Path.dirname()
    |> existing_ancestors(root_dir)
    |> Enum.reduce_while(:ok, fn path, :ok ->
      case File.lstat(path) do
        {:ok, %{type: :directory}} -> {:cont, :ok}
        {:error, :enoent} -> {:cont, :ok}
        _unsafe -> {:halt, {:error, :unsafe_parent}}
      end
    end)
  end

  defp existing_ancestors(path, root_dir) do
    Stream.iterate(path, &Path.dirname/1)
    |> Enum.take_while(&inside_root?(&1, root_dir))
  end

  defp preflight(targets) do
    Enum.reduce_while(targets, {:ok, %{}}, fn {path, content}, {:ok, statuses} ->
      case File.lstat(path) do
        {:error, :enoent} ->
          {:cont, {:ok, Map.put(statuses, path, :missing)}}

        {:ok, %{type: :regular}} ->
          case File.read(path) do
            {:ok, ^content} ->
              {:cont, {:ok, Map.put(statuses, path, :identical)}}

            {:ok, _modified} ->
              {:halt, {:error, {:compose_scaffold_modified, path}}}

            {:error, reason} ->
              {:halt, {:error, {:compose_scaffold_read_failed, path, reason}}}
          end

        {:ok, _other} ->
          {:halt, {:error, {:unsafe_compose_scaffold_target, path}}}

        {:error, reason} ->
          {:halt, {:error, {:compose_scaffold_read_failed, path, reason}}}
      end
    end)
  end

  defp write_missing(targets, statuses) do
    missing =
      targets
      |> Enum.filter(fn {path, _content} -> statuses[path] == :missing end)
      |> Enum.map(fn {path, content} -> {path, content, temporary_path(path)} end)

    with :ok <- prepare_temporaries(missing),
         :ok <- install_temporaries(missing) do
      :ok
    end
  end

  defp prepare_temporaries(targets) do
    Enum.reduce_while(targets, :ok, fn {path, content, temporary}, :ok ->
      with :ok <- File.mkdir_p(Path.dirname(path)),
           :ok <- File.write(temporary, content, [:binary, :exclusive]),
           :ok <- File.chmod(temporary, 0o644) do
        {:cont, :ok}
      else
        {:error, reason} ->
          cleanup_temporaries(targets)
          {:halt, {:error, {:compose_scaffold_write_failed, path, reason}}}
      end
    end)
  end

  defp install_temporaries(targets) do
    Enum.reduce_while(targets, {:ok, []}, fn {path, _content, temporary}, {:ok, installed} ->
      case File.ln(temporary, path) do
        :ok ->
          case File.rm(temporary) do
            :ok ->
              {:cont, {:ok, [path | installed]}}

            {:error, reason} ->
              rollback_install(installed, targets)
              File.rm(path)
              {:halt, {:error, {:compose_scaffold_write_failed, path, reason}}}
          end

        {:error, :eexist} ->
          rollback_install(installed, targets)
          {:halt, {:error, {:compose_scaffold_modified, path}}}

        {:error, reason} ->
          rollback_install(installed, targets)
          {:halt, {:error, {:compose_scaffold_write_failed, path, reason}}}
      end
    end)
    |> case do
      {:ok, _installed} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp rollback_install(installed, targets) do
    Enum.each(installed, &File.rm/1)
    cleanup_temporaries(targets)
  end

  defp cleanup_temporaries(targets) do
    Enum.each(targets, fn {_path, _content, temporary} -> File.rm(temporary) end)
  end

  defp temporary_path(path) do
    suffix = :crypto.strong_rand_bytes(16) |> Base.url_encode64(padding: false)
    path <> ".favn-new-" <> suffix
  end

  defp relative_paths(root_dir, targets, statuses, status) do
    targets
    |> Enum.filter(fn {path, _content} -> statuses[path] == status end)
    |> Enum.map(fn {path, _content} -> Path.relative_to(path, root_dir) end)
  end

  defp inside_root?(path, root_dir) do
    relative = Path.relative_to(path, root_dir)
    relative != ".." and not String.starts_with?(relative, "../") and relative != path
  end

  defp compose_relative_path("."), do: "./"
  defp compose_relative_path("../" <> _rest = path), do: path
  defp compose_relative_path(path), do: "./" <> path

  defp deployment_relative_path(target, root_dir, output_dir) do
    target_parts = target |> Path.relative_to(root_dir) |> Path.split()

    parents =
      output_dir
      |> Path.relative_to(root_dir)
      |> Path.split()
      |> Enum.reject(&(&1 == "."))
      |> Enum.map(fn _part -> ".." end)

    parents
    |> Kernel.++(target_parts)
    |> Path.join()
    |> compose_relative_path()
  end

  defp environment_example(:local) do
    """
    # Reference values for the committed local Compose template. mix favn.dev
    # writes actual local values and credentials below .favn/compose/.
    FAVN_COMPOSE_PROJECT=favn-project
    FAVN_POSTGRES_VOLUME=favn-project-postgres-data
    FAVN_RUNNER_IMAGE=favn-local/favn-project-runner:dev
    FAVN_LOG_LEVEL=info
    FAVN_RUNNER_ENV_FILE=/absolute/path/to/.favn/compose/runner.env
    FAVN_RUNNER_UID=1000
    FAVN_RUNNER_GID=1000
    FAVN_POSTGRES_DATABASE=favn_dev
    FAVN_POSTGRES_ADMIN_USER=favn_migrator
    FAVN_POSTGRES_ADMIN_PASSWORD=set-locally
    FAVN_POSTGRES_RUNTIME_PASSWORD=set-locally
    FAVN_POSTGRES_ADMIN_DATABASE_URL=ecto://set-locally
    FAVN_POSTGRES_RUNTIME_DATABASE_URL=ecto://set-locally
    FAVN_CONTROL_PLANE_IMAGE=ghcr.io/eirhop/favn-control-plane@sha256:replace
    FAVN_RUNTIME_INPUT_PIN_KEYS=set-locally
    FAVN_RUNTIME_INPUT_PIN_KEY_VERSION=1
    FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS=120000
    FAVN_WORKSPACE_ID=local-dev
    FAVN_WORKSPACE_NAME=Local Development
    FAVN_DISTRIBUTION_COOKIE=set-locally
    FAVN_ORCHESTRATOR_API_SERVICE_TOKENS=set-locally
    FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD=set-locally
    FAVN_VIEW_PUBLIC_ORIGIN=http://127.0.0.1:4173
    FAVN_VIEW_SECRET_KEY_BASE=set-locally
    FAVN_VIEW_PORT=4173
    FAVN_ORCHESTRATOR_PORT=4101
    FAVN_SCHEDULER_ENABLED=false
    """
  end

  defp environment_example(:single_host) do
    """
    # Reference names only. Supply production values through your deployment
    # environment or secret manager; do not commit a populated copy.

    # Immutable runner and control-plane images
    FAVN_RUNNER_IMAGE=registry.example/favn-runner@sha256:replace
    FAVN_CONTROL_PLANE_IMAGE=ghcr.io/eirhop/favn-control-plane@sha256:replace

    # Privileged database operations
    FAVN_POSTGRES_ADMIN_DATABASE_URL=ecto://set-in-secret-store

    # Runtime-role verification and control-plane storage
    FAVN_POSTGRES_RUNTIME_DATABASE_URL=ecto://set-in-secret-store
    FAVN_RUNTIME_INPUT_PIN_KEYS=set-in-secret-store
    FAVN_RUNTIME_INPUT_PIN_KEY_VERSION=1

    # Runner identity and private distribution
    FAVN_LOG_LEVEL=info
    FAVN_WORKSPACE_ID=production
    FAVN_WORKSPACE_NAME=Production
    FAVN_DISTRIBUTION_COOKIE=set-in-secret-store

    # Control-plane authentication, UI, and operations
    FAVN_ORCHESTRATOR_API_SERVICE_TOKENS=set-in-secret-store
    FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD=set-in-secret-store
    FAVN_VIEW_SECRET_KEY_BASE=set-in-secret-store
    FAVN_VIEW_PUBLIC_ORIGIN=https://favn.example.com
    FAVN_VIEW_PORT=4173
    FAVN_ORCHESTRATOR_PORT=4101
    FAVN_SCHEDULER_ENABLED=true
    FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS=120000
    """
  end

  defp local_template(postgres_init, runner_data) do
    """
    # Local Favn development topology.
    #
    # This file belongs to the customer project. Change ports, local data paths,
    # and add project services as needed. Keep the io.favn.compose labels: Favn
    # uses them to identify the services it may start, inspect, and stop.
    name: ${FAVN_COMPOSE_PROJECT}

    # Shared labels form the small contract between this customer-owned file
    # and Favn's local lifecycle commands.
    x-favn-local-labels: &favn-local-labels
      io.favn.compose.contract-version: "1"
      io.favn.compose.profile: local

    x-local-logging: &local-logging
      driver: local
      options:
        max-size: "10m"
        max-file: "3"

    services:
      # PostgreSQL stores Favn control-plane state. The floating major-version
      # tag receives PostgreSQL 18 patch releases while avoiding an automatic
      # incompatible major-version upgrade.
      postgres:
        image: #{ComposeProject.postgres_image()}
        pull_policy: always
        restart: unless-stopped
        logging: *local-logging
        labels:
          <<: *favn-local-labels
          io.favn.compose.role: postgres
        environment:
          POSTGRES_DB: ${FAVN_POSTGRES_DATABASE}
          POSTGRES_USER: ${FAVN_POSTGRES_ADMIN_USER}
          POSTGRES_PASSWORD: ${FAVN_POSTGRES_ADMIN_PASSWORD}
          POSTGRES_INITDB_ARGS: >-
            --encoding=UTF8
            --locale-provider=builtin
            --builtin-locale=C.UTF-8
          FAVN_POSTGRES_RUNTIME_PASSWORD: ${FAVN_POSTGRES_RUNTIME_PASSWORD}
        volumes:
          - postgres-data:/var/lib/postgresql
          - #{postgres_init}:/docker-entrypoint-initdb.d/10-favn-runtime-role.sh:ro
        healthcheck:
          test: ["CMD-SHELL", "pg_isready -U ${FAVN_POSTGRES_ADMIN_USER} -d ${FAVN_POSTGRES_DATABASE}"]
          interval: 2s
          timeout: 3s
          retries: 30
          start_period: 5s
        networks:
          default:
            aliases: [postgres.favn.internal]

      # The operations services use the same prebuilt control-plane image for
      # short-lived database migration and verification commands. Their
      # profile keeps them out of the steady-state three-container topology.
      control-plane-ops:
        image: ${FAVN_CONTROL_PLANE_IMAGE}
        profiles: [operations]
        logging: *local-logging
        labels:
          <<: *favn-local-labels
          io.favn.compose.role: control-plane-ops
        entrypoint: ["/app/bin/favn_control_plane_ops"]
        read_only: true
        cap_drop: [ALL]
        security_opt: ["no-new-privileges:true"]
        tmpfs: ["/tmp/favn:rw,noexec,nosuid,nodev,uid=10001,gid=10001,mode=0700"]
        environment: &control-plane-operations-environment
          FAVN_LOG_LEVEL: ${FAVN_LOG_LEVEL:-info}
          FAVN_DEPLOYMENT_MODE: local-development
          FAVN_DATABASE_URL: ${FAVN_POSTGRES_ADMIN_DATABASE_URL}
          FAVN_DATABASE_SSL_MODE: disable
          FAVN_DATABASE_RUNTIME_ROLE: favn_runtime
          FAVN_INSTANCE_ID: ${FAVN_COMPOSE_PROJECT}-operations
          FAVN_RUNTIME_INPUT_PIN_KEYS: ${FAVN_RUNTIME_INPUT_PIN_KEYS}
          FAVN_RUNTIME_INPUT_PIN_KEY_VERSION: ${FAVN_RUNTIME_INPUT_PIN_KEY_VERSION}
          FAVN_WORKSPACE_ID: ${FAVN_WORKSPACE_ID}
          FAVN_WORKSPACE_SLUG: ${FAVN_WORKSPACE_ID}
          FAVN_WORKSPACE_NAME: ${FAVN_WORKSPACE_NAME}
          FAVN_CONTROL_PLANE_NODE: favn_control_plane_ops@control-plane-ops.favn.internal
          FAVN_DISTRIBUTION_COOKIE: ${FAVN_DISTRIBUTION_COOKIE}
          FAVN_BEAM_DISTRIBUTION_PORT: "9102"
          ERL_EPMD_PORT: "4369"
        depends_on:
          postgres: {condition: service_healthy}
        networks:
          default:
            aliases: [control-plane-ops.favn.internal]

      control-plane-verify:
        image: ${FAVN_CONTROL_PLANE_IMAGE}
        profiles: [operations]
        logging: *local-logging
        labels:
          <<: *favn-local-labels
          io.favn.compose.role: control-plane-verify
        entrypoint: ["/app/bin/favn_control_plane_ops"]
        read_only: true
        cap_drop: [ALL]
        security_opt: ["no-new-privileges:true"]
        tmpfs: ["/tmp/favn:rw,noexec,nosuid,nodev,uid=10001,gid=10001,mode=0700"]
        environment:
          <<: *control-plane-operations-environment
          FAVN_DATABASE_URL: ${FAVN_POSTGRES_RUNTIME_DATABASE_URL}
          FAVN_CONTROL_PLANE_NODE: favn_control_plane_verify@control-plane-verify.favn.internal
          FAVN_BEAM_DISTRIBUTION_PORT: "9103"
        depends_on:
          postgres: {condition: service_healthy}
        networks:
          default:
            aliases: [control-plane-verify.favn.internal]

      # The runner image is built from deploy/runner/Dockerfile by mix
      # favn.dev unless the user selects an existing --runner-image.
      runner:
        image: ${FAVN_RUNNER_IMAGE}
        restart: unless-stopped
        logging: *local-logging
        user: ${FAVN_RUNNER_UID}:${FAVN_RUNNER_GID}
        labels:
          <<: *favn-local-labels
          io.favn.compose.role: runner
        read_only: true
        init: true
        cap_drop: [ALL]
        security_opt: ["no-new-privileges:true"]
        stop_grace_period: 3m
        env_file: ["${FAVN_RUNNER_ENV_FILE}"]
        tmpfs: ["/tmp/favn:rw,noexec,nosuid,nodev,uid=${FAVN_RUNNER_UID},gid=${FAVN_RUNNER_GID},mode=0700"]
        volumes:
          # Local application files, including DuckDB data, remain visible in
          # the repository's .data directory. Change the source when the
          # project uses another local path. If persistence is unnecessary,
          # replace this bind mount with a tmpfs mounted at /var/lib/favn/data.
          - type: bind
            source: #{runner_data}
            target: /var/lib/favn/data
        environment:
          # Keep normal development logs at info. Set FAVN_LOG_LEVEL=debug
          # temporarily when diagnosing a problem.
          FAVN_LOG_LEVEL: ${FAVN_LOG_LEVEL:-info}
          FAVN_RUNNER_NODE: favn_runner@runner.favn.internal
          FAVN_CONTROL_PLANE_NODE: favn_control_plane@control-plane.favn.internal
          FAVN_DISTRIBUTION_COOKIE: ${FAVN_DISTRIBUTION_COOKIE}
          FAVN_BEAM_DISTRIBUTION_PORT: "9100"
          ERL_EPMD_PORT: "4369"
          FAVN_RUNNER_DATA_DIR: /var/lib/favn/data
          FAVN_LOCAL_SAMPLE_DATABASE_PATH: /var/lib/favn/data/local_smoke.duckdb
          FAVN_LOCAL_SAMPLE_RAW_CATALOG_PATH: /var/lib/favn/data/raw.duckdb
          FAVN_LOCAL_SAMPLE_MART_CATALOG_PATH: /var/lib/favn/data/mart.duckdb
        expose: ["4369", "9100"]
        healthcheck:
          test: ["CMD-SHELL", "/opt/favn/bin/favn_runner rpc 'FavnRunner.release_info()' >/dev/null"]
          interval: 2s
          timeout: 5s
          retries: 30
          start_period: 5s
        networks:
          default:
            aliases: [runner.favn.internal]

      # The prebuilt control plane contains the Favn web application and
      # orchestrator. Customer application code runs only in the runner.
      control-plane:
        image: ${FAVN_CONTROL_PLANE_IMAGE}
        restart: unless-stopped
        logging: *local-logging
        labels:
          <<: *favn-local-labels
          io.favn.compose.role: control-plane
        read_only: true
        init: true
        cap_drop: [ALL]
        security_opt: ["no-new-privileges:true"]
        stop_grace_period: 3m
        tmpfs: ["/tmp/favn:rw,noexec,nosuid,nodev,uid=10001,gid=10001,mode=0700"]
        environment:
          FAVN_LOG_LEVEL: ${FAVN_LOG_LEVEL:-info}
          FAVN_DEPLOYMENT_MODE: local-development
          FAVN_DATABASE_URL: ${FAVN_POSTGRES_RUNTIME_DATABASE_URL}
          FAVN_DATABASE_SSL_MODE: disable
          FAVN_DATABASE_POOL_SIZE: "10"
          FAVN_RUNTIME_INPUT_PIN_KEYS: ${FAVN_RUNTIME_INPUT_PIN_KEYS}
          FAVN_RUNTIME_INPUT_PIN_KEY_VERSION: ${FAVN_RUNTIME_INPUT_PIN_KEY_VERSION}
          FAVN_INSTANCE_ID: ${FAVN_COMPOSE_PROJECT}
          FAVN_WORKSPACE_IDS: ${FAVN_WORKSPACE_ID}
          FAVN_ORCHESTRATOR_API_BIND_HOST: 0.0.0.0
          FAVN_ORCHESTRATOR_API_PORT: "4101"
          FAVN_ORCHESTRATOR_API_SERVICE_TOKENS: ${FAVN_ORCHESTRATOR_API_SERVICE_TOKENS}
          FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME: admin
          FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD: ${FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD}
          FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME: Favn Local Admin
          FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES: admin
          FAVN_SCHEDULER_ENABLED: ${FAVN_SCHEDULER_ENABLED}
          FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS: ${FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS}
          FAVN_CONTROL_PLANE_NODE: favn_control_plane@control-plane.favn.internal
          FAVN_RUNNER_NODE: favn_runner@runner.favn.internal
          FAVN_DISTRIBUTION_COOKIE: ${FAVN_DISTRIBUTION_COOKIE}
          FAVN_BEAM_DISTRIBUTION_PORT: "9101"
          ERL_EPMD_PORT: "4369"
          FAVN_VIEW_PUBLIC_ORIGIN: ${FAVN_VIEW_PUBLIC_ORIGIN}
          FAVN_VIEW_SECRET_KEY_BASE: ${FAVN_VIEW_SECRET_KEY_BASE}
          FAVN_VIEW_BIND_HOST: 0.0.0.0
          FAVN_VIEW_PORT: "4000"
          FAVN_VIEW_TRUSTED_PROXY_CIDRS: 127.0.0.1/32
        ports:
          - "127.0.0.1:${FAVN_VIEW_PORT}:4000"
          - "127.0.0.1:${FAVN_ORCHESTRATOR_PORT}:4101"
        expose: ["4369", "9101"]
        healthcheck:
          test: ["CMD-SHELL", "/app/bin/favn_control_plane rpc 'FavnView.Readiness.liveness()' >/dev/null"]
          interval: 2s
          timeout: 5s
          retries: 30
          start_period: 5s
        depends_on:
          postgres: {condition: service_healthy}
          runner: {condition: service_healthy}
        networks:
          default:
            aliases: [control-plane.favn.internal]

    volumes:
      # Keep PostgreSQL internals in a Docker-managed volume. Do not bind the
      # database directory into .data; PostgreSQL owns its filesystem layout.
      postgres-data:
        name: ${FAVN_POSTGRES_VOLUME}

    networks:
      # All steady-state services communicate through this private network.
      default:
        name: ${FAVN_COMPOSE_PROJECT}-network
        driver: bridge
    """
  end

  defp single_host_template do
    """
    name: favn-single-host

    x-favn-single-host-labels: &favn-single-host-labels
      io.favn.compose.contract-version: "1"
      io.favn.compose.profile: single-host

    x-control-plane-environment: &control-plane-environment
      FAVN_LOG_LEVEL: ${FAVN_LOG_LEVEL:-info}
      FAVN_DEPLOYMENT_MODE: production
      FAVN_DATABASE_URL: ${FAVN_POSTGRES_RUNTIME_DATABASE_URL}
      FAVN_DATABASE_SSL_MODE: verify_full
      FAVN_RUNTIME_INPUT_PIN_KEYS: ${FAVN_RUNTIME_INPUT_PIN_KEYS}
      FAVN_RUNTIME_INPUT_PIN_KEY_VERSION: ${FAVN_RUNTIME_INPUT_PIN_KEY_VERSION}
      FAVN_WORKSPACE_IDS: ${FAVN_WORKSPACE_ID}
      FAVN_DISTRIBUTION_COOKIE: ${FAVN_DISTRIBUTION_COOKIE}

    services:
      control-plane-ops:
        image: ${FAVN_CONTROL_PLANE_IMAGE}
        profiles: [operations]
        labels:
          <<: *favn-single-host-labels
          io.favn.compose.role: control-plane-ops
        entrypoint: ["/app/bin/favn_control_plane_ops"]
        user: "10001:10001"
        read_only: true
        cap_drop: [ALL]
        security_opt: ["no-new-privileges:true"]
        tmpfs: ["/tmp/favn:rw,noexec,nosuid,nodev,uid=10001,gid=10001,mode=0700"]
        environment:
          <<: *control-plane-environment
          FAVN_DATABASE_URL: ${FAVN_POSTGRES_ADMIN_DATABASE_URL}
          FAVN_DATABASE_RUNTIME_ROLE: favn_runtime
          FAVN_WORKSPACE_ID: ${FAVN_WORKSPACE_ID}
          FAVN_WORKSPACE_SLUG: ${FAVN_WORKSPACE_ID}
          FAVN_WORKSPACE_NAME: ${FAVN_WORKSPACE_NAME}
          FAVN_CONTROL_PLANE_NODE: favn_control_plane_ops@control-plane-ops.favn.internal
          FAVN_BEAM_DISTRIBUTION_PORT: "9102"
        networks:
          default:
            aliases: [control-plane-ops.favn.internal]

      control-plane-verify:
        image: ${FAVN_CONTROL_PLANE_IMAGE}
        profiles: [operations]
        labels:
          <<: *favn-single-host-labels
          io.favn.compose.role: control-plane-verify
        entrypoint: ["/app/bin/favn_control_plane_ops"]
        user: "10001:10001"
        read_only: true
        cap_drop: [ALL]
        security_opt: ["no-new-privileges:true"]
        tmpfs: ["/tmp/favn:rw,noexec,nosuid,nodev,uid=10001,gid=10001,mode=0700"]
        environment:
          <<: *control-plane-environment
          FAVN_CONTROL_PLANE_NODE: favn_control_plane_verify@control-plane-verify.favn.internal
          FAVN_BEAM_DISTRIBUTION_PORT: "9103"
        networks:
          default:
            aliases: [control-plane-verify.favn.internal]

      runner:
        image: ${FAVN_RUNNER_IMAGE}
        user: "10001:10001"
        restart: unless-stopped
        labels:
          <<: *favn-single-host-labels
          io.favn.compose.role: runner
        read_only: true
        init: true
        cap_drop: [ALL]
        security_opt: ["no-new-privileges:true"]
        stop_grace_period: 3m
        tmpfs: ["/tmp/favn:rw,noexec,nosuid,nodev,uid=10001,gid=10001,mode=0700"]
        environment:
          FAVN_LOG_LEVEL: ${FAVN_LOG_LEVEL:-info}
          FAVN_RUNNER_NODE: favn_runner@runner.favn.internal
          FAVN_CONTROL_PLANE_NODE: favn_control_plane@control-plane.favn.internal
          FAVN_DISTRIBUTION_COOKIE: ${FAVN_DISTRIBUTION_COOKIE}
          FAVN_BEAM_DISTRIBUTION_PORT: "9100"
          ERL_EPMD_PORT: "4369"
        expose: ["4369", "9100"]
        healthcheck:
          test: ["CMD-SHELL", "/opt/favn/bin/favn_runner rpc 'FavnRunner.release_info()' >/dev/null"]
          interval: 5s
          timeout: 5s
          retries: 30
        networks:
          default:
            aliases: [runner.favn.internal]

      control-plane:
        image: ${FAVN_CONTROL_PLANE_IMAGE}
        user: "10001:10001"
        restart: unless-stopped
        labels:
          <<: *favn-single-host-labels
          io.favn.compose.role: control-plane
        read_only: true
        init: true
        cap_drop: [ALL]
        security_opt: ["no-new-privileges:true"]
        stop_grace_period: 3m
        tmpfs: ["/tmp/favn:rw,noexec,nosuid,nodev,uid=10001,gid=10001,mode=0700"]
        environment:
          <<: *control-plane-environment
          FAVN_INSTANCE_ID: favn-single-host
          FAVN_ORCHESTRATOR_API_BIND_HOST: 0.0.0.0
          FAVN_ORCHESTRATOR_API_PORT: "4101"
          FAVN_ORCHESTRATOR_API_SERVICE_TOKENS: ${FAVN_ORCHESTRATOR_API_SERVICE_TOKENS}
          FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME: admin
          FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD: ${FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD}
          FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME: Favn Administrator
          FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES: admin
          FAVN_SCHEDULER_ENABLED: ${FAVN_SCHEDULER_ENABLED}
          FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS: ${FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS}
          FAVN_CONTROL_PLANE_NODE: favn_control_plane@control-plane.favn.internal
          FAVN_RUNNER_NODE: favn_runner@runner.favn.internal
          FAVN_BEAM_DISTRIBUTION_PORT: "9101"
          ERL_EPMD_PORT: "4369"
          FAVN_VIEW_PUBLIC_ORIGIN: ${FAVN_VIEW_PUBLIC_ORIGIN}
          FAVN_VIEW_SECRET_KEY_BASE: ${FAVN_VIEW_SECRET_KEY_BASE}
          FAVN_VIEW_BIND_HOST: 0.0.0.0
          FAVN_VIEW_PORT: "4000"
          FAVN_VIEW_TRUSTED_PROXY_CIDRS: 127.0.0.1/32
        ports:
          - "127.0.0.1:${FAVN_VIEW_PORT}:4000"
          - "127.0.0.1:${FAVN_ORCHESTRATOR_PORT}:4101"
        expose: ["4369", "9101"]
        healthcheck:
          test: ["CMD-SHELL", "/app/bin/favn_control_plane rpc 'FavnView.Readiness.liveness()' >/dev/null"]
          interval: 5s
          timeout: 5s
          retries: 30
        depends_on:
          runner: {condition: service_healthy}
        networks:
          default:
            aliases: [control-plane.favn.internal]

    networks:
      default:
        driver: bridge
    """
  end
end
