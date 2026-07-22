defmodule Favn.Dev.ComposeProject do
  @moduledoc """
  Generates the project-scoped local Docker Compose contract.

  The generated topology contains one PostgreSQL 18 service, one customer
  runner release, and the installed control-plane release. Only the browser and
  private HTTP API ports bind to loopback. PostgreSQL, EPMD, and BEAM
  distribution ports remain private to the project network.
  """

  alias Favn.Dev.{ComposeEnv, Config, Paths}

  @schema_version 1
  @postgres_image "postgres@sha256:1961f96e6029a02c3812d7cb329a3b03a3ac2bb067058dec17b0f5596aca9296"
  @safe_identifier ~r/\A[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\z/
  @environment_key ~r/\A[A-Za-z_][A-Za-z0-9_]*\z/
  @max_runner_environment 512
  @max_environment_value_bytes 65_536

  @type project :: %{
          required(String.t()) => String.t() | pos_integer()
        }

  @doc "Returns the supported digest-pinned PostgreSQL 18 image."
  @spec postgres_image() :: String.t()
  def postgres_image, do: @postgres_image

  @doc "Builds and atomically writes Compose metadata and its secret environment."
  @spec write(map(), map(), Config.t(), keyword()) :: {:ok, project()} | {:error, term()}
  def write(install, secrets, %Config{} = config, opts)
      when is_map(install) and is_map(secrets) and is_list(opts) do
    root_dir = opts |> Paths.root_dir() |> Path.expand()

    with {:ok, control_plane_reference} <- fetch_install_reference(install),
         :ok <- validate_inputs(secrets, config),
         project = project(root_dir, control_plane_reference, config),
         :ok <- File.mkdir_p(Paths.compose_dir(root_dir)),
         :ok <- atomic_write(project["compose_path"], compose_yaml(project)),
         :ok <- atomic_write(project["env_path"], env_file(project, secrets, config), 0o600),
         :ok <- atomic_write(project["runner_env_path"], "", 0o600),
         :ok <-
           atomic_write(
             project["postgres_init_path"],
             postgres_init_script(),
             0o555
           ) do
      {:ok,
       Map.put(
         project,
         "compose_sha256",
         project["compose_path"] |> File.read!() |> sha256()
       )}
    end
  end

  @doc "Writes the bounded customer environment consumed only by the runner service."
  @spec put_runner_environment(project(), map()) :: :ok | {:error, term()}
  def put_runner_environment(project, environment)
      when is_map(project) and is_map(environment) do
    with :ok <- validate_runner_environment(environment),
         {:ok, contents} <- ComposeEnv.encode(environment) do
      atomic_write(Map.fetch!(project, "runner_env_path"), contents, 0o600)
    end
  end

  @doc "Atomically changes only the generated local runner image reference."
  @spec put_runner_image(project(), String.t()) :: :ok | {:error, term()}
  def put_runner_image(project, image)
      when is_map(project) and is_binary(image) and image != "" do
    path = Map.fetch!(project, "env_path")

    with {:ok, contents} <- File.read(path),
         {:ok, updated} <- replace_env(contents, "FAVN_RUNNER_IMAGE", image),
         :ok <- atomic_write(path, updated, 0o600) do
      :ok
    end
  end

  @doc "Atomically changes the scheduler flag consumed on control-plane recreation."
  @spec put_scheduler_enabled(project(), boolean()) :: :ok | {:error, term()}
  def put_scheduler_enabled(project, enabled?) when is_map(project) and is_boolean(enabled?) do
    path = Map.fetch!(project, "env_path")

    with {:ok, contents} <- File.read(path),
         {:ok, updated} <-
           replace_env(
             contents,
             "FAVN_SCHEDULER_ENABLED",
             if(enabled?, do: "true", else: "false")
           ),
         :ok <- atomic_write(path, updated, 0o600) do
      :ok
    end
  end

  @doc "Returns the stable Compose project name for one canonical project root."
  @spec project_name(Path.t()) :: String.t()
  def project_name(root_dir) when is_binary(root_dir) do
    root_dir = Path.expand(root_dir)

    slug =
      root_dir
      |> Path.basename()
      |> String.downcase()
      |> String.replace(~r/[^a-z0-9]+/, "-")
      |> String.trim("-")
      |> case do
        "" -> "project"
        value -> String.slice(value, 0, 32)
      end

    suffix = root_dir |> sha256() |> binary_part(0, 12)
    "favn-#{slug}-#{suffix}"
  end

  defp project(root_dir, control_plane_reference, config) do
    project_name = project_name(root_dir)

    %{
      "schema_version" => @schema_version,
      "project_name" => project_name,
      "network_name" => project_name <> "-network",
      "postgres_volume_name" => project_name <> "-postgres-data",
      "compose_path" => Paths.compose_path(root_dir),
      "env_path" => Paths.compose_env_path(root_dir),
      "runner_env_path" => Paths.compose_runner_env_path(root_dir),
      "postgres_init_path" => Paths.compose_postgres_init_path(root_dir),
      "control_plane_image" => control_plane_reference,
      "postgres_image" => @postgres_image,
      "view_url" => "http://127.0.0.1:#{config.web_port}",
      "orchestrator_url" => "http://127.0.0.1:#{config.orchestrator_port}",
      "view_port" => config.web_port,
      "orchestrator_port" => config.orchestrator_port,
      "workspace_id" => config.workspace_id
    }
  end

  defp validate_inputs(secrets, config) do
    required_secrets = [
      "service_token",
      "web_session_secret",
      "rpc_cookie",
      "runtime_input_pin_key",
      "postgres_admin_password",
      "postgres_runtime_password",
      "bootstrap_password"
    ]

    cond do
      not Regex.match?(@safe_identifier, config.workspace_id) ->
        {:error, {:invalid_local_compose_value, :workspace_id}}

      not Enum.all?(required_secrets, fn key ->
        case Map.get(secrets, key) do
          value when is_binary(value) and value != "" ->
            not String.contains?(value, ["\n", "\r", "'"])

          _missing ->
            false
        end
      end) ->
        {:error, :invalid_local_secrets}

      true ->
        :ok
    end
  end

  defp fetch_install_reference(%{"image_reference" => reference})
       when is_binary(reference) and reference != "",
       do: {:ok, reference}

  defp fetch_install_reference(_install), do: {:error, :invalid_control_plane_install_state}

  defp compose_yaml(project) do
    """
    name: #{project["project_name"]}

    services:
      postgres:
        image: #{project["postgres_image"]}
        restart: unless-stopped
        environment:
          POSTGRES_DB: ${FAVN_POSTGRES_DATABASE}
          POSTGRES_USER: ${FAVN_POSTGRES_ADMIN_USER}
          POSTGRES_PASSWORD: ${FAVN_POSTGRES_ADMIN_PASSWORD}
          FAVN_POSTGRES_RUNTIME_PASSWORD: ${FAVN_POSTGRES_RUNTIME_PASSWORD}
        volumes:
          - postgres-data:/var/lib/postgresql
          - ./postgres-init.sh:/docker-entrypoint-initdb.d/10-favn-runtime-role.sh:ro
        healthcheck:
          test: ["CMD-SHELL", "pg_isready -U ${FAVN_POSTGRES_ADMIN_USER} -d ${FAVN_POSTGRES_DATABASE}"]
          interval: 2s
          timeout: 3s
          retries: 30
          start_period: 5s
        networks:
          default:
            aliases: [postgres.favn.internal]

      control-plane-ops:
        image: #{project["control_plane_image"]}
        profiles: [operations]
        entrypoint: ["/app/bin/favn_control_plane_ops"]
        read_only: true
        tmpfs:
          - /tmp/favn:rw,noexec,nosuid,nodev,uid=10001,gid=10001,mode=0700
        environment: &control-plane-operations-environment
          FAVN_DEPLOYMENT_MODE: local-development
          FAVN_DATABASE_URL: ${FAVN_POSTGRES_ADMIN_DATABASE_URL}
          FAVN_DATABASE_SSL_MODE: disable
          FAVN_DATABASE_RUNTIME_ROLE: favn_runtime
          FAVN_INSTANCE_ID: #{project["project_name"]}-operations
          FAVN_RUNTIME_INPUT_PIN_KEYS: ${FAVN_RUNTIME_INPUT_PIN_KEYS}
          FAVN_RUNTIME_INPUT_PIN_KEY_VERSION: "1"
          FAVN_WORKSPACE_ID: ${FAVN_WORKSPACE_ID}
          FAVN_WORKSPACE_SLUG: ${FAVN_WORKSPACE_ID}
          FAVN_WORKSPACE_NAME: ${FAVN_WORKSPACE_NAME}
          FAVN_CONTROL_PLANE_NODE: favn_control_plane_ops@control-plane-ops.favn.internal
          FAVN_DISTRIBUTION_COOKIE: ${FAVN_DISTRIBUTION_COOKIE}
          FAVN_BEAM_DISTRIBUTION_PORT: "9102"
          ERL_EPMD_PORT: "4369"
        depends_on:
          postgres:
            condition: service_healthy
        networks:
          default:
            aliases: [control-plane-ops.favn.internal]

      control-plane-verify:
        image: #{project["control_plane_image"]}
        profiles: [operations]
        entrypoint: ["/app/bin/favn_control_plane_ops"]
        read_only: true
        tmpfs:
          - /tmp/favn:rw,noexec,nosuid,nodev,uid=10001,gid=10001,mode=0700
        environment:
          <<: *control-plane-operations-environment
          FAVN_DATABASE_URL: ${FAVN_POSTGRES_RUNTIME_DATABASE_URL}
          FAVN_CONTROL_PLANE_NODE: favn_control_plane_verify@control-plane-verify.favn.internal
          FAVN_BEAM_DISTRIBUTION_PORT: "9103"
        depends_on:
          postgres:
            condition: service_healthy
        networks:
          default:
            aliases: [control-plane-verify.favn.internal]

      runner:
        image: ${FAVN_RUNNER_IMAGE}
        restart: unless-stopped
        read_only: true
        init: true
        stop_grace_period: 3m
        env_file:
          - ./runner.env
        tmpfs:
          - /tmp/favn:rw,noexec,nosuid,nodev,uid=10001,gid=10001,mode=0700
        environment:
          FAVN_RUNNER_NODE: favn_runner@runner.favn.internal
          FAVN_CONTROL_PLANE_NODE: favn_control_plane@control-plane.favn.internal
          FAVN_DISTRIBUTION_COOKIE: ${FAVN_DISTRIBUTION_COOKIE}
          FAVN_BEAM_DISTRIBUTION_PORT: "9100"
          ERL_EPMD_PORT: "4369"
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

      control-plane:
        image: #{project["control_plane_image"]}
        restart: unless-stopped
        read_only: true
        init: true
        stop_grace_period: 3m
        tmpfs:
          - /tmp/favn:rw,noexec,nosuid,nodev,uid=10001,gid=10001,mode=0700
        environment:
          FAVN_DEPLOYMENT_MODE: local-development
          FAVN_DATABASE_URL: ${FAVN_POSTGRES_RUNTIME_DATABASE_URL}
          FAVN_DATABASE_SSL_MODE: disable
          FAVN_DATABASE_POOL_SIZE: "10"
          FAVN_RUNTIME_INPUT_PIN_KEYS: ${FAVN_RUNTIME_INPUT_PIN_KEYS}
          FAVN_RUNTIME_INPUT_PIN_KEY_VERSION: "1"
          FAVN_INSTANCE_ID: #{project["project_name"]}
          FAVN_WORKSPACE_IDS: ${FAVN_WORKSPACE_ID}
          FAVN_ORCHESTRATOR_API_BIND_HOST: 0.0.0.0
          FAVN_ORCHESTRATOR_API_PORT: "4101"
          FAVN_ORCHESTRATOR_API_SERVICE_TOKENS: ${FAVN_ORCHESTRATOR_API_SERVICE_TOKENS}
          FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME: admin
          FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD: ${FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD}
          FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME: Favn Local Admin
          FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES: admin
          FAVN_SCHEDULER_ENABLED: ${FAVN_SCHEDULER_ENABLED}
          FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS: "120000"
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
          postgres:
            condition: service_healthy
          runner:
            condition: service_healthy
        networks:
          default:
            aliases: [control-plane.favn.internal]

    volumes:
      postgres-data:
        name: #{project["postgres_volume_name"]}

    networks:
      default:
        name: #{project["network_name"]}
        driver: bridge
    """
  end

  defp env_file(project, secrets, config) do
    admin_user = "favn_migrator"
    runtime_user = "favn_runtime"
    database = "favn_dev"

    admin_url =
      database_url(admin_user, secrets["postgres_admin_password"], database)

    runtime_url =
      database_url(runtime_user, secrets["postgres_runtime_password"], database)

    runtime_input_pin_keys = JSON.encode!(%{"1" => secrets["runtime_input_pin_key"]})

    values = [
      {"FAVN_RUNNER_IMAGE", "favn-local-runner-#{project["project_name"]}:unbuilt"},
      {"FAVN_POSTGRES_DATABASE", database},
      {"FAVN_POSTGRES_ADMIN_USER", admin_user},
      {"FAVN_POSTGRES_ADMIN_PASSWORD", secrets["postgres_admin_password"]},
      {"FAVN_POSTGRES_RUNTIME_PASSWORD", secrets["postgres_runtime_password"]},
      {"FAVN_POSTGRES_ADMIN_DATABASE_URL", admin_url},
      {"FAVN_POSTGRES_RUNTIME_DATABASE_URL", runtime_url},
      {"FAVN_RUNTIME_INPUT_PIN_KEYS", runtime_input_pin_keys},
      {"FAVN_WORKSPACE_ID", project["workspace_id"]},
      {"FAVN_WORKSPACE_NAME", workspace_display_name(project["workspace_id"])},
      {"FAVN_DISTRIBUTION_COOKIE", secrets["rpc_cookie"]},
      {"FAVN_ORCHESTRATOR_API_SERVICE_TOKENS",
       "local-tooling-v1|platform_reader+platform_operator+platform_admin:#{secrets["service_token"]}"},
      {"FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD", secrets["bootstrap_password"]},
      {"FAVN_VIEW_PUBLIC_ORIGIN", project["view_url"]},
      {"FAVN_VIEW_SECRET_KEY_BASE", secrets["web_session_secret"]},
      {"FAVN_VIEW_PORT", Integer.to_string(project["view_port"])},
      {"FAVN_ORCHESTRATOR_PORT", Integer.to_string(project["orchestrator_port"])},
      {"FAVN_SCHEDULER_ENABLED", if(config.scheduler_enabled, do: "true", else: "false")}
    ]

    Enum.map_join(values, "", fn {key, value} -> "#{key}=#{quote_env(value)}\n" end)
  end

  defp workspace_display_name("local-dev"), do: "Local Development"
  defp workspace_display_name(workspace_id), do: workspace_id

  defp postgres_init_script do
    """
    #!/bin/sh
    set -eu

    psql --set=ON_ERROR_STOP=1 \
      --username "$POSTGRES_USER" \
      --dbname "$POSTGRES_DB" \
      --set=runtime_password="$FAVN_POSTGRES_RUNTIME_PASSWORD" <<'SQL'
    CREATE ROLE favn_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
      PASSWORD :'runtime_password';
    SQL
    """
  end

  defp database_url(user, password, database) do
    encoded_user = URI.encode(user, &URI.char_unreserved?/1)
    encoded_password = URI.encode(password, &URI.char_unreserved?/1)
    "ecto://#{encoded_user}:#{encoded_password}@postgres.favn.internal:5432/#{database}"
  end

  defp quote_env(value) when is_binary(value), do: ComposeEnv.encode_value(value)

  defp validate_runner_environment(environment)
       when map_size(environment) <= @max_runner_environment do
    if Enum.all?(environment, fn
         {key, value} when is_binary(key) and is_binary(value) ->
           Regex.match?(@environment_key, key) and
             byte_size(value) <= @max_environment_value_bytes and
             not String.contains?(value, <<0>>)

         _invalid ->
           false
       end) do
      :ok
    else
      {:error, :invalid_runner_environment}
    end
  end

  defp validate_runner_environment(_environment), do: {:error, :runner_environment_too_large}

  defp replace_env(contents, key, value) do
    prefix = key <> "="
    lines = String.split(contents, "\n", trim: false)

    if Enum.any?(lines, &String.starts_with?(&1, prefix)) do
      updated =
        Enum.map_join(lines, "\n", fn line ->
          if String.starts_with?(line, prefix), do: prefix <> quote_env(value), else: line
        end)

      {:ok, updated}
    else
      {:error, {:compose_env_key_missing, key}}
    end
  end

  defp atomic_write(path, contents, mode \\ 0o644) do
    temporary = path <> ".tmp-#{System.unique_integer([:positive, :monotonic])}"

    result =
      with :ok <- File.write(temporary, contents, [:binary]),
           :ok <- File.chmod(temporary, mode),
           :ok <- File.rename(temporary, path) do
        :ok
      end

    _ = File.rm(temporary)
    result
  end

  defp sha256(value) when is_binary(value),
    do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
end
