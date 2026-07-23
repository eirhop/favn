defmodule Favn.Dev.ComposeProject do
  @moduledoc """
  Owns generated local Compose interpolation state, never deployment YAML.

  The selected Compose file is consumer-owned. This module writes only ignored
  credentials, selected image references, bounded runner environment, and the local
  PostgreSQL role bootstrap below `.favn/`.
  """

  alias Favn.Dev.{ComposeEnv, Config, Paths}

  @postgres_image "postgres:18"
  @safe_identifier ~r/\A[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\z/
  @environment_key ~r/\A[A-Za-z_][A-Za-z0-9_]*\z/
  @max_runner_environment 512
  @max_environment_value_bytes 65_536

  @type project :: %{
          required(String.t()) => String.t() | pos_integer()
        }

  @doc "Returns the supported floating PostgreSQL 18 image tag."
  @spec postgres_image() :: String.t()
  def postgres_image, do: @postgres_image

  @doc "Writes generated local interpolation and bootstrap state."
  @spec write(map(), map(), Config.t(), keyword()) :: {:ok, project()} | {:error, term()}
  def write(install, secrets, %Config{} = config, opts)
      when is_map(install) and is_map(secrets) and is_list(opts) do
    root_dir = opts |> Paths.root_dir() |> Path.expand()

    with {:ok, control_plane_reference} <- fetch_install_reference(install),
         :ok <- validate_inputs(secrets, config),
         {:ok, uid, gid} <- project_owner(root_dir),
         project = project(root_dir, control_plane_reference, config, uid, gid, opts),
         :ok <- File.mkdir_p(Paths.compose_dir(root_dir)),
         :ok <- File.mkdir_p(Paths.local_data_dir(root_dir)),
         :ok <- atomic_write(project["env_path"], env_file(project, secrets, config), 0o600),
         :ok <- ensure_runner_env(project["runner_env_path"]),
         :ok <- ensure_file(project["postgres_init_path"], postgres_init_script(), 0o555) do
      {:ok, project}
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

  @doc "Returns a non-secret identity for the current bounded runner environment."
  @spec runner_environment_identity(project()) :: {:ok, String.t()} | {:error, term()}
  def runner_environment_identity(project) when is_map(project) do
    case File.read(Map.fetch!(project, "runner_env_path")) do
      {:ok, contents} -> {:ok, sha256(contents)}
      {:error, reason} -> {:error, {:runner_environment_unavailable, reason}}
    end
  end

  @doc "Atomically changes the scheduler flag consumed on control-plane recreation."
  @spec put_scheduler_enabled(project(), boolean()) :: :ok | {:error, term()}
  def put_scheduler_enabled(project, enabled?) when is_map(project) and is_boolean(enabled?) do
    replace_env_file(project, "FAVN_SCHEDULER_ENABLED", if(enabled?, do: "true", else: "false"))
  end

  @doc false
  @spec put_runner_image(project(), String.t()) :: :ok | {:error, term()}
  def put_runner_image(project, image)
      when is_map(project) and is_binary(image) and image != "" do
    replace_env_file(project, "FAVN_RUNNER_IMAGE", image)
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

  defp project(root_dir, control_plane_reference, config, uid, gid, opts) do
    project_name = project_name(root_dir)
    compose_file = Keyword.fetch!(opts, :compose_file)

    %{
      "project_name" => project_name,
      "postgres_volume_name" => project_name <> "-postgres-data",
      "compose_path" => compose_file,
      "env_path" => Paths.compose_env_path(root_dir),
      "runner_env_path" => Paths.compose_runner_env_path(root_dir),
      "postgres_init_path" => Paths.compose_postgres_init_path(root_dir),
      "control_plane_image" => control_plane_reference,
      "runner_image" => config.runner_image,
      "view_url" => "http://127.0.0.1:#{config.web_port}",
      "orchestrator_url" => "http://127.0.0.1:#{config.orchestrator_port}",
      "view_port" => config.web_port,
      "orchestrator_port" => config.orchestrator_port,
      "workspace_id" => config.workspace_id,
      "runner_uid" => uid,
      "runner_gid" => gid
    }
  end

  defp project_owner(root_dir) do
    case File.stat(root_dir) do
      {:ok, %{uid: 0}} ->
        {:error, {:root_owned_local_project, root_dir}}

      {:ok, %{uid: uid, gid: gid}} when is_integer(uid) and uid > 0 and is_integer(gid) ->
        {:ok, uid, gid}

      {:ok, _unsupported} ->
        {:error, {:project_owner_unavailable, root_dir}}

      {:error, reason} ->
        {:error, {:project_owner_unavailable, root_dir, reason}}
    end
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

  defp env_file(project, secrets, config) do
    admin_user = "favn_migrator"
    runtime_user = "favn_runtime"
    database = "favn_dev"
    runtime_input_pin_keys = JSON.encode!(%{"1" => secrets["runtime_input_pin_key"]})

    values = [
      {"FAVN_COMPOSE_PROJECT", project["project_name"]},
      {"FAVN_POSTGRES_VOLUME", project["postgres_volume_name"]},
      {"FAVN_RUNNER_IMAGE", project["runner_image"] || ""},
      {"FAVN_RUNNER_ENV_FILE", project["runner_env_path"]},
      {"FAVN_RUNNER_UID", Integer.to_string(project["runner_uid"])},
      {"FAVN_RUNNER_GID", Integer.to_string(project["runner_gid"])},
      {"FAVN_POSTGRES_DATABASE", database},
      {"FAVN_POSTGRES_ADMIN_USER", admin_user},
      {"FAVN_POSTGRES_ADMIN_PASSWORD", secrets["postgres_admin_password"]},
      {"FAVN_POSTGRES_RUNTIME_PASSWORD", secrets["postgres_runtime_password"]},
      {"FAVN_POSTGRES_ADMIN_DATABASE_URL",
       database_url(admin_user, secrets["postgres_admin_password"], database)},
      {"FAVN_POSTGRES_RUNTIME_DATABASE_URL",
       database_url(runtime_user, secrets["postgres_runtime_password"], database)},
      {"FAVN_CONTROL_PLANE_IMAGE", project["control_plane_image"]},
      {"FAVN_RUNTIME_INPUT_PIN_KEYS", runtime_input_pin_keys},
      {"FAVN_RUNTIME_INPUT_PIN_KEY_VERSION", "1"},
      {"FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS", "120000"},
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

  defp ensure_runner_env(path) do
    case File.lstat(path) do
      {:ok, %{type: :regular}} -> File.chmod(path, 0o600)
      {:error, :enoent} -> atomic_write(path, "", 0o600)
      _unsafe -> {:error, {:unsafe_runner_environment_file, path}}
    end
  end

  defp ensure_file(path, contents, mode) do
    case File.lstat(path) do
      {:error, :enoent} ->
        atomic_write(path, contents, mode)

      {:ok, %{type: :regular}} ->
        with {:ok, existing} <- File.read(path) do
          if existing == contents,
            do: File.chmod(path, mode),
            else: rewrite_existing(path, contents, mode)
        end

      _unsafe ->
        {:error, {:unsafe_generated_file, path}}
    end
  end

  defp rewrite_existing(path, contents, mode) do
    with :ok <- File.write(path, contents, [:binary]),
         :ok <- File.chmod(path, mode) do
      :ok
    end
  end

  defp workspace_display_name("local-dev"), do: "Local Development"
  defp workspace_display_name(workspace_id), do: workspace_id

  defp postgres_init_script do
    """
    #!/bin/sh
    set -eu

    psql --set=ON_ERROR_STOP=1 \\
      --username "$POSTGRES_USER" \\
      --dbname "$POSTGRES_DB" \\
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

  defp replace_env_file(project, key, value) do
    path = Map.get(project, "env_path") || Map.fetch!(project, :env_file)

    with {:ok, contents} <- File.read(path),
         {:ok, updated} <- replace_env(contents, key, value),
         :ok <- atomic_write(path, updated, 0o600) do
      :ok
    end
  end

  defp replace_env(contents, key, value) do
    prefix = key <> "="
    lines = String.split(contents, "\n", trim: false)

    if Enum.any?(lines, &String.starts_with?(&1, prefix)) do
      {:ok,
       Enum.map_join(lines, "\n", fn line ->
         if String.starts_with?(line, prefix), do: prefix <> quote_env(value), else: line
       end)}
    else
      {:error, {:compose_env_key_missing, key}}
    end
  end

  defp atomic_write(path, contents, mode) do
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
