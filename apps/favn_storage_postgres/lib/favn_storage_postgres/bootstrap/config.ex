defmodule FavnStoragePostgres.Bootstrap.Config do
  @moduledoc false

  alias FavnStoragePostgres.Bootstrap.Profile
  alias FavnStoragePostgres.Bootstrap.Scram
  alias FavnStoragePostgres.Config, as: DatabaseConfig

  @profile_lifecycles [
    :bootstrap_maintenance,
    :bootstrap_target,
    :migrator_operation,
    :migrator_lock,
    :runtime_operation
  ]
  @base_env_names [
    "FAVN_DEPLOYMENT_MODE",
    "FAVN_DATABASE_SSL_MODE",
    "FAVN_DATABASE_SSL_CA_FILE",
    "FAVN_DATABASE_POOL_SIZE",
    "FAVN_DATABASE_QUEUE_TARGET_MS",
    "FAVN_DATABASE_QUEUE_INTERVAL_MS",
    "FAVN_DATABASE_TIMEOUT_MS",
    "FAVN_INSTANCE_ID"
  ]
  @uuid ~r/\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\z/
  @identifier ~r/\A[a-z_][a-z0-9_]{0,62}\z/

  @enforce_keys [:operation, :target_database, :maintenance_database, :migrator, :runtime]
  defstruct [
    :operation,
    :target_database,
    :maintenance_database,
    :bootstrap,
    :migrator,
    :runtime,
    :workspace
  ]

  @type operation :: :status | :bootstrap | :upgrade
  @type workspace :: %{workspace_id: String.t(), slug: String.t(), display_name: String.t()}
  @type t :: %__MODULE__{
          operation: operation(),
          target_database: String.t(),
          maintenance_database: String.t(),
          bootstrap: Profile.t() | nil,
          migrator: Profile.t(),
          runtime: Profile.t(),
          workspace: workspace() | nil
        }

  @spec from_env(operation(), map()) :: {:ok, t()} | {:error, atom()}
  def from_env(operation, env)
      when operation in [:status, :bootstrap, :upgrade] and is_map(env) do
    with {:ok, runtime} <- profile(env, :runtime, true),
         {:ok, migrator} <- profile(env, :migrator, true),
         {:ok, bootstrap} <- bootstrap_profile(operation, env),
         {:ok, target_database} <- target_database(env, runtime, migrator),
         {:ok, maintenance_database} <-
           database_name(env, "FAVN_DATABASE_MAINTENANCE_NAME", "postgres"),
         {:ok, workspace} <- workspace(operation, env),
         :ok <- distinct_roles(bootstrap, migrator, runtime),
         :ok <- safe_target_roles(migrator, runtime),
         :ok <- distinct_objects(migrator, runtime),
         config <- %__MODULE__{
           operation: operation,
           target_database: target_database,
           maintenance_database: maintenance_database,
           bootstrap: bootstrap,
           migrator: migrator,
           runtime: runtime,
           workspace: workspace
         },
         :ok <- validate_maintenance(config),
         :ok <- validate_topology(config),
         :ok <- validate_connections(config) do
      {:ok, config}
    else
      {:error, _reason} -> {:error, :invalid_database_bootstrap_configuration}
    end
  end

  @spec connection_env(t(), Profile.t(), String.t(), atom()) :: map()
  def connection_env(%__MODULE__{}, %Profile{} = profile, database, lifecycle)
      when lifecycle in @profile_lifecycles do
    Profile.connection_env(profile, database, lifecycle)
  end

  defp bootstrap_profile(:bootstrap, env), do: profile(env, :bootstrap, true)
  defp bootstrap_profile(:status, env), do: profile(env, :bootstrap, false)

  defp bootstrap_profile(:upgrade, env) do
    if profile_present?(env, :bootstrap),
      do: {:error, :bootstrap_profile_not_allowed_for_upgrade},
      else: {:ok, nil}
  end

  defp profile(env, purpose, required?) do
    prefix = "FAVN_DATABASE_" <> (purpose |> Atom.to_string() |> String.upcase())
    mode_name = prefix <> "_AUTH_MODE"
    url_name = prefix <> "_URL"
    username_name = prefix <> "_USERNAME"
    client_id_name = prefix <> "_AZURE_MANAGED_IDENTITY_CLIENT_ID"
    object_id_name = prefix <> "_AZURE_OBJECT_ID"

    fallback_url = if purpose == :runtime, do: Map.get(env, "FAVN_DATABASE_URL"), else: nil
    present? = profile_present?(env, purpose)

    if not required? and not present? do
      {:ok, nil}
    else
      mode = Map.get(env, mode_name, if(Map.get(env, url_name, fallback_url), do: "password"))
      base_env = Map.take(env, @base_env_names)

      case mode do
        "password" ->
          with {:ok, url} <- required_value(Map.get(env, url_name, fallback_url)),
               {:ok, role} <- password_role(url),
               :ok <- reject_present(env, [username_name, client_id_name, object_id_name]) do
            {:ok,
             %Profile{
               purpose: purpose,
               authentication_mode: :password,
               role: role,
               source: {:url, url},
               base_env: base_env
             }}
          end

        "azure_managed_identity" ->
          with {:ok, hostname} <- required_value(Map.get(env, "FAVN_DATABASE_HOST")),
               {:ok, port} <- port(Map.get(env, "FAVN_DATABASE_PORT", "5432")),
               {:ok, role} <- role(Map.get(env, username_name)),
               {:ok, client_id} <- uuid(Map.get(env, client_id_name)),
               {:ok, object_id} <- object_id(purpose, Map.get(env, object_id_name)),
               :ok <- reject_present(env, [url_name]) do
            {:ok,
             %Profile{
               purpose: purpose,
               authentication_mode: :azure_managed_identity,
               role: role,
               source: {:azure, hostname, port, client_id},
               object_id: object_id,
               base_env: base_env
             }}
          end

        _invalid ->
          {:error, :invalid_authentication_mode}
      end
    end
  end

  defp profile_present?(env, purpose) do
    prefix = "FAVN_DATABASE_" <> (purpose |> Atom.to_string() |> String.upcase())

    Enum.any?(
      [
        prefix <> "_AUTH_MODE",
        prefix <> "_URL",
        prefix <> "_USERNAME",
        prefix <> "_AZURE_MANAGED_IDENTITY_CLIENT_ID",
        prefix <> "_AZURE_OBJECT_ID"
      ],
      &Map.has_key?(env, &1)
    )
  end

  defp target_database(env, runtime, migrator) do
    configured = Map.get(env, "FAVN_DATABASE_NAME")
    runtime_database = profile_database(runtime)
    migrator_database = profile_database(migrator)
    target = configured || runtime_database || migrator_database

    with {:ok, target} <- database_name(%{"value" => target}, "value", nil),
         true <- is_nil(runtime_database) or runtime_database == target,
         true <- is_nil(migrator_database) or migrator_database == target do
      {:ok, target}
    else
      _invalid -> {:error, :database_profile_mismatch}
    end
  end

  defp profile_database(%Profile{} = profile), do: Profile.database(profile)

  defp database_name(env, name, default) do
    case Map.get(env, name, default) do
      value when is_binary(value) and value != "" and byte_size(value) <= 63 ->
        if Regex.match?(@identifier, value), do: {:ok, value}, else: {:error, :invalid_database}

      _invalid ->
        {:error, :invalid_database}
    end
  end

  defp workspace(:upgrade, _env), do: {:ok, nil}

  defp workspace(operation, env) when operation in [:bootstrap, :status] do
    id = Map.get(env, "FAVN_WORKSPACE_ID")

    if operation == :status and is_nil(id) do
      {:ok, nil}
    else
      slug = Map.get(env, "FAVN_WORKSPACE_SLUG", id)
      display_name = Map.get(env, "FAVN_WORKSPACE_NAME", slug)

      if valid_workspace_value?(id) and valid_workspace_slug?(slug) and
           valid_workspace_value?(display_name) do
        {:ok, %{workspace_id: id, slug: slug, display_name: display_name}}
      else
        {:error, :invalid_workspace}
      end
    end
  end

  defp distinct_roles(nil, migrator, runtime), do: distinct?([migrator.role, runtime.role])

  defp distinct_roles(bootstrap, migrator, runtime),
    do: distinct?([bootstrap.role, migrator.role, runtime.role])

  defp distinct_objects(%Profile{object_id: nil}, %Profile{object_id: nil}), do: :ok

  defp distinct_objects(migrator, runtime) do
    if migrator.object_id != runtime.object_id,
      do: :ok,
      else: {:error, :duplicate_external_identity}
  end

  defp distinct?(values) do
    if Enum.uniq(values) == values, do: :ok, else: {:error, :duplicate_database_role}
  end

  defp safe_target_roles(migrator, runtime) do
    if Enum.any?([migrator.role, runtime.role], &protected_target_role?/1),
      do: {:error, :protected_database_role},
      else: :ok
  end

  defp protected_target_role?(role) do
    role in ["postgres", "azure_pg_admin", "azuresu", "rds_superuser", "cloudsqlsuperuser"] or
      String.starts_with?(role, "pg_")
  end

  defp validate_connections(config) do
    checks =
      [
        {config.migrator, config.target_database, :migrator_operation},
        {config.runtime, config.target_database, :runtime_operation}
      ] ++
        if(config.bootstrap,
          do: [{config.bootstrap, config.maintenance_database, :bootstrap_maintenance}],
          else: []
        )

    Enum.reduce_while(checks, :ok, fn {profile, database, lifecycle}, :ok ->
      env = connection_env(config, profile, database, lifecycle)

      case DatabaseConfig.connection_config_from_env(env) do
        {:ok, _connection_config} -> {:cont, :ok}
        {:error, _reason} -> {:halt, {:error, :invalid_connection_profile}}
      end
    end)
  end

  defp validate_maintenance(%__MODULE__{operation: :bootstrap} = config)
       when config.target_database == config.maintenance_database,
       do: {:error, :target_database_cannot_be_maintenance_database}

  defp validate_maintenance(%__MODULE__{operation: operation, bootstrap: %Profile{}} = config)
       when operation in [:bootstrap, :status] do
    azure_mapping? =
      Enum.any?([config.migrator, config.runtime], fn profile ->
        profile.authentication_mode == :azure_managed_identity
      end)

    if azure_mapping? and config.maintenance_database != "postgres",
      do: {:error, :azure_mapping_requires_postgres_database},
      else: :ok
  end

  defp validate_maintenance(_config), do: :ok

  defp validate_topology(config) do
    profiles = Enum.reject([config.bootstrap, config.migrator, config.runtime], &is_nil/1)

    with {:ok, endpoints} <- profile_endpoints(profiles),
         true <- length(Enum.uniq(endpoints)) == 1,
         :ok <- validate_profile_database(config.bootstrap, config.maintenance_database),
         :ok <- validate_profile_database(config.migrator, config.target_database),
         :ok <- validate_profile_database(config.runtime, config.target_database) do
      :ok
    else
      _mismatch -> {:error, :database_topology_mismatch}
    end
  end

  defp profile_endpoints(profiles) do
    Enum.reduce_while(profiles, {:ok, []}, fn profile, {:ok, endpoints} ->
      case Profile.endpoint(profile) do
        {:ok, endpoint} -> {:cont, {:ok, [endpoint | endpoints]}}
        {:error, _reason} -> {:halt, {:error, :invalid_endpoint}}
      end
    end)
  end

  defp validate_profile_database(nil, _expected), do: :ok

  defp validate_profile_database(profile, expected) do
    case Profile.database(profile) do
      nil -> :ok
      ^expected -> :ok
      _other -> {:error, :database_profile_mismatch}
    end
  end

  defp password_role(url) do
    try do
      options = Ecto.Repo.Supervisor.parse_url(url)

      with role when is_binary(role) <- Keyword.get(options, :username),
           :ok <- valid_role(role),
           password when is_binary(password) <- Keyword.get(options, :password),
           true <- Scram.password_supported?(password),
           database when is_binary(database) and database != "" <- Keyword.get(options, :database) do
        {:ok, role}
      else
        _invalid -> {:error, :invalid_password_url}
      end
    rescue
      _error -> {:error, :invalid_password_url}
    end
  end

  defp role(value) do
    with {:ok, value} <- required_value(value), :ok <- valid_role(value), do: {:ok, value}
  end

  defp valid_role(value) do
    if Regex.match?(@identifier, value), do: :ok, else: {:error, :invalid_role}
  end

  defp uuid(value) do
    with {:ok, value} <- required_value(value), true <- Regex.match?(@uuid, value) do
      {:ok, String.downcase(value)}
    else
      _invalid -> {:error, :invalid_uuid}
    end
  end

  defp object_id(:bootstrap, nil), do: {:ok, nil}
  defp object_id(_purpose, value), do: uuid(value)

  defp port(value) when is_binary(value) do
    case Integer.parse(value) do
      {port, ""} when port in 1..65_535 -> {:ok, port}
      _invalid -> {:error, :invalid_port}
    end
  end

  defp port(_value), do: {:error, :invalid_port}

  defp required_value(value) when is_binary(value) and value != "", do: {:ok, value}
  defp required_value(_value), do: {:error, :missing_value}

  defp reject_present(env, names) do
    if Enum.any?(names, &Map.has_key?(env, &1)),
      do: {:error, :ambiguous_profile},
      else: :ok
  end

  defp valid_workspace_value?(value),
    do: is_binary(value) and value != "" and byte_size(value) <= 255

  defp valid_workspace_slug?(value),
    do: is_binary(value) and Regex.match?(~r/\A[a-z0-9][a-z0-9-]{0,62}\z/, value)
end
