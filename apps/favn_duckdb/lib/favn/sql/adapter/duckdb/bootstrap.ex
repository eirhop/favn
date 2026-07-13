defmodule Favn.SQL.Adapter.DuckDB.Bootstrap do
  @moduledoc false

  alias Favn.Connection.Resolved
  alias Favn.Azure.{PostgresEntraToken, TokenError}
  alias Favn.SQL.Adapter.DuckDB
  alias Favn.SQL.Error

  @config_key :duckdb
  @ducklake_sqlite_prefix "ducklake:sqlite:"
  @old_config_message "DuckDB connection config now uses open: [database: ...] and duckdb: [...]; move duckdb_bootstrap entries under duckdb and move write_concurrency under duckdb.attach.<catalog>.write_concurrency"
  @azure_transport_option_type_values ~w(default curl)
  @azure_credential_chain_values ~w(cli managed_identity workload_identity env default)
  @postgres_sslmodes ~w(disable allow prefer require verify-ca verify-full)

  @type step :: %{id: String.t(), kind: atom(), statement: iodata(), safe_statement: iodata()}

  @spec config_schema_fields() :: [Favn.Connection.Definition.field()]
  def config_schema_fields do
    [
      %{key: :open, required: true, type: {:custom, &validate_open/1}},
      %{key: @config_key, type: {:custom, &validate_config/1}, secret: true},
      %{key: :database, type: {:custom, &reject_old_key/1}},
      %{key: :duckdb_bootstrap, type: {:custom, &reject_old_key/1}},
      %{key: :write_concurrency, type: {:custom, &reject_old_key/1}}
    ]
  end

  @spec schema_field() :: Favn.Connection.Definition.field()
  def schema_field do
    %{key: @config_key, type: {:custom, &validate_config/1}, secret: true}
  end

  @spec database(Resolved.t()) :: {:ok, String.t()} | {:error, Error.t()}
  def database(%Resolved{} = resolved) do
    config = resolved.config || %{}

    with :ok <- reject_old_runtime_keys(resolved, config) do
      case normalize_open(Map.get(config, :open)) do
        {:ok, %{database: database}} -> {:ok, database}
        {:error, reason} -> {:error, config_error(resolved, reason)}
      end
    end
  end

  @spec validate_open(term()) :: :ok | {:error, term()}
  def validate_open(value) do
    case normalize_open(value) do
      {:ok, _open} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  def reject_old_key(_value), do: {:error, @old_config_message}

  @spec validate_config(term()) :: :ok | {:error, term()}
  def validate_config(nil), do: :ok
  def validate_config([]), do: :ok

  def validate_config(value) when is_map(value) or is_list(value) do
    case normalize_config(value) do
      {:ok, _config} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  def validate_config(_value), do: {:error, :expected_duckdb_keyword_or_map}

  @spec run(DuckDB.Conn.t(), Resolved.t(), keyword()) :: :ok | {:error, Error.t()}
  def run(%DuckDB.Conn{} = conn, %Resolved{} = resolved, opts) do
    with {:ok, steps} <- build_steps(resolved, opts) do
      execute_steps(conn, resolved, steps, opts)
    end
  end

  @spec build_steps(Resolved.t()) :: {:ok, [step()]} | {:error, Error.t()}
  def build_steps(%Resolved{} = resolved), do: build_steps(resolved, [])

  @spec build_steps(Resolved.t(), keyword()) :: {:ok, [step()]} | {:error, Error.t()}
  def build_steps(%Resolved{} = resolved, opts) do
    config = resolved.config || %{}

    with :ok <- reject_old_runtime_keys(resolved, config) do
      case Map.get(config, @config_key) do
        nil ->
          {:ok, []}

        [] ->
          {:ok, []}

        config ->
          config
          |> normalize_config()
          |> case do
            {:ok, normalized} -> build_normalized_steps(normalized, opts)
            {:error, reason} -> {:error, config_error(resolved, reason)}
          end
      end
    end
  end

  defp build_normalized_steps(normalized, opts) do
    scoped = scope_catalogs(normalized, Keyword.get(opts, :required_catalogs))

    {:ok, steps(scoped)}
  end

  defp execute_steps(_conn, _resolved, [], _opts), do: :ok

  defp execute_steps(%DuckDB.Conn{} = conn, %Resolved{} = resolved, steps, opts) do
    Enum.reduce_while(steps, :ok, fn step, :ok ->
      case materialize_step(step, opts) do
        {:ok, executable_step} ->
          case DuckDB.execute(conn, executable_step.statement, []) do
            {:ok, _result} ->
              {:cont, :ok}

            {:error, %Error{} = error} ->
              {:halt, {:error, bootstrap_error(resolved, executable_step, error)}}
          end

        {:error, %TokenError{} = error} ->
          {:halt, {:error, token_error(resolved, step, error)}}
      end
    end)
  end

  defp normalize_open(open) do
    with {:ok, open} <- normalize_keyword_config(open, :open) do
      case Keyword.fetch(open, :database) do
        {:ok, ":memory:"} ->
          {:ok, %{database: ":memory:"}}

        {:ok, database} when is_binary(database) and database != "" ->
          {:ok, %{database: database}}

        {:ok, _database} ->
          {:error, {:invalid_open_database, :expected_memory_or_non_empty_path}}

        :error ->
          {:error, {:missing_open_field, :database}}
      end
    end
  end

  defp normalize_config(config) do
    with {:ok, normalized} <- normalize_keyword_config(config, :bootstrap),
         :ok <- validate_duckdb_keys(normalized),
         {:ok, load} <- normalize_extension_list(Keyword.get(normalized, :load, [])),
         {:ok, settings} <- normalize_settings(Keyword.get(normalized, :settings, [])),
         {:ok, secrets} <- normalize_secrets(Keyword.get(normalized, :secrets, [])),
         {:ok, attach} <- normalize_attach(Keyword.get(normalized, :attach)),
         :ok <- validate_attach_secrets(attach, secrets),
         {:ok, use_catalog} <- normalize_optional_identifier(Keyword.get(normalized, :use)),
         :ok <- validate_use_catalog(use_catalog, attach) do
      {:ok,
       %{
         load: load,
         settings: settings,
         secrets: secrets,
         attach: attach,
         use: use_catalog
       }}
    end
  end

  defp normalize_extension_list(values) when is_list(values) do
    values
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
      case normalize_extension_name(value) do
        {:ok, name} -> {:cont, {:ok, [name | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, names} -> {:ok, Enum.reverse(names)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_extension_list(_values), do: {:error, :invalid_extension_list}

  defp normalize_extension_name(name), do: normalize_identifier(name)

  defp normalize_settings(config) do
    with {:ok, settings} <- normalize_keyword_config(config, :settings) do
      settings
      |> Enum.reduce_while({:ok, []}, fn {name, value}, {:ok, acc} ->
        case normalize_setting(name, value) do
          {:ok, setting} -> {:cont, {:ok, [setting | acc]}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
      |> case do
        {:ok, settings} -> {:ok, Enum.reverse(settings)}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp normalize_setting(:azure_transport_option_type, value) do
    case normalize_azure_transport_option_type(value) do
      {:ok, normalized} -> {:ok, %{name: "azure_transport_option_type", value: normalized}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_setting(name, _value), do: {:error, {:unsupported_setting, name}}

  defp normalize_azure_transport_option_type(value) when is_atom(value) do
    value
    |> Atom.to_string()
    |> normalize_azure_transport_option_type()
  end

  defp normalize_azure_transport_option_type(value) when is_binary(value) do
    if value in @azure_transport_option_type_values do
      {:ok, value}
    else
      {:error, {:invalid_setting_value, :azure_transport_option_type, value}}
    end
  end

  defp normalize_azure_transport_option_type(value),
    do: {:error, {:invalid_setting_value, :azure_transport_option_type, value}}

  defp normalize_secrets(config) do
    with {:ok, secrets} <- normalize_keyword_config(config, :secrets) do
      secrets
      |> Enum.reduce_while({:ok, []}, fn {name, secret_config}, {:ok, acc} ->
        with {:ok, identifier} <- normalize_identifier(name),
             {:ok, secret} <- normalize_secret(identifier, secret_config) do
          {:cont, {:ok, [secret | acc]}}
        else
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
      |> case do
        {:ok, secrets} -> {:ok, Enum.reverse(secrets)}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp normalize_secret(name, config) do
    with {:ok, secret} <- normalize_keyword_config(config, {:secret, name}) do
      normalize_secret_config(name, secret)
    end
  end

  defp normalize_secret_config(name, secret) do
    case {Keyword.get(secret, :type), Keyword.get(secret, :provider)} do
      {:azure, :credential_chain} ->
        with {:ok, account_name} <- fetch_secret_value(secret, name, :account_name),
             {:ok, chain} <- normalize_azure_chain(Keyword.get(secret, :chain)),
             {:ok, scope} <- normalize_azure_scope(Keyword.get(secret, :scope)) do
          {:ok,
           %{
             name: name,
             type: :azure,
             provider: :credential_chain,
             account_name: account_name,
             chain: chain,
             scope: scope
           }}
        end

      {:postgres, nil} ->
        with {:ok, host} <- fetch_secret_value(secret, name, :host),
             {:ok, port} <- normalize_postgres_port(Keyword.get(secret, :port), name),
             {:ok, database} <- fetch_secret_value(secret, name, :database),
             {:ok, user} <- fetch_secret_value(secret, name, :user),
             {:ok, password} <- normalize_postgres_password(secret, name),
             {:ok, auth} <- normalize_postgres_auth(Keyword.get(secret, :auth), name),
             {:ok, sslmode} <- normalize_postgres_sslmode(Keyword.get(secret, :sslmode)) do
          if password && auth do
            {:error, {:conflicting_secret_fields, name, [:password, :auth]}}
          else
            {:ok,
             %{
               name: name,
               type: :postgres,
               host: host,
               port: port,
               database: database,
               user: user,
               password: password,
               auth: auth,
               sslmode: sslmode
             }}
          end
        end

      other ->
        {:error, {:unsupported_secret, name, other}}
    end
  end

  defp normalize_postgres_password(secret, name) do
    normalize_optional_secret_string(Keyword.get(secret, :password), name, :password)
  end

  defp normalize_postgres_auth(nil, _name), do: {:ok, nil}

  defp normalize_postgres_auth(auth, name) when is_map(auth) or is_list(auth) do
    with {:ok, auth} <- normalize_keyword_config(auth, {:postgres_auth, name}) do
      case {Keyword.get(auth, :type), Keyword.get(auth, :provider)} do
        {:azure_postgres_entra, :managed_identity} ->
          with {:ok, endpoint} <-
                 normalize_managed_identity_endpoint(Keyword.get(auth, :endpoint, :auto)) do
            {:ok,
             [
               type: :azure_postgres_entra,
               provider: :managed_identity,
               client_id: Keyword.get(auth, :client_id),
               endpoint: endpoint
             ]}
          end

        {:azure_postgres_entra, :azure_cli} ->
          {:ok, [type: :azure_postgres_entra, provider: :azure_cli]}

        other ->
          {:error, {:unsupported_postgres_auth, name, other}}
      end
    end
  end

  defp normalize_postgres_auth(_auth, name), do: {:error, {:invalid_secret_field, name, :auth}}

  defp normalize_managed_identity_endpoint(endpoint)
       when endpoint in [:auto, :imds, :azure_app_service],
       do: {:ok, endpoint}

  defp normalize_managed_identity_endpoint(endpoint),
    do: {:error, {:invalid_managed_identity_endpoint, endpoint}}

  defp materialize_step(%{postgres_secret: secret, postgres_auth: auth}, opts)
       when is_list(auth) do
    token_opts =
      case Keyword.get(opts, :azure_token_provider_module) do
        nil -> []
        provider_module -> [provider_module: provider_module]
      end

    case PostgresEntraToken.fetch_token(auth, token_opts) do
      {:ok, token} -> {:ok, postgres_secret_step(secret, token.access_token, nil)}
      {:error, %TokenError{} = error} -> {:error, error}
    end
  end

  defp materialize_step(step, _opts), do: {:ok, step}

  defp normalize_attach(nil), do: {:ok, []}
  defp normalize_attach([]), do: {:ok, []}

  defp normalize_attach(config) when is_map(config), do: normalize_attach(Map.to_list(config))

  defp normalize_attach(config) when is_list(config) do
    if Keyword.keyword?(config) do
      config
      |> Enum.reduce_while({:ok, []}, fn {name, attach_config}, {:ok, acc} ->
        case normalize_single_attach(name, attach_config) do
          {:ok, attach} -> {:cont, {:ok, [attach | acc]}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
      |> case do
        {:ok, attach} -> {:ok, Enum.reverse(attach)}
        {:error, reason} -> {:error, reason}
      end
    else
      {:error, :invalid_attach_catalogs}
    end
  end

  defp normalize_attach(_config), do: {:error, :invalid_attach_catalogs}

  defp normalize_single_attach(catalog, config) do
    with {:ok, name} <- normalize_identifier(catalog),
         {:ok, attach} <- normalize_keyword_config(config, {:attach, name}),
         type <- Keyword.get(attach, :type) do
      case type do
        :duckdb ->
          with {:ok, path} <- fetch_present_value(attach, :path),
               {:ok, write_concurrency} <-
                 normalize_write_concurrency(Keyword.get(attach, :write_concurrency, 1)) do
            {:ok, %{name: name, type: :duckdb, path: path, write_concurrency: write_concurrency}}
          end

        :ducklake ->
          with {:ok, metadata} <- fetch_present_value(attach, :metadata),
               {:ok, meta_secret} <- normalize_ducklake_meta_secret(metadata, attach),
               {:ok, data_path} <- fetch_present_value(attach, :data_path),
               {:ok, write_concurrency} <-
                 normalize_write_concurrency(Keyword.get(attach, :write_concurrency, :unlimited)) do
            {:ok,
             %{
               name: name,
               type: :ducklake,
               metadata: metadata,
               meta_secret: meta_secret,
               data_path: data_path,
               write_concurrency: write_concurrency
             }}
          end

        other ->
          {:error, {:unsupported_attach_type, other}}
      end
    end
  end

  defp fetch_secret_value(keyword, name, key) do
    case Keyword.fetch(keyword, key) do
      {:ok, value} ->
        if present_string?(value) do
          {:ok, value}
        else
          {:error, {:missing_secret_field, name, key}}
        end

      :error ->
        {:error, {:missing_secret_field, name, key}}
    end
  end

  defp normalize_azure_chain(nil), do: {:ok, nil}
  defp normalize_azure_chain([]), do: {:error, {:invalid_azure_credential_chain, []}}

  defp normalize_azure_chain(chain) when is_list(chain) do
    chain
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
      case normalize_azure_chain_value(value) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_azure_chain(chain), do: normalize_azure_chain([chain])

  defp normalize_azure_chain_value(value) when is_atom(value),
    do: normalize_azure_chain_value(Atom.to_string(value))

  defp normalize_azure_chain_value(value) when is_binary(value) do
    if value in @azure_credential_chain_values do
      {:ok, value}
    else
      {:error, {:invalid_azure_credential_chain, value}}
    end
  end

  defp normalize_azure_chain_value(value), do: {:error, {:invalid_azure_credential_chain, value}}

  defp normalize_azure_scope(nil), do: {:ok, nil}

  defp normalize_azure_scope(scope) when is_binary(scope) and scope != "" do
    if String.ends_with?(scope, "/") do
      {:ok, scope}
    else
      {:error, {:invalid_azure_scope, :missing_trailing_slash}}
    end
  end

  defp normalize_azure_scope(scope), do: {:error, {:invalid_azure_scope, scope}}

  defp normalize_postgres_port(port, _name) when is_integer(port) and port in 1..65_535,
    do: {:ok, port}

  defp normalize_postgres_port(_port, name), do: {:error, {:missing_secret_field, name, :port}}

  defp normalize_optional_secret_string(nil, _name, _key), do: {:ok, nil}

  defp normalize_optional_secret_string(value, _name, _key) when is_binary(value),
    do: {:ok, value}

  defp normalize_optional_secret_string(_value, name, key),
    do: {:error, {:invalid_secret_field, name, key}}

  defp normalize_postgres_sslmode(nil), do: {:ok, nil}

  defp normalize_postgres_sslmode(value) when is_atom(value) do
    value
    |> Atom.to_string()
    |> String.replace("_", "-")
    |> normalize_postgres_sslmode()
  end

  defp normalize_postgres_sslmode(value) when is_binary(value) do
    if value in @postgres_sslmodes do
      {:ok, value}
    else
      {:error, {:invalid_postgres_sslmode, value}}
    end
  end

  defp normalize_postgres_sslmode(value), do: {:error, {:invalid_postgres_sslmode, value}}

  defp normalize_optional_identifier(nil), do: {:ok, nil}
  defp normalize_optional_identifier(value), do: normalize_identifier(value)

  defp validate_duckdb_keys(keyword) do
    allowed = [:load, :settings, :secrets, :attach, :use]

    case Enum.find(Keyword.keys(keyword), &(&1 not in allowed)) do
      nil -> :ok
      key -> {:error, {:unsupported_duckdb_key, key}}
    end
  end

  defp normalize_write_concurrency(:unlimited), do: {:ok, :unlimited}
  defp normalize_write_concurrency(:single), do: {:ok, 1}
  defp normalize_write_concurrency(1), do: {:ok, 1}
  defp normalize_write_concurrency(value) when is_integer(value) and value > 0, do: {:ok, value}
  defp normalize_write_concurrency(value), do: {:error, {:invalid_write_concurrency, value}}

  defp normalize_ducklake_meta_secret(
         @ducklake_sqlite_prefix <> sqlite_path,
         _attach
       ) do
    if String.trim(sqlite_path) == "",
      do: {:error, :empty_ducklake_sqlite_metadata_path},
      else: {:ok, nil}
  end

  defp normalize_ducklake_meta_secret(_metadata, attach) do
    case Keyword.fetch(attach, :meta_secret) do
      {:ok, meta_secret} -> normalize_identifier(meta_secret)
      :error -> {:error, {:missing_attach_field, :meta_secret}}
    end
  end

  defp validate_attach_secrets(attach, secrets) do
    postgres_secret_names =
      secrets
      |> Enum.filter(&(&1.type == :postgres))
      |> MapSet.new(& &1.name)

    Enum.reduce_while(attach, :ok, fn
      %{type: :ducklake, meta_secret: nil}, :ok ->
        {:cont, :ok}

      %{type: :ducklake, meta_secret: secret, name: name}, :ok ->
        if MapSet.member?(postgres_secret_names, secret) do
          {:cont, :ok}
        else
          {:halt, {:error, {:unknown_ducklake_meta_secret, name, secret}}}
        end

      _attach, :ok ->
        {:cont, :ok}
    end)
  end

  defp validate_use_catalog(nil, _attach), do: :ok

  defp validate_use_catalog(use_catalog, attach) do
    if Enum.any?(attach, &(&1.name == use_catalog)) do
      :ok
    else
      {:error, {:unknown_use_catalog, use_catalog}}
    end
  end

  defp scope_catalogs(normalized, nil), do: normalized

  defp scope_catalogs(
         %{attach: attach, secrets: secrets, use: use_catalog} = normalized,
         required_catalogs
       ) do
    required = MapSet.new(List.wrap(required_catalogs), &to_string/1)
    attach = Enum.filter(attach, &MapSet.member?(required, &1.name))
    secrets = filter_scoped_secrets(secrets, attach)

    use_catalog =
      if use_catalog && Enum.any?(attach, &(&1.name == use_catalog)) do
        use_catalog
      end

    %{normalized | attach: attach, secrets: secrets, use: use_catalog}
  end

  defp filter_scoped_secrets(secrets, attach) do
    meta_secret_names = MapSet.new(attach, &Map.get(&1, :meta_secret))

    Enum.filter(secrets, fn
      %{type: :postgres, name: name} ->
        MapSet.member?(meta_secret_names, name)

      %{type: :azure, scope: nil} ->
        attach != []

      %{type: :azure, scope: scope} when is_binary(scope) ->
        Enum.any?(attach, fn
          %{data_path: data_path} when is_binary(data_path) ->
            String.starts_with?(data_path, scope)

          _attach ->
            false
        end)

      _secret ->
        false
    end)
  end

  defp normalize_keyword_config(nil, _context), do: {:ok, []}

  defp normalize_keyword_config(config, _context) when is_map(config) do
    if Enum.all?(Map.keys(config), &is_atom/1) do
      {:ok, Map.to_list(config)}
    else
      {:error, :invalid_bootstrap_map_keys}
    end
  end

  defp normalize_keyword_config(config, _context) when is_list(config) do
    if Keyword.keyword?(config), do: {:ok, config}, else: {:error, :invalid_bootstrap_keyword}
  end

  defp normalize_keyword_config(_config, context),
    do: {:error, {:invalid_bootstrap_config, context}}

  defp fetch_present_value(keyword, key) do
    case Keyword.fetch(keyword, key) do
      {:ok, value} ->
        if present_string?(value) do
          {:ok, value}
        else
          {:error, {:missing_attach_field, key}}
        end

      _missing_or_blank ->
        {:error, {:missing_attach_field, key}}
    end
  end

  defp steps(%{
         load: load,
         settings: settings,
         secrets: secrets,
         attach: attach,
         use: use_catalog
       }) do
    extension_steps(:load, load) ++
      setting_steps(settings) ++
      Enum.map(secrets, &secret_step/1) ++
      Enum.flat_map(attach, &attach_steps(&1, secrets)) ++
      use_steps(use_catalog)
  end

  defp extension_steps(kind, names) do
    Enum.map(names, fn name ->
      sql = [String.upcase(Atom.to_string(kind)), " ", name]

      %{
        id: step_id(kind, name),
        kind: kind,
        statement: sql,
        safe_statement: sql,
        sensitive_values: []
      }
    end)
  end

  defp setting_steps(settings) do
    Enum.map(settings, fn %{name: name, value: value} ->
      sql = ["SET ", name, " = ", quote_literal(value)]

      %{
        id: step_id(:set, name),
        kind: :set_setting,
        statement: sql,
        safe_statement: sql,
        sensitive_values: []
      }
    end)
  end

  defp secret_step(%{
         name: name,
         type: :azure,
         provider: :credential_chain,
         account_name: account_name,
         chain: chain,
         scope: scope
       }) do
    options = [
      "TYPE azure",
      "PROVIDER credential_chain",
      ["ACCOUNT_NAME ", quote_literal(account_name)]
    ]

    options =
      options ++ if(chain, do: [["CHAIN ", quote_literal(Enum.join(chain, ";"))]], else: [])

    options = options ++ if(scope, do: [["SCOPE ", quote_literal(scope)]], else: [])

    safe_options = [
      "TYPE azure",
      "PROVIDER credential_chain",
      ["ACCOUNT_NAME ", quote_literal(:redacted)]
    ]

    safe_options =
      safe_options ++ if(chain, do: [["CHAIN ", quote_literal(Enum.join(chain, ";"))]], else: [])

    safe_options = safe_options ++ if(scope, do: [["SCOPE ", quote_literal(:redacted)]], else: [])

    statement = [
      "CREATE SECRET ",
      quote_ident(name),
      " (",
      Enum.intersperse(options, ", "),
      ")"
    ]

    safe_statement = [
      "CREATE SECRET ",
      quote_ident(name),
      " (",
      Enum.intersperse(safe_options, ", "),
      ")"
    ]

    %{
      id: step_id(:create_secret, name),
      kind: :create_secret,
      statement: statement,
      safe_statement: safe_statement,
      sensitive_values: [account_name, scope]
    }
  end

  defp secret_step(
         %{
           name: name,
           type: :postgres,
           host: host,
           port: port,
           database: database,
           user: user,
           password: password,
           auth: auth,
           sslmode: _sslmode
         } = secret
       ) do
    step = postgres_secret_step(secret, password, auth)

    if is_list(auth) do
      step
      |> Map.put(:postgres_secret, %{
        name: name,
        host: host,
        port: port,
        database: database,
        user: user
      })
      |> Map.put(:postgres_auth, auth)
    else
      step
    end
  end

  defp postgres_secret_step(
         %{name: name, host: host, port: port, database: database, user: user},
         password,
         auth
       ) do
    options = [
      "TYPE postgres",
      ["HOST ", quote_literal(host)],
      ["PORT ", Integer.to_string(port)],
      ["DATABASE ", quote_literal(database)],
      ["USER ", quote_literal(user)]
    ]

    options = options ++ if(password, do: [["PASSWORD ", quote_literal(password)]], else: [])

    safe_options = [
      "TYPE postgres",
      ["HOST ", quote_literal(host)],
      ["PORT ", Integer.to_string(port)],
      ["DATABASE ", quote_literal(database)],
      ["USER ", quote_literal(user)]
    ]

    safe_options =
      safe_options ++
        if(password || auth, do: [["PASSWORD ", quote_literal(:redacted)]], else: [])

    %{
      id: step_id(:create_secret, name),
      kind: :create_secret,
      statement: ["CREATE SECRET ", quote_ident(name), " (", Enum.intersperse(options, ", "), ")"],
      safe_statement: [
        "CREATE SECRET ",
        quote_ident(name),
        " (",
        Enum.intersperse(safe_options, ", "),
        ")"
      ],
      sensitive_values: [password]
    }
  end

  defp attach_steps(nil, _secrets), do: []

  defp attach_steps(%{name: name, type: :duckdb, path: path}, _secrets) do
    statement = ["ATTACH ", quote_literal(path), " AS ", quote_ident(name)]

    [
      %{
        id: step_id(:attach, name),
        kind: :duckdb_attach,
        statement: statement,
        safe_statement: statement,
        sensitive_values: []
      }
    ]
  end

  defp attach_steps(
         %{
           name: name,
           type: :ducklake,
           metadata: metadata,
           meta_secret: nil,
           data_path: data_path
         },
         _secrets
       ) do
    statement = [
      "ATTACH ",
      quote_literal(metadata),
      " AS ",
      quote_ident(name),
      " (DATA_PATH ",
      quote_literal(data_path),
      ")"
    ]

    safe_statement = [
      "ATTACH ",
      quote_literal(:redacted),
      " AS ",
      quote_ident(name),
      " (DATA_PATH ",
      quote_literal(:redacted),
      ")"
    ]

    [
      %{
        id: step_id(:attach, name),
        kind: :ducklake_attach,
        statement: statement,
        safe_statement: safe_statement,
        sensitive_values: [metadata, data_path]
      }
    ]
  end

  defp attach_steps(
         %{
           name: name,
           type: :ducklake,
           metadata: metadata,
           meta_secret: secret,
           data_path: data_path
         },
         secrets
       ) do
    metadata = metadata_with_postgres_secret_options(metadata, secret, secrets)

    statement = [
      "ATTACH ",
      quote_literal(metadata),
      " AS ",
      quote_ident(name),
      " (DATA_PATH ",
      quote_literal(data_path),
      ", META_SECRET ",
      quote_ident(secret),
      ")"
    ]

    safe_statement = [
      "ATTACH ",
      quote_literal(:redacted),
      " AS ",
      quote_ident(name),
      " (DATA_PATH ",
      quote_literal(:redacted),
      ", META_SECRET ",
      quote_ident(secret),
      ")"
    ]

    [
      %{
        id: step_id(:attach, name),
        kind: :ducklake_attach,
        statement: statement,
        safe_statement: safe_statement,
        sensitive_values: [metadata, data_path]
      }
    ]
  end

  defp metadata_with_postgres_secret_options(metadata, secret, secrets) do
    case Enum.find(secrets, &(&1.type == :postgres and &1.name == secret)) do
      %{sslmode: sslmode} when is_binary(sslmode) -> append_ducklake_sslmode(metadata, sslmode)
      _secret -> metadata
    end
  end

  defp append_ducklake_sslmode("ducklake:postgres:", sslmode),
    do: "ducklake:postgres:sslmode=#{sslmode}"

  defp append_ducklake_sslmode(metadata, _sslmode), do: metadata

  defp use_steps(nil), do: []

  defp use_steps(name) do
    statement = ["USE ", quote_ident(name)]

    [
      %{
        id: step_id(:use, name),
        kind: :use_catalog,
        statement: statement,
        safe_statement: statement,
        sensitive_values: []
      }
    ]
  end

  defp step_id(kind, name) do
    "#{kind}_#{name}"
  end

  defp normalize_identifier(value) when is_atom(value),
    do: normalize_identifier(Atom.to_string(value))

  defp normalize_identifier(value) when is_binary(value) do
    if Regex.match?(~r/^[A-Za-z_][A-Za-z0-9_]*$/, value) do
      {:ok, value}
    else
      {:error, {:invalid_identifier, value}}
    end
  end

  defp normalize_identifier(value), do: {:error, {:invalid_identifier, value}}

  defp present_string?(value), do: is_binary(value) and value != ""

  defp quote_ident(identifier) do
    identifier = to_string(identifier)
    [~s("), String.replace(identifier, ~s("), ~s("")), ~s(")]
  end

  defp quote_literal(value) do
    value = to_string(value)
    ["'", String.replace(value, "'", "''"), "'"]
  end

  defp config_error(%Resolved{} = resolved, reason) do
    %Error{
      type: :invalid_config,
      message: "invalid DuckDB connection config",
      retryable?: false,
      adapter: DuckDB,
      operation: :bootstrap,
      connection: resolved.name,
      details: %{reason: inspect(reason)}
    }
  end

  defp reject_old_runtime_keys(%Resolved{} = resolved, config) do
    old_keys = [:database, :duckdb_bootstrap, :write_concurrency]

    case Enum.find(old_keys, &Map.has_key?(config, &1)) do
      nil ->
        :ok

      key ->
        {:error,
         config_error(resolved, {:unsupported_duckdb_connection_key, key, @old_config_message})}
    end
  end

  defp token_error(%Resolved{} = resolved, step, %TokenError{} = error) do
    %Error{
      type: error.type,
      message: "DuckDB connection bootstrap failed at #{step.id}",
      retryable?: error.retryable?,
      adapter: DuckDB,
      operation: :bootstrap,
      connection: resolved.name,
      details: %{
        step: step.id,
        bootstrap_kind: step.kind,
        statement: IO.iodata_to_binary(step.safe_statement),
        reason: error.message,
        adapter_error_type: error.type,
        adapter_details: Error.redact(error.details)
      }
    }
  end

  defp bootstrap_error(%Resolved{} = resolved, step, %Error{} = error) do
    %Error{
      type: error.type,
      message: "DuckDB connection bootstrap failed at #{step.id}",
      retryable?: error.retryable?,
      adapter: DuckDB,
      operation: :bootstrap,
      connection: resolved.name,
      details: %{
        step: step.id,
        bootstrap_kind: step.kind,
        statement: IO.iodata_to_binary(step.safe_statement),
        reason: redact(error.message, step.sensitive_values),
        adapter_error_type: error.type,
        adapter_details: redact(error.details, step.sensitive_values)
      }
    }
  end

  defp redact(value, sensitive_values) when is_binary(value) do
    Enum.reduce(sensitive_values, value, fn
      secret, acc when is_binary(secret) and secret != "" ->
        String.replace(acc, secret, "redacted")

      _secret, acc ->
        acc
    end)
  end

  defp redact(value, sensitive_values) when is_map(value) do
    Map.new(value, fn {key, child} -> {key, redact(child, sensitive_values)} end)
  end

  defp redact(value, sensitive_values) when is_list(value) do
    Enum.map(value, &redact(&1, sensitive_values))
  end

  defp redact(value, sensitive_values) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&redact(&1, sensitive_values))
    |> List.to_tuple()
  end

  defp redact(value, _sensitive_values), do: value
end
