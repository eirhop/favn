defmodule Favn.SQL.Adapter.DuckDB.ADBC.Bootstrap do
  @moduledoc false

  alias Favn.Connection.Resolved
  alias Favn.Azure.{PostgresEntraToken, TokenError}
  alias Favn.SQL.Adapter.DuckDB.ADBC
  alias Favn.SQL.Error

  @config_key :duckdb_bootstrap
  @azure_credential_chain_values ~w(cli managed_identity workload_identity env default)
  @postgres_sslmodes ~w(disable allow prefer require verify-ca verify-full)

  @type step :: %{id: String.t(), kind: atom(), statement: iodata(), safe_statement: iodata()}

  @spec schema_field() :: Favn.Connection.Definition.field()
  def schema_field do
    %{key: @config_key, type: {:custom, &validate_config/1}}
  end

  @spec validate_config(term()) :: :ok | {:error, term()}
  def validate_config(nil), do: :ok
  def validate_config([]), do: :ok

  def validate_config(value) when is_map(value) or is_list(value) do
    case normalize_config(value) do
      {:ok, _config} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  def validate_config(_value), do: {:error, :expected_duckdb_bootstrap_keyword_or_map}

  @spec run(ADBC.Conn.t(), Resolved.t(), keyword()) :: :ok | {:error, Error.t()}
  def run(%ADBC.Conn{} = conn, %Resolved{} = resolved, opts) do
    with {:ok, steps} <- build_steps(resolved) do
      execute_steps(conn, resolved, steps, opts)
    end
  end

  @spec build_steps(Resolved.t()) :: {:ok, [step()]} | {:error, Error.t()}
  def build_steps(%Resolved{} = resolved), do: build_steps(resolved, [])

  @spec build_steps(Resolved.t(), keyword()) :: {:ok, [step()]} | {:error, Error.t()}
  def build_steps(%Resolved{} = resolved, opts) do
    case Map.get(resolved.config || %{}, @config_key) do
      nil -> {:ok, []}
      [] -> {:ok, []}
      config -> build_configured_steps(resolved, config, opts)
    end
  end

  defp build_configured_steps(%Resolved{} = resolved, config, _opts) do
    case normalize_config(config) do
      {:ok, normalized} -> build_normalized_steps(normalized)
      {:error, reason} -> {:error, config_error(resolved, reason)}
    end
  end

  defp build_normalized_steps(normalized), do: {:ok, steps(normalized)}

  defp execute_steps(_conn, _resolved, [], _opts), do: :ok

  defp execute_steps(%ADBC.Conn{} = conn, %Resolved{} = resolved, steps, opts) do
    Enum.reduce_while(steps, :ok, fn step, :ok ->
      with {:ok, step} <- materialize_step(step, opts),
           {:ok, _result} <- ADBC.execute(conn, step.statement, []) do
        {:cont, :ok}
      else
        {:error, %TokenError{} = error} ->
          {:halt, {:error, token_error(resolved, step, error)}}

        {:error, %Error{} = error} -> {:halt, {:error, bootstrap_error(resolved, step, error)}}
      end
    end)
  end

  defp normalize_config(config) do
    with {:ok, normalized} <- normalize_keyword_config(config, :bootstrap),
         {:ok, extensions} <- normalize_extensions(Keyword.get(normalized, :extensions, [])),
         {:ok, secrets} <- normalize_secrets(Keyword.get(normalized, :secrets, [])),
         {:ok, attach} <- normalize_attach(Keyword.get(normalized, :attach)),
         {:ok, attach} <- inherit_attach_metadata_options(attach, secrets),
         {:ok, use_catalog} <- normalize_optional_identifier(Keyword.get(normalized, :use)) do
      {:ok, %{extensions: extensions, secrets: secrets, attach: attach, use: use_catalog}}
    end
  end

  defp normalize_extensions(config) do
    with {:ok, extensions} <- normalize_keyword_config(config, :extensions),
         {:ok, install} <- normalize_extension_list(Keyword.get(extensions, :install, [])),
         {:ok, load} <- normalize_extension_list(Keyword.get(extensions, :load, [])) do
      {:ok, %{install: install, load: load}}
    end
  end

  defp normalize_extension_list(values) when is_list(values) do
    values
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
      case normalize_identifier(value) do
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
          with {:ok, endpoint} <- normalize_managed_identity_endpoint(Keyword.get(auth, :endpoint, :auto)) do
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

  defp normalize_managed_identity_endpoint(endpoint) when endpoint in [:auto, :imds, :azure_app_service],
    do: {:ok, endpoint}

  defp normalize_managed_identity_endpoint(endpoint),
    do: {:error, {:invalid_managed_identity_endpoint, endpoint}}

  defp materialize_step(%{postgres_secret: secret, postgres_auth: auth}, opts) when is_list(auth) do
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

  defp normalize_attach(nil), do: {:ok, nil}

  defp normalize_attach(config) do
    with {:ok, attach} <- normalize_keyword_config(config, :attach),
         {:ok, name} <- normalize_identifier(Keyword.get(attach, :name)),
         {:ok, metadata} <- normalize_attach_metadata(Keyword.get(attach, :metadata)),
         {:ok, data_path} <- fetch_present_value(attach, :data_path) do
      case Keyword.get(attach, :type) do
        :ducklake ->
          {:ok, %{name: name, type: :ducklake, metadata: metadata, data_path: data_path}}

        other ->
          {:error, {:unsupported_attach_type, other}}
      end
    end
  end

  defp fetch_secret_value(keyword, name, key) do
    case Keyword.fetch(keyword, key) do
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      _missing_or_blank -> {:error, {:missing_secret_field, name, key}}
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
    if value in @azure_credential_chain_values,
      do: {:ok, value},
      else: {:error, {:invalid_azure_credential_chain, value}}
  end

  defp normalize_azure_chain_value(value), do: {:error, {:invalid_azure_credential_chain, value}}

  defp normalize_azure_scope(nil), do: {:ok, nil}

  defp normalize_azure_scope(scope) when is_binary(scope) and scope != "" do
    if String.ends_with?(scope, "/"),
      do: {:ok, scope},
      else: {:error, {:invalid_azure_scope, :missing_trailing_slash}}
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
    if value in @postgres_sslmodes,
      do: {:ok, value},
      else: {:error, {:invalid_postgres_sslmode, value}}
  end

  defp normalize_postgres_sslmode(value), do: {:error, {:invalid_postgres_sslmode, value}}

  defp inherit_attach_metadata_options(nil, _secrets), do: {:ok, nil}

  defp inherit_attach_metadata_options(
         %{metadata: {:postgres_secret, secret, metadata_options}} = attach,
         secrets
       ) do
    secret_options =
      secrets
      |> Enum.find(%{}, &(&1.name == secret and &1.type == :postgres))
      |> Map.take([:sslmode])

    {:ok,
     %{attach | metadata: {:postgres_secret, secret, Map.merge(secret_options, metadata_options)}}}
  end

  defp inherit_attach_metadata_options(attach, _secrets), do: {:ok, attach}

  defp normalize_attach_metadata(metadata) when is_binary(metadata) and metadata != "",
    do: {:ok, {:dsn, metadata}}

  defp normalize_attach_metadata(metadata) when is_map(metadata) or is_list(metadata) do
    with {:ok, metadata} <- normalize_keyword_config(metadata, :attach_metadata) do
      case {Keyword.get(metadata, :type), Keyword.get(metadata, :secret)} do
        {:postgres, secret} ->
          with {:ok, secret} <- normalize_identifier(secret),
               {:ok, sslmode} <- normalize_postgres_sslmode(Keyword.get(metadata, :sslmode)) do
            metadata_options = if sslmode, do: %{sslmode: sslmode}, else: %{}

            {:ok, {:postgres_secret, secret, metadata_options}}
          end

        other ->
          {:error, {:unsupported_attach_metadata, other}}
      end
    end
  end

  defp normalize_attach_metadata(_metadata), do: {:error, {:missing_attach_field, :metadata}}

  defp normalize_optional_identifier(nil), do: {:ok, nil}
  defp normalize_optional_identifier(value), do: normalize_identifier(value)

  defp normalize_keyword_config(nil, _context), do: {:ok, []}

  defp normalize_keyword_config(config, _context) when is_map(config) do
    if Enum.all?(Map.keys(config), &is_atom/1),
      do: {:ok, Map.to_list(config)},
      else: {:error, :invalid_bootstrap_map_keys}
  end

  defp normalize_keyword_config(config, _context) when is_list(config) do
    if Keyword.keyword?(config), do: {:ok, config}, else: {:error, :invalid_bootstrap_keyword}
  end

  defp normalize_keyword_config(_config, context),
    do: {:error, {:invalid_bootstrap_config, context}}

  defp fetch_present_value(keyword, key) do
    case Keyword.fetch(keyword, key) do
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      _missing_or_blank -> {:error, {:missing_attach_field, key}}
    end
  end

  defp steps(%{extensions: extensions, secrets: secrets, attach: attach, use: use_catalog}) do
    extension_steps(:install, extensions.install) ++
      extension_steps(:load, extensions.load) ++
      Enum.map(secrets, &secret_step/1) ++
      attach_steps(attach) ++
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

    statement = ["CREATE SECRET ", quote_ident(name), " (", Enum.intersperse(options, ", "), ")"]

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

  defp secret_step(%{
         name: name,
         type: :postgres,
         host: host,
         port: port,
         database: database,
         user: user,
         password: password,
         auth: auth,
         sslmode: _sslmode
       } = secret) do
    step = postgres_secret_step(secret, password, auth)

    if is_list(auth) do
      step
      |> Map.put(:postgres_secret, %{name: name, host: host, port: port, database: database, user: user})
      |> Map.put(:postgres_auth, auth)
    else
      step
    end
  end

  defp postgres_secret_step(%{name: name, host: host, port: port, database: database, user: user}, password, auth) do
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
      safe_options ++ if(password || auth, do: [["PASSWORD ", quote_literal(:redacted)]], else: [])

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

  defp attach_steps(nil), do: []

  defp attach_steps(%{
         name: name,
         type: :ducklake,
         metadata: {:dsn, metadata},
         data_path: data_path
       }) do
    statement = [
      "ATTACH ",
      quote_literal(metadata),
      " AS ",
      quote_ident(name),
      " (TYPE ducklake, DATA_PATH ",
      quote_literal(data_path),
      ")"
    ]

    safe_statement = [
      "ATTACH ",
      quote_literal(:redacted),
      " AS ",
      quote_ident(name),
      " (TYPE ducklake, DATA_PATH ",
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

  defp attach_steps(%{
         name: name,
         type: :ducklake,
         metadata: {:postgres_secret, secret, metadata_options},
         data_path: data_path
       }) do
    metadata_path = postgres_ducklake_metadata_path(metadata_options)

    statement = [
      "ATTACH ",
      quote_literal(metadata_path),
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
      quote_literal(metadata_path),
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
        sensitive_values: [data_path]
      }
    ]
  end

  defp postgres_ducklake_metadata_path(%{sslmode: nil}), do: "ducklake:postgres:"

  defp postgres_ducklake_metadata_path(metadata_options) when map_size(metadata_options) == 0,
    do: "ducklake:postgres:"

  defp postgres_ducklake_metadata_path(%{sslmode: sslmode}) do
    "ducklake:postgres:sslmode=#{sslmode}"
  end

  defp use_steps(nil), do: []

  defp use_steps(name),
    do: [
      %{
        id: step_id(:use, name),
        kind: :use_catalog,
        statement: ["USE ", quote_ident(name)],
        safe_statement: ["USE ", quote_ident(name)],
        sensitive_values: []
      }
    ]

  defp step_id(kind, name), do: "#{kind}_#{name}"

  defp normalize_identifier(value) when is_atom(value),
    do: normalize_identifier(Atom.to_string(value))

  defp normalize_identifier(value) when is_binary(value) do
    if Regex.match?(~r/^[A-Za-z_][A-Za-z0-9_]*$/, value),
      do: {:ok, value},
      else: {:error, {:invalid_identifier, value}}
  end

  defp normalize_identifier(value), do: {:error, {:invalid_identifier, value}}

  defp quote_ident(identifier),
    do: [~s("), String.replace(to_string(identifier), ~s("), ~s("")), ~s(")]

  defp quote_literal(value), do: ["'", String.replace(to_string(value), "'", "''"), "'"]

  defp config_error(%Resolved{} = resolved, reason) do
    %Error{
      type: :invalid_config,
      message: "invalid DuckDB bootstrap config",
      retryable?: false,
      adapter: ADBC,
      operation: :bootstrap,
      connection: resolved.name,
      details: %{reason: inspect(reason)}
    }
  end

  defp token_error(%Resolved{} = resolved, step, %TokenError{} = error) do
    %Error{
      type: error.type,
      message: "DuckDB ADBC connection bootstrap failed at #{step.id}",
      retryable?: error.retryable?,
      adapter: ADBC,
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
      message: "DuckDB ADBC connection bootstrap failed at #{step.id}",
      retryable?: error.retryable?,
      adapter: ADBC,
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

  defp redact(value, sensitive_values) when is_map(value),
    do: Map.new(value, fn {key, child} -> {key, redact(child, sensitive_values)} end)

  defp redact(value, sensitive_values) when is_list(value),
    do: Enum.map(value, &redact(&1, sensitive_values))

  defp redact(value, sensitive_values) when is_tuple(value) do
    value |> Tuple.to_list() |> Enum.map(&redact(&1, sensitive_values)) |> List.to_tuple()
  end

  defp redact(value, _sensitive_values), do: value
end
