defmodule Favn.SQL.Adapter.DuckDB.Bootstrap do
  @moduledoc false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB
  alias Favn.SQL.Error

  @config_key :duckdb_bootstrap

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

  @spec run(DuckDB.Conn.t(), Resolved.t(), keyword()) :: :ok | {:error, Error.t()}
  def run(%DuckDB.Conn{} = conn, %Resolved{} = resolved, _opts) do
    with {:ok, steps} <- build_steps(resolved) do
      execute_steps(conn, resolved, steps)
    end
  end

  @spec build_steps(Resolved.t()) :: {:ok, [step()]} | {:error, Error.t()}
  def build_steps(%Resolved{} = resolved) do
    case Map.get(resolved.config || %{}, @config_key) do
      nil ->
        {:ok, []}

      [] ->
        {:ok, []}

      config ->
        config
        |> normalize_config()
        |> case do
          {:ok, normalized} -> {:ok, steps(normalized)}
          {:error, reason} -> {:error, config_error(resolved, reason)}
        end
    end
  end

  defp execute_steps(_conn, _resolved, []), do: :ok

  defp execute_steps(%DuckDB.Conn{} = conn, %Resolved{} = resolved, steps) do
    Enum.reduce_while(steps, :ok, fn step, :ok ->
      case DuckDB.execute(conn, step.statement, []) do
        {:ok, _result} ->
          {:cont, :ok}

        {:error, %Error{} = error} ->
          {:halt, {:error, bootstrap_error(resolved, step, error)}}
      end
    end)
  end

  defp normalize_config(config) do
    with {:ok, normalized} <- normalize_keyword_config(config, :bootstrap),
         {:ok, extensions} <- normalize_extensions(Keyword.get(normalized, :extensions, [])),
         {:ok, secrets} <- normalize_secrets(Keyword.get(normalized, :secrets, [])),
         {:ok, attach} <- normalize_attach(Keyword.get(normalized, :attach)),
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
        account_name = Keyword.get(secret, :account_name)

        if present_string?(account_name) do
          {:ok,
           %{name: name, type: :azure, provider: :credential_chain, account_name: account_name}}
        else
          {:error, {:missing_secret_field, name, :account_name}}
        end

      other ->
        {:error, {:unsupported_secret, name, other}}
    end
  end

  defp normalize_attach(nil), do: {:ok, nil}

  defp normalize_attach(config) do
    with {:ok, attach} <- normalize_keyword_config(config, :attach),
         {:ok, name} <- normalize_identifier(Keyword.get(attach, :name)),
         {:ok, metadata} <- fetch_present_value(attach, :metadata),
         {:ok, data_path} <- fetch_present_value(attach, :data_path) do
      case Keyword.get(attach, :type) do
        :ducklake ->
          {:ok,
           %{
             name: name,
             type: :ducklake,
             metadata: metadata,
             data_path: data_path
           }}

        other ->
          {:error, {:unsupported_attach_type, other}}
      end
    end
  end

  defp normalize_optional_identifier(nil), do: {:ok, nil}
  defp normalize_optional_identifier(value), do: normalize_identifier(value)

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
         account_name: account_name
       }) do
    statement = [
      "CREATE SECRET ",
      quote_ident(name),
      " (TYPE azure, PROVIDER credential_chain, ACCOUNT_NAME ",
      quote_literal(account_name),
      ")"
    ]

    safe_statement = [
      "CREATE SECRET ",
      quote_ident(name),
      " (TYPE azure, PROVIDER credential_chain, ACCOUNT_NAME ",
      quote_literal(:redacted),
      ")"
    ]

    %{
      id: step_id(:create_secret, name),
      kind: :create_secret,
      statement: statement,
      safe_statement: safe_statement,
      sensitive_values: [account_name]
    }
  end

  defp attach_steps(nil), do: []

  defp attach_steps(%{name: name, type: :ducklake, metadata: metadata, data_path: data_path}) do
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
      message: "invalid DuckDB bootstrap config",
      retryable?: false,
      adapter: DuckDB,
      operation: :bootstrap,
      connection: resolved.name,
      details: %{reason: inspect(reason)}
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
