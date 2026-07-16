defmodule Favn.SQL.Adapter.DuckDB.Bootstrap do
  @moduledoc false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB
  alias Favn.SQL.Error
  alias Favn.SQL.SessionScript
  alias Favn.SQL.SessionScript.Config
  alias Favn.SQL.SessionScript.Plan
  alias Favn.SQL.SessionScript.Step

  @old_config_message "DuckDB connection config uses open: [database: ...] and duckdb: [startup: ..., resources: ..., catalogs: ...]; structured load/settings/secrets/attach/use configuration is not supported"

  @spec config_schema_fields() :: [Favn.Connection.Definition.field()]
  def config_schema_fields do
    [
      %{key: :open, required: true, type: {:custom, &validate_open/1}},
      %{key: :duckdb, type: {:custom, &validate_config/1}},
      %{key: :database, type: {:custom, &reject_old_key/1}},
      %{key: :duckdb_bootstrap, type: {:custom, &reject_old_key/1}},
      %{key: :write_concurrency, type: {:custom, &reject_old_key/1}}
    ]
  end

  @spec database(Resolved.t()) :: {:ok, String.t()} | {:error, Error.t()}
  def database(%Resolved{} = resolved) do
    case normalize_open(Map.get(resolved.config || %{}, :open)) do
      {:ok, %{database: database}} -> {:ok, database}
      {:error, reason} -> {:error, config_error(resolved, reason)}
    end
  end

  @spec validate_open(term()) :: :ok | {:error, term()}
  def validate_open(value) do
    case normalize_open(value) do
      {:ok, _open} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec validate_config(term()) :: :ok | {:error, term()}
  def validate_config(value), do: Config.validate(value)

  def reject_old_key(_value), do: {:error, @old_config_message}

  @spec run(DuckDB.Conn.t(), Resolved.t(), keyword()) :: :ok | {:error, Error.t()}
  def run(%DuckDB.Conn{} = conn, %Resolved{} = resolved, opts) do
    with {:ok, %Plan{} = plan} <- session_script_plan(resolved, opts),
         :ok <- validate_expected_fingerprint(plan, resolved, opts) do
      execute_steps(conn, plan.steps)
    end
  end

  defp session_script_plan(resolved, opts) do
    case Keyword.get(opts, :favn_pool_preparation) do
      nil -> SessionScript.plan(resolved, opts)
      %Plan{} = plan -> {:ok, plan}
      _other -> {:error, invalid_prepared_plan_error(resolved)}
    end
  end

  defp execute_steps(_conn, []), do: :ok

  defp execute_steps(%DuckDB.Conn{} = conn, steps) do
    Enum.reduce_while(steps, :ok, fn %Step{} = step, :ok ->
      case DuckDB.execute(conn, step.statement, []) do
        {:ok, _result} ->
          {:cont, :ok}

        {:error, %Error{} = error} ->
          {:halt, {:error, SessionScript.redact_step_error(error, step)}}

        {:error, reason} ->
          error = %Error{
            type: :execution_error,
            message: "DuckDB session script failed",
            cause: reason
          }

          {:halt, {:error, SessionScript.redact_step_error(error, step)}}
      end
    end)
  end

  defp validate_expected_fingerprint(%Plan{} = plan, resolved, opts) do
    expected =
      case Keyword.get(opts, :favn_pool_fingerprint) do
        %{session_scripts: fingerprint} -> fingerprint
        fingerprint -> fingerprint
      end

    case expected do
      nil ->
        :ok

      expected when expected == plan.fingerprint ->
        :ok

      _other ->
        {:error,
         %Error{
           type: :invalid_config,
           message: "DuckDB session script changed while the session was being prepared",
           adapter: resolved.adapter,
           connection: resolved.name,
           operation: :bootstrap,
           retryable?: true,
           details: %{reason: :session_script_fingerprint_changed}
         }}
    end
  end

  defp invalid_prepared_plan_error(resolved) do
    %Error{
      type: :invalid_config,
      message: "DuckDB session script preparation is invalid",
      adapter: resolved.adapter,
      connection: resolved.name,
      operation: :bootstrap,
      retryable?: false,
      details: %{reason: :invalid_prepared_session_script_plan}
    }
  end

  defp normalize_open(open) do
    with {:ok, open} <- keyword_or_map(open, :open) do
      case fetch(open, :database) do
        ":memory:" -> {:ok, %{database: ":memory:"}}
        database when is_binary(database) and database != "" -> {:ok, %{database: database}}
        nil -> {:error, {:missing_open_field, :database}}
        _database -> {:error, {:invalid_open_database, :expected_memory_or_non_empty_path}}
      end
    end
  end

  defp keyword_or_map(value, _context) when is_map(value), do: {:ok, value}

  defp keyword_or_map(value, context) when is_list(value) do
    if Keyword.keyword?(value),
      do: {:ok, Map.new(value)},
      else: {:error, {:invalid_config, context}}
  end

  defp keyword_or_map(_value, context), do: {:error, {:invalid_config, context}}

  defp fetch(config, key), do: Map.get(config, key, Map.get(config, Atom.to_string(key)))

  defp config_error(%Resolved{} = resolved, reason) do
    %Error{
      type: :invalid_config,
      message: "DuckDB connection configuration is invalid",
      adapter: resolved.adapter,
      connection: resolved.name,
      operation: :connect,
      retryable?: false,
      details: %{reason: reason}
    }
  end
end
