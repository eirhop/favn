defmodule Favn.SQL.SessionScript do
  @moduledoc """
  Plans bounded, native SQL files for physical-session initialization.

  This module does not understand DuckDB extensions, settings, secrets, or
  attachments. It selects named files, renders declared value parameters, and
  produces deterministic, redacted steps for an adapter to execute when opening
  a new physical session.

  Scripts are trusted deployment code. They must be self-contained,
  idempotent/retry-safe, and limited to session preparation. Favn cannot inspect
  arbitrary SQL to prove those properties. Runtime references resolve at runner
  startup, not on each pool checkout; deployments must use refresh-capable
  native credential providers or restart the runner after rotating resolved
  credentials.
  """

  alias Favn.Connection.Resolved
  alias Favn.SQL.Error
  alias Favn.SQL.SessionRequirements
  alias Favn.SQL.SessionScript.Config
  alias Favn.SQL.SessionScript.Config.Script
  alias Favn.SQL.SessionScript.Template

  @max_script_bytes 4_194_304
  @max_diagnostic_statement_characters 4_096

  defmodule Step do
    @moduledoc "One rendered startup or named-resource SQL file."

    @enforce_keys [:id, :kind, :statement, :safe_statement, :content_hash, :parameter_hash]
    defstruct [
      :id,
      :kind,
      :resource,
      :statement,
      :safe_statement,
      :content_hash,
      :parameter_hash,
      secret_values: []
    ]

    @type t :: %__MODULE__{
            id: String.t(),
            kind: :startup | :resource,
            resource: String.t() | nil,
            statement: String.t(),
            safe_statement: String.t(),
            content_hash: String.t(),
            parameter_hash: String.t(),
            secret_values: [String.t()]
          }
  end

  defmodule Plan do
    @moduledoc "Deterministic physical-session initialization plan."

    @enforce_keys [:catalogs, :resources, :steps, :fingerprint]
    defstruct [:catalogs, :resources, :steps, :fingerprint]

    @type t :: %__MODULE__{
            catalogs: [String.t()],
            resources: [String.t()],
            steps: [Step.t()],
            fingerprint: map()
          }
  end

  @doc """
  Normalizes the `:duckdb` session-script configuration for a connection.
  """
  @spec config(Resolved.t()) :: {:ok, Config.t()} | {:error, Error.t()}
  def config(%Resolved{} = resolved) do
    duckdb = Map.get(resolved.config || %{}, :duckdb)

    case Config.normalize(duckdb, secret_paths: resolved.secret_paths) do
      {:ok, %Config{} = config} -> {:ok, config}
      {:error, reason} -> {:error, invalid_config_error(resolved, reason)}
    end
  end

  @doc """
  Returns configured catalog names without reading or rendering script files.
  """
  @spec configured_catalogs(Resolved.t()) :: {:ok, [String.t()]} | {:error, Error.t()}
  def configured_catalogs(%Resolved{} = resolved) do
    with {:ok, %Config{} = config} <- config(resolved) do
      {:ok, config.catalogs |> Map.keys() |> Enum.sort()}
    end
  end

  @doc """
  Builds the exact session initialization plan selected by adapter options.

  When `:required_catalogs` is omitted, all configured catalogs are selected to
  preserve explicit raw-client and inspection bootstrap behavior. When present,
  only that normalized catalog set is selected.
  """
  @spec plan(Resolved.t(), keyword()) :: {:ok, Plan.t()} | {:error, Error.t()}
  def plan(%Resolved{} = resolved, opts) when is_list(opts) do
    with {:ok, %Config{} = config} <- config(resolved),
         {:ok, catalogs} <- select_catalogs(config, opts, resolved),
         {:ok, explicit_resources} <- normalize_required_resources(opts, resolved),
         {:ok, resources} <- select_resources(config, catalogs, explicit_resources, resolved),
         {:ok, steps} <- build_steps(config, resources, resolved),
         fingerprint <- fingerprint(catalogs, resources, steps) do
      {:ok,
       %Plan{
         catalogs: catalogs,
         resources: resources,
         steps: steps,
         fingerprint: fingerprint
       }}
    end
  end

  @doc """
  Returns a redacted fingerprint suitable for exact SQL session-pool identity.
  """
  @spec fingerprint(Resolved.t(), keyword()) :: {:ok, map()} | {:error, Error.t()}
  def fingerprint(%Resolved{} = resolved, opts) do
    case plan(resolved, opts) do
      {:ok, %Plan{fingerprint: fingerprint}} -> {:ok, fingerprint}
      {:error, %Error{} = error} -> {:error, Error.redact(error)}
    end
  end

  @doc false
  @spec redact_step_error(Error.t(), Step.t()) :: Error.t()
  def redact_step_error(%Error{} = error, %Step{} = step) do
    redacted = Error.redact(error)

    message =
      Enum.reduce(step.secret_values, redacted.message, fn secret, acc ->
        if secret == "", do: acc, else: String.replace(acc, secret, "[REDACTED]")
      end)

    %Error{
      redacted
      | message: message,
        operation: :bootstrap,
        details: %{
          step: step.id,
          kind: step.kind,
          resource: step.resource,
          statement: bounded_statement(step.safe_statement)
        },
        cause: nil
    }
  end

  defp select_catalogs(%Config{} = config, opts, resolved) do
    with {:ok, requested} <- normalize_required_catalogs(config, opts, resolved) do
      unknown = requested -- Map.keys(config.catalogs)

      if unknown == [] do
        {:ok, requested}
      else
        {:error, invalid_config_error(resolved, {:unknown_required_catalogs, unknown})}
      end
    end
  end

  defp normalize_required_catalogs(config, opts, resolved) do
    if Keyword.has_key?(opts, :required_catalogs) do
      catalogs = opts |> Keyword.get(:required_catalogs) |> List.wrap()
      {:ok, SessionRequirements.normalize_resources!(catalogs)}
    else
      {:ok, config.catalogs |> Map.keys() |> Enum.sort()}
    end
  rescue
    error in ArgumentError ->
      {:error, invalid_config_error(resolved, {:invalid_required_catalogs, error.message})}
  end

  defp normalize_required_resources(opts, resolved) do
    resources = Keyword.get(opts, :required_resources, []) |> List.wrap()
    {:ok, SessionRequirements.normalize_resources!(resources)}
  rescue
    error in ArgumentError ->
      {:error, invalid_config_error(resolved, {:invalid_required_resources, error.message})}
  end

  defp select_resources(config, catalogs, explicit_resources, resolved) do
    catalog_resources =
      Enum.flat_map(catalogs, fn catalog ->
        case Map.fetch!(config.catalogs, catalog).resource do
          nil -> []
          resource -> [resource]
        end
      end)

    resources = (explicit_resources ++ catalog_resources) |> Enum.uniq() |> Enum.sort()
    unknown = resources -- Map.keys(config.resources)

    if unknown == [] do
      {:ok, resources}
    else
      {:error, invalid_config_error(resolved, {:unknown_required_resources, unknown})}
    end
  end

  defp build_steps(config, resources, resolved) do
    scripts =
      List.wrap(config.startup) ++ Enum.map(resources, &Map.fetch!(config.resources, &1))

    Enum.reduce_while(scripts, {:ok, []}, fn script, {:ok, acc} ->
      case build_step(script, resolved) do
        {:ok, %Step{} = step} -> {:cont, {:ok, [step | acc]}}
        {:error, %Error{} = error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, steps} -> {:ok, Enum.reverse(steps)}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp build_step(%Script{} = script, resolved) do
    with {:ok, path} <- resolve_file(script.file, resolved),
         {:ok, sql} <- read_file(path, script.name, resolved),
         {:ok, rendered} <- render_script(sql, script, resolved) do
      kind = if script.name == "startup", do: :startup, else: :resource
      resource = if kind == :resource, do: script.name, else: nil

      {:ok,
       %Step{
         id: if(kind == :startup, do: "startup", else: "resource:#{script.name}"),
         kind: kind,
         resource: resource,
         statement: rendered.statement,
         safe_statement: rendered.safe_statement,
         content_hash: sha256(sql),
         parameter_hash: sha256(:erlang.term_to_binary(script.params)),
         secret_values: rendered.secret_values
       }}
    end
  end

  defp resolve_file({:priv, app, relative}, resolved) do
    case :code.priv_dir(app) do
      path when is_list(path) ->
        root = path |> List.to_string() |> Path.expand()
        resolved_path = Path.expand(relative, root)

        if within_root?(resolved_path, root) do
          {:ok, resolved_path}
        else
          {:error, invalid_config_error(resolved, {:script_path_escape, app})}
        end

      {:error, reason} ->
        {:error, invalid_config_error(resolved, {:priv_directory_unavailable, app, reason})}
    end
  end

  defp resolve_file(path, _resolved) when is_binary(path), do: {:ok, path}

  defp read_file(path, script_name, resolved) do
    with {:ok, stat} <- File.stat(path),
         {:ok, sql} <- read_regular_file(stat, path, script_name, resolved) do
      {:ok, sql}
    else
      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error, invalid_config_error(resolved, {:script_file_unavailable, script_name, reason})}
    end
  end

  defp read_regular_file(%File.Stat{type: type}, _path, script_name, resolved)
       when type != :regular do
    {:error, invalid_config_error(resolved, {:script_not_regular_file, script_name, type})}
  end

  defp read_regular_file(%File.Stat{size: size}, _path, script_name, resolved)
       when size > @max_script_bytes do
    {:error,
     invalid_config_error(
       resolved,
       {:script_file_too_large, script_name, @max_script_bytes}
     )}
  end

  defp read_regular_file(%File.Stat{}, path, script_name, resolved) do
    case File.read(path) do
      {:ok, sql} when byte_size(sql) > @max_script_bytes ->
        {:error,
         invalid_config_error(
           resolved,
           {:script_file_too_large, script_name, @max_script_bytes}
         )}

      {:ok, sql} ->
        if String.valid?(sql) do
          {:ok, sql}
        else
          {:error, invalid_config_error(resolved, {:script_file_invalid_utf8, script_name})}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp render_script(sql, %Script{} = script, resolved) do
    case Template.render(sql, script.params, script.secret_params) do
      {:ok, rendered} ->
        {:ok, rendered}

      {:error, reason} ->
        {:error, invalid_config_error(resolved, {:script_render_failed, script.name, reason})}
    end
  end

  defp fingerprint(catalogs, resources, steps) do
    %{
      version: 1,
      catalogs: catalogs,
      resources: resources,
      scripts:
        Enum.map(steps, fn step ->
          %{
            id: step.id,
            content_hash: step.content_hash,
            parameter_hash: step.parameter_hash
          }
        end)
    }
  end

  defp invalid_config_error(%Resolved{} = resolved, reason) do
    %Error{
      type: :invalid_config,
      message: "SQL session script configuration is invalid",
      adapter: resolved.adapter,
      connection: resolved.name,
      operation: :bootstrap,
      retryable?: false,
      details: %{reason: safe_reason(reason)}
    }
  end

  defp safe_reason({:invalid_script_file, reason})
       when reason in [
              :absolute_path_required,
              :priv_path_must_be_relative,
              :priv_path_cannot_escape
            ],
       do: {:invalid_script_file, reason}

  defp safe_reason({:invalid_script_file, _value}), do: {:invalid_script_file, :redacted}

  defp safe_reason(reason) do
    Error.redact(reason)
  end

  defp within_root?(path, root) do
    case Path.relative_to(path, root) do
      "." -> true
      relative -> Path.type(relative) == :relative and ".." not in Path.split(relative)
    end
  end

  defp bounded_statement(statement) do
    if String.length(statement) <= @max_diagnostic_statement_characters do
      statement
    else
      String.slice(statement, 0, @max_diagnostic_statement_characters) <> "\n-- [TRUNCATED]"
    end
  end

  defp sha256(value) do
    :sha256
    |> :crypto.hash(value)
    |> Base.encode16(case: :lower)
  end
end
