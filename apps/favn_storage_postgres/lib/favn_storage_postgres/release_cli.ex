defmodule FavnStoragePostgres.ReleaseCLI do
  @moduledoc """
  Bounded command dispatcher used by one-off control-plane release containers.

  Operation names are fixed by the release overlay. Values needed by an
  operation are read from environment variables, so database credentials and
  other secrets never appear in process arguments.
  """

  require Logger

  alias FavnStoragePostgres.Release
  alias FavnStoragePostgres.Bootstrap.Result

  @operations [
    :bootstrap,
    :status,
    :upgrade,
    :migrate,
    :verify_schema,
    :verify_restore,
    :grant_runtime,
    :provision_workspace,
    :runtime_input_key_inventory,
    :compact_runtime_input_keys
  ]
  @max_key_versions 100

  @type operation ::
          :bootstrap
          | :status
          | :upgrade
          | :migrate
          | :verify_schema
          | :verify_restore
          | :grant_runtime
          | :provision_workspace
          | :runtime_input_key_inventory
          | :compact_runtime_input_keys

  @doc "Runs one fixed release operation and exits with its stable result code."
  @spec run!(operation()) :: :ok | no_return()
  def run!(operation) when operation in @operations do
    with_release_logger(fn ->
      Logger.info("favn.release.operation_started operation=#{operation}")
      exit_code = run(operation, System.get_env(), Release)

      if exit_code == 0 do
        :ok
      else
        Logger.flush()
        System.halt(exit_code)
      end
    end)
  end

  @doc false
  @spec run(operation(), map(), module()) :: non_neg_integer()
  def run(operation, env, release) when operation in @operations and is_map(env) do
    result = dispatch(operation, env, release)

    case result do
      {tag, %{operation: ^operation} = details} when tag in [:ok, :error] ->
        emit_result(details)

      _invalid ->
        details = %{
          contract_version: 1,
          operation: operation,
          status: :error,
          outcome: :failed,
          state: :operation_failed,
          code: :invalid_result,
          safe_to_retry: false,
          completed_stages: [],
          findings: [%{code: :invalid_result, stage: :dispatch, details: %{}}],
          runtime_verified: false
        }

        emit_result(details)
    end
  end

  @doc false
  @spec run!(operation(), map(), module()) :: :ok | no_return()
  def run!(operation, env, release) do
    with_release_logger(fn ->
      Logger.info("favn.release.operation_started operation=#{operation}")
      exit_code = run(operation, env, release)
      if exit_code == 0, do: :ok, else: raise("release operation failed with exit #{exit_code}")
    end)
  end

  defp dispatch(:bootstrap, env, release), do: release.bootstrap(env)
  defp dispatch(:status, env, release), do: release.database_status(env)
  defp dispatch(:upgrade, env, release), do: release.upgrade(env)

  defp dispatch(:migrate, _env, release), do: release.migrate()
  defp dispatch(:verify_schema, _env, release), do: release.verify_schema()
  defp dispatch(:verify_restore, _env, release), do: release.verify_restore()
  defp dispatch(:grant_runtime, _env, release), do: release.grant_runtime()

  defp dispatch(:provision_workspace, env, release) do
    with {:ok, workspace} <- workspace(env) do
      release.provision_workspace(workspace)
    end
  end

  defp dispatch(:runtime_input_key_inventory, _env, release),
    do: release.runtime_input_key_inventory()

  defp dispatch(:compact_runtime_input_keys, env, release) do
    with {:ok, versions} <- key_versions(env) do
      release.compact_runtime_input_keys(versions)
    end
  end

  defp workspace(env) do
    with {:ok, workspace_id} <- required(env, "FAVN_WORKSPACE_ID"),
         {:ok, slug} <- optional(env, "FAVN_WORKSPACE_SLUG", workspace_id),
         {:ok, display_name} <- optional(env, "FAVN_WORKSPACE_NAME", slug) do
      {:ok, %{workspace_id: workspace_id, slug: slug, display_name: display_name}}
    else
      {:error, code} -> operation_error(:provision_workspace, code)
    end
  end

  defp key_versions(env) do
    with {:ok, encoded} <- required(env, "FAVN_RUNTIME_INPUT_KEY_VERSIONS"),
         versions when versions != [] <- String.split(encoded, ",", trim: true),
         true <- length(versions) <= @max_key_versions,
         {:ok, versions} <- parse_versions(versions) do
      {:ok, Enum.uniq(versions)}
    else
      _invalid -> operation_error(:compact_runtime_input_keys, :invalid_key_versions)
    end
  end

  defp parse_versions(values) do
    values
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
      case Integer.parse(value) do
        {version, ""} when version > 0 -> {:cont, {:ok, [version | acc]}}
        _invalid -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, versions} -> {:ok, Enum.reverse(versions)}
      :error -> :error
    end
  end

  defp required(env, name) do
    case Map.get(env, name) do
      value when is_binary(value) and value != "" and byte_size(value) <= 255 -> {:ok, value}
      _invalid -> {:error, :missing_or_invalid_environment}
    end
  end

  defp optional(env, name, default) do
    case Map.get(env, name, default) do
      value when is_binary(value) and value != "" and byte_size(value) <= 255 -> {:ok, value}
      _invalid -> {:error, :missing_or_invalid_environment}
    end
  end

  defp operation_error(operation, code),
    do: {:error, %{operation: operation, status: :error, code: code}}

  defp emit_result(details) do
    exit_code = exit_code(details)
    state = Map.get(details, :state, Map.get(details, :status, :error))
    diagnostic = diagnostic_summary(details)

    IO.puts(
      :stderr,
      "favn.release operation=#{details.operation} state=#{state} exit=#{exit_code} " <>
        "stage=#{diagnostic.stage} retryability=#{diagnostic.retryability} " <>
        "failure_class=#{diagnostic.failure_class} diagnostic_id=#{diagnostic.diagnostic_id}"
    )

    IO.puts(Jason.encode!(details))
    exit_code
  end

  defp diagnostic_summary(details) do
    findings =
      details
      |> Map.get(:findings, [])
      |> List.wrap()

    finding =
      Enum.find(findings, fn
        %{details: %{diagnostic_id: diagnostic_id}} when not is_nil(diagnostic_id) -> true
        _finding -> false
      end) || List.first(findings)

    finding_details =
      case finding do
        %{details: value} when is_map(value) -> value
        _finding -> %{}
      end

    %{
      stage: summary_label(map_value(finding, :stage), "none"),
      retryability: retryability(Map.get(details, :safe_to_retry)),
      failure_class:
        summary_label(
          Map.get(finding_details, :failure_class, map_value(finding, :code)),
          "none"
        ),
      diagnostic_id: summary_label(Map.get(finding_details, :diagnostic_id), "none")
    }
  end

  defp map_value(value, key) when is_map(value), do: Map.get(value, key)
  defp map_value(_value, _key), do: nil

  defp retryability(true), do: "safe_to_retry"
  defp retryability(false), do: "not_safe_to_retry"
  defp retryability(_value), do: "not_applicable"

  defp summary_label(value, fallback) when is_atom(value),
    do: summary_label(Atom.to_string(value), fallback)

  defp summary_label(value, fallback) when is_binary(value) do
    if String.match?(value, ~r/\A[A-Za-z0-9_.\/:\-]+\z/) do
      String.slice(value, 0, 120)
    else
      fallback
    end
  end

  defp summary_label(_value, fallback), do: fallback

  defp with_release_logger(function) do
    case redirect_default_logger() do
      {:ok, original_handler} ->
        original_level = primary_logger_level()
        :ok = :logger.set_primary_config(:level, configured_log_level())

        try do
          function.()
        after
          Logger.flush()
          restore_primary_logger_level(original_level)
          replace_default_handler(original_handler)
        end

      :unavailable ->
        function.()
    end
  end

  defp redirect_default_logger do
    case :logger.get_handler_config(:default) do
      {:ok, original_handler} ->
        redirected = put_in(original_handler, [:config, :type], :standard_error)

        case replace_default_handler(redirected, original_handler) do
          :ok -> {:ok, original_handler}
          :error -> :unavailable
        end

      _missing ->
        :unavailable
    end
  end

  defp replace_default_handler(handler, fallback_handler \\ nil) do
    module = Map.fetch!(handler, :module)
    config = Map.drop(handler, [:id, :module])

    case :logger.remove_handler(:default) do
      :ok ->
        case :logger.add_handler(:default, module, config) do
          :ok ->
            :ok

          _failure ->
            restore_fallback_handler(fallback_handler)
            :error
        end

      _failure ->
        :error
    end
  end

  defp restore_fallback_handler(nil), do: :ok

  defp restore_fallback_handler(handler) do
    :logger.add_handler(
      :default,
      Map.fetch!(handler, :module),
      Map.drop(handler, [:id, :module])
    )
  end

  defp primary_logger_level do
    :logger.get_primary_config()
    |> Map.get(:level, :info)
  end

  defp restore_primary_logger_level(level) do
    :logger.set_primary_config(:level, level)
  end

  defp configured_log_level do
    case System.get_env("FAVN_LOG_LEVEL", "info") do
      "debug" -> :debug
      "info" -> :info
      "notice" -> :notice
      "warning" -> :warning
      "error" -> :error
      "critical" -> :critical
      "alert" -> :alert
      "emergency" -> :emergency
      _invalid -> :info
    end
  end

  defp exit_code(%{contract_version: 1} = details), do: Result.exit_code(details)
  defp exit_code(%{status: :ok}), do: 0
  defp exit_code(_details), do: 70
end
