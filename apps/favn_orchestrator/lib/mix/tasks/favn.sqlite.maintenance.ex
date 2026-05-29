defmodule Mix.Tasks.Favn.Sqlite.Maintenance do
  @moduledoc """
  Runs local SQLite control-plane maintenance commands.

      mix favn.sqlite.maintenance status
      mix favn.sqlite.maintenance migrate --dry-run
      mix favn.sqlite.maintenance migrate --apply
      mix favn.sqlite.maintenance backup --to /var/backups/favn/control-plane.db
      mix favn.sqlite.maintenance verify-backup --path /var/backups/favn/control-plane.db

  The task is a thin local operator wrapper around
  `FavnOrchestrator.Operator.Maintenance`.
  """

  use Mix.Task

  alias FavnOrchestrator.Operator.Maintenance
  alias FavnOrchestrator.Operator.Maintenance.MaintenanceError

  @shortdoc "Runs SQLite control-plane maintenance"

  @impl true
  def run(args) do
    Mix.Task.run("app.start")

    with {:ok, command, opts} <- parse_args(args),
         {:ok, result} <- run_command(command, opts) do
      print_result(result)
    else
      {:error, %MaintenanceError{} = error} ->
        print_error(error)
        Mix.raise("sqlite maintenance failed: #{error.category}")

      {:error, reason} ->
        Mix.raise("invalid sqlite maintenance command: #{inspect(reason)}")
    end
  end

  defp parse_args([]), do: {:error, :missing_subcommand}

  defp parse_args(["status" | argv]) do
    with {:ok, opts} <- parse_options(argv, strict: []) do
      {:ok, :status, opts}
    end
  end

  defp parse_args(["migrate" | argv]) do
    with {:ok, opts} <- parse_options(argv, strict: [dry_run: :boolean, apply: :boolean]),
         :ok <- reject_both(opts, :dry_run, :apply) do
      apply? = Keyword.get(opts, :apply, false)
      {:ok, :migrate, [apply?: apply?, dry_run?: not apply?]}
    end
  end

  defp parse_args(["backup" | argv]) do
    with {:ok, opts} <-
           parse_options(argv,
             strict: [to: :string, verify: :boolean, no_verify: :boolean, overwrite: :boolean]
           ),
         {:ok, destination} <- fetch_required(opts, :to),
         :ok <- reject_both(opts, :verify, :no_verify) do
      {:ok, :backup,
       [
         to: destination,
         verify?: Keyword.get(opts, :verify, true),
         overwrite?: Keyword.get(opts, :overwrite, false)
       ]}
    end
  end

  defp parse_args(["verify-backup" | argv]) do
    with {:ok, opts} <- parse_options(argv, strict: [path: :string]),
         {:ok, path} <- fetch_required(opts, :path) do
      {:ok, :verify_backup, [path: path]}
    end
  end

  defp parse_args([command | _argv]), do: {:error, {:unknown_subcommand, command}}

  defp parse_options(argv, parser_opts) do
    {opts, rest, invalid} = OptionParser.parse(argv, parser_opts)

    cond do
      invalid != [] -> {:error, {:invalid_options, invalid}}
      rest != [] -> {:error, {:unexpected_args, rest}}
      true -> {:ok, opts}
    end
  end

  defp reject_both(opts, left, right) do
    if Keyword.get(opts, left, false) and Keyword.get(opts, right, false) do
      {:error, {:mutually_exclusive, left, right}}
    else
      :ok
    end
  end

  defp fetch_required(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:missing_required_option, key}}
    end
  end

  defp run_command(:status, opts), do: Maintenance.status(opts)
  defp run_command(:migrate, opts), do: Maintenance.migrate(opts)
  defp run_command(:backup, opts), do: Maintenance.backup(opts)
  defp run_command(:verify_backup, opts), do: Maintenance.verify_backup(opts)

  defp print_result(%Maintenance.StatusResult{} = result) do
    Mix.shell().info("operation=status")
    Mix.shell().info("adapter=#{result.adapter}")
    Mix.shell().info("ready=#{result.ready?}")
    Mix.shell().info("readiness_status=#{result.readiness_status}")
    Mix.shell().info("schema_status=#{result.schema_status}")
    Mix.shell().info("migration_mode=#{result.migration_mode}")
    Mix.shell().info("missing_versions=#{length(result.missing_versions)}")
    Mix.shell().info("future_versions=#{length(result.future_versions)}")
    Mix.shell().info("missing_tables=#{length(result.missing_tables)}")
  end

  defp print_result(%Maintenance.MigrationResult{} = result) do
    Mix.shell().info("operation=migrate")
    Mix.shell().info("adapter=#{result.adapter}")
    Mix.shell().info("action=#{result.action}")
    Mix.shell().info("dry_run=#{result.dry_run?}")
    Mix.shell().info("previous_schema_status=#{result.previous_schema_status}")
    Mix.shell().info("final_schema_status=#{result.final_schema_status}")
    Mix.shell().info("migrated_count=#{result.migrated_count}")
    Mix.shell().info("duration_ms=#{result.duration_ms}")
  end

  defp print_result(%Maintenance.BackupResult{} = result) do
    Mix.shell().info("operation=backup")
    Mix.shell().info("adapter=#{result.adapter}")
    Mix.shell().info("destination=#{inspect(result.destination_identity)}")
    Mix.shell().info("byte_size=#{result.byte_size}")
    Mix.shell().info("checksum=#{result.checksum}")
    Mix.shell().info("checkpoint_policy=#{result.checkpoint_policy}")
    Mix.shell().info("duration_ms=#{result.duration_ms}")

    if result.verification do
      Mix.shell().info("verification_status=#{result.verification.backup_status}")
      Mix.shell().info("verification_schema_status=#{result.verification.schema_status}")
    end
  end

  defp print_result(%Maintenance.VerificationResult{} = result) do
    Mix.shell().info("operation=verify_backup")
    Mix.shell().info("adapter=#{result.adapter}")
    Mix.shell().info("backup_status=#{result.backup_status}")
    Mix.shell().info("integrity_check_status=#{result.integrity_check_status}")
    Mix.shell().info("schema_status=#{result.schema_status}")
    Mix.shell().info("byte_size=#{result.byte_size}")
    Mix.shell().info("checksum=#{result.checksum}")
  end

  defp print_error(%MaintenanceError{} = error) do
    Mix.shell().error("operation=#{error.operation}")
    Mix.shell().error("adapter=#{error.adapter}")
    Mix.shell().error("category=#{error.category}")
    Mix.shell().error("reason=#{inspect(error.reason)}")
    Mix.shell().error("retryable=#{error.retryable?}")

    if error.details != %{} do
      Mix.shell().error("details=#{inspect(error.details)}")
    end
  end
end
