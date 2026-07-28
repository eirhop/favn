defmodule Mix.Tasks.Favn.Recover do
  use Mix.Task

  @shortdoc "Safely recovers an interrupted Favn-owned target"

  @moduledoc """
  Plans and controls evidence-backed target recovery through the local Favn stack.

      mix favn.recover plan MyApp.Assets.Orders --reason "interrupted materialization"
      mix favn.recover start PLAN_ID --plan-hash PLAN_HASH
      mix favn.recover status OPERATION_ID
      mix favn.recover reconcile OPERATION_ID

  Planning is read-only. Review the immutable evidence and pass its exact id and
  hash to `start`. Recovery refuses arbitrary or unproven physical relations.
  """

  alias Favn.CLI
  alias Favn.CLI.Error

  @plan_switches [root_dir: :string, reason: :string]
  @start_switches [root_dir: :string, plan_hash: :string]
  @id_switches [root_dir: :string]

  @impl Mix.Task
  def run(args) do
    case parse_args(args) do
      {:ok, {:plan, asset, opts}} -> plan(asset, opts)
      {:ok, {:start, plan_id, opts}} -> start(plan_id, opts)
      {:ok, {:status, operation_id, opts}} -> status(operation_id, opts)
      {:ok, {:reconcile, operation_id, opts}} -> reconcile(operation_id, opts)
      {:error, message} -> Mix.raise(message)
    end
  end

  @doc false
  def parse_args(["plan" | args]) do
    with {:ok, asset, opts} <- one_argument(args, @plan_switches, "plan", "ASSET"),
         {:ok, _reason} <- required_option(opts, :reason) do
      {:ok, {:plan, asset, opts}}
    end
  end

  def parse_args(["start" | args]) do
    with {:ok, plan_id, opts} <- one_argument(args, @start_switches, "start", "PLAN_ID"),
         {:ok, _hash} <- required_hash(opts) do
      {:ok, {:start, plan_id, opts}}
    end
  end

  def parse_args(["status" | args]), do: id_command(args, :status)
  def parse_args(["reconcile" | args]), do: id_command(args, :reconcile)
  def parse_args([]), do: {:error, "missing subcommand; usage: #{usage()}"}

  def parse_args([unknown | _args]),
    do: {:error, "unknown subcommand #{inspect(unknown)}; usage: #{usage()}"}

  defp id_command(args, command) do
    with {:ok, operation_id, opts} <-
           one_argument(args, @id_switches, Atom.to_string(command), "OPERATION_ID") do
      {:ok, {command, operation_id, opts}}
    end
  end

  defp one_argument(args, switches, command, argument_name) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: switches)
    command_usage = "mix favn.recover #{command} #{argument_name}"

    case {invalid, rest} do
      {[], [argument]} -> {:ok, argument, opts}
      {[], []} -> {:error, "missing #{argument_name}; usage: #{command_usage}"}
      {[], _many} -> {:error, "expected one #{argument_name}; usage: #{command_usage}"}
      {_invalid, _rest} -> {:error, "invalid option for mix favn.recover #{command}"}
    end
  end

  defp required_hash(opts) do
    with {:ok, hash} <- required_option(opts, :plan_hash),
         true <- Regex.match?(~r/\A[0-9a-f]{64}\z/, hash) do
      {:ok, hash}
    else
      false -> {:error, "--plan-hash must be 64 lowercase hexadecimal characters"}
      {:error, _reason} = error -> error
    end
  end

  defp required_option(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and byte_size(value) > 0 ->
        if String.trim(value) == "", do: missing_option(key), else: {:ok, value}

      _missing ->
        missing_option(key)
    end
  end

  defp missing_option(key) do
    {:error, "missing required option: --#{key |> Atom.to_string() |> String.replace("_", "-")}"}
  end

  defp plan(asset, opts) do
    case CLI.plan_target_recovery(asset, Keyword.fetch!(opts, :reason), opts) do
      {:ok, recovery_plan} ->
        print_plan(recovery_plan)

      {:error, reason} ->
        Mix.raise(error_message(reason))
    end
  end

  defp start(plan_id, opts) do
    case CLI.start_target_recovery(plan_id, Keyword.fetch!(opts, :plan_hash), opts) do
      {:ok, operation} -> print_operation(operation)
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp status(operation_id, opts) do
    case CLI.get_target_recovery(operation_id, opts) do
      {:ok, operation} -> print_operation(operation)
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp reconcile(operation_id, opts) do
    case CLI.reconcile_target_recovery(operation_id, opts) do
      {:ok, operation} -> print_operation(operation)
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  @doc false
  def print_plan(plan) when is_map(plan) do
    payload = value(plan, "payload", %{})

    print_fields([
      {"Plan", value(plan, "plan_id")},
      {"Hash", value(plan, "plan_hash")},
      {"Expires", value(plan, "expires_at")},
      {"Target", value(payload, "target_id")},
      {"Generation", value(payload, "target_generation_id")},
      {"Materialization", value(payload, "materialization_id")},
      {"Physical fingerprint", value(payload, "physical_fingerprint")}
    ])

    Mix.shell().info(
      "Start only after review: mix favn.recover start #{value(plan, "plan_id")} --plan-hash #{value(plan, "plan_hash")}"
    )
  end

  @doc false
  def print_operation(operation) when is_map(operation) do
    print_fields([
      {"Operation", value(operation, "operation_id")},
      {"Target", value(operation, "target_id")},
      {"State", value(operation, "state")},
      {"Phase", value(operation, "phase")},
      {"Generation", value(operation, "target_generation_id")},
      {"Compatibility", value(value(operation, "compatibility_result", %{}), "status")},
      {"Unknown outcome", value(value(operation, "unknown_outcome", %{}), "reason_code")},
      {"Error", value(value(operation, "terminal_error", %{}), "reason_code")}
    ])
  end

  defp print_fields(fields) do
    Enum.each(fields, fn {label, value} ->
      if value not in [nil, ""], do: Mix.shell().info("#{label}: #{value}")
    end)
  end

  defp value(map, key, default \\ nil)

  defp value(map, key, default) when is_map(map),
    do: Map.get(map, key, Map.get(map, String.to_atom(key), default))

  defp value(_value, _key, default), do: default

  @doc false
  def error_message(:target_recovery_requires_asset), do: "recovery target must be an asset"

  def error_message(reason) do
    Error.format(reason,
      context: "target recovery",
      next: "create a new plan or inspect the existing operation"
    )
  end

  defp usage do
    "mix favn.recover plan ASSET --reason REASON | start PLAN_ID --plan-hash HASH | status OPERATION_ID | reconcile OPERATION_ID"
  end
end
