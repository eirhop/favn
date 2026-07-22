defmodule Mix.Tasks.Favn.Rebuild do
  use Mix.Task

  @shortdoc "Plans and controls local asset rebuilds"

  @moduledoc """
  Plans and controls manual asset rebuilds through the running local Favn stack.

      mix favn.rebuild plan MyApp.Assets.Orders --reason "schema changed"
      mix favn.rebuild start PLAN_ID --plan-hash PLAN_HASH
      mix favn.rebuild status OPERATION_ID
      mix favn.rebuild cancel OPERATION_ID --reason "operator request"
      mix favn.rebuild retry OPERATION_ID
      mix favn.rebuild reconcile OPERATION_ID

  Planning does not execute work. Review the returned immutable plan, then pass
  its exact id and hash to `start` to approve it explicitly.
  """

  alias Favn.Dev

  @plan_switches [root_dir: :string, reason: :string]
  @start_switches [root_dir: :string, plan_hash: :string]
  @reason_switches [root_dir: :string, reason: :string]
  @id_switches [root_dir: :string]

  @impl Mix.Task
  def run(args) do
    case parse_args(args) do
      {:ok, {:plan, asset, opts}} -> plan(asset, opts)
      {:ok, {:start, plan_id, opts}} -> start(plan_id, opts)
      {:ok, {:status, operation_id, opts}} -> status(operation_id, opts)
      {:ok, {:cancel, operation_id, opts}} -> cancel(operation_id, opts)
      {:ok, {:retry, operation_id, opts}} -> retry(operation_id, opts)
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
         {:ok, _hash} <- required_plan_hash(opts) do
      {:ok, {:start, plan_id, opts}}
    end
  end

  def parse_args(["status" | args]), do: id_command(args, :status)

  def parse_args(["cancel" | args]) do
    with {:ok, operation_id, opts} <-
           one_argument(args, @reason_switches, "cancel", "OPERATION_ID"),
         {:ok, _reason} <- required_option(opts, :reason) do
      {:ok, {:cancel, operation_id, opts}}
    end
  end

  def parse_args(["retry" | args]), do: id_command(args, :retry)
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
    command_usage = "mix favn.rebuild #{command} #{argument_name}"

    case {invalid, rest} do
      {[], [argument]} -> {:ok, argument, opts}
      {[], []} -> {:error, "missing #{argument_name}; usage: #{command_usage}"}
      {[], _many} -> {:error, "expected one #{argument_name}; usage: #{command_usage}"}
      {_invalid, _rest} -> {:error, "invalid option for mix favn.rebuild #{command}"}
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

  defp required_plan_hash(opts) do
    with {:ok, hash} <- required_option(opts, :plan_hash),
         true <- Regex.match?(~r/\A[0-9a-f]{64}\z/, hash) do
      {:ok, hash}
    else
      false -> {:error, "--plan-hash must be 64 lowercase hexadecimal characters"}
      {:error, _reason} = error -> error
    end
  end

  defp missing_option(key) do
    {:error, "missing required option: --#{key |> Atom.to_string() |> String.replace("_", "-")}"}
  end

  defp plan(asset, opts) do
    reason = Keyword.fetch!(opts, :reason)

    case Dev.plan_rebuild(asset, reason, opts) do
      {:ok, rebuild_plan} -> print_plan(rebuild_plan)
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp start(plan_id, opts) do
    case Dev.start_rebuild(plan_id, Keyword.fetch!(opts, :plan_hash), opts) do
      {:ok, operation} -> print_operation(operation)
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp status(operation_id, opts) do
    case Dev.get_rebuild(operation_id, opts) do
      {:ok, operation} -> print_operation(operation)
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp cancel(operation_id, opts) do
    case Dev.cancel_rebuild(operation_id, Keyword.fetch!(opts, :reason), opts) do
      {:ok, operation} -> print_operation(operation)
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp retry(operation_id, opts) do
    case Dev.retry_rebuild(operation_id, opts) do
      {:ok, operation} -> print_operation(operation)
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp reconcile(operation_id, opts) do
    case Dev.reconcile_rebuild(operation_id, opts) do
      {:ok, operation} -> print_operation(operation)
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  @doc false
  def print_plan(plan) when is_map(plan) do
    payload = value(plan, "payload", %{})
    root_target_id = value(payload, "root_target_id")
    coverage = value(payload, "coverage", %{})
    evaluated_range = value(payload, "evaluated_range", %{})
    root_binding = payload |> value("binding_snapshot", %{}) |> Map.get(root_target_id, %{})

    print_fields([
      {"Plan", value(plan, "plan_id")},
      {"Hash", value(plan, "plan_hash")},
      {"Expires", value(plan, "expires_at")},
      {"Evaluated", value(payload, "evaluated_at")},
      {"Target", root_target_id},
      {"Manifest", value(payload, "manifest_version_id")},
      {"Runner release", value(payload, "required_runner_release_id")},
      {"Deployment", value(payload, "deployment_id")},
      {"Declared coverage", period_text(value(coverage, "declared_from"))},
      {"Effective coverage", period_text(value(coverage, "effective_from"))},
      {"Coverage through", coverage_through_text(value(coverage, "through"))},
      {"Coverage timezone", value(coverage, "timezone")},
      {"Availability delay", duration_text(value(coverage, "availability_delay_seconds"))},
      {"Evaluated range", range_text(evaluated_range)},
      {"Active generation", value(payload, "active_generation_id")},
      {"Candidate generation", value(payload, "candidate_generation_id")},
      {"Compatibility", value(root_binding, "compatibility_status")},
      {"Compatibility reason", value(root_binding, "reason_code")},
      {"Compatibility diff", json_text(value(root_binding, "compatibility_diff", %{}))},
      {"Actions", value(payload, "action_count", count(value(payload, "actions", [])))},
      {"Items", value(payload, "item_count", count(value(payload, "items", [])))},
      {"Items digest", value(payload, "items_digest")}
    ])

    print_capabilities(value(payload, "capabilities", %{}))
    print_actions(value(payload, "actions", []))

    Mix.shell().info(
      "Start only after review: mix favn.rebuild start #{value(plan, "plan_id")} --plan-hash #{value(plan, "plan_hash")}"
    )
  end

  @doc false
  def print_operation(operation) when is_map(operation) do
    progress = value(operation, "progress", %{})

    print_fields([
      {"Operation", value(operation, "operation_id")},
      {"Target", value(operation, "root_target_id")},
      {"State", value(operation, "state")},
      {"Phase", value(operation, "phase")},
      {"Plan hash", value(operation, "plan_hash")},
      {"Progress", progress_text(progress)},
      {"Error", error_text(value(operation, "terminal_error"))}
    ])
  end

  defp print_fields(fields) do
    Enum.each(fields, fn {label, value} ->
      if present?(value), do: Mix.shell().info("#{label}: #{value}")
    end)
  end

  defp print_capabilities(capabilities)
       when is_map(capabilities) and map_size(capabilities) > 0 do
    Mix.shell().info("Adapter capabilities:")

    capabilities
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.each(fn {target_id, target_capabilities} ->
      Mix.shell().info("  #{target_id}: #{json_text(target_capabilities)}")
    end)
  end

  defp print_capabilities(_capabilities), do: :ok

  defp print_actions(actions) when is_list(actions) and actions != [] do
    Mix.shell().info("Planned actions:")

    Enum.each(actions, fn action ->
      Mix.shell().info(
        "  #{value(action, "ordinal", 0)}. #{value(action, "target_id")} — #{value(action, "action")}"
      )

      print_action_detail("reason", value(action, "reason"))
      print_action_detail("mapping proof", value(action, "mapping_proof"))
      print_action_detail("pinned inputs", value(action, "pinned_input_generation_ids", []))

      candidate_generation = value(action, "candidate_generation", %{})

      print_action_detail(
        "candidate generation",
        value(candidate_generation, "target_generation_id")
      )
    end)
  end

  defp print_actions(_actions), do: :ok

  defp print_action_detail(_label, value) when value in [nil, [], %{}], do: :ok

  defp print_action_detail(label, value),
    do: Mix.shell().info("     #{label}: #{json_text(value)}")

  defp progress_text(progress) when is_map(progress) do
    completed = value(progress, "completed", 0)
    total = value(progress, "total", 0)
    "#{completed}/#{total}"
  end

  defp progress_text(_progress), do: nil
  defp error_text(nil), do: nil
  defp error_text(error) when is_binary(error), do: error
  defp error_text(error) when is_map(error), do: value(error, "message") || value(error, "code")
  defp error_text(_error), do: "rebuild failed"

  defp period_text(period) when is_map(period) do
    [value(period, "kind"), range_text(period), value(period, "timezone")]
    |> Enum.filter(&present?/1)
    |> Enum.join(" · ")
  end

  defp period_text(_period), do: nil

  defp coverage_through_text(through) when is_map(through), do: period_text(through)
  defp coverage_through_text(through), do: through

  defp range_text(range) when is_map(range) do
    case {value(range, "start_at"), value(range, "end_at")} do
      {nil, nil} -> nil
      {start_at, end_at} -> "#{start_at || "-"}..#{end_at || "-"}"
    end
  end

  defp range_text(_range), do: nil

  defp duration_text(seconds) when is_integer(seconds), do: "#{seconds} seconds"
  defp duration_text(_seconds), do: nil

  defp json_text(value) when is_binary(value), do: value
  defp json_text(value), do: JSON.encode!(value)

  defp value(map, key, default \\ nil) do
    Map.get(map, key, Map.get(map, String.to_atom(key), default))
  end

  defp count(items) when is_list(items), do: length(items)
  defp count(_items), do: 0
  defp present?(value), do: not is_nil(value) and value != ""

  defp error_message(:stack_not_running), do: "stack not running; use mix favn.dev"
  defp error_message(:install_required), do: "local install is missing; use mix favn.install"
  defp error_message(:install_stale), do: "local install is stale; use mix favn.install"
  defp error_message(:invalid_local_secrets), do: "local service credentials are unavailable"
  defp error_message(:rebuild_requires_asset), do: "rebuild target must be an asset"
  defp error_message(_reason), do: "rebuild request failed; inspect orchestrator logs for details"

  defp usage do
    "mix favn.rebuild plan ASSET --reason REASON | start PLAN_ID --plan-hash HASH | status OPERATION_ID | cancel OPERATION_ID --reason REASON | retry OPERATION_ID | reconcile OPERATION_ID"
  end
end
