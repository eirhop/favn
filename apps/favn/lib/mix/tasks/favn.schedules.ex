defmodule Mix.Tasks.Favn.Schedules do
  use Mix.Task

  @shortdoc "Lists, previews, activates, and deactivates schedules"

  @moduledoc """
  Operates schedules in the running local Favn stack.

      mix favn.schedules list
      mix favn.schedules show SCHEDULE_ID
      mix favn.schedules preview SCHEDULE_ID --limit 5
      mix favn.schedules activate SCHEDULE_ID --reason "reviewed"
      mix favn.schedules deactivate SCHEDULE_ID --reason "maintenance"

  Publishing a schedule never activates it. New schedules, and changed
  definitions whose fingerprint no longer matches the approved fingerprint,
  remain disabled until `activate` is run.
  """

  alias Favn.CLI
  alias Favn.CLI.Error

  @root_switches [root_dir: :string]
  @preview_switches [root_dir: :string, limit: :integer]
  @change_switches [root_dir: :string, reason: :string]
  @output_keys %{
    "activation_state" => :activation_state,
    "due_at" => :due_at,
    "id" => :id,
    "next_due_at" => :next_due_at,
    "status" => :status,
    "window" => :window
  }

  @impl Mix.Task
  def run(args) do
    case parse_args(args) do
      {:ok, {:list, opts}} -> print_list(CLI.list_schedules(opts))
      {:ok, {:show, id, opts}} -> print_json(CLI.get_schedule(id, opts))
      {:ok, {:preview, id, opts}} -> print_preview(CLI.preview_schedule(id, opts))
      {:ok, {:activate, id, opts}} -> change(id, true, opts)
      {:ok, {:deactivate, id, opts}} -> change(id, false, opts)
      {:error, message} -> Mix.raise(message)
    end
  end

  @doc false
  def parse_args(["list" | args]), do: no_argument(:list, args, @root_switches)
  def parse_args(["show" | args]), do: one_argument(:show, args, @root_switches)
  def parse_args(["preview" | args]), do: one_argument(:preview, args, @preview_switches)

  def parse_args([command | args]) when command in ["activate", "deactivate"] do
    action = String.to_existing_atom(command)

    with {:ok, {^action, id, opts}} <- one_argument(action, args, @change_switches),
         reason when is_binary(reason) <- Keyword.get(opts, :reason),
         true <- String.trim(reason) != "" do
      {:ok, {action, id, opts}}
    else
      false -> {:error, "--reason must not be blank"}
      nil -> {:error, "missing required option: --reason"}
      {:error, _reason} = error -> error
    end
  end

  def parse_args([]), do: {:error, "missing subcommand; usage: #{usage()}"}

  def parse_args([unknown | _]),
    do: {:error, "unknown subcommand #{inspect(unknown)}; usage: #{usage()}"}

  defp no_argument(command, args, switches) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: switches)

    if invalid == [] and rest == [],
      do: {:ok, {command, opts}},
      else: {:error, "invalid arguments"}
  end

  defp one_argument(command, args, switches) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: switches)

    case {invalid, rest} do
      {[], [id]} -> {:ok, {command, id, opts}}
      {[], []} -> {:error, "missing SCHEDULE_ID"}
      _invalid -> {:error, "expected one SCHEDULE_ID with valid options"}
    end
  end

  defp change(id, enabled, opts) do
    reason = Keyword.fetch!(opts, :reason)

    result =
      if enabled,
        do: CLI.activate_schedule(id, reason, opts),
        else: CLI.deactivate_schedule(id, reason, opts)

    print_json(result)
  end

  defp print_list({:ok, schedules}) do
    IO.puts("Schedules")
    IO.puts("count: #{length(schedules)}")

    Enum.each(schedules, fn schedule ->
      IO.puts(
        "schedule=#{value(schedule, "id")} state=#{value(schedule, "activation_state")} " <>
          "next_due_at=#{value(schedule, "next_due_at") || "n/a"}"
      )
    end)
  end

  defp print_list({:error, reason}), do: Mix.raise(error_message(reason))

  defp print_preview({:ok, occurrences}) do
    occurrences
    |> preview_lines()
    |> Enum.each(&IO.puts/1)
  end

  defp print_preview({:error, reason}), do: Mix.raise(error_message(reason))

  @doc false
  def preview_lines(occurrences) do
    Enum.map(occurrences, fn occurrence ->
      "due_at=#{value(occurrence, "due_at")} status=#{value(occurrence, "status")} " <>
        "window=#{inspect(value(occurrence, "window"))}"
    end)
  end

  defp print_json({:ok, payload}), do: IO.puts(JSON.encode!(payload))
  defp print_json({:error, reason}), do: Mix.raise(error_message(reason))

  defp value(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Map.fetch!(@output_keys, key))
    end
  end

  defp error_message(:not_running), do: "Favn is not running; start it with mix favn.dev"

  defp error_message(reason),
    do:
      Error.format(reason,
        context: "schedule command",
        next: "list schedules and retry with the exact schedule id"
      )

  defp usage, do: "mix favn.schedules list|show|preview|activate|deactivate"
end
