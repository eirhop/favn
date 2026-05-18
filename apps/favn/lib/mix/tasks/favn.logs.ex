defmodule Mix.Tasks.Favn.Logs do
  use Mix.Task

  @shortdoc "Prints local Favn service logs"

  @moduledoc """
  Reads project-local logs under `.favn/logs`.

  Passing a run id prints persisted run events from the orchestrator API:

      mix favn.logs RUN_ID
  """

  alias Favn.Dev

  @impl Mix.Task
  def run(args) do
    case parse_args(args) do
      {:ok, {:services, opts}} ->
        :ok = opts |> normalize_service() |> Dev.logs()

      {:ok, {:run_events, run_id, opts}} ->
        print_run_events(run_id, opts)

      {:error, message} ->
        Mix.raise(message)
    end
  end

  @doc false
  def parse_args(args) do
    {opts, rest, invalid} =
      OptionParser.parse(args,
        strict: [root_dir: :string, service: :string, tail: :integer, follow: :boolean]
      )

    case {invalid, rest, Keyword.has_key?(opts, :service), Keyword.get(opts, :follow, false)} do
      {[], [], _service?, _follow?} ->
        {:ok, {:services, opts}}

      {[], [run_id], false, false} ->
        {:ok, {:run_events, run_id, opts}}

      {[], [_run_id], _service?, _follow?} ->
        {:error, "RUN_ID cannot be combined with --service or --follow"}

      {[], _many, _service?, _follow?} ->
        {:error, "expected at most one RUN_ID for mix favn.logs"}

      {_invalid, _rest, _service?, _follow?} ->
        {:error, "invalid option for mix favn.logs"}
    end
  end

  defp normalize_service(opts) do
    case Keyword.get(opts, :service) do
      nil ->
        opts

      "operator" ->
        Keyword.put(opts, :service, :operator)

      "web" ->
        Keyword.put(opts, :service, :web)

      "orchestrator" ->
        Keyword.put(opts, :service, :orchestrator)

      "runner" ->
        Keyword.put(opts, :service, :runner)

      "all" ->
        Keyword.put(opts, :service, :all)

      other ->
        Mix.raise(
          "invalid service #{inspect(other)}; expected operator|web|orchestrator|runner|all"
        )
    end
  end

  defp print_run_events(run_id, opts) do
    event_opts =
      Keyword.take(opts, [:root_dir]) |> Keyword.put(:limit, Keyword.get(opts, :tail, 100))

    case Dev.list_run_events(run_id, event_opts) do
      {:ok, events} ->
        IO.puts("Run events for #{run_id}")
        IO.puts("count: #{length(events)}")
        Enum.each(events, &IO.puts(format_event(&1)))

      {:error, reason} ->
        Mix.raise(error_message(reason))
    end
  end

  defp format_event(event) do
    Enum.join(
      [
        "seq=#{field(event, "sequence") || "?"}",
        "at=#{field(event, "occurred_at") || "n/a"}",
        "type=#{field(event, "event_type") || "unknown"}",
        "entity=#{field(event, "entity") || "unknown"}",
        "status=#{field(event, "status") || "n/a"}",
        "asset=#{field(event, "asset_ref") || "n/a"}"
      ],
      " "
    )
  end

  defp field(map, key), do: Map.get(map, key) || Map.get(map, atom_key(key))

  defp atom_key("sequence"), do: :sequence
  defp atom_key("occurred_at"), do: :occurred_at
  defp atom_key("event_type"), do: :event_type
  defp atom_key("entity"), do: :entity
  defp atom_key("status"), do: :status
  defp atom_key("asset_ref"), do: :asset_ref

  defp error_message(:stack_not_running), do: "stack not running; use mix favn.dev"

  defp error_message(:stack_not_healthy),
    do: "stack not healthy; use mix favn.stop then mix favn.dev"

  defp error_message(reason), do: "run events unavailable: #{inspect(reason)}"
end
