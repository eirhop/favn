defmodule Mix.Tasks.Favn.Query do
  use Mix.Task

  @shortdoc "Runs a local Favn SQL query"

  @moduledoc """
  Runs a local Favn SQL query.

      mix favn.query "select * from raw.sales.orders"

  Queries use a best-effort read-only guardrail by default. This prevents common
  accidental mutations before connecting, but it is not a SQL sandbox or security
  boundary. Pass `--allow-write` for deliberate local mutation and `--connection
  NAME` when more than one SQL connection is configured.

  Environment variables must be loaded before invoking Mix. The task starts
  only `:favn_sql_runtime`; it does not start the consumer application.
  """

  alias Favn.CLI

  @switches [connection: :string, allow_write: :boolean, limit: :integer]

  @requirements ["app.config"]

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("compile")

    case parse_args(args) do
      {:ok, {sql, opts}} -> run_query(sql, opts)
      {:error, message} -> Mix.raise(message)
    end
  end

  @doc false
  def parse_args(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)

    case {invalid, rest} do
      {[], [sql]} ->
        {:ok, {sql, opts}}

      {[], _other} ->
        {:error,
         "usage: mix favn.query \"select ...\" [--connection NAME] [--limit N] [--allow-write]"}

      {_invalid, _rest} ->
        {:error, "invalid option for mix favn.query"}
    end
  end

  def print_result(%Favn.SQL.Result{} = result, rows, limit) do
    columns = result.columns || []

    if columns == [] do
      IO.puts(inspect(result))
    else
      IO.puts(Enum.join(columns, "\t"))

      Enum.each(rows, fn row ->
        columns
        |> Enum.map(&format_cell(Map.get(row, &1)))
        |> Enum.join("\t")
        |> IO.puts()
      end)

      total = length(result.rows || [])

      if total > length(rows) do
        IO.puts("showing #{length(rows)} of #{total} rows (limit #{limit})")
      else
        IO.puts("#{total} row(s)")
      end
    end
  end

  defp run_query(sql, opts) do
    case CLI.query(sql, opts) do
      {:ok, %{result: %Favn.SQL.Result{} = result, displayed_rows: rows, display_limit: limit}} ->
        print_result(result, rows, limit)

      {:ok, result} ->
        IO.puts(inspect(result))

      {:error, reason} ->
        Mix.raise(format_error(reason))
    end
  end

  defp format_cell(nil), do: "NULL"
  defp format_cell(value) when is_binary(value), do: value
  defp format_cell(value), do: inspect(value)

  defp format_error(reason) when is_binary(reason), do: reason
  defp format_error(reason), do: inspect(reason)
end
