defmodule Mix.Tasks.Favn.Inspect do
  use Mix.Task

  @shortdoc "Inspects local Favn SQL relations"

  @moduledoc """
  Inspects local Favn SQL relations.

      mix favn.inspect relation raw.sales.orders
      mix favn.inspect partitions raw.sales.orders

  Pass `--connection NAME` when more than one Favn SQL connection is configured.

  The task starts the current Mix app before connecting, and the local data
  inspection boundary starts `:favn_sql_runtime`, including the supervised SQL
  session pool. Users do not need to wrap it in `mix do app.start + ...`.
  """

  alias Favn.Dev

  @switches [connection: :string]

  @impl Mix.Task
  def run(args) do
    case parse_args(args) do
      {:ok, {command, relation, opts}} ->
        with :ok <- ensure_app_started() do
          run_command(command, relation, opts)
        else
          {:error, reason} -> Mix.raise(format_error(reason))
        end

      {:error, message} ->
        Mix.raise(message)
    end
  end

  @doc false
  def parse_args([command | rest]) when command in ["relation", "partitions"] do
    {opts, args, invalid} = OptionParser.parse(rest, strict: @switches)

    case {invalid, args} do
      {[], [relation]} -> {:ok, {command, relation, opts}}
      {[], _other} -> {:error, "usage: mix favn.inspect #{command} RELATION [--connection NAME]"}
      {_invalid, _args} -> {:error, "invalid option for mix favn.inspect #{command}"}
    end
  end

  def parse_args(_args) do
    {:error, "usage: mix favn.inspect relation RELATION | mix favn.inspect partitions RELATION"}
  end

  defp run_command("relation", relation, opts) do
    case Dev.inspect_relation(relation, opts) do
      {:ok, result} -> print_relation(result)
      {:error, reason} -> Mix.raise(format_error(reason))
    end
  end

  defp run_command("partitions", relation, opts) do
    case Dev.inspect_partitions(relation, opts) do
      {:ok, result} -> print_partitions(result)
      {:error, reason} -> Mix.raise(format_error(reason))
    end
  end

  defp ensure_app_started do
    Mix.Task.run("app.start")
    :ok
  end

  defp print_relation(result) do
    IO.puts("relation: #{format_relation(result.relation)}")
    IO.puts("metadata: #{inspect(result.metadata)}")
    IO.puts("row_count: #{inspect(result.row_count)}")
    IO.puts("columns:")

    Enum.each(result.columns, fn column ->
      IO.puts("  #{column.name}\t#{column.data_type || "unknown"}")
    end)

    IO.puts("sample:")
    print_query_result(result.sample)
  end

  defp print_partitions(result) do
    IO.puts("relation: #{format_relation(result.relation)}")
    IO.puts("partitions: #{inspect(result.partitions)}")
    IO.puts("metadata: #{inspect(result.metadata)}")
  end

  defp print_query_result({:error, reason}), do: IO.puts("  unavailable: #{format_error(reason)}")

  defp print_query_result(%Favn.SQL.Result{} = result),
    do: Mix.Tasks.Favn.Query.print_result(result, result.rows || [], length(result.rows || []))

  defp print_query_result(other), do: IO.puts("  #{inspect(other)}")

  defp format_relation(ref) do
    [ref.connection, ref.catalog, ref.schema, ref.name]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(".")
  end

  defp format_error(reason) when is_binary(reason), do: reason
  defp format_error(reason), do: inspect(reason)
end
