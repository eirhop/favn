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

  The project's `.env` is loaded before `config/runtime.exs` is evaluated. The
  task starts only `:favn_sql_runtime`, including the supervised SQL session
  pool; it does not start the consumer application or configured plugins.
  """

  alias Favn.Dev
  alias Favn.Dev.EnvBootstrap

  @switches [connection: :string, allow_write: :boolean, limit: :integer, root_dir: :string]

  @requirements ["loadpaths"]

  @impl Mix.Task
  def run(args) do
    case parse_args(args) do
      {:ok, {_sql, opts}} -> run_bootstrapped(args, opts)
      {:error, message} -> Mix.raise(message)
    end
  end

  @doc false
  @spec run_configured([String.t()]) :: :ok | no_return()
  def run_configured(args) do
    with {:ok, {sql, opts}} <- parse_args(args),
         {:ok, opts} <- EnvBootstrap.consume(:query, opts) do
      run_query(sql, inspection_opts(opts))
    else
      {:error, message} when is_binary(message) ->
        Mix.raise(message)

      {:error, :env_bootstrap_required} ->
        Mix.raise("favn.query.configured is an internal task; run mix favn.query")

      {:error, reason} ->
        Mix.raise(
          "invalid favn.query environment bootstrap: #{inspect(reason)}; run mix favn.query"
        )
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
    case Dev.query(sql, opts) do
      {:ok, %{result: %Favn.SQL.Result{} = result, displayed_rows: rows, display_limit: limit}} ->
        print_result(result, rows, limit)

      {:ok, result} ->
        IO.puts(inspect(result))

      {:error, reason} ->
        Mix.raise(format_error(reason))
    end
  end

  defp run_bootstrapped(args, opts) do
    case EnvBootstrap.exec(:query, args, opts) do
      {:ok, 0} -> :ok
      {:ok, status} -> System.halt(status)
      {:error, reason} -> Mix.raise("query environment bootstrap failed: #{inspect(reason)}")
    end
  end

  defp inspection_opts(opts),
    do: Keyword.drop(opts, [:env_bootstrap, :env_file_loaded, :root_dir])

  defp format_cell(nil), do: "NULL"
  defp format_cell(value) when is_binary(value), do: value
  defp format_cell(value), do: inspect(value)

  defp format_error(reason) when is_binary(reason), do: reason
  defp format_error(reason), do: inspect(reason)
end
