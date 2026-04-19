defmodule Mix.Tasks.Favn.Dev do
  use Mix.Task

  @shortdoc "Starts local Favn dev stack"

  @moduledoc """
  Starts local `favn_web + favn_orchestrator + favn_runner` in foreground mode.
  """

  alias Favn.Dev

  @impl Mix.Task
  def run(args) do
    {opts, _rest, _invalid} =
      OptionParser.parse(args, strict: [root_dir: :string, sqlite: :boolean])

    opts = maybe_sqlite(opts)

    case Dev.dev(opts) do
      :ok -> :ok
      {:error, :stack_already_running} -> Mix.raise("local stack already running")
      {:error, reason} -> Mix.raise("failed to start local stack: #{inspect(reason)}")
    end
  end

  defp maybe_sqlite(opts) do
    if Keyword.get(opts, :sqlite, false) do
      opts |> Keyword.delete(:sqlite) |> Keyword.put(:storage, :sqlite)
    else
      Keyword.delete(opts, :sqlite)
    end
  end
end
