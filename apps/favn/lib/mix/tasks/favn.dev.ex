defmodule Mix.Tasks.Favn.Dev do
  use Mix.Task

  @dialyzer {:nowarn_function, run: 1}

  @shortdoc "Starts local Favn dev stack"

  @moduledoc """
  Starts local `favn_web + favn_orchestrator + favn_runner` in foreground mode.
  """

  alias Favn.Dev

  @impl Mix.Task
  def run(args) do
    {opts, _rest, _invalid} =
      OptionParser.parse(args, strict: [root_dir: :string, sqlite: :boolean, postgres: :boolean])

    opts = normalize_storage_flags(opts)

    case Dev.dev(opts) do
      :ok -> :ok
      {:error, :stack_already_running} -> Mix.raise("local stack already running")
      {:error, :install_required} -> Mix.raise("install required; run mix favn.install")
      {:error, :install_stale} -> Mix.raise("install stale; run mix favn.install --force")
      {:error, reason} -> Mix.raise("failed to start local stack: #{inspect(reason)}")
    end
  end

  defp normalize_storage_flags(opts) do
    sqlite? = Keyword.get(opts, :sqlite, false)
    postgres? = Keyword.get(opts, :postgres, false)

    opts = opts |> Keyword.delete(:sqlite) |> Keyword.delete(:postgres)

    cond do
      sqlite? and postgres? -> Mix.raise("choose only one storage flag: --sqlite or --postgres")
      sqlite? -> Keyword.put(opts, :storage, :sqlite)
      postgres? -> Keyword.put(opts, :storage, :postgres)
      true -> opts
    end
  end
end
