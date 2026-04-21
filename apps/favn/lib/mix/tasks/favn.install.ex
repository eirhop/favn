defmodule Mix.Tasks.Favn.Install do
  use Mix.Task

  @shortdoc "Resolves project-local Favn install inputs"

  @moduledoc """
  Resolves and validates project-local install inputs under `.favn/install`.
  """

  alias Favn.Dev

  @impl Mix.Task
  def run(args) do
    {opts, _rest, _invalid} =
      OptionParser.parse(args,
        strict: [
          root_dir: :string,
          force: :boolean,
          skip_web_install: :boolean,
          skip_tool_checks: :boolean
        ]
      )

    case Dev.install(opts) do
      {:ok, :installed} ->
        IO.puts("Favn install complete")

      {:ok, :already_installed} ->
        IO.puts("Favn install is already up to date")

      {:error, {:missing_tool, tool}} ->
        Mix.raise("install failed: missing required tool #{tool}")

      {:error, reason} ->
        Mix.raise("install failed: #{inspect(reason)}")
    end
  end
end
