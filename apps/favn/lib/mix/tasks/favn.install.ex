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

      {:error, {:tool_check_failed, tool, status, output}} ->
        Mix.raise(
          "install failed: required tool #{tool} check failed (status=#{status}): #{output}"
        )

      {:error, {:web_install_failed, status, output}} ->
        Mix.raise("install failed: web dependency install failed (status=#{status}): #{output}")

      {:error, {:runtime_deps_install_failed, status, output}} ->
        Mix.raise("install failed: runtime deps install failed (status=#{status}): #{output}")

      {:error, reason} ->
        Mix.raise("install failed: #{inspect(reason)}")
    end
  end
end
