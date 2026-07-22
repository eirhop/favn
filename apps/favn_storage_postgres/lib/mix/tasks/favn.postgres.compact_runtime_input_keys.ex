defmodule Mix.Tasks.Favn.Postgres.CompactRuntimeInputKeys do
  @moduledoc """
  Removes unreferenced runtime-input key versions from the Storage V2 inventory.

  This task never reads, writes, or reports key material.
  """

  use Mix.Task

  alias FavnStoragePostgres.Release
  alias Mix.Tasks.Favn.Postgres.ReleaseHelpers

  @shortdoc "Removes unreferenced runtime-input key versions"

  @impl true
  def run(args) do
    {options, positional, invalid} = OptionParser.parse(args, strict: [version: :keep])

    if positional != [] or invalid != [] or Keyword.get_values(options, :version) == [] do
      Mix.raise(
        "usage: mix favn.postgres.compact_runtime_input_keys --version VERSION [--version VERSION]"
      )
    end

    versions =
      Enum.map(Keyword.get_values(options, :version), fn value ->
        case Integer.parse(value) do
          {version, ""} -> version
          _invalid -> Mix.raise("runtime-input key versions must be positive integers")
        end
      end)

    Mix.Task.run("app.config")

    Release.compact_runtime_input_keys(versions)
    |> ReleaseHelpers.report("Runtime-input key inventory is compact")
  end
end
