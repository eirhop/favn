defmodule Mix.Tasks.Favn.Logs do
  use Mix.Task

  @shortdoc "Prints local Favn service logs"

  @moduledoc """
  Reads project-local logs under `.favn/logs`.
  """

  alias Favn.Dev
  alias Mix.Tasks.Favn.CLIArgs

  @impl Mix.Task
  def run(args) do
    opts =
      CLIArgs.parse_no_args!("favn.logs", args,
        root_dir: :string,
        service: :string,
        tail: :integer,
        follow: :boolean
      )

    opts = normalize_service(opts)

    :ok = Dev.logs(opts)
  end

  defp normalize_service(opts) do
    case Keyword.get(opts, :service) do
      nil ->
        opts

      "web" ->
        Keyword.put(opts, :service, :web)

      "orchestrator" ->
        Keyword.put(opts, :service, :orchestrator)

      "runner" ->
        Keyword.put(opts, :service, :runner)

      "all" ->
        Keyword.put(opts, :service, :all)

      other ->
        Mix.raise("invalid service #{inspect(other)}; expected web|orchestrator|runner|all")
    end
  end
end
