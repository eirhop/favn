defmodule Favn.Dev.Init do
  @moduledoc """
  Dispatches Favn project, deployment, runner, and sample initialization.

  The default project mode scaffolds the complete local deployment and runner
  starting point. DuckDB authoring samples retain their explicit legacy flags.
  """

  alias Favn.Dev.Init.{Compose, Project, Runner, Sample}

  @type result :: Sample.result() | Compose.result() | Runner.result() | Project.result()

  @doc "Initializes the selected authoring sample or deployment template."
  @spec run(keyword()) :: {:ok, result()} | {:error, term()}
  def run(opts) when is_list(opts) do
    case Keyword.get(opts, :target) do
      :compose -> Compose.run(opts)
      "compose" -> Compose.run(opts)
      :runner -> Runner.run(opts)
      "runner" -> Runner.run(opts)
      nil -> default_init(opts)
      target -> {:error, {:unsupported_init_target, target}}
    end
  end

  defp default_init(opts) do
    if Keyword.get(opts, :duckdb, false) or Keyword.get(opts, :sample, false) do
      Sample.run(opts)
    else
      Project.run(opts)
    end
  end
end
