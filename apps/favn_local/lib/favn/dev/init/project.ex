defmodule Favn.Dev.Init.Project do
  @moduledoc """
  Scaffolds the complete customer-owned local Compose and runner starting point.

  The two templates retain their separate ownership contracts. If runner
  scaffolding fails, files newly created by the Compose step are removed so a
  default `mix favn.init` does not leave a misleading partial result.
  """

  alias Favn.Dev.Init.{Compose, Runner}

  @type result :: %{
          compose: Compose.result(),
          created: [Path.t()],
          existing: [Path.t()],
          runner: Runner.result(),
          target: :project
        }

  @doc "Writes the default local Compose file and customer runner template."
  @spec run(keyword()) :: {:ok, result()} | {:error, term()}
  def run(opts) when is_list(opts) do
    compose_opts = Keyword.drop(opts, [:include])
    runner_opts = Keyword.drop(opts, [:profile])

    with {:ok, compose} <- Compose.run(compose_opts) do
      case Runner.run(runner_opts) do
        {:ok, runner} ->
          {:ok,
           %{
             compose: compose,
             created: compose.created ++ runner.created,
             existing: compose.existing ++ runner.existing,
             runner: runner,
             target: :project
           }}

        {:error, _reason} = error ->
          rollback_created(compose.created, opts)
          error
      end
    end
  end

  defp rollback_created(paths, opts) do
    root_dir = opts |> Favn.Dev.Paths.root_dir() |> Path.expand()

    Enum.each(paths, fn path ->
      path
      |> then(&Path.join(root_dir, &1))
      |> File.rm()
    end)
  end
end
