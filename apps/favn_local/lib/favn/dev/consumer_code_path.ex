defmodule Favn.Dev.ConsumerCodePath do
  @moduledoc false

  @spec ebin_paths(keyword()) :: [Path.t()]
  def ebin_paths(_opts \\ []) do
    build_path = Mix.Project.build_path() |> Path.expand(File.cwd!())

    build_path
    |> Path.join("lib/*/ebin")
    |> Path.wildcard()
    |> Enum.filter(&File.dir?/1)
    |> Enum.uniq()
    |> Enum.sort()
  end
end
