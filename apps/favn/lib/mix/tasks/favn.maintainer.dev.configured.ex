defmodule Mix.Tasks.Favn.Maintainer.Dev.Configured do
  use Mix.Task

  @moduledoc false
  @requirements ["app.config"]

  @impl Mix.Task
  def run(args), do: Mix.Tasks.Favn.Maintainer.Dev.run_configured(args)
end
