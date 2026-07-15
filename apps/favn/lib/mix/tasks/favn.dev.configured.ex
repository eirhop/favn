defmodule Mix.Tasks.Favn.Dev.Configured do
  use Mix.Task

  @moduledoc false
  @requirements ["app.config"]

  @impl Mix.Task
  def run(args), do: Mix.Tasks.Favn.Dev.run_configured(args)
end
