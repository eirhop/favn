defmodule Mix.Tasks.Favn.Inspect.Configured do
  use Mix.Task

  @moduledoc false
  @requirements ["app.config"]

  @impl Mix.Task
  def run(args), do: Mix.Tasks.Favn.Inspect.run_configured(args)
end
