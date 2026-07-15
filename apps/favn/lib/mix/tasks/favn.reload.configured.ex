defmodule Mix.Tasks.Favn.Reload.Configured do
  use Mix.Task

  @moduledoc false
  @requirements ["app.config"]

  @impl Mix.Task
  def run(args), do: Mix.Tasks.Favn.Reload.run_configured(args)
end
