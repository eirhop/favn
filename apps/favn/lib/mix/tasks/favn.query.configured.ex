defmodule Mix.Tasks.Favn.Query.Configured do
  use Mix.Task

  @moduledoc false
  @requirements ["app.config"]

  @impl Mix.Task
  def run(args), do: Mix.Tasks.Favn.Query.run_configured(args)
end
