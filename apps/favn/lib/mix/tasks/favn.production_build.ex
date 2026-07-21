defmodule Mix.Tasks.Favn.ProductionBuild do
  @moduledoc false

  @spec run(String.t(), [String.t()], (-> term())) :: term()
  def run(task_name, args, operation)
      when is_binary(task_name) and is_list(args) and is_function(operation, 0) do
    if Mix.env() == :prod do
      operation.()
    else
      executable =
        System.find_executable("mix") ||
          Mix.raise("production build failed: could not find the mix executable")

      case System.cmd(executable, [task_name | args],
             env: [{"MIX_ENV", "prod"}],
             into: IO.stream(:stdio, :line),
             stderr_to_stdout: true
           ) do
        {_stream, 0} -> :ok
        {_stream, status} -> Mix.raise("production build failed (status=#{status})")
      end
    end
  end
end
