defmodule Mix.Tasks.Favn.CLIArgs do
  @moduledoc false

  @spec parse_no_args!(String.t(), [String.t()], keyword()) :: keyword()
  def parse_no_args!(task_name, args, switches) when is_binary(task_name) and is_list(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: switches)

    case {invalid, rest} do
      {[], []} -> opts
      {[], _rest} -> Mix.raise("unexpected argument for mix #{task_name}")
      {_invalid, _rest} -> Mix.raise("invalid option for mix #{task_name}")
    end
  end
end
