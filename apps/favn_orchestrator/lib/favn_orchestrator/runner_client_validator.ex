defmodule FavnOrchestrator.RunnerClientValidator do
  @moduledoc false

  alias Favn.Contracts.RunnerClient

  @required_callbacks RunnerClient.behaviour_info(:callbacks) --
                        RunnerClient.behaviour_info(:optional_callbacks)

  @spec validate(module() | term()) :: :ok | {:error, :runner_client_not_available}
  def validate(module) when is_atom(module) do
    if Code.ensure_loaded?(module) and
         Enum.all?(@required_callbacks, fn {name, arity} ->
           function_exported?(module, name, arity)
         end) do
      :ok
    else
      {:error, :runner_client_not_available}
    end
  end

  def validate(_module), do: {:error, :runner_client_not_available}
end
