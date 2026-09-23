defmodule Favn.Contracts.RuntimeInputResolutionRequest do
  @moduledoc "Read-only resolution of pinned rebuild inputs without submitting a run."

  alias Favn.Contracts.RunnerTask.PersistenceSchema
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.ExecutionPackage

  @enforce_keys [:work]
  defstruct @enforce_keys
  @type t :: %__MODULE__{work: RunnerWork.t()}

  @doc "Validates the resolver context; the enclosing task owns no write authority."
  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{
        work:
          %RunnerWork{
            execution_package: %ExecutionPackage{sql_execution: %{runtime_inputs: resolver}}
          } = work
      })
      when not is_nil(resolver) do
    with :ok <- PersistenceSchema.payload(:asset_attempt, work),
         true <- is_binary(work.rebuild_operation_id),
         true <- is_nil(work.runtime_input_pin),
         false <- RunnerWork.runtime_input_resolution_only?(work) do
      :ok
    else
      _ -> {:error, :invalid_runtime_input_resolution_request}
    end
  end

  def validate(_), do: {:error, :invalid_runtime_input_resolution_request}
end
