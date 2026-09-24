defmodule Favn.Contracts.GenerationCommit do
  @moduledoc """
  Generation evidence committed with the original managed asset transaction.

  This receipt is accepted only as part of the exact fenced asset result. It
  cannot establish that another attempt committed, or resolve an unknown write.
  """
  alias Favn.Contracts.{GenerationMarker, GenerationPrecondition}

  @enforce_keys [:marker, :physical_fingerprint]
  defstruct @enforce_keys
  @type t :: %__MODULE__{marker: GenerationMarker.t(), physical_fingerprint: String.t()}

  @doc "Validates a bounded generation receipt."
  @spec validate(term()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = value) do
    with :ok <- GenerationMarker.validate(value.marker),
         do: GenerationPrecondition.fingerprint(value.physical_fingerprint)
  end

  def validate(_), do: {:error, :invalid_generation_commit}

  @doc "Matches the commit to the precondition saved for this assignment."
  @spec validate(t(), GenerationPrecondition.t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = value, %GenerationPrecondition{} = expected) do
    with :ok <- validate(value),
         :ok <- GenerationPrecondition.validate(expected),
         true <- value.marker == expected.marker,
         true <-
           expected.mode == :initial or
             value.physical_fingerprint == expected.physical_fingerprint do
      :ok
    else
      false -> {:error, :generation_commit_mismatch}
      error -> error
    end
  end

  def validate(_, _), do: {:error, :invalid_generation_commit}
end
