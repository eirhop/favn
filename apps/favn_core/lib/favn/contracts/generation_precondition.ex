defmodule Favn.Contracts.GenerationPrecondition do
  @moduledoc """
  Physical generation evidence pinned by the control plane for one assignment.

  Initial assignments require an absent table and marker. Existing assignments
  require the exact marker, physical fingerprint and token-bound table instance.
  Replaying an assignment never refreshes this evidence.
  """
  alias Favn.Contracts.{GenerationMarker, RunnerWork}

  @enforce_keys [:mode, :marker]
  defstruct [:mode, :marker, :physical_fingerprint]

  @type t :: %__MODULE__{
          mode: :initial | :existing,
          marker: GenerationMarker.t(),
          physical_fingerprint: String.t() | nil
        }

  @doc "Validates the bounded physical precondition."
  @spec validate(term()) :: :ok | {:error, term()}
  def validate(%__MODULE__{marker: marker} = value) do
    with :ok <- GenerationMarker.validate(marker) do
      case value do
        %{mode: :initial, physical_fingerprint: nil} -> :ok
        %{mode: :existing, physical_fingerprint: fingerprint} -> fingerprint(fingerprint)
        _ -> {:error, :invalid_generation_precondition}
      end
    end
  end

  def validate(_), do: {:error, :invalid_generation_precondition}

  @doc "Validates evidence against the immutable target identity in runner work."
  @spec validate_work(t() | nil, RunnerWork.t()) :: :ok | {:error, term()}
  def validate_work(%__MODULE__{marker: marker} = value, %RunnerWork{
        target_operation: :normal_materialization,
        logical_target_id: target_id,
        target_generation_id: generation_id,
        write_relation: relation
      }) do
    with :ok <- validate(value),
         true <-
           marker.target_id == target_id and marker.active_generation_id == generation_id and
             marker.active_relation == relation do
      :ok
    else
      false -> {:error, :generation_precondition_identity_mismatch}
      error -> error
    end
  end

  def validate_work(nil, %RunnerWork{target_operation: operation})
      when operation != :normal_materialization, do: :ok

  def validate_work(_, _), do: {:error, :generation_precondition_required}

  @doc false
  @spec fingerprint(term()) :: :ok | {:error, atom()}
  def fingerprint(value) when is_binary(value) and byte_size(value) == 64 do
    if Regex.match?(~r/\A[0-9a-f]{64}\z/, value),
      do: :ok,
      else: {:error, :invalid_generation_fingerprint}
  end

  def fingerprint(_), do: {:error, :invalid_generation_fingerprint}
end
