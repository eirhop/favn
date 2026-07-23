defmodule Favn.Contracts.GenerationMarkerInitializationResult do
  @moduledoc """
  Structured outcome from establishing an initial generation marker.

  An unknown outcome must be reconciled through the read-only marker boundary.
  """

  alias Favn.Contracts.GenerationMarker
  alias Favn.Contracts.GenerationMarkerInitializationRequest
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerReleaseBinding

  @type outcome :: :succeeded | :safe_failure | :outcome_unknown

  @enforce_keys [
    :required_runner_release_id,
    :target_id,
    :target_generation_id,
    :initialization_token,
    :outcome,
    :completed_at
  ]
  defstruct [
    :required_runner_release_id,
    :target_id,
    :target_generation_id,
    :initialization_token,
    :outcome,
    :observed_marker,
    :physical_fingerprint,
    :completed_at,
    :error
  ]

  @type t :: %__MODULE__{
          required_runner_release_id: String.t(),
          target_id: String.t(),
          target_generation_id: String.t(),
          initialization_token: String.t(),
          outcome: outcome(),
          observed_marker: GenerationMarker.t() | nil,
          physical_fingerprint: String.t() | nil,
          completed_at: DateTime.t(),
          error: RunnerError.t() | nil
        }

  @doc "Validates an initialization result against the exact request."
  @spec validate(t(), GenerationMarkerInitializationRequest.t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = result, %GenerationMarkerInitializationRequest{} = request) do
    with :ok <- GenerationMarkerInitializationRequest.validate(request),
         :ok <- RunnerReleaseBinding.validate(result.required_runner_release_id),
         :ok <- same(:required_runner_release_id, result, request),
         :ok <- same(:target_id, result, request),
         :ok <- same(:target_generation_id, result, request),
         :ok <- same(:initialization_token, result, request),
         true <- match?(%DateTime{}, result.completed_at) or {:error, :invalid_completed_at} do
      validate_outcome(result, request)
    end
  end

  def validate(result, request),
    do: {:error, {:invalid_generation_marker_initialization_result, result, request}}

  defp validate_outcome(%__MODULE__{outcome: :succeeded} = result, request) do
    with %GenerationMarker{} = marker <- result.observed_marker,
         :ok <- GenerationMarker.validate(marker),
         :ok <- match_value(:marker_target_id, marker.target_id, request.target_id),
         :ok <-
           match_value(
             :marker_generation_id,
             marker.active_generation_id,
             request.target_generation_id
           ),
         :ok <- match_value(:marker_relation, marker.active_relation, request.active_relation),
         :ok <-
           match_value(
             :marker_operation_id,
             marker.activation_operation_id,
             request.initialization_operation_id
           ),
         :ok <-
           match_value(
             :marker_token,
             marker.activation_token,
             request.initialization_token
           ),
         :ok <-
           match_value(
             :physical_fingerprint,
             result.physical_fingerprint,
             request.expected_physical_fingerprint
           ) do
      if is_nil(result.error), do: :ok, else: {:error, :unexpected_initialization_error}
    else
      nil -> {:error, :initialization_marker_required}
      {:error, _reason} = error -> error
    end
  end

  defp validate_outcome(
         %__MODULE__{outcome: :safe_failure, error: %RunnerError{} = error},
         _request
       )
       when error.outcome == :safe_failure,
       do: :ok

  defp validate_outcome(
         %__MODULE__{outcome: :outcome_unknown, error: %RunnerError{} = error},
         _request
       )
       when error.outcome == :unknown,
       do: :ok

  defp validate_outcome(%__MODULE__{outcome: outcome}, _request),
    do: {:error, {:invalid_initialization_outcome, outcome}}

  defp same(field, result, request),
    do: match_value(field, Map.fetch!(result, field), Map.fetch!(request, field))

  defp match_value(_field, value, value), do: :ok
  defp match_value(field, actual, expected), do: {:error, {field, actual, expected}}
end
