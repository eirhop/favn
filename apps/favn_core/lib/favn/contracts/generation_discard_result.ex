defmodule Favn.Contracts.GenerationDiscardResult do
  @moduledoc """
  Structured result from an idempotent candidate discard request.

  `already_absent` is a successful replay. `outcome_unknown` keeps cleanup
  pending until the candidate relation can be inspected again.
  """

  alias Favn.Contracts.GenerationDiscardRequest
  alias Favn.Contracts.GenerationMarker
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerReleaseBinding

  @type outcome :: :discarded | :already_absent | :safe_failure | :outcome_unknown

  @enforce_keys [
    :required_runner_release_id,
    :target_id,
    :candidate_generation_id,
    :discard_token,
    :outcome,
    :completed_at
  ]
  defstruct [
    :required_runner_release_id,
    :target_id,
    :candidate_generation_id,
    :discard_token,
    :outcome,
    :observed_marker,
    :candidate_present,
    :completed_at,
    :error
  ]

  @type t :: %__MODULE__{
          required_runner_release_id: String.t(),
          target_id: String.t(),
          candidate_generation_id: String.t(),
          discard_token: String.t(),
          outcome: outcome(),
          observed_marker: GenerationMarker.t() | nil,
          candidate_present: boolean() | nil,
          completed_at: DateTime.t(),
          error: RunnerError.t() | nil
        }

  @doc "Validates a discard result against the exact dispatched request."
  @spec validate(t(), GenerationDiscardRequest.t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = result, %GenerationDiscardRequest{} = request) do
    with :ok <- GenerationDiscardRequest.validate(request),
         :ok <- RunnerReleaseBinding.validate(result.required_runner_release_id),
         :ok <-
           match_field(
             :required_runner_release_id,
             result.required_runner_release_id,
             request.required_runner_release_id
           ),
         :ok <- match_field(:target_id, result.target_id, request.target_id),
         :ok <-
           match_field(
             :candidate_generation_id,
             result.candidate_generation_id,
             request.candidate_generation_id
           ),
         :ok <- match_field(:discard_token, result.discard_token, request.discard_token),
         :ok <- timestamp(result.completed_at) do
      validate_outcome(result, request)
    end
  end

  def validate(result, request),
    do: {:error, {:invalid_generation_discard_result, result, request}}

  defp validate_outcome(%__MODULE__{outcome: outcome} = result, request)
       when outcome in [:discarded, :already_absent] do
    with :ok <- marker_does_not_activate_candidate(result.observed_marker, request),
         :ok <- match_field(:candidate_present, result.candidate_present, false) do
      require_no_error(result.error)
    end
  end

  defp validate_outcome(
         %__MODULE__{outcome: :safe_failure, error: %RunnerError{} = error} = result,
         request
       ) do
    with :ok <- marker_does_not_activate_candidate(result.observed_marker, request) do
      if error.outcome == :safe_failure,
        do: :ok,
        else: {:error, {:discard_error_outcome_mismatch, error.outcome}}
    end
  end

  defp validate_outcome(
         %__MODULE__{outcome: :outcome_unknown, error: %RunnerError{} = error} = result,
         request
       ) do
    if error.outcome == :unknown and is_nil(result.candidate_present),
      do: validate_observed_marker(result.observed_marker, request),
      else: {:error, {:discard_error_outcome_mismatch, error.outcome}}
  end

  defp validate_outcome(%__MODULE__{outcome: outcome}, _request),
    do: {:error, {:invalid_generation_discard_outcome, outcome}}

  defp marker_does_not_activate_candidate(nil, _request), do: :ok

  defp marker_does_not_activate_candidate(%GenerationMarker{} = marker, request) do
    with :ok <- GenerationMarker.validate(marker),
         :ok <- match_field(:marker_target_id, marker.target_id, request.target_id) do
      if marker.active_generation_id == request.candidate_generation_id,
        do: {:error, :cannot_discard_active_candidate_generation},
        else: :ok
    end
  end

  defp marker_does_not_activate_candidate(marker, _request),
    do: {:error, {:invalid_discard_generation_marker, marker}}

  defp validate_observed_marker(nil, _request), do: :ok

  defp validate_observed_marker(%GenerationMarker{} = marker, request) do
    with :ok <- GenerationMarker.validate(marker) do
      match_field(:marker_target_id, marker.target_id, request.target_id)
    end
  end

  defp validate_observed_marker(marker, _request),
    do: {:error, {:invalid_discard_generation_marker, marker}}

  defp timestamp(%DateTime{}), do: :ok
  defp timestamp(value), do: {:error, {:invalid_discard_timestamp, value}}

  defp require_no_error(nil), do: :ok
  defp require_no_error(error), do: {:error, {:unexpected_discard_error, error}}

  defp match_field(_field, value, value), do: :ok

  defp match_field(field, actual, expected),
    do: {:error, {:generation_discard_result_mismatch, field, actual, expected}}
end
