defmodule Favn.Contracts.GenerationActivationResult do
  @moduledoc """
  Structured outcome of one data-plane generation activation attempt.

  `outcome_unknown` means the runner cannot prove whether the activation
  transaction committed. The orchestrator must reconcile the marker and must
  not blindly submit a different activation token.
  """

  alias Favn.Contracts.GenerationActivationRequest
  alias Favn.Contracts.GenerationMarker
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerReleaseBinding
  alias Favn.RelationRef

  @type outcome :: :succeeded | :safe_failure | :outcome_unknown

  @enforce_keys [
    :required_runner_release_id,
    :target_id,
    :candidate_generation_id,
    :activation_token,
    :outcome,
    :completed_at
  ]
  defstruct [
    :required_runner_release_id,
    :target_id,
    :candidate_generation_id,
    :activation_token,
    :outcome,
    :observed_marker,
    :candidate_fingerprint,
    :physical_fingerprint,
    :retired_relation,
    :completed_at,
    :error
  ]

  @type t :: %__MODULE__{
          required_runner_release_id: String.t(),
          target_id: String.t(),
          candidate_generation_id: String.t(),
          activation_token: String.t(),
          outcome: outcome(),
          observed_marker: GenerationMarker.t() | nil,
          candidate_fingerprint: String.t() | nil,
          physical_fingerprint: String.t() | nil,
          retired_relation: RelationRef.t() | nil,
          completed_at: DateTime.t(),
          error: RunnerError.t() | nil
        }

  @doc "Validates an activation result against the exact dispatched request."
  @spec validate(t(), GenerationActivationRequest.t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = result, %GenerationActivationRequest{} = request) do
    with :ok <- GenerationActivationRequest.validate(request),
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
         :ok <- match_field(:activation_token, result.activation_token, request.activation_token),
         :ok <- timestamp(result.completed_at) do
      validate_outcome(result, request)
    end
  end

  def validate(result, request),
    do: {:error, {:invalid_generation_activation_result, result, request}}

  defp validate_outcome(%__MODULE__{outcome: :succeeded} = result, request) do
    with :ok <- marker_matches_candidate(result.observed_marker, request),
         :ok <- fingerprint(result.candidate_fingerprint),
         :ok <-
           match_field(
             :candidate_fingerprint,
             result.candidate_fingerprint,
             request.expected_candidate_fingerprint
           ),
         :ok <- fingerprint(result.physical_fingerprint),
         :ok <- match_field(:retired_relation, result.retired_relation, request.retired_relation) do
      require_no_error(result.error)
    end
  end

  defp validate_outcome(
         %__MODULE__{outcome: :safe_failure, error: %RunnerError{} = error},
         _request
       ) do
    if error.outcome == :safe_failure,
      do: :ok,
      else: {:error, {:activation_error_outcome_mismatch, error.outcome}}
  end

  defp validate_outcome(
         %__MODULE__{outcome: :outcome_unknown, error: %RunnerError{} = error},
         _request
       ) do
    if error.outcome == :unknown,
      do: :ok,
      else: {:error, {:activation_error_outcome_mismatch, error.outcome}}
  end

  defp validate_outcome(%__MODULE__{outcome: outcome}, _request),
    do: {:error, {:invalid_generation_activation_outcome, outcome}}

  defp marker_matches_candidate(%GenerationMarker{} = marker, request) do
    with :ok <- GenerationMarker.validate(marker),
         :ok <- match_field(:marker_target_id, marker.target_id, request.target_id),
         :ok <-
           match_field(
             :marker_generation_id,
             marker.active_generation_id,
             request.candidate_generation_id
           ),
         :ok <-
           match_field(
             :marker_activation_token,
             marker.activation_token,
             request.activation_token
           ),
         :ok <-
           match_field(
             :marker_operation_id,
             marker.activation_operation_id,
             request.rebuild_operation_id
           ) do
      match_field(:marker_active_relation, marker.active_relation, request.active_relation)
    end
  end

  defp marker_matches_candidate(marker, _request),
    do: {:error, {:activation_success_marker_required, marker}}

  defp fingerprint(value) when is_binary(value) do
    if Regex.match?(~r/\A[0-9a-f]{64}\z/, value),
      do: :ok,
      else: {:error, {:invalid_physical_fingerprint, value}}
  end

  defp fingerprint(value), do: {:error, {:invalid_physical_fingerprint, value}}

  defp timestamp(%DateTime{}), do: :ok
  defp timestamp(value), do: {:error, {:invalid_activation_timestamp, value}}

  defp require_no_error(nil), do: :ok
  defp require_no_error(error), do: {:error, {:unexpected_activation_error, error}}

  defp match_field(_field, value, value), do: :ok

  defp match_field(field, actual, expected),
    do: {:error, {:generation_activation_result_mismatch, field, actual, expected}}
end
