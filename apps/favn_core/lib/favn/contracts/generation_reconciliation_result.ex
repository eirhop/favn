defmodule Favn.Contracts.GenerationReconciliationResult do
  @moduledoc """
  Observed data-plane state for one activation reconciliation.

  `candidate_active` proves activation committed. `previous_active` proves the
  old generation remains active and reports whether the candidate still exists.
  `unknown` requires operator-visible recovery and continues to block writes.
  """

  alias Favn.Contracts.GenerationMarker
  alias Favn.Contracts.GenerationReconciliationRequest
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerReleaseBinding

  @type disposition :: :candidate_active | :previous_active | :unknown

  @enforce_keys [
    :required_runner_release_id,
    :target_id,
    :candidate_generation_id,
    :activation_token,
    :disposition,
    :reconciled_at
  ]
  defstruct [
    :required_runner_release_id,
    :target_id,
    :candidate_generation_id,
    :activation_token,
    :disposition,
    :observed_marker,
    :candidate_present,
    :physical_fingerprint,
    :reconciled_at,
    :error
  ]

  @type t :: %__MODULE__{
          required_runner_release_id: String.t(),
          target_id: String.t(),
          candidate_generation_id: String.t(),
          activation_token: String.t(),
          disposition: disposition(),
          observed_marker: GenerationMarker.t() | nil,
          candidate_present: boolean() | nil,
          physical_fingerprint: String.t() | nil,
          reconciled_at: DateTime.t(),
          error: RunnerError.t() | nil
        }

  @doc "Validates a reconciliation result against the original activation request."
  @spec validate(t(), GenerationReconciliationRequest.t()) :: :ok | {:error, term()}
  def validate(
        %__MODULE__{} = result,
        %GenerationReconciliationRequest{activation: activation} = request
      ) do
    with :ok <- GenerationReconciliationRequest.validate(request),
         :ok <- RunnerReleaseBinding.validate(result.required_runner_release_id),
         :ok <-
           match_field(
             :required_runner_release_id,
             result.required_runner_release_id,
             activation.required_runner_release_id
           ),
         :ok <- match_field(:target_id, result.target_id, activation.target_id),
         :ok <-
           match_field(
             :candidate_generation_id,
             result.candidate_generation_id,
             activation.candidate_generation_id
           ),
         :ok <-
           match_field(:activation_token, result.activation_token, activation.activation_token),
         :ok <- timestamp(result.reconciled_at) do
      validate_disposition(result, activation)
    end
  end

  def validate(result, request),
    do: {:error, {:invalid_generation_reconciliation_result, result, request}}

  defp validate_disposition(%__MODULE__{disposition: :candidate_active} = result, activation) do
    with :ok <- candidate_marker(result.observed_marker, activation),
         :ok <- match_field(:candidate_present, result.candidate_present, false),
         :ok <- fingerprint(result.physical_fingerprint) do
      require_no_error(result.error)
    end
  end

  defp validate_disposition(%__MODULE__{disposition: :previous_active} = result, activation) do
    with :ok <- previous_marker(result.observed_marker, activation),
         :ok <- boolean(:candidate_present, result.candidate_present) do
      require_no_error(result.error)
    end
  end

  defp validate_disposition(
         %__MODULE__{disposition: :unknown, error: %RunnerError{} = error},
         _activation
       ) do
    if error.outcome == :unknown,
      do: :ok,
      else: {:error, {:reconciliation_error_outcome_mismatch, error.outcome}}
  end

  defp validate_disposition(%__MODULE__{disposition: disposition}, _activation),
    do: {:error, {:invalid_generation_reconciliation_disposition, disposition}}

  defp candidate_marker(%GenerationMarker{} = marker, activation) do
    with :ok <- common_marker(marker, activation),
         :ok <-
           match_field(
             :marker_generation_id,
             marker.active_generation_id,
             activation.candidate_generation_id
           ),
         :ok <-
           match_field(
             :marker_activation_token,
             marker.activation_token,
             activation.activation_token
           ) do
      match_field(
        :marker_operation_id,
        marker.activation_operation_id,
        activation.rebuild_operation_id
      )
    end
  end

  defp candidate_marker(marker, _activation),
    do: {:error, {:candidate_active_marker_required, marker}}

  defp previous_marker(%GenerationMarker{} = marker, activation) do
    expected = activation.expected_marker

    with :ok <- common_marker(marker, activation),
         :ok <-
           match_field(
             :marker_generation_id,
             marker.active_generation_id,
             activation.previous_generation_id
           ),
         :ok <-
           match_field(
             :marker_operation_id,
             marker.activation_operation_id,
             expected.activation_operation_id
           ) do
      match_field(
        :marker_activation_token,
        marker.activation_token,
        expected.activation_token
      )
    end
  end

  defp previous_marker(marker, _activation),
    do: {:error, {:previous_active_marker_required, marker}}

  defp common_marker(marker, activation) do
    with :ok <- GenerationMarker.validate(marker),
         :ok <- match_field(:marker_target_id, marker.target_id, activation.target_id) do
      match_field(:marker_active_relation, marker.active_relation, activation.active_relation)
    end
  end

  defp fingerprint(value) when is_binary(value) do
    if Regex.match?(~r/\A[0-9a-f]{64}\z/, value),
      do: :ok,
      else: {:error, {:invalid_physical_fingerprint, value}}
  end

  defp fingerprint(value), do: {:error, {:invalid_physical_fingerprint, value}}

  defp boolean(_field, value) when is_boolean(value), do: :ok
  defp boolean(field, value), do: {:error, {:invalid_reconciliation_field, field, value}}

  defp timestamp(%DateTime{}), do: :ok
  defp timestamp(value), do: {:error, {:invalid_reconciliation_timestamp, value}}

  defp require_no_error(nil), do: :ok
  defp require_no_error(error), do: {:error, {:unexpected_reconciliation_error, error}}

  defp match_field(_field, value, value), do: :ok

  defp match_field(field, actual, expected),
    do: {:error, {:generation_reconciliation_result_mismatch, field, actual, expected}}
end
