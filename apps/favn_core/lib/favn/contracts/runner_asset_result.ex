defmodule Favn.Contracts.RunnerAssetResult do
  @moduledoc """
  Runner-owned per-asset result envelope.

  Persisted SQL results echo the target operation, generation, and write
  relation from runner work. `write_outcome` separates a failure known not to
  have committed from one that requires reconciliation before retry.
  """

  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerWork
  alias Favn.Ref
  alias Favn.RelationRef

  @type status :: :ok | :error | :cancelled | :timed_out
  @type write_outcome :: :succeeded | :safe_failure | :outcome_unknown

  @type attempt_result :: %{
          attempt: pos_integer(),
          started_at: DateTime.t(),
          finished_at: DateTime.t(),
          duration_ms: non_neg_integer(),
          status: status(),
          meta: map(),
          error: RunnerError.t() | nil
        }

  @type t :: %__MODULE__{
          ref: Ref.t(),
          status: status(),
          started_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          duration_ms: non_neg_integer() | nil,
          meta: map(),
          error: RunnerError.t() | nil,
          attempt_count: non_neg_integer(),
          max_attempts: pos_integer(),
          attempts: [attempt_result()],
          asset_step_id: String.t() | nil,
          target_operation: RunnerWork.target_operation() | nil,
          logical_target_id: String.t() | nil,
          target_generation_id: String.t() | nil,
          write_relation: RelationRef.t() | nil,
          write_outcome: write_outcome() | nil
        }

  defstruct [
    :ref,
    :status,
    :started_at,
    :finished_at,
    :duration_ms,
    :error,
    :asset_step_id,
    :target_operation,
    :logical_target_id,
    :target_generation_id,
    :write_relation,
    :write_outcome,
    meta: %{},
    attempt_count: 0,
    max_attempts: 1,
    attempts: []
  ]

  @doc """
  Validates that a persisted SQL result echoes the exact generation and write
  relation supplied by runner work.
  """
  @spec validate_generation_result(t(), RunnerWork.t()) :: :ok | {:error, term()}
  def validate_generation_result(
        %__MODULE__{} = result,
        %RunnerWork{target_operation: nil}
      ) do
    if Enum.all?(
         [
           result.target_operation,
           result.logical_target_id,
           result.target_generation_id,
           result.write_relation,
           result.write_outcome
         ],
         &is_nil/1
       ),
       do: :ok,
       else: {:error, :unexpected_target_generation_result}
  end

  def validate_generation_result(%__MODULE__{} = result, %RunnerWork{} = work) do
    with :ok <- RunnerWork.validate_generation_contract(work),
         :ok <- match_field(:target_operation, result.target_operation, work.target_operation),
         :ok <-
           match_field(:logical_target_id, result.logical_target_id, work.logical_target_id),
         :ok <-
           match_field(
             :target_generation_id,
             result.target_generation_id,
             work.target_generation_id
           ),
         :ok <- match_field(:write_relation, result.write_relation, work.write_relation) do
      validate_write_outcome(result.status, result.write_outcome)
    end
  end

  def validate_generation_result(result, work),
    do: {:error, {:invalid_runner_generation_result, result, work}}

  defp validate_write_outcome(:ok, :succeeded), do: :ok

  defp validate_write_outcome(status, outcome)
       when status in [:error, :cancelled, :timed_out] and
              outcome in [:safe_failure, :outcome_unknown],
       do: :ok

  defp validate_write_outcome(status, outcome),
    do: {:error, {:invalid_runner_write_outcome, status, outcome}}

  defp match_field(_field, value, value), do: :ok

  defp match_field(field, actual, expected),
    do: {:error, {:runner_result_identity_mismatch, field, actual, expected}}
end
