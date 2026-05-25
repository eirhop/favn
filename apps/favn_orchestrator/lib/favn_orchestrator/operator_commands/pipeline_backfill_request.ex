defmodule FavnOrchestrator.OperatorCommands.PipelineBackfillRequest do
  @moduledoc """
  Operator intent for submitting a manifest pipeline backfill.
  """

  alias Favn.Backfill.RangeRequest
  alias FavnOrchestrator.OperatorCommands.Input

  @type refresh_mode :: :auto | :missing | :force_all

  @type t :: %__MODULE__{
          refresh_mode: refresh_mode(),
          range: RangeRequest.t(),
          metadata: map() | nil,
          coverage_baseline_id: String.t() | nil,
          max_attempts: pos_integer() | nil,
          retry_backoff_ms: non_neg_integer() | nil,
          timeout_ms: pos_integer() | nil
        }

  defstruct refresh_mode: :auto,
            range: nil,
            metadata: nil,
            coverage_baseline_id: nil,
            max_attempts: nil,
            retry_backoff_ms: nil,
            timeout_ms: nil

  @doc """
  Normalizes map, keyword, or struct input into a pipeline backfill request.
  """
  @spec from_input(t() | map() | keyword()) :: {:ok, t()} | {:error, term()}
  def from_input(%__MODULE__{} = request) do
    request
    |> Map.from_struct()
    |> from_input()
  end

  def from_input(input) when is_map(input) or is_list(input) do
    refresh_value = Input.field(input, :refresh_mode, Input.field(input, :refresh, :auto))
    range_value = Input.field(input, :range, Input.field(input, :range_request))

    with {:ok, refresh_mode} <- Input.pipeline_refresh_mode(refresh_value),
         {:ok, range} <- Input.range(range_value),
         {:ok, coverage_baseline_id} <-
           Input.non_empty_binary(
             Input.field(input, :coverage_baseline_id),
             :coverage_baseline_id
           ),
         {:ok, max_attempts} <-
           Input.positive_integer(Input.field(input, :max_attempts), :max_attempts),
         {:ok, retry_backoff_ms} <-
           Input.non_neg_integer(Input.field(input, :retry_backoff_ms), :retry_backoff_ms),
         {:ok, timeout_ms} <- Input.timeout_ms(Input.field(input, :timeout_ms)) do
      {:ok,
       %__MODULE__{
         refresh_mode: refresh_mode,
         range: range,
         metadata: Input.field(input, :metadata),
         coverage_baseline_id: coverage_baseline_id,
         max_attempts: max_attempts,
         retry_backoff_ms: retry_backoff_ms,
         timeout_ms: timeout_ms
       }}
    end
  end

  def from_input(input), do: {:error, {:invalid_operator_pipeline_backfill_request, input}}
end
