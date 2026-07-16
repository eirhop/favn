defmodule FavnOrchestrator.OperatorCommands.PipelineBackfillRequest do
  @moduledoc """
  Operator intent for submitting a manifest pipeline backfill.
  """

  alias Favn.Backfill.RangeRequest
  alias Favn.Retry.Policy
  alias FavnOrchestrator.OperatorCommands.Input

  @type refresh_mode :: :auto | :missing | :force_all

  @type t :: %__MODULE__{
          refresh_mode: refresh_mode(),
          range: RangeRequest.t(),
          metadata: map() | nil,
          coverage_baseline_id: String.t() | nil,
          retry_policy: Policy.t() | nil,
          timeout_ms: pos_integer() | nil
        }

  defstruct refresh_mode: :auto,
            range: nil,
            metadata: nil,
            coverage_baseline_id: nil,
            retry_policy: nil,
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

  def from_input(input) when is_map(input), do: normalize_input(input)

  def from_input(input) when is_list(input) do
    if Keyword.keyword?(input),
      do: normalize_input(input),
      else: {:error, {:invalid_operator_pipeline_backfill_request, input}}
  end

  def from_input(input), do: {:error, {:invalid_operator_pipeline_backfill_request, input}}

  defp normalize_input(input) do
    refresh_value = Input.field(input, :refresh_mode, Input.field(input, :refresh, :auto))
    range_value = Input.field(input, :range, Input.field(input, :range_request))

    with :ok <- Input.reject_legacy_retry_fields(input),
         {:ok, refresh_mode} <- Input.pipeline_refresh_mode(refresh_value),
         {:ok, range} <- Input.range(range_value),
         {:ok, metadata} <- Input.metadata(Input.field(input, :metadata)),
         {:ok, coverage_baseline_id} <-
           Input.non_empty_binary(
             Input.field(input, :coverage_baseline_id),
             :coverage_baseline_id
           ),
         {:ok, retry_policy} <- Input.retry_policy(Input.field(input, :retry_policy)),
         {:ok, timeout_ms} <- Input.timeout_ms(Input.field(input, :timeout_ms)) do
      {:ok,
       %__MODULE__{
         refresh_mode: refresh_mode,
         range: range,
         metadata: metadata,
         coverage_baseline_id: coverage_baseline_id,
         retry_policy: retry_policy,
         timeout_ms: timeout_ms
       }}
    end
  end
end
