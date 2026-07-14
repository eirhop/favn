defmodule FavnOrchestrator.OperatorCommands.AssetBackfillRequest do
  @moduledoc """
  Operator intent for submitting a manifest asset backfill.
  """

  alias Favn.Backfill.RangeRequest
  alias FavnOrchestrator.OperatorCommands.Input

  @type dependency_mode :: :all | :none
  @type refresh_mode :: :auto | :missing | :force_all | :force_selected | :force_selected_upstream

  @type t :: %__MODULE__{
          dependency_mode: dependency_mode(),
          refresh_mode: refresh_mode(),
          range: RangeRequest.t(),
          metadata: map() | nil,
          max_attempts: pos_integer() | nil,
          retry_backoff_ms: non_neg_integer() | nil,
          timeout_ms: pos_integer() | nil
        }

  defstruct dependency_mode: :all,
            refresh_mode: :auto,
            range: nil,
            metadata: nil,
            max_attempts: nil,
            retry_backoff_ms: nil,
            timeout_ms: nil

  @doc """
  Normalizes map, keyword, or struct input into an asset backfill request.
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
      else: {:error, {:invalid_operator_asset_backfill_request, input}}
  end

  def from_input(input), do: {:error, {:invalid_operator_asset_backfill_request, input}}

  defp normalize_input(input) do
    dependency_value =
      Input.field(input, :dependency_mode, Input.field(input, :dependencies, :all))

    refresh_value = Input.field(input, :refresh_mode, Input.field(input, :refresh, :auto))
    range_value = Input.field(input, :range, Input.field(input, :range_request))

    with {:ok, dependency_mode} <- Input.dependency_mode(dependency_value),
         {:ok, refresh_mode} <- Input.asset_refresh_mode(refresh_value),
         {:ok, range} <- Input.range(range_value),
         {:ok, metadata} <- Input.metadata(Input.field(input, :metadata)),
         {:ok, max_attempts} <-
           Input.positive_integer(Input.field(input, :max_attempts), :max_attempts),
         {:ok, retry_backoff_ms} <-
           Input.non_neg_integer(Input.field(input, :retry_backoff_ms), :retry_backoff_ms),
         {:ok, timeout_ms} <- Input.timeout_ms(Input.field(input, :timeout_ms)) do
      {:ok,
       %__MODULE__{
         dependency_mode: dependency_mode,
         refresh_mode: refresh_mode,
         range: range,
         metadata: metadata,
         max_attempts: max_attempts,
         retry_backoff_ms: retry_backoff_ms,
         timeout_ms: timeout_ms
       }}
    end
  end
end
