defmodule FavnOrchestrator.OperatorCommands.AssetBackfillRequest do
  @moduledoc """
  Operator intent for submitting a manifest asset backfill.
  """

  alias Favn.Backfill.RangeRequest
  alias Favn.Retry.Policy
  alias FavnOrchestrator.OperatorCommands.Input

  @type dependency_mode :: :all | :none
  @type refresh_mode :: :auto | :missing | :force_all | :force_selected | :force_selected_upstream

  @type t :: %__MODULE__{
          dependency_mode: dependency_mode(),
          refresh_mode: refresh_mode(),
          range: RangeRequest.t(),
          metadata: map() | nil,
          retry_policy: Policy.t() | nil,
          timeout_ms: pos_integer() | nil
        }

  defstruct dependency_mode: :all,
            refresh_mode: :auto,
            range: nil,
            metadata: nil,
            retry_policy: nil,
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

    with :ok <- Input.reject_legacy_retry_fields(input),
         {:ok, dependency_mode} <- Input.dependency_mode(dependency_value),
         {:ok, refresh_mode} <- Input.asset_refresh_mode(refresh_value),
         {:ok, range} <- Input.range(range_value),
         {:ok, metadata} <- Input.metadata(Input.field(input, :metadata)),
         {:ok, retry_policy} <- Input.retry_policy(Input.field(input, :retry_policy)),
         {:ok, timeout_ms} <- Input.timeout_ms(Input.field(input, :timeout_ms)) do
      {:ok,
       %__MODULE__{
         dependency_mode: dependency_mode,
         refresh_mode: refresh_mode,
         range: range,
         metadata: metadata,
         retry_policy: retry_policy,
         timeout_ms: timeout_ms
       }}
    end
  end
end
