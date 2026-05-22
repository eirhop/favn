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
          timeout_ms: non_neg_integer() | nil
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
  def from_input(%__MODULE__{} = request), do: {:ok, request}

  def from_input(input) when is_map(input) or is_list(input) do
    dependency_value =
      Input.field(input, :dependency_mode, Input.field(input, :dependencies, :all))

    refresh_value = Input.field(input, :refresh_mode, Input.field(input, :refresh, :auto))
    range_value = Input.field(input, :range, Input.field(input, :range_request))

    with {:ok, dependency_mode} <- Input.dependency_mode(dependency_value),
         {:ok, refresh_mode} <- Input.asset_refresh_mode(refresh_value),
         {:ok, range} <- Input.range(range_value) do
      {:ok,
       %__MODULE__{
         dependency_mode: dependency_mode,
         refresh_mode: refresh_mode,
         range: range,
         metadata: Input.field(input, :metadata),
         max_attempts: Input.field(input, :max_attempts),
         retry_backoff_ms: Input.field(input, :retry_backoff_ms),
         timeout_ms: Input.field(input, :timeout_ms)
       }}
    end
  end

  def from_input(input), do: {:error, {:invalid_operator_asset_backfill_request, input}}
end
