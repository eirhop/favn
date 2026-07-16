defmodule FavnOrchestrator.OperatorCommands.AssetRunRequest do
  @moduledoc """
  Operator intent for submitting a single manifest asset run.

  This DTO is accepted at the public orchestrator operator facade. It describes
  what the operator requested, not the runtime options used to execute the run.
  """

  alias FavnOrchestrator.OperatorCommands.Input
  alias Favn.Retry.Policy

  @type dependency_mode :: :all | :none
  @type refresh_mode :: :auto | :missing | :force_all | :force_selected | :force_selected_upstream

  @type selection :: %{
          required(:source) => :refresh_timeline | :data_coverage_timeline,
          required(:id) => String.t(),
          optional(:kind) => atom() | String.t() | nil,
          optional(:value) => String.t() | nil,
          optional(:timezone) => String.t(),
          optional(:run_id) => String.t() | nil
        }

  @type t :: %__MODULE__{
          dependency_mode: dependency_mode(),
          refresh_mode: refresh_mode(),
          selection: selection() | nil,
          metadata: map() | nil,
          retry_policy: Policy.t() | nil,
          timeout_ms: pos_integer() | nil
        }

  defstruct dependency_mode: :all,
            refresh_mode: :auto,
            selection: nil,
            metadata: nil,
            retry_policy: nil,
            timeout_ms: nil

  @doc """
  Normalizes map, keyword, or struct input into an asset run request.
  """
  @spec from_input(t() | map() | keyword() | nil) :: {:ok, t()} | {:error, term()}
  def from_input(%__MODULE__{} = request) do
    request
    |> Map.from_struct()
    |> from_input()
  end

  def from_input(nil), do: normalize_input(%{})
  def from_input(input) when is_map(input), do: normalize_input(input)

  def from_input(input) when is_list(input) do
    if Keyword.keyword?(input),
      do: normalize_input(input),
      else: {:error, {:invalid_operator_asset_run_request, input}}
  end

  def from_input(input), do: {:error, {:invalid_operator_asset_run_request, input}}

  defp normalize_input(input) do
    dependency_value =
      Input.field(input, :dependency_mode, Input.field(input, :dependencies, :all))

    refresh_value = Input.field(input, :refresh_mode, Input.field(input, :refresh, :auto))

    with :ok <- Input.reject_legacy_retry_fields(input),
         {:ok, dependency_mode} <- Input.dependency_mode(dependency_value),
         {:ok, refresh_mode} <- Input.asset_refresh_mode(refresh_value),
         {:ok, selection} <- Input.selection(Input.field(input, :selection)),
         {:ok, metadata} <- Input.metadata(Input.field(input, :metadata)),
         {:ok, retry_policy} <- Input.retry_policy(Input.field(input, :retry_policy)),
         {:ok, timeout_ms} <- Input.timeout_ms(Input.field(input, :timeout_ms)) do
      {:ok,
       %__MODULE__{
         dependency_mode: dependency_mode,
         refresh_mode: refresh_mode,
         selection: selection,
         metadata: metadata,
         retry_policy: retry_policy,
         timeout_ms: timeout_ms
       }}
    end
  end
end
