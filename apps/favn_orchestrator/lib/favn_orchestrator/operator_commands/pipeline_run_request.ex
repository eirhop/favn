defmodule FavnOrchestrator.OperatorCommands.PipelineRunRequest do
  @moduledoc """
  Operator intent for submitting a single manifest pipeline run.
  """

  alias Favn.Window.Request, as: WindowRequest
  alias FavnOrchestrator.OperatorCommands.Input

  @type refresh_mode :: :auto | :missing | :force_all

  @type t :: %__MODULE__{
          refresh_mode: refresh_mode(),
          window: WindowRequest.t() | nil,
          metadata: map() | nil,
          timeout_ms: pos_integer() | nil
        }

  defstruct refresh_mode: :auto,
            window: nil,
            metadata: nil,
            timeout_ms: nil

  @doc """
  Normalizes map, keyword, or struct input into a pipeline run request.
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
      else: {:error, {:invalid_operator_pipeline_run_request, input}}
  end

  def from_input(input), do: {:error, {:invalid_operator_pipeline_run_request, input}}

  defp normalize_input(input) do
    refresh_value = Input.field(input, :refresh_mode, Input.field(input, :refresh, :auto))

    with {:ok, refresh_mode} <- Input.pipeline_refresh_mode(refresh_value),
         {:ok, window} <- Input.window(Input.field(input, :window)),
         {:ok, metadata} <- Input.metadata(Input.field(input, :metadata)),
         {:ok, timeout_ms} <- Input.timeout_ms(Input.field(input, :timeout_ms)) do
      {:ok,
       %__MODULE__{
         refresh_mode: refresh_mode,
         window: window,
         metadata: metadata,
         timeout_ms: timeout_ms
       }}
    end
  end
end
