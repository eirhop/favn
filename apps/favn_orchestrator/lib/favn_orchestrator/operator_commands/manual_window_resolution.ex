defmodule FavnOrchestrator.OperatorCommands.ManualWindowResolution do
  @moduledoc """
  Resolves and records one pipeline window choice for a manual submission.

  Explicit requests remain exact. A missing request on a windowed pipeline
  selects the latest complete period after the greatest selected-asset
  availability delay. The caller supplies the evaluation instant so the choice
  can be persisted and replayed without consulting the clock again.
  """

  alias Favn.Coverage.Effective
  alias Favn.Manifest.Index
  alias Favn.Manifest.Pipeline
  alias Favn.Window.Policy
  alias Favn.Window.Request
  alias Favn.Window.Selection

  @type mode :: :explicit | :latest_complete | :unwindowed

  @enforce_keys [:mode, :selection, :evaluated_at, :availability_delay_seconds]
  defstruct [:mode, :selection, :evaluated_at, :availability_delay_seconds]

  @type t :: %__MODULE__{
          mode: mode(),
          selection: Selection.t() | nil,
          evaluated_at: DateTime.t(),
          availability_delay_seconds: non_neg_integer()
        }

  @doc "Resolves one manual pipeline window decision at `evaluated_at`."
  @spec resolve(Index.t(), Pipeline.t(), [Favn.Ref.t()], Request.t() | nil, DateTime.t()) ::
          {:ok, t()} | {:error, term()}
  def resolve(
        %Index{} = index,
        %Pipeline{} = pipeline,
        target_refs,
        request,
        %DateTime{} = evaluated_at
      )
      when is_list(target_refs) do
    delay = availability_delay_seconds(index, target_refs)

    case {pipeline.window, request} do
      {nil, nil} ->
        result(:unwindowed, nil, evaluated_at, 0)

      {policy, nil} ->
        with {:ok, selection} <- Policy.select_latest_complete(policy, evaluated_at, delay) do
          result(:latest_complete, selection, evaluated_at, delay)
        end

      {policy, %Request{} = explicit_request} ->
        with {:ok, selection} <- Policy.select_manual(policy, explicit_request) do
          result(:explicit, selection, evaluated_at, 0)
        end
    end
  end

  def resolve(%Index{}, %Pipeline{}, _target_refs, _request, _evaluated_at),
    do: {:error, :invalid_manual_window_evaluation}

  defp availability_delay_seconds(index, target_refs) do
    target_refs
    |> Enum.map(&asset_availability_delay(index, &1))
    |> Enum.max(fn -> 0 end)
  end

  defp asset_availability_delay(%Index{} = index, ref) do
    case Index.fetch_asset(index, ref) do
      {:ok, %{coverage: %Effective{through: :latest_closed} = coverage}} ->
        coverage.availability_delay_seconds

      _other ->
        0
    end
  end

  defp result(mode, selection, evaluated_at, delay) do
    {:ok,
     %__MODULE__{
       mode: mode,
       selection: selection,
       evaluated_at: evaluated_at,
       availability_delay_seconds: delay
     }}
  end
end
