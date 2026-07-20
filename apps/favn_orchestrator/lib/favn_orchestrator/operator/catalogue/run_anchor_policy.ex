defmodule FavnOrchestrator.Operator.Catalogue.RunAnchorPolicy do
  @moduledoc """
  Resolves the operational pipeline window policy selecting one manifest asset.

  Catalogue projections use this contract so run-anchor labels, exact expected
  windows, and freshness explanations share the same pipeline policy and linked
  schedule timezone.
  """

  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.Version
  alias Favn.Window.Anchor
  alias Favn.Window.Policy
  alias FavnOrchestrator.ManifestIndexCache
  alias FavnOrchestrator.Operator.Catalogue.Targets

  @default_timezone "Etc/UTC"

  @enforce_keys [:pipeline, :index, :policy, :timezone]
  defstruct [:pipeline, :index, :policy, :schedule_timezone, :timezone]

  @type t :: %__MODULE__{
          pipeline: Pipeline.t(),
          index: Index.t(),
          policy: Policy.t(),
          schedule_timezone: String.t() | nil,
          timezone: String.t()
        }

  @doc "Returns the first manifest pipeline with a window policy selecting the asset."
  @spec resolve(Version.t(), Asset.t() | Favn.Ref.t()) :: {:ok, t()} | {:error, term()}
  def resolve(%Version{} = version, %Asset{ref: asset_ref}), do: resolve(version, asset_ref)

  def resolve(%Version{} = version, asset_ref) when is_tuple(asset_ref) do
    with {:ok, %Index{} = index} <- ManifestIndexCache.fetch(version),
         %Pipeline{window: %Policy{} = policy} = pipeline <-
           Enum.find(
             List.wrap(version.manifest.pipelines),
             &(asset_ref in Targets.selected_refs(index, &1))
           ) do
      schedule_timezone = schedule_timezone(index, pipeline)

      {:ok,
       %__MODULE__{
         pipeline: pipeline,
         index: index,
         policy: policy,
         schedule_timezone: schedule_timezone,
         timezone: policy.timezone || schedule_timezone || @default_timezone
       }}
    else
      nil -> {:error, :selecting_pipeline_not_found}
      %Pipeline{} -> {:error, :selecting_pipeline_has_no_window_policy}
      {:error, _reason} = error -> error
    end
  end

  @doc "Resolves the scheduled operational anchor at an absolute occurrence time."
  @spec anchor(t(), DateTime.t()) :: {:ok, Anchor.t()} | {:error, term()}
  def anchor(%__MODULE__{} = run_policy, %DateTime{} = due_at) do
    Policy.resolve_scheduled(run_policy.policy, due_at, run_policy.schedule_timezone)
  end

  defp schedule_timezone(index, %Pipeline{schedule: {:ref, ref}}) do
    case Index.fetch_schedule(index, ref) do
      {:ok, %Schedule{timezone: timezone}} -> timezone
      {:error, _reason} -> nil
    end
  end

  defp schedule_timezone(_index, %Pipeline{schedule: {:inline, %Schedule{} = schedule}}),
    do: schedule.timezone

  defp schedule_timezone(_index, %Pipeline{schedule: %Schedule{} = schedule}),
    do: schedule.timezone

  defp schedule_timezone(_index, _pipeline), do: nil
end
