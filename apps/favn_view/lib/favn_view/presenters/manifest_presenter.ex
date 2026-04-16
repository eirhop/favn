defmodule FavnView.Presenters.ManifestPresenter do
  @moduledoc """
  Stable UI-facing projection for manifest list/detail pages.
  """

  @spec summary(FavnOrchestrator.manifest_summary(), String.t() | nil) :: map()
  def summary(manifest_summary, active_manifest_id \\ nil) when is_map(manifest_summary) do
    manifest_version_id = Map.get(manifest_summary, :manifest_version_id)

    %{
      manifest_version_id: manifest_version_id,
      content_hash: Map.get(manifest_summary, :content_hash),
      asset_count: normalize_count(Map.get(manifest_summary, :asset_count)),
      pipeline_count: normalize_count(Map.get(manifest_summary, :pipeline_count)),
      schedule_count: normalize_count(Map.get(manifest_summary, :schedule_count)),
      active: manifest_version_id == active_manifest_id
    }
  end

  @spec summaries([FavnOrchestrator.manifest_summary()], String.t() | nil) :: [map()]
  def summaries(manifest_summaries, active_manifest_id \\ nil) when is_list(manifest_summaries) do
    Enum.map(manifest_summaries, &summary(&1, active_manifest_id))
  end

  @spec detail(FavnOrchestrator.manifest_summary(), String.t() | nil) :: map()
  def detail(manifest_summary, active_manifest_id \\ nil) when is_map(manifest_summary) do
    summary(manifest_summary, active_manifest_id)
  end

  @spec asset_options(FavnOrchestrator.manifest_targets()) :: [
          FavnOrchestrator.manifest_target_option()
        ]
  def asset_options(%{assets: assets}) when is_list(assets), do: assets
  def asset_options(_targets), do: []

  @spec pipeline_options(FavnOrchestrator.manifest_targets()) ::
          [FavnOrchestrator.manifest_target_option()]
  def pipeline_options(%{pipelines: pipelines}) when is_list(pipelines), do: pipelines
  def pipeline_options(_targets), do: []

  defp normalize_count(value) when is_integer(value) and value >= 0, do: value
  defp normalize_count(_value), do: 0
end
