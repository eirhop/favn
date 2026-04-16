defmodule FavnView.Presenters.ManifestPresenter do
  @moduledoc """
  Stable UI-facing projection for manifest list/detail pages.
  """

  @spec summary(Favn.Manifest.Version.t(), String.t() | nil) :: map()
  def summary(version, active_manifest_id \\ nil) when is_map(version) do
    %{
      manifest_version_id: Map.get(version, :manifest_version_id),
      content_hash: Map.get(version, :content_hash),
      active: Map.get(version, :manifest_version_id) == active_manifest_id
    }
  end

  @spec summaries([Favn.Manifest.Version.t()], String.t() | nil) :: [map()]
  def summaries(versions, active_manifest_id \\ nil) when is_list(versions) do
    Enum.map(versions, &summary(&1, active_manifest_id))
  end

  @spec detail(Favn.Manifest.Version.t()) :: map()
  def detail(version) when is_map(version) do
    manifest = Map.get(version, :manifest, %{})

    %{
      manifest_version_id: Map.get(version, :manifest_version_id),
      content_hash: Map.get(version, :content_hash),
      asset_count: count_items(manifest, :assets),
      pipeline_count: count_items(manifest, :pipelines),
      schedule_count: count_items(manifest, :schedules)
    }
  end

  @spec asset_options([Favn.Manifest.Version.t()]) :: [{String.t(), String.t()}]
  def asset_options(versions) when is_list(versions) do
    versions
    |> Enum.flat_map(fn version ->
      assets = Map.get(Map.get(version, :manifest, %{}), :assets, [])

      Enum.map(assets, fn asset ->
        ref = Map.get(asset, :ref)
        {inspect(ref), encode_term(ref)}
      end)
    end)
    |> Enum.uniq()
    |> Enum.sort_by(&elem(&1, 0))
  end

  @spec pipeline_options([Favn.Manifest.Version.t()]) :: [{String.t(), String.t()}]
  def pipeline_options(versions) when is_list(versions) do
    versions
    |> Enum.flat_map(fn version ->
      pipelines = Map.get(Map.get(version, :manifest, %{}), :pipelines, [])

      Enum.map(pipelines, fn pipeline ->
        pipeline_module = Map.get(pipeline, :module)
        {inspect(pipeline_module), encode_term(pipeline_module)}
      end)
    end)
    |> Enum.uniq()
    |> Enum.sort_by(&elem(&1, 0))
  end

  @spec decode_term(String.t()) :: {:ok, term()} | {:error, :invalid_term_payload}
  def decode_term(encoded) when is_binary(encoded) do
    with {:ok, binary} <- normalize_decoded_term(Base.url_decode64(encoded, padding: false)) do
      try do
        {:ok, :erlang.binary_to_term(binary, [:safe])}
      rescue
        _error -> {:error, :invalid_term_payload}
      end
    end
  end

  defp encode_term(term),
    do: term |> :erlang.term_to_binary() |> Base.url_encode64(padding: false)

  defp normalize_decoded_term({:ok, binary}) when is_binary(binary), do: {:ok, binary}
  defp normalize_decoded_term(:error), do: {:error, :invalid_term_payload}

  defp count_items(manifest, key) when is_map(manifest) do
    manifest
    |> Map.get(key, [])
    |> List.wrap()
    |> length()
  end
end
