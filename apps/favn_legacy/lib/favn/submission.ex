defmodule Favn.Submission do
  @moduledoc """
  Internal submission helpers that normalize input and delegate to runtime layers.
  """

  alias Favn.SQLAsset.Error, as: SQLAssetError
  alias Favn.SQLAsset.Input, as: SQLAssetInput
  alias Favn.SQLAsset.Runtime, as: SQLAssetRuntime

  @type input :: term()
  @type opts :: [params: map(), runtime: map(), timeout_ms: pos_integer()]

  @spec normalize_input(input()) :: {:ok, Favn.Asset.t()} | {:error, SQLAssetError.t()}
  def normalize_input(asset_input) do
    SQLAssetInput.normalize(asset_input)
  end

  @spec render(input(), opts()) :: {:ok, Favn.SQL.Render.t()} | {:error, SQLAssetError.t()}
  def render(asset_input, opts \\ []) do
    with {:ok, asset} <- normalize_input(asset_input) do
      SQLAssetRuntime.render(asset, opts)
    end
  end

  @spec preview(input(), keyword()) :: {:ok, Favn.SQL.Preview.t()} | {:error, SQLAssetError.t()}
  def preview(asset_input, opts \\ []) do
    with {:ok, asset} <- normalize_input(asset_input) do
      SQLAssetRuntime.preview(asset, opts)
    end
  end

  @spec explain(input(), keyword()) :: {:ok, Favn.SQL.Explain.t()} | {:error, SQLAssetError.t()}
  def explain(asset_input, opts \\ []) do
    with {:ok, asset} <- normalize_input(asset_input) do
      SQLAssetRuntime.explain(asset, opts)
    end
  end

  @spec materialize(input(), opts()) ::
          {:ok, Favn.SQL.MaterializationResult.t()} | {:error, SQLAssetError.t()}
  def materialize(asset_input, opts \\ []) do
    with {:ok, asset} <- normalize_input(asset_input) do
      SQLAssetRuntime.materialize(asset, opts)
    end
  end

  @doc """
  Normalizes user-facing `pipeline_context` option to internal `_pipeline_context` key.
  """
  @spec normalize_pipeline_context(keyword()) :: keyword()
  def normalize_pipeline_context(opts) when is_list(opts) do
    case Keyword.fetch(opts, :pipeline_context) do
      :error ->
        opts

      {:ok, context} ->
        opts |> Keyword.delete(:pipeline_context) |> Keyword.put(:_pipeline_context, context)
    end
  end

  @doc """
  Shaping helper for run submission options.
  """
  @spec shape_run_opts(Favn.Ref.t() | module(), keyword(), :asset | :pipeline) :: keyword()
  def shape_run_opts(ref, opts, kind) do
    opts
    |> Keyword.put_new(:_submit_kind, kind)
    |> Keyword.put_new(:_submit_ref, ref)
    |> normalize_pipeline_context()
  end
end
