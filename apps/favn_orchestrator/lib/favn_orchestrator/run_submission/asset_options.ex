defmodule FavnOrchestrator.RunSubmission.AssetOptions do
  @moduledoc """
  Translates manifest asset-run requests into runtime submission options.

  This module owns dependency and refresh normalization plus timeline-selection
  resolution. Both direct manifest commands and authenticated operator commands
  use the same window and metadata semantics.
  """

  alias Favn.Manifest.Asset
  alias Favn.Retry.Policy
  alias Favn.Window.Anchor
  alias Favn.Window.Request, as: WindowRequest
  alias Favn.Window.Runtime, as: RuntimeWindow
  alias FavnOrchestrator.Operator.WindowSelection
  alias FavnOrchestrator.OperatorCommands.AssetRunRequest

  @doc """
  Normalizes a plain manifest asset-run request.

  The request accepts `:config` with `:dependencies`, `:refresh`, and
  `:metadata`, plus an optional timeline `:selection`. Atom and string keys are
  accepted at this external-input boundary.
  """
  @spec from_input(Asset.t(), map()) :: {:ok, keyword()} | {:error, term()}
  def from_input(%Asset{} = asset, request) when is_map(request) do
    config = field(request, :config, %{}) || %{}
    selection = field(request, :selection)

    with {:ok, opts} <- config_options(asset, config) do
      apply_selection(opts, asset, selection)
    end
  end

  @doc "Translates validated operator asset intent into runtime options."
  @spec from_operator_request(Asset.t(), AssetRunRequest.t()) ::
          {:ok, keyword()} | {:error, term()}
  def from_operator_request(%Asset{} = asset, %AssetRunRequest{} = request) do
    with {:ok, refresh} <-
           operator_refresh(request.refresh_mode, asset.ref, request.dependency_mode),
         {:ok, opts} <- put_metadata([], request.metadata) do
      opts =
        opts
        |> Keyword.put(:dependencies, request.dependency_mode)
        |> Keyword.put(:refresh, refresh)
        |> maybe_put(:retry_policy, request.retry_policy)
        |> maybe_put(:timeout_ms, request.timeout_ms)

      apply_selection(opts, asset, request.selection)
    end
  end

  @doc "Applies one validated timeline selection to runtime submission options."
  @spec apply_selection(keyword(), Asset.t(), map() | nil) ::
          {:ok, keyword()} | {:error, term()}
  def apply_selection(opts, %Asset{}, nil) when is_list(opts), do: {:ok, opts}

  def apply_selection(opts, %Asset{} = asset, selection)
      when is_list(opts) and is_map(selection) do
    case normalize_selection_source(field(selection, :source)) do
      {:ok, :data_coverage_timeline} ->
        apply_data_coverage_selection(opts, asset, field(selection, :id), selection)

      {:ok, :refresh_timeline} ->
        apply_refresh_selection(opts, field(selection, :id), selection)

      {:error, _reason} = error ->
        error
    end
  end

  def apply_selection(_opts, %Asset{}, _selection), do: {:error, :invalid_asset_run_selection}

  defp config_options(asset, config) when is_map(config) do
    dependencies_value = field(config, :dependencies, :all) || :all
    refresh_value = field(config, :refresh, :auto) || :auto

    with {:ok, dependencies} <- dependency_option(dependencies_value),
         {:ok, refresh} <- refresh_option(refresh_value, asset.ref, dependencies),
         {:ok, retry_policy} <- optional_retry_policy(field(config, :retry_policy)),
         {:ok, opts} <- put_metadata([], field(config, :metadata, :missing)) do
      {:ok,
       opts
       |> Keyword.put(:dependencies, dependencies)
       |> Keyword.put(:refresh, refresh)
       |> maybe_put(:retry_policy, retry_policy)}
    end
  end

  defp config_options(_asset, _config), do: {:error, :invalid_asset_run_config}

  defp optional_retry_policy(nil), do: {:ok, nil}
  defp optional_retry_policy(value), do: Policy.new(value)

  defp apply_data_coverage_selection(opts, asset, id, selection) when is_binary(id) do
    with {:ok, window_request} <- WindowSelection.data_coverage_request(id),
         {:ok, anchor_window} <- WindowSelection.resolve(asset, window_request),
         {:ok, runtime_window} <- runtime_window(anchor_window),
         {:ok, opts} <- merge_metadata(opts, window_metadata(id, anchor_window)),
         {:ok, opts} <-
           merge_metadata(opts, selection_metadata(:data_coverage_timeline, selection)) do
      {:ok,
       opts
       |> Keyword.put(:anchor_window, anchor_window)
       |> Keyword.put(:exact_windows, %{asset.ref => [runtime_window]})}
    end
  end

  defp apply_data_coverage_selection(_opts, _asset, _id, _selection),
    do: {:error, :invalid_asset_run_selection}

  defp apply_refresh_selection(opts, id, selection) when is_binary(id) do
    with {:ok, window_request} <- WindowSelection.refresh_request(id),
         {:ok, anchor_window} <-
           WindowRequest.to_anchor(window_request, selection_timezone(selection)),
         {:ok, opts} <- merge_metadata(opts, selection_metadata(:refresh_timeline, selection)) do
      {:ok, Keyword.put(opts, :anchor_window, anchor_window)}
    end
  end

  defp apply_refresh_selection(_opts, _id, _selection),
    do: {:error, :invalid_asset_run_selection}

  defp runtime_window(%Anchor{} = anchor_window) do
    RuntimeWindow.new(
      anchor_window.kind,
      anchor_window.start_at,
      anchor_window.end_at,
      anchor_window.key,
      timezone: anchor_window.timezone
    )
  end

  defp dependency_option(value) when value in [:all, "all"], do: {:ok, :all}
  defp dependency_option(value) when value in [:none, "none"], do: {:ok, :none}
  defp dependency_option(value), do: {:error, {:invalid_dependencies_mode, value}}

  defp refresh_option(value, _asset_ref, _dependencies) when value in [:auto, "auto"],
    do: {:ok, :auto}

  defp refresh_option(value, _asset_ref, _dependencies) when value in [:missing, "missing"],
    do: {:ok, :missing}

  defp refresh_option(value, _asset_ref, _dependencies)
       when value in [:force, :force_all, "force", "force_all"],
       do: {:ok, :force}

  defp refresh_option({:force_assets, refs}, _asset_ref, _dependencies) when is_list(refs),
    do: {:ok, {:force_assets, refs}}

  defp refresh_option({:force_assets, refs, opts}, _asset_ref, _dependencies)
       when is_list(refs) and is_list(opts),
       do: {:ok, {:force_assets, refs, opts}}

  defp refresh_option(value, asset_ref, _dependencies)
       when value in [:force_selected, "force_selected"] and is_tuple(asset_ref),
       do: {:ok, {:force_assets, [asset_ref]}}

  defp refresh_option(value, _asset_ref, :none)
       when value in [:force_selected_upstream, "force_selected_upstream"],
       do: {:error, {:refresh_include_upstream_requires_dependencies, :all}}

  defp refresh_option(value, asset_ref, :all)
       when value in [:force_selected_upstream, "force_selected_upstream"] and
              is_tuple(asset_ref),
       do: {:ok, {:force_assets, [asset_ref], include_upstream: true}}

  defp refresh_option(value, _asset_ref, _dependencies),
    do: {:error, {:invalid_refresh_policy, value}}

  @doc "Translates validated operator refresh intent into a runtime refresh policy."
  @spec operator_refresh(AssetRunRequest.refresh_mode(), Favn.Ref.t(), :all | :none) ::
          {:ok, term()} | {:error, term()}
  def operator_refresh(:auto, _asset_ref, _dependencies), do: {:ok, :auto}
  def operator_refresh(:missing, _asset_ref, _dependencies), do: {:ok, :missing}
  def operator_refresh(:force_all, _asset_ref, _dependencies), do: {:ok, :force}

  def operator_refresh(:force_selected, asset_ref, _dependencies),
    do: {:ok, {:force_assets, [asset_ref]}}

  def operator_refresh(:force_selected_upstream, _asset_ref, :none),
    do: {:error, {:refresh_include_upstream_requires_dependencies, :all}}

  def operator_refresh(:force_selected_upstream, asset_ref, :all),
    do: {:ok, {:force_assets, [asset_ref], include_upstream: true}}

  defp normalize_selection_source(source) when source in [:refresh_timeline, "refresh_timeline"],
    do: {:ok, :refresh_timeline}

  defp normalize_selection_source(source)
       when source in [:data_coverage_timeline, "data_coverage_timeline"],
       do: {:ok, :data_coverage_timeline}

  defp normalize_selection_source(source), do: {:error, {:invalid_selection_source, source}}

  defp selection_timezone(selection), do: field(selection, :timezone, "Etc/UTC") || "Etc/UTC"

  defp selection_metadata(source, selection) do
    %{
      timeline_selection: %{
        source: source,
        id: field(selection, :id),
        kind: field(selection, :kind),
        value: field(selection, :value),
        run_id: field(selection, :run_id)
      }
    }
  end

  defp window_metadata(window_id, %Anchor{} = anchor_window) do
    %{
      selected_window: %{
        id: window_id,
        kind: anchor_window.kind,
        key: anchor_window.key,
        start_at: anchor_window.start_at,
        end_at: anchor_window.end_at,
        timezone: anchor_window.timezone
      }
    }
  end

  defp put_metadata(opts, value) when value in [:missing, nil], do: {:ok, opts}

  defp put_metadata(opts, value) when is_map(value),
    do: {:ok, Keyword.put(opts, :metadata, value)}

  defp put_metadata(_opts, _value), do: {:error, :invalid_run_metadata}

  defp merge_metadata(opts, patch) do
    case Keyword.get(opts, :metadata, %{}) do
      metadata when is_map(metadata) ->
        {:ok, Keyword.put(opts, :metadata, Map.merge(metadata, patch))}

      _invalid_metadata ->
        {:error, :invalid_run_metadata}
    end
  end

  defp field(value, key, default \\ nil) when is_map(value) do
    case Map.fetch(value, key) do
      {:ok, field_value} -> field_value
      :error -> Map.get(value, Atom.to_string(key), default)
    end
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)
end
