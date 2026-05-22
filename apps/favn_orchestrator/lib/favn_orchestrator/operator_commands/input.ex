defmodule FavnOrchestrator.OperatorCommands.Input do
  @moduledoc false

  alias Favn.Backfill.RangeRequest
  alias Favn.Window.Request, as: WindowRequest

  @dependency_modes [:all, :none]
  @asset_refresh_modes [:auto, :missing, :force_all, :force_selected, :force_selected_upstream]
  @pipeline_refresh_modes [:auto, :missing, :force_all]
  @selection_sources [:refresh_timeline, :data_coverage_timeline]

  def empty?(nil), do: true
  def empty?(value) when value in [%{}, []], do: true
  def empty?(_value), do: false

  def field(value, field, default \\ nil)

  def field(value, field, default) when is_map(value) do
    Map.get(value, field, Map.get(value, Atom.to_string(field), default))
  end

  def field(value, field, default) when is_list(value) do
    Keyword.get(value, field, default)
  end

  def field(_value, _field, default), do: default

  def dependency_mode(value) when value in @dependency_modes, do: {:ok, value}

  def dependency_mode(value) when is_binary(value) do
    case value do
      "all" -> {:ok, :all}
      "none" -> {:ok, :none}
      _other -> {:error, {:invalid_operator_dependency_mode, value}}
    end
  end

  def dependency_mode(value), do: {:error, {:invalid_operator_dependency_mode, value}}

  def asset_refresh_mode(value), do: refresh_mode(value, @asset_refresh_modes)
  def pipeline_refresh_mode(value), do: refresh_mode(value, @pipeline_refresh_modes)

  def selection(nil), do: {:ok, nil}

  def selection(value) when is_map(value) do
    source = field(value, :source)
    id = field(value, :id)
    kind = field(value, :kind)
    timeline_value = field(value, :value)
    timezone = field(value, :timezone, "Etc/UTC")
    run_id = field(value, :run_id)

    with {:ok, source} <- selection_source(source),
         {:ok, id} <- selection_id(source, id, kind, timeline_value) do
      {:ok,
       %{
         source: source,
         id: id,
         kind: kind,
         value: timeline_value,
         timezone: timezone,
         run_id: run_id
       }}
    end
  end

  def selection(value), do: {:error, {:invalid_operator_selection, value}}

  def range(value) do
    case RangeRequest.from_value(value) do
      {:ok, %RangeRequest{} = request} -> {:ok, request}
      {:error, _reason} -> {:error, {:invalid_operator_range, value}}
    end
  end

  def window(nil), do: {:ok, nil}

  def window(%WindowRequest{} = request) do
    normalize_window_result(request, fn -> WindowRequest.from_value(request) end)
  end

  def window(value) when is_binary(value) do
    normalize_window_result(value, fn -> WindowRequest.parse(value) end)
  end

  def window(value) when is_map(value) do
    normalize_window_result(value, fn -> WindowRequest.from_value(value) end)
  end

  def window(value), do: {:error, {:invalid_operator_window, value}}

  defp normalize_window_result(original, fun) do
    case fun.() do
      {:ok, %WindowRequest{} = request} -> {:ok, request}
      {:error, _reason} -> {:error, {:invalid_operator_window, original}}
    end
  rescue
    _exception -> {:error, {:invalid_operator_window, original}}
  end

  defp refresh_mode(:force, modes), do: maybe_refresh_mode(:force_all, modes, :force)

  defp refresh_mode(value, modes) when is_atom(value) do
    if value in modes do
      {:ok, value}
    else
      {:error, {:invalid_operator_refresh_mode, value}}
    end
  end

  defp refresh_mode(value, modes) when is_binary(value) do
    case value do
      "auto" -> {:ok, :auto}
      "missing" -> {:ok, :missing}
      "force" -> {:ok, :force_all}
      "force_all" -> {:ok, :force_all}
      "force_selected" -> maybe_refresh_mode(:force_selected, modes, value)
      "force_selected_upstream" -> maybe_refresh_mode(:force_selected_upstream, modes, value)
      _other -> {:error, {:invalid_operator_refresh_mode, value}}
    end
  end

  defp refresh_mode(value, _modes), do: {:error, {:invalid_operator_refresh_mode, value}}

  defp maybe_refresh_mode(mode, modes, original) do
    if mode in modes do
      {:ok, mode}
    else
      {:error, {:invalid_operator_refresh_mode, original}}
    end
  end

  defp selection_source(source) when source in @selection_sources, do: {:ok, source}

  defp selection_source(source) when is_binary(source) do
    case source do
      "refresh_timeline" -> {:ok, :refresh_timeline}
      "data_coverage_timeline" -> {:ok, :data_coverage_timeline}
      _other -> {:error, {:invalid_operator_selection_source, source}}
    end
  end

  defp selection_source(source), do: {:error, {:invalid_operator_selection_source, source}}

  defp selection_id(_source, id, _kind, _value) when is_binary(id) and id != "", do: {:ok, id}

  defp selection_id(source, _id, kind, value)
       when is_binary(kind) and kind != "" and is_binary(value) and value != "" do
    {:ok, derived_selection_id(source, kind, value)}
  end

  defp selection_id(_source, id, _kind, _value),
    do: {:error, {:invalid_operator_selection_id, id}}

  defp derived_selection_id(:refresh_timeline, kind, value), do: "refresh:#{kind}:#{value}"
  defp derived_selection_id(:data_coverage_timeline, kind, value), do: "window:#{kind}:#{value}"
end
