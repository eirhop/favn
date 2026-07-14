defmodule FavnOrchestrator.Operator.Lineage.Request do
  @moduledoc false

  alias FavnOrchestrator.Operator.Lineage.Error
  alias FavnOrchestrator.Operator.Lineage.Graph
  alias FavnOrchestrator.Operator.Lineage.Limits

  @allowed_opts [
    :expanded_group_ids,
    :filters,
    :limit,
    :manifest_version_id,
    :offset,
    :scope,
    :selected_id,
    :timeout_ms,
    :view_mode
  ]
  @limit_specs %{
    group_asset_page_size: {50, 100},
    max_dependency_previews_per_edge: {5, 20},
    max_inspector_adjacent_groups: {12, 50},
    max_preview_assets_per_group: {4, 10},
    max_visible_asset_nodes: {160, 300},
    max_visible_edges: {300, 600},
    max_visible_groups: {40, 80},
    search_page_size: {20, 50},
    timeout_ms: {250, 1_000}
  }
  @max_expanded_groups 80
  @max_id_bytes 1_024

  @enforce_keys [
    :manifest_version_id,
    :scope,
    :selected_id,
    :view_mode,
    :expanded_group_ids,
    :limits
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          manifest_version_id: String.t() | :active,
          scope: Graph.scope(),
          selected_id: String.t() | nil,
          view_mode: Graph.view_mode(),
          expanded_group_ids: MapSet.t(String.t()),
          limits: Limits.t()
        }

  @spec normalize(keyword()) :: {:ok, t()} | {:error, Error.t()}
  def normalize(opts) when is_list(opts) do
    with :ok <- validate_keyword(opts),
         :ok <- validate_keys(opts, @allowed_opts),
         {:ok, manifest_version_id} <-
           normalize_manifest_version(Keyword.get(opts, :manifest_version_id, :active)),
         {:ok, scope} <- normalize_enum(Keyword.get(opts, :scope, :global), [:global]),
         {:ok, view_mode} <- normalize_enum(Keyword.get(opts, :view_mode, :all), [:all]),
         :ok <- validate_filters(Keyword.get(opts, :filters, %{})),
         {:ok, selected_id} <- normalize_optional_id(Keyword.get(opts, :selected_id)),
         {:ok, expanded_group_ids} <-
           normalize_expanded_ids(Keyword.get(opts, :expanded_group_ids, [])),
         {:ok, limits} <-
           normalize_limits(Keyword.get(opts, :limit, []), Keyword.get(opts, :timeout_ms)) do
      {:ok,
       %__MODULE__{
         manifest_version_id: manifest_version_id,
         scope: scope,
         selected_id: selected_id,
         view_mode: view_mode,
         expanded_group_ids: expanded_group_ids,
         limits: limits
       }}
    else
      {:error, reason} -> {:error, invalid_request(reason)}
    end
  end

  def normalize(_opts), do: {:error, invalid_request(:options_must_be_a_keyword_list)}

  defp validate_keyword(opts) do
    if Keyword.keyword?(opts), do: :ok, else: {:error, :options_must_be_a_keyword_list}
  end

  defp validate_keys(opts, allowed) do
    case Keyword.keys(opts) -- allowed do
      [] -> :ok
      keys -> {:error, {:unsupported_options, Enum.uniq(keys)}}
    end
  end

  defp normalize_manifest_version(:active), do: {:ok, :active}
  defp normalize_manifest_version(value), do: normalize_required_id(value, :manifest_version_id)

  defp normalize_enum(value, allowed) when is_atom(value) do
    if value in allowed, do: {:ok, value}, else: {:error, :unsupported_enum_value}
  end

  defp normalize_enum(value, allowed) when is_binary(value) do
    case Enum.find(allowed, &(Atom.to_string(&1) == value)) do
      nil -> {:error, :unsupported_enum_value}
      normalized -> {:ok, normalized}
    end
  end

  defp normalize_enum(_value, _allowed), do: {:error, :unsupported_enum_value}

  defp validate_filters(filters) when filters in [%{}, nil], do: :ok
  defp validate_filters(_filters), do: {:error, :unsupported_filters}

  defp normalize_optional_id(nil), do: {:ok, nil}
  defp normalize_optional_id(value), do: normalize_required_id(value, :selected_id)

  defp normalize_required_id(value, _field)
       when is_binary(value) and byte_size(value) > 0 and byte_size(value) <= @max_id_bytes,
       do: {:ok, value}

  defp normalize_required_id(_value, field), do: {:error, {:invalid_identifier, field}}

  defp normalize_expanded_ids(%MapSet{} = ids),
    do: ids |> MapSet.to_list() |> normalize_expanded_ids()

  defp normalize_expanded_ids(ids) when is_list(ids) and length(ids) <= @max_expanded_groups do
    if Enum.all?(ids, &valid_id?/1) do
      {:ok, MapSet.new(ids)}
    else
      {:error, :invalid_expanded_group_ids}
    end
  end

  defp normalize_expanded_ids(_ids), do: {:error, :invalid_expanded_group_ids}

  defp valid_id?(value),
    do: is_binary(value) and byte_size(value) > 0 and byte_size(value) <= @max_id_bytes

  defp normalize_limits(value, timeout_ms) when is_integer(value) do
    with :ok <- validate_optional_timeout(timeout_ms) do
      build_limits([], timeout_ms)
    end
  end

  defp normalize_limits(limit_opts, timeout_ms) when is_list(limit_opts) do
    with :ok <- validate_keyword(limit_opts),
         :ok <- validate_keys(limit_opts, Map.keys(@limit_specs)),
         :ok <- validate_optional_timeout(timeout_ms) do
      build_limits(limit_opts, timeout_ms)
    end
  end

  defp normalize_limits(_limit_opts, _timeout_ms), do: {:error, :invalid_lineage_limits}

  defp validate_optional_timeout(nil), do: :ok

  defp validate_optional_timeout(value) do
    {_default, max} = Map.fetch!(@limit_specs, :timeout_ms)
    validate_limit(:timeout_ms, value, max)
  end

  defp build_limits(limit_opts, timeout_ms) do
    Enum.reduce_while(@limit_specs, {:ok, %Limits{}}, fn {key, {default, max}}, {:ok, limits} ->
      requested =
        if key == :timeout_ms and not is_nil(timeout_ms),
          do: timeout_ms,
          else: Keyword.get(limit_opts, key, default)

      case validate_limit(key, requested, max) do
        :ok -> {:cont, {:ok, Map.put(limits, key, requested)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp validate_limit(_key, value, max) when is_integer(value) and value >= 1 and value <= max,
    do: :ok

  defp validate_limit(key, _value, _max), do: {:error, {:invalid_limit, key}}

  defp invalid_request(reason) when reason in [:unsupported_enum_value, :unsupported_filters] do
    %Error{
      code: :invalid_scope,
      message: "Invalid lineage scope, view mode, or filters.",
      details: %{reason: reason}
    }
  end

  defp invalid_request(reason) do
    %Error{
      code: :invalid_request,
      message: "Invalid lineage request.",
      details: %{reason: safe_reason(reason)}
    }
  end

  defp safe_reason({kind, keys}) when is_atom(kind) and is_list(keys), do: {kind, keys}
  defp safe_reason({kind, field}) when is_atom(kind) and is_atom(field), do: {kind, field}
  defp safe_reason(reason) when is_atom(reason), do: reason
  defp safe_reason(_reason), do: :invalid_request
end
