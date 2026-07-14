defmodule FavnOrchestrator.Backfill.AssetWindowState do
  @moduledoc """
  Latest normalized state for a concrete asset/window.

  Backfill child runs project terminal asset results into this row. It is the
  control-plane read model for questions such as "what last happened for this
  asset in this window?" and preserves the latest successful run separately from
  the latest attempt.
  """

  alias FavnOrchestrator.Backfill.ReadModelValues

  @enforce_keys [
    :asset_ref_module,
    :asset_ref_name,
    :manifest_version_id,
    :window_kind,
    :window_start_at,
    :window_end_at,
    :timezone,
    :window_key,
    :status,
    :latest_run_id,
    :updated_at
  ]
  defstruct [
    :asset_ref_module,
    :asset_ref_name,
    :pipeline_module,
    :manifest_version_id,
    :window_kind,
    :window_start_at,
    :window_end_at,
    :timezone,
    :window_key,
    :status,
    :latest_run_id,
    :latest_parent_run_id,
    :latest_success_run_id,
    :latest_error,
    :rows_written,
    :updated_at,
    errors: [],
    metadata: %{}
  ]

  @type status :: ReadModelValues.status()

  @type t :: %__MODULE__{
          asset_ref_module: module(),
          asset_ref_name: atom(),
          pipeline_module: module() | nil,
          manifest_version_id: String.t(),
          window_kind: atom(),
          window_start_at: DateTime.t(),
          window_end_at: DateTime.t(),
          timezone: String.t(),
          window_key: String.t(),
          status: status(),
          latest_run_id: String.t(),
          latest_parent_run_id: String.t() | nil,
          latest_success_run_id: String.t() | nil,
          latest_error: term(),
          errors: [term()],
          rows_written: non_neg_integer() | nil,
          metadata: map(),
          updated_at: DateTime.t()
        }

  @required_keys @enforce_keys

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

    with {:ok, attrs} <- normalize_attrs(attrs),
         :ok <- require_keys(attrs, @required_keys),
         :ok <- validate_fields(attrs) do
      {:ok, struct(__MODULE__, Map.merge(%{errors: [], metadata: %{}}, attrs))}
    end
  end

  def new(_attrs), do: {:error, :invalid_attrs}

  defp require_keys(attrs, keys) do
    missing = Enum.filter(keys, &missing?(attrs, &1))

    case missing do
      [] -> :ok
      keys -> {:error, {:missing_required_keys, keys}}
    end
  end

  defp missing?(attrs, key), do: Map.get(attrs, key) in [nil, ""]

  defp normalize_attrs(attrs) do
    with {:ok, window_kind} <- ReadModelValues.normalize_window_kind(Map.get(attrs, :window_kind)),
         {:ok, status} <- ReadModelValues.normalize_status(Map.get(attrs, :status)),
         {:ok, window_start_at} <- normalize_datetime(Map.get(attrs, :window_start_at)),
         {:ok, window_end_at} <- normalize_datetime(Map.get(attrs, :window_end_at)),
         {:ok, updated_at} <- normalize_datetime(Map.get(attrs, :updated_at)) do
      {:ok,
       attrs
       |> Map.put(:window_kind, window_kind)
       |> Map.put(:status, status)
       |> Map.put(:window_start_at, window_start_at)
       |> Map.put(:window_end_at, window_end_at)
       |> Map.put(:updated_at, updated_at)}
    end
  end

  defp normalize_datetime(%DateTime{} = value), do: {:ok, value}

  defp normalize_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, _reason} -> {:error, {:invalid_datetime, value}}
    end
  end

  defp normalize_datetime(value), do: {:error, {:invalid_datetime, value}}

  defp validate_fields(attrs) do
    with :ok <- validate_asset_ref(attrs.asset_ref_module, attrs.asset_ref_name),
         :ok <- validate_pipeline_module(Map.get(attrs, :pipeline_module)),
         :ok <- validate_required_binaries(attrs),
         :ok <- validate_optional_binaries(attrs),
         :ok <- validate_window_range(attrs.window_start_at, attrs.window_end_at),
         :ok <- validate_rows_written(Map.get(attrs, :rows_written)) do
      validate_collections(Map.get(attrs, :errors, []), Map.get(attrs, :metadata, %{}))
    end
  end

  defp validate_asset_ref(module, name) do
    cond do
      not is_atom(module) -> {:error, {:invalid_asset_ref_module, module}}
      not is_atom(name) -> {:error, {:invalid_asset_ref_name, name}}
      true -> :ok
    end
  end

  defp validate_pipeline_module(nil), do: :ok
  defp validate_pipeline_module(module) when is_atom(module), do: :ok
  defp validate_pipeline_module(module), do: {:error, {:invalid_pipeline_module, module}}

  defp validate_required_binaries(attrs) do
    validate_binaries(
      attrs,
      [:manifest_version_id, :timezone, :window_key, :latest_run_id],
      false
    )
  end

  defp validate_optional_binaries(attrs) do
    validate_binaries(attrs, [:latest_parent_run_id, :latest_success_run_id], true)
  end

  defp validate_binaries(attrs, fields, optional?) do
    Enum.reduce_while(fields, :ok, fn field, :ok ->
      value = Map.get(attrs, field)

      if (optional? and is_nil(value)) or (is_binary(value) and value != "") do
        {:cont, :ok}
      else
        {:halt, {:error, {:invalid_asset_window_field, field, value}}}
      end
    end)
  end

  defp validate_window_range(%DateTime{} = start_at, %DateTime{} = end_at) do
    if DateTime.compare(start_at, end_at) == :lt,
      do: :ok,
      else: {:error, {:invalid_window_range, start_at, end_at}}
  end

  defp validate_rows_written(nil), do: :ok
  defp validate_rows_written(value) when is_integer(value) and value >= 0, do: :ok
  defp validate_rows_written(value), do: {:error, {:invalid_rows_written, value}}

  defp validate_collections(errors, metadata) do
    cond do
      not is_list(errors) -> {:error, {:invalid_errors, errors}}
      not is_map(metadata) -> {:error, {:invalid_metadata, metadata}}
      true -> :ok
    end
  end
end
