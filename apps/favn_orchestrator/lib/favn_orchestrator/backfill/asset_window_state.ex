defmodule FavnOrchestrator.Backfill.AssetWindowState do
  @moduledoc """
  Latest normalized state for a concrete asset/window.

  Backfill child runs project terminal asset results into this row. It is the
  control-plane read model for questions such as "what last happened for this
  asset in this window?" and preserves the latest successful run separately from
  the latest attempt.
  """

  @enforce_keys [
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

  @type status :: :pending | :running | :ok | :partial | :error | :cancelled | :timed_out

  @type t :: %__MODULE__{
          asset_ref_module: module(),
          asset_ref_name: atom(),
          pipeline_module: module(),
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
         :ok <- require_keys(attrs, @required_keys) do
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
    with {:ok, window_kind} <- normalize_window_kind(Map.get(attrs, :window_kind)),
         {:ok, status} <- normalize_status(Map.get(attrs, :status)) do
      {:ok, attrs |> Map.put(:window_kind, window_kind) |> Map.put(:status, status)}
    end
  end

  defp normalize_window_kind(value) when value in [:hour, :day, :month, :year], do: {:ok, value}
  defp normalize_window_kind(:hourly), do: {:ok, :hour}
  defp normalize_window_kind(:daily), do: {:ok, :day}
  defp normalize_window_kind(:monthly), do: {:ok, :month}
  defp normalize_window_kind(:yearly), do: {:ok, :year}
  defp normalize_window_kind("hour"), do: {:ok, :hour}
  defp normalize_window_kind("hourly"), do: {:ok, :hour}
  defp normalize_window_kind("day"), do: {:ok, :day}
  defp normalize_window_kind("daily"), do: {:ok, :day}
  defp normalize_window_kind("month"), do: {:ok, :month}
  defp normalize_window_kind("monthly"), do: {:ok, :month}
  defp normalize_window_kind("year"), do: {:ok, :year}
  defp normalize_window_kind("yearly"), do: {:ok, :year}
  defp normalize_window_kind(value), do: {:error, {:invalid_window_kind, value}}

  defp normalize_status(value)
       when value in [:pending, :running, :ok, :partial, :error, :cancelled, :timed_out],
       do: {:ok, value}

  defp normalize_status("pending"), do: {:ok, :pending}
  defp normalize_status("running"), do: {:ok, :running}
  defp normalize_status("ok"), do: {:ok, :ok}
  defp normalize_status("partial"), do: {:ok, :partial}
  defp normalize_status("error"), do: {:ok, :error}
  defp normalize_status("cancelled"), do: {:ok, :cancelled}
  defp normalize_status("timed_out"), do: {:ok, :timed_out}
  defp normalize_status(value), do: {:error, {:invalid_status, value}}
end
