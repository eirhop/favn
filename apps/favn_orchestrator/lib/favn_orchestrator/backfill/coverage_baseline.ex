defmodule FavnOrchestrator.Backfill.CoverageBaseline do
  @moduledoc """
  Normalized coverage/cutover state established by a full-load or baseline run.

  Source identity fields are intentionally limited to stable keys plus hashed or
  redacted values. Raw source IDs, tokens, and secrets must not be stored here.

  Backfill range resolution can use this state to choose relative windows after
  a safe baseline/cutover point. The baseline is derived state; the source run
  and run event stream remain authoritative.
  """

  @enforce_keys [
    :baseline_id,
    :pipeline_module,
    :source_key,
    :segment_key_hash,
    :window_kind,
    :timezone,
    :coverage_until,
    :created_by_run_id,
    :manifest_version_id,
    :status,
    :created_at,
    :updated_at
  ]
  defstruct [
    :baseline_id,
    :pipeline_module,
    :source_key,
    :segment_key_hash,
    :segment_key_redacted,
    :window_kind,
    :timezone,
    :coverage_start_at,
    :coverage_until,
    :created_by_run_id,
    :manifest_version_id,
    :status,
    errors: [],
    metadata: %{},
    created_at: nil,
    updated_at: nil
  ]

  @type status :: :pending | :running | :ok | :partial | :error | :cancelled | :timed_out

  @type t :: %__MODULE__{
          baseline_id: String.t(),
          pipeline_module: module(),
          source_key: String.t(),
          segment_key_hash: String.t(),
          segment_key_redacted: String.t() | nil,
          window_kind: atom(),
          timezone: String.t(),
          coverage_start_at: DateTime.t() | nil,
          coverage_until: DateTime.t(),
          created_by_run_id: String.t(),
          manifest_version_id: String.t(),
          status: status(),
          errors: [term()],
          metadata: map(),
          created_at: DateTime.t(),
          updated_at: DateTime.t()
        }

  @required_keys @enforce_keys
  @raw_source_keys [:segment_id, :source_id, :source_secret, :token, :secret]

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

    with {:ok, attrs} <- normalize_attrs(attrs),
         :ok <- reject_raw_source_identity(attrs),
         :ok <- require_keys(attrs, @required_keys) do
      {:ok,
       struct(
         __MODULE__,
         Map.merge(%{errors: [], metadata: %{}}, attrs)
       )}
    end
  end

  def new(_attrs), do: {:error, :invalid_attrs}

  defp reject_raw_source_identity(attrs) do
    case find_raw_source_key(attrs) do
      nil -> :ok
      key -> {:error, {:raw_source_identity_not_allowed, key}}
    end
  end

  defp find_raw_source_key(%_struct{}), do: nil

  defp find_raw_source_key(value) when is_map(value) do
    Enum.find_value(value, fn {key, nested} ->
      if raw_source_key?(key) do
        normalize_key(key)
      else
        find_raw_source_key(nested)
      end
    end)
  end

  defp find_raw_source_key(value) when is_list(value),
    do: Enum.find_value(value, &find_raw_source_key/1)

  defp find_raw_source_key(_value), do: nil

  defp raw_source_key?(key), do: normalize_key(key) in @raw_source_keys

  defp normalize_key(key) when is_atom(key), do: key

  defp normalize_key(key) when is_binary(key) do
    case key do
      "segment_id" -> :segment_id
      "source_id" -> :source_id
      "source_secret" -> :source_secret
      "token" -> :token
      "secret" -> :secret
      _other -> key
    end
  end

  defp normalize_key(key), do: key

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
         {:ok, status} <- normalize_status(Map.get(attrs, :status)),
         {:ok, coverage_until} <- normalize_datetime(Map.get(attrs, :coverage_until)),
         {:ok, coverage_start_at} <-
           normalize_optional_datetime(Map.get(attrs, :coverage_start_at)) do
      {:ok,
       attrs
       |> Map.put(:window_kind, window_kind)
       |> Map.put(:status, status)
       |> Map.put(:coverage_until, coverage_until)
       |> Map.put(:coverage_start_at, coverage_start_at)}
    end
  end

  defp normalize_optional_datetime(nil), do: {:ok, nil}
  defp normalize_optional_datetime(""), do: {:ok, nil}
  defp normalize_optional_datetime(value), do: normalize_datetime(value)

  defp normalize_datetime(%DateTime{} = value), do: {:ok, value}

  defp normalize_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, _reason} -> {:error, {:invalid_datetime, value}}
    end
  end

  defp normalize_datetime(value), do: {:error, {:invalid_datetime, value}}

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
