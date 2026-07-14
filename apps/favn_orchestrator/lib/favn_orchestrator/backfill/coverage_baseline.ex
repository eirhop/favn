defmodule FavnOrchestrator.Backfill.CoverageBaseline do
  @moduledoc """
  Normalized coverage/cutover state established by a full-load or baseline run.

  Source identity fields are intentionally limited to stable keys plus hashed or
  redacted values. Raw source IDs, tokens, and secrets must not be stored here.

  Backfill range resolution can use this state to choose relative windows after
  a safe baseline/cutover point. The baseline is derived state; the source run
  and run event stream remain authoritative.
  """

  alias FavnOrchestrator.Backfill.ReadModelValues

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

  @type status :: ReadModelValues.status()

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
         :ok <- require_keys(attrs, @required_keys),
         :ok <- validate_fields(attrs) do
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
    with {:ok, window_kind} <- ReadModelValues.normalize_window_kind(Map.get(attrs, :window_kind)),
         {:ok, status} <- ReadModelValues.normalize_status(Map.get(attrs, :status)),
         {:ok, coverage_until} <- normalize_datetime(Map.get(attrs, :coverage_until)),
         {:ok, coverage_start_at} <-
           normalize_optional_datetime(Map.get(attrs, :coverage_start_at)),
         {:ok, created_at} <- normalize_datetime(Map.get(attrs, :created_at)),
         {:ok, updated_at} <- normalize_datetime(Map.get(attrs, :updated_at)) do
      {:ok,
       attrs
       |> Map.put(:window_kind, window_kind)
       |> Map.put(:status, status)
       |> Map.put(:coverage_until, coverage_until)
       |> Map.put(:coverage_start_at, coverage_start_at)
       |> Map.put(:created_at, created_at)
       |> Map.put(:updated_at, updated_at)}
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

  defp validate_fields(attrs) do
    cond do
      not is_atom(attrs.pipeline_module) ->
        {:error, {:invalid_pipeline_module, attrs.pipeline_module}}

      not required_binary_fields?(attrs) ->
        {:error, :invalid_coverage_baseline_identity}

      not optional_binary?(Map.get(attrs, :segment_key_redacted)) ->
        {:error, {:invalid_segment_key_redacted, Map.get(attrs, :segment_key_redacted)}}

      not valid_coverage_range?(Map.get(attrs, :coverage_start_at), attrs.coverage_until) ->
        {:error,
         {:invalid_coverage_range, Map.get(attrs, :coverage_start_at), attrs.coverage_until}}

      not is_list(Map.get(attrs, :errors, [])) ->
        {:error, {:invalid_errors, Map.get(attrs, :errors)}}

      not is_map(Map.get(attrs, :metadata, %{})) ->
        {:error, {:invalid_metadata, Map.get(attrs, :metadata)}}

      true ->
        :ok
    end
  end

  defp required_binary_fields?(attrs) do
    Enum.all?(
      [
        :baseline_id,
        :source_key,
        :segment_key_hash,
        :timezone,
        :created_by_run_id,
        :manifest_version_id
      ],
      fn field ->
        value = Map.get(attrs, field)
        is_binary(value) and value != ""
      end
    )
  end

  defp optional_binary?(nil), do: true
  defp optional_binary?(value), do: is_binary(value) and value != ""
  defp valid_coverage_range?(nil, %DateTime{}), do: true

  defp valid_coverage_range?(%DateTime{} = start_at, %DateTime{} = until),
    do: DateTime.compare(start_at, until) in [:lt, :eq]
end
