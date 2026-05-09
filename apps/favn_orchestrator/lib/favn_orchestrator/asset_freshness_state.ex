defmodule FavnOrchestrator.AssetFreshnessState do
  @moduledoc """
  Latest normalized freshness state for a concrete asset/freshness key.

  This read model is owned by the orchestrator storage layer. It preserves the
  latest successful freshness-producing run separately from the latest attempt so
  callers can distinguish currently running or failed attempts from the last
  usable freshness version.

  ## Important Fields

  - `asset_ref_module` and `asset_ref_name`: canonical asset ref parts.
  - `freshness_key`: stable key from `Favn.Freshness.Key`.
  - `freshness_version`: version compared by downstream stale checks.
  - `latest_success_node_key`: concrete planned node key that produced the latest
    usable success.
  - `input_versions`: upstream freshness versions consumed by the latest success.

  Query this read model through `FavnOrchestrator.get_asset_freshness/2`,
  `FavnOrchestrator.list_asset_freshness/1`, and
  `FavnOrchestrator.explain_asset_staleness/2`.
  """

  @enforce_keys [
    :asset_ref_module,
    :asset_ref_name,
    :freshness_key,
    :status,
    :updated_at
  ]
  defstruct [
    :asset_ref_module,
    :asset_ref_name,
    :freshness_key,
    :status,
    :freshness_version,
    :latest_success_run_id,
    :latest_success_node_key,
    :latest_success_at,
    :latest_attempt_run_id,
    :latest_attempt_status,
    :latest_attempt_at,
    :manifest_version_id,
    :manifest_content_hash,
    input_versions: %{},
    metadata: %{},
    updated_at: nil
  ]

  @type status :: :ok | :error | :cancelled | :timed_out | :skipped_fresh | :blocked | :running

  @type t :: %__MODULE__{
          asset_ref_module: module(),
          asset_ref_name: atom(),
          freshness_key: String.t(),
          status: status(),
          freshness_version: String.t() | nil,
          latest_success_run_id: String.t() | nil,
          latest_success_node_key: term() | nil,
          latest_success_at: DateTime.t() | nil,
          latest_attempt_run_id: String.t() | nil,
          latest_attempt_status: atom() | nil,
          latest_attempt_at: DateTime.t() | nil,
          manifest_version_id: String.t() | nil,
          manifest_content_hash: String.t() | nil,
          input_versions: map() | list(),
          metadata: map(),
          updated_at: DateTime.t()
        }

  @required_keys @enforce_keys
  @statuses [:ok, :error, :cancelled, :timed_out, :skipped_fresh, :blocked, :running]

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

    with {:ok, attrs} <- normalize_attrs(attrs),
         :ok <- require_keys(attrs, @required_keys),
         :ok <- validate_identity(attrs),
         :ok <- validate_input_versions(Map.get(attrs, :input_versions, %{})),
         :ok <- validate_metadata(Map.get(attrs, :metadata, %{})) do
      {:ok, struct(__MODULE__, Map.merge(%{input_versions: %{}, metadata: %{}}, attrs))}
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

  defp validate_identity(attrs) do
    cond do
      not is_atom(Map.fetch!(attrs, :asset_ref_module)) ->
        {:error, {:invalid_asset_ref_module, Map.fetch!(attrs, :asset_ref_module)}}

      not is_atom(Map.fetch!(attrs, :asset_ref_name)) ->
        {:error, {:invalid_asset_ref_name, Map.fetch!(attrs, :asset_ref_name)}}

      not is_binary(Map.fetch!(attrs, :freshness_key)) ->
        {:error, {:invalid_freshness_key, Map.fetch!(attrs, :freshness_key)}}

      true ->
        :ok
    end
  end

  defp normalize_attrs(attrs) do
    with {:ok, status} <- normalize_status(Map.get(attrs, :status)),
         {:ok, latest_attempt_status} <-
           normalize_optional_status(Map.get(attrs, :latest_attempt_status)),
         {:ok, latest_success_at} <-
           normalize_optional_datetime(Map.get(attrs, :latest_success_at)),
         {:ok, latest_attempt_at} <-
           normalize_optional_datetime(Map.get(attrs, :latest_attempt_at)),
         {:ok, updated_at} <- normalize_datetime(Map.get(attrs, :updated_at)) do
      {:ok,
       attrs
       |> Map.put(:status, status)
       |> Map.put(:latest_attempt_status, latest_attempt_status)
       |> Map.put(:latest_success_at, latest_success_at)
       |> Map.put(:latest_attempt_at, latest_attempt_at)
       |> Map.put(:updated_at, updated_at)}
    end
  end

  defp normalize_optional_status(nil), do: {:ok, nil}
  defp normalize_optional_status(""), do: {:ok, nil}
  defp normalize_optional_status(value), do: normalize_status(value)

  defp normalize_status(value) when value in @statuses, do: {:ok, value}

  defp normalize_status(value) when is_binary(value) do
    value
    |> String.trim()
    |> String.downcase()
    |> status_from_string()
  end

  defp normalize_status(value), do: {:error, {:invalid_status, value}}

  defp status_from_string("ok"), do: {:ok, :ok}
  defp status_from_string("error"), do: {:ok, :error}
  defp status_from_string("cancelled"), do: {:ok, :cancelled}
  defp status_from_string("timed_out"), do: {:ok, :timed_out}
  defp status_from_string("skipped_fresh"), do: {:ok, :skipped_fresh}
  defp status_from_string("blocked"), do: {:ok, :blocked}
  defp status_from_string("running"), do: {:ok, :running}
  defp status_from_string(value), do: {:error, {:invalid_status, value}}

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

  defp validate_input_versions(value) when is_map(value) or is_list(value), do: :ok
  defp validate_input_versions(value), do: {:error, {:invalid_input_versions, value}}

  defp validate_metadata(value) when is_map(value), do: :ok
  defp validate_metadata(value), do: {:error, {:invalid_metadata, value}}
end
