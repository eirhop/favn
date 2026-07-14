defmodule FavnOrchestrator.TargetStatus do
  @moduledoc """
  Persisted current operator status for one manifest target.

  Target statuses are an orchestrator-owned derived read model. Authoritative
  truth remains in manifest versions, run snapshots/events, freshness state, and
  asset-window state. This projection is intentionally small, fast to query for
  catalogue/detail pages, and repairable from the authoritative records.

  A single generic target projection is used for assets and pipelines because the
  operator questions are the same: current health, in-flight work, and latest
  success/failure evidence. Asset-only freshness fields are nullable for pipeline
  rows.
  """

  @target_kinds [:asset, :pipeline]
  @statuses [:healthy, :running, :failed, :unknown]
  @run_statuses [
    :pending,
    :queued,
    :running,
    :retrying,
    :ok,
    :partial,
    :error,
    :blocked,
    :cancelled,
    :timed_out,
    :skipped,
    :skipped_fresh
  ]
  @freshness_statuses [:ok, :error, :cancelled, :timed_out, :skipped_fresh, :blocked, :running]
  @optional_binary_fields [
    :latest_run_id,
    :latest_success_run_id,
    :latest_failure_run_id,
    :in_flight_run_id,
    :freshness_key
  ]

  @enforce_keys [
    :manifest_version_id,
    :target_kind,
    :target_id,
    :target_ref_text,
    :status,
    :updated_at
  ]

  defstruct [
    :manifest_version_id,
    :target_kind,
    :target_id,
    :target_ref_text,
    :status,
    :latest_run_id,
    :latest_run_status,
    :latest_run_at,
    :latest_run_duration_ms,
    :latest_success_run_id,
    :latest_success_at,
    :latest_failure_run_id,
    :latest_failure_at,
    :in_flight_run_id,
    :freshness_status,
    :freshness_key,
    :updated_at,
    updated_seq: 0,
    payload: %{}
  ]

  @type target_kind :: :asset | :pipeline
  @type status :: :healthy | :running | :failed | :unknown

  @type t :: %__MODULE__{
          manifest_version_id: String.t(),
          target_kind: target_kind(),
          target_id: String.t(),
          target_ref_text: String.t(),
          status: status(),
          latest_run_id: String.t() | nil,
          latest_run_status: atom() | nil,
          latest_run_at: DateTime.t() | nil,
          latest_run_duration_ms: non_neg_integer() | nil,
          latest_success_run_id: String.t() | nil,
          latest_success_at: DateTime.t() | nil,
          latest_failure_run_id: String.t() | nil,
          latest_failure_at: DateTime.t() | nil,
          in_flight_run_id: String.t() | nil,
          freshness_status: atom() | nil,
          freshness_key: String.t() | nil,
          updated_at: DateTime.t(),
          updated_seq: non_neg_integer(),
          payload: map()
        }

  @doc """
  Builds a normalized target status row.
  """
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

    with {:ok, attrs} <- normalize_attrs(attrs),
         :ok <- require_binary(attrs, :manifest_version_id),
         :ok <- require_binary(attrs, :target_id),
         :ok <- require_binary(attrs, :target_ref_text),
         :ok <- validate_kind(Map.fetch!(attrs, :target_kind)),
         :ok <- validate_status(Map.fetch!(attrs, :status)),
         :ok <- validate_optional_binaries(attrs),
         :ok <- validate_duration(Map.get(attrs, :latest_run_duration_ms)),
         :ok <- validate_payload(Map.get(attrs, :payload, %{})) do
      {:ok, struct(__MODULE__, Map.merge(%{updated_seq: 0, payload: %{}}, attrs))}
    end
  end

  def new(_attrs), do: {:error, :invalid_attrs}

  @doc """
  Returns an explicit unknown row for a manifest target with no projection row.
  """
  @spec unknown(String.t(), target_kind(), String.t(), String.t()) :: t()
  def unknown(manifest_version_id, target_kind, target_id, target_ref_text)
      when is_binary(manifest_version_id) and is_binary(target_id) and is_binary(target_ref_text) do
    {:ok, status} =
      new(%{
        manifest_version_id: manifest_version_id,
        target_kind: target_kind,
        target_id: target_id,
        target_ref_text: target_ref_text,
        status: :unknown,
        updated_at: DateTime.utc_now()
      })

    status
  end

  @doc false
  @spec target_id_for_asset(Favn.Ref.t()) :: String.t()
  def target_id_for_asset({module, name}) when is_atom(module) and is_atom(name) do
    "asset:" <> Atom.to_string(module) <> ":" <> Atom.to_string(name)
  end

  @doc false
  @spec target_id_for_pipeline(module()) :: String.t()
  def target_id_for_pipeline(module) when is_atom(module),
    do: "pipeline:" <> Atom.to_string(module)

  @doc false
  @spec ref_text(Favn.Ref.t() | module()) :: String.t()
  def ref_text({module, name}) when is_atom(module) and is_atom(name) do
    Atom.to_string(module) <> ":" <> Atom.to_string(name)
  end

  def ref_text(module) when is_atom(module), do: Atom.to_string(module)
  def ref_text(value), do: inspect(value)

  @doc false
  @spec status_from_run(atom() | nil) :: status()
  def status_from_run(status) when status in [:pending, :running], do: :running
  def status_from_run(:ok), do: :healthy

  def status_from_run(status) when status in [:partial, :error, :cancelled, :timed_out],
    do: :failed

  def status_from_run(_status), do: :unknown

  @doc false
  @spec status_from_freshness(atom() | nil) :: status()
  def status_from_freshness(status) when status in [:ok, :skipped_fresh], do: :healthy
  def status_from_freshness(:running), do: :running

  def status_from_freshness(status) when status in [:error, :cancelled, :timed_out, :blocked],
    do: :failed

  def status_from_freshness(_status), do: :unknown

  defp normalize_attrs(attrs) do
    with {:ok, target_kind} <- normalize_kind(Map.get(attrs, :target_kind)),
         {:ok, status} <- normalize_status(Map.get(attrs, :status)),
         {:ok, latest_run_status} <-
           normalize_optional_status(
             Map.get(attrs, :latest_run_status),
             :latest_run_status,
             @run_statuses
           ),
         {:ok, freshness_status} <-
           normalize_optional_status(
             Map.get(attrs, :freshness_status),
             :freshness_status,
             @freshness_statuses
           ),
         {:ok, latest_run_at} <- normalize_optional_datetime(Map.get(attrs, :latest_run_at)),
         {:ok, latest_success_at} <-
           normalize_optional_datetime(Map.get(attrs, :latest_success_at)),
         {:ok, latest_failure_at} <-
           normalize_optional_datetime(Map.get(attrs, :latest_failure_at)),
         {:ok, updated_at} <- normalize_datetime(Map.get(attrs, :updated_at)),
         {:ok, updated_seq} <- normalize_updated_seq(Map.get(attrs, :updated_seq, 0)) do
      {:ok,
       attrs
       |> Map.put(:target_kind, target_kind)
       |> Map.put(:status, status)
       |> Map.put(:latest_run_status, latest_run_status)
       |> Map.put(:freshness_status, freshness_status)
       |> Map.put(:latest_run_at, latest_run_at)
       |> Map.put(:latest_success_at, latest_success_at)
       |> Map.put(:latest_failure_at, latest_failure_at)
       |> Map.put(:updated_at, updated_at)
       |> Map.put(:updated_seq, updated_seq)}
    end
  end

  defp require_binary(attrs, key) do
    case Map.get(attrs, key) do
      value when is_binary(value) and value != "" -> :ok
      value -> {:error, {:invalid_target_status_field, key, value}}
    end
  end

  defp validate_kind(kind) when kind in @target_kinds, do: :ok
  defp validate_kind(kind), do: {:error, {:invalid_target_kind, kind}}

  defp validate_status(status) when status in @statuses, do: :ok
  defp validate_status(status), do: {:error, {:invalid_status, status}}

  defp validate_payload(payload) when is_map(payload), do: :ok
  defp validate_payload(payload), do: {:error, {:invalid_payload, payload}}

  defp validate_optional_binaries(attrs) do
    Enum.reduce_while(@optional_binary_fields, :ok, fn field, :ok ->
      case Map.get(attrs, field) do
        nil -> {:cont, :ok}
        value when is_binary(value) and value != "" -> {:cont, :ok}
        value -> {:halt, {:error, {:invalid_target_status_field, field, value}}}
      end
    end)
  end

  defp validate_duration(nil), do: :ok
  defp validate_duration(value) when is_integer(value) and value >= 0, do: :ok

  defp validate_duration(value),
    do: {:error, {:invalid_target_status_field, :latest_run_duration_ms, value}}

  defp normalize_kind(kind) when kind in @target_kinds, do: {:ok, kind}
  defp normalize_kind(kind) when is_binary(kind), do: existing_atom(kind, &validate_kind/1)
  defp normalize_kind(kind), do: {:error, {:invalid_target_kind, kind}}

  defp normalize_status(status) when status in @statuses, do: {:ok, status}

  defp normalize_status(status) when is_binary(status),
    do: existing_atom(status, &validate_status/1)

  defp normalize_status(status), do: {:error, {:invalid_status, status}}

  defp normalize_optional_status(nil, _field, _allowed), do: {:ok, nil}

  defp normalize_optional_status(value, field, allowed) when is_atom(value) do
    if value in allowed,
      do: {:ok, value},
      else: {:error, {:invalid_target_status_field, field, value}}
  end

  defp normalize_optional_status(value, field, allowed) when is_binary(value) do
    case Enum.find(allowed, &(Atom.to_string(&1) == value)) do
      nil -> {:error, {:invalid_target_status_field, field, value}}
      status -> {:ok, status}
    end
  end

  defp normalize_optional_status(value, field, _allowed),
    do: {:error, {:invalid_target_status_field, field, value}}

  defp existing_atom(value, validator) when is_binary(value) and is_function(validator, 1) do
    atom = String.to_existing_atom(value)

    case validator.(atom) do
      :ok -> {:ok, atom}
      {:error, reason} -> {:error, reason}
    end
  rescue
    ArgumentError -> {:error, {:invalid_atom, value}}
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

  defp normalize_updated_seq(value) when is_integer(value) and value >= 0, do: {:ok, value}
  defp normalize_updated_seq(nil), do: {:ok, 0}
  defp normalize_updated_seq(value), do: {:error, {:invalid_updated_seq, value}}
end
