defmodule FavnOrchestrator.RunState do
  @moduledoc """
  Persisted run snapshot owned by the orchestrator control plane.
  """

  @default_timeout_ms 30 * 60 * 1000

  @type status :: :pending | :running | :ok | :partial | :error | :cancelled | :timed_out

  @type t :: %__MODULE__{
          id: String.t(),
          manifest_version_id: String.t(),
          manifest_content_hash: String.t(),
          asset_ref: Favn.Ref.t(),
          target_refs: [Favn.Ref.t()],
          plan: Favn.Plan.t() | nil,
          status: status(),
          event_seq: pos_integer(),
          snapshot_hash: String.t() | nil,
          params: map(),
          trigger: map(),
          metadata: map(),
          submit_kind: :manual | :rerun | :pipeline | :backfill_asset | :backfill_pipeline,
          rerun_of_run_id: String.t() | nil,
          parent_run_id: String.t() | nil,
          root_run_id: String.t() | nil,
          lineage_depth: non_neg_integer(),
          max_attempts: pos_integer(),
          retry_backoff_ms: non_neg_integer(),
          timeout_ms: pos_integer(),
          runner_execution_id: String.t() | nil,
          result: map() | nil,
          error: term() | nil,
          inserted_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil
        }

  defstruct [
    :id,
    :manifest_version_id,
    :manifest_content_hash,
    :asset_ref,
    :plan,
    :snapshot_hash,
    :inserted_at,
    :updated_at,
    status: :pending,
    event_seq: 1,
    target_refs: [],
    params: %{},
    trigger: %{},
    metadata: %{},
    submit_kind: :manual,
    rerun_of_run_id: nil,
    parent_run_id: nil,
    root_run_id: nil,
    lineage_depth: 0,
    max_attempts: 1,
    retry_backoff_ms: 0,
    timeout_ms: @default_timeout_ms,
    runner_execution_id: nil,
    result: nil,
    error: nil
  ]

  @spec new(keyword()) :: t()
  def new(opts) when is_list(opts) do
    now = DateTime.utc_now()

    %__MODULE__{
      id: Keyword.fetch!(opts, :id),
      manifest_version_id: Keyword.fetch!(opts, :manifest_version_id),
      manifest_content_hash: Keyword.fetch!(opts, :manifest_content_hash),
      asset_ref: Keyword.fetch!(opts, :asset_ref),
      target_refs: normalize_refs(Keyword.get(opts, :target_refs, [])),
      plan: Keyword.get(opts, :plan),
      params: normalize_map(Keyword.get(opts, :params, %{})),
      trigger: normalize_map(Keyword.get(opts, :trigger, %{})),
      metadata: normalize_map(Keyword.get(opts, :metadata, %{})),
      submit_kind: normalize_submit_kind(Keyword.get(opts, :submit_kind, :manual)),
      rerun_of_run_id: normalize_optional_string(Keyword.get(opts, :rerun_of_run_id)),
      parent_run_id: normalize_optional_string(Keyword.get(opts, :parent_run_id)),
      root_run_id: normalize_optional_string(Keyword.get(opts, :root_run_id)),
      lineage_depth: normalize_non_neg_int(Keyword.get(opts, :lineage_depth, 0), 0),
      max_attempts: normalize_positive_int(Keyword.get(opts, :max_attempts, 1), 1),
      retry_backoff_ms: normalize_non_neg_int(Keyword.get(opts, :retry_backoff_ms, 0), 0),
      timeout_ms:
        normalize_positive_int(
          Keyword.get(opts, :timeout_ms, @default_timeout_ms),
          @default_timeout_ms
        ),
      inserted_at: now,
      updated_at: now
    }
    |> with_snapshot_hash()
  end

  @doc false
  @spec default_timeout_ms() :: pos_integer()
  def default_timeout_ms, do: @default_timeout_ms

  @spec transition(t(), keyword()) :: t()
  def transition(%__MODULE__{} = run, attrs) when is_list(attrs) do
    run
    |> Map.merge(Enum.into(attrs, %{}))
    |> Map.put(:event_seq, run.event_seq + 1)
    |> Map.put(:updated_at, DateTime.utc_now())
    |> with_snapshot_hash()
  end

  @spec with_snapshot_hash(t()) :: t()
  def with_snapshot_hash(%__MODULE__{} = run) do
    payload =
      run
      |> Map.from_struct()
      |> Map.delete(:snapshot_hash)

    hash =
      payload
      |> :erlang.term_to_binary()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    %{run | snapshot_hash: hash}
  end

  defp normalize_map(value) when is_map(value), do: value
  defp normalize_map(_value), do: %{}

  defp normalize_refs(refs) when is_list(refs) do
    refs
    |> Enum.filter(fn
      {module, name} when is_atom(module) and is_atom(name) -> true
      _other -> false
    end)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp normalize_refs(_value), do: []

  defp normalize_positive_int(value, _default) when is_integer(value) and value > 0, do: value
  defp normalize_positive_int(_value, default), do: default

  defp normalize_non_neg_int(value, _default) when is_integer(value) and value >= 0, do: value
  defp normalize_non_neg_int(_value, default), do: default

  defp normalize_submit_kind(:manual), do: :manual
  defp normalize_submit_kind(:rerun), do: :rerun
  defp normalize_submit_kind(:pipeline), do: :pipeline
  defp normalize_submit_kind(:backfill_asset), do: :backfill_asset
  defp normalize_submit_kind(:backfill_pipeline), do: :backfill_pipeline
  defp normalize_submit_kind(_value), do: :manual

  defp normalize_optional_string(value) when is_binary(value) and value != "", do: value
  defp normalize_optional_string(_value), do: nil
end
