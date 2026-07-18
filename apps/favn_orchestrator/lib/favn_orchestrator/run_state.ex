defmodule FavnOrchestrator.RunState do
  @moduledoc """
  Persisted run snapshot owned by the orchestrator control plane.
  """

  @default_timeout_ms 30 * 60 * 1000
  @terminal_statuses [:ok, :partial, :error, :cancelled, :timed_out]
  @terminal_status_strings Enum.map(@terminal_statuses, &Atom.to_string/1)
  @terminal_event_types [:run_finished, :run_failed, :run_cancelled, :run_timed_out]
  @terminal_event_type_strings Enum.map(@terminal_event_types, &Atom.to_string/1)

  @type status :: :pending | :running | :ok | :partial | :error | :cancelled | :timed_out

  @type t :: %__MODULE__{
          id: String.t(),
          workspace_id: String.t() | nil,
          deployment_id: String.t() | nil,
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
          updated_at: DateTime.t() | nil,
          storage_owner_id: String.t() | nil,
          storage_fencing_token: pos_integer() | nil
        }

  defstruct [
    :id,
    :workspace_id,
    :deployment_id,
    :manifest_version_id,
    :manifest_content_hash,
    :asset_ref,
    :plan,
    :snapshot_hash,
    :inserted_at,
    :updated_at,
    :storage_owner_id,
    :storage_fencing_token,
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
      workspace_id: Keyword.get(opts, :workspace_id),
      deployment_id: Keyword.get(opts, :deployment_id),
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

  @doc "Returns true when a persisted run snapshot has been finalized."
  @spec finalized?(t()) :: boolean()
  def finalized?(%__MODULE__{metadata: metadata}), do: finalized_metadata?(metadata)

  @doc "Returns true when the run can still admit execution work."
  @spec execution_admissible?(t()) :: boolean()
  def execution_admissible?(%__MODULE__{} = run), do: not finalized?(run)

  @doc "Returns true when the status represents terminal persisted run state."
  @spec terminal_status?(status() | term()) :: boolean()
  def terminal_status?(status) when is_atom(status), do: status in @terminal_statuses
  def terminal_status?(status) when is_binary(status), do: status in @terminal_status_strings
  def terminal_status?(_status), do: false

  @doc "Returns the persisted terminal event type for a terminal run status."
  @spec terminal_event_type(status() | term()) :: atom() | nil
  def terminal_event_type(:ok), do: :run_finished
  def terminal_event_type("ok"), do: :run_finished
  def terminal_event_type(:cancelled), do: :run_cancelled
  def terminal_event_type("cancelled"), do: :run_cancelled
  def terminal_event_type(:timed_out), do: :run_timed_out
  def terminal_event_type("timed_out"), do: :run_timed_out
  def terminal_event_type(:partial), do: :run_failed
  def terminal_event_type("partial"), do: :run_failed
  def terminal_event_type(:error), do: :run_failed
  def terminal_event_type("error"), do: :run_failed
  def terminal_event_type(_status), do: nil

  defp finalized_metadata?(metadata) when is_map(metadata) do
    terminal_event_type =
      Map.get(metadata, :terminal_event_type) || Map.get(metadata, "terminal_event_type")

    cancelled? = Map.get(metadata, :cancelled) || Map.get(metadata, "cancelled")

    terminal_event_type?(terminal_event_type) or cancelled? == true
  end

  defp finalized_metadata?(_metadata), do: false

  defp terminal_event_type?(value) when is_atom(value), do: value in @terminal_event_types

  defp terminal_event_type?(value) when is_binary(value),
    do: value in @terminal_event_type_strings

  defp terminal_event_type?(_value), do: false

  @spec transition(t(), keyword()) :: t()
  def transition(%__MODULE__{} = run, attrs) when is_list(attrs) do
    transition(run, attrs, DateTime.utc_now())
  end

  @doc false
  @spec transition(t(), keyword(), DateTime.t()) :: t()
  def transition(%__MODULE__{} = run, attrs, %DateTime{} = occurred_at) when is_list(attrs) do
    run
    |> Map.merge(Enum.into(attrs, %{}))
    |> Map.put(:event_seq, run.event_seq + 1)
    |> Map.put(:updated_at, occurred_at)
    |> with_snapshot_hash()
  end

  @doc false
  @spec with_storage_fence(t(), String.t(), pos_integer()) :: t()
  def with_storage_fence(%__MODULE__{} = run, owner_id, fencing_token)
      when is_binary(owner_id) and owner_id != "" and is_integer(fencing_token) and
             fencing_token > 0 do
    %{run | storage_owner_id: owner_id, storage_fencing_token: fencing_token}
  end

  @doc false
  @spec without_storage_fence(t()) :: t()
  def without_storage_fence(%__MODULE__{} = run) do
    %{run | storage_owner_id: nil, storage_fencing_token: nil}
  end

  @spec with_snapshot_hash(t()) :: t()
  def with_snapshot_hash(%__MODULE__{} = run) do
    payload =
      run
      |> Map.from_struct()
      |> Map.delete(:snapshot_hash)
      |> Map.delete(:storage_owner_id)
      |> Map.delete(:storage_fencing_token)

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
