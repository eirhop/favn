defmodule FavnOrchestrator.OperatorRunView do
  @moduledoc """
  Purpose-built, bounded operator reads for one exact run.

  Flow contains only the row fields rendered by the run page. Window choices,
  events, and complete asset-attempt diagnostics are separate lazy reads.
  """

  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetRunAssetAttempt
  alias FavnOrchestrator.Persistence.Queries.GetRunFlow
  alias FavnOrchestrator.OperationCancellation
  alias FavnOrchestrator.Persistence.Queries.GetRunHeader
  alias FavnOrchestrator.Persistence.Queries.ListRunEventSummaries
  alias FavnOrchestrator.Persistence.Queries.ListRunWindows
  alias FavnOrchestrator.Persistence.Results.RunAssetAttempt
  alias FavnOrchestrator.Persistence.Results.RunFlowCandidate
  alias FavnOrchestrator.Persistence.Results.RunFlowSnapshot
  alias FavnOrchestrator.Persistence.Results.RunViewHeader
  alias FavnOrchestrator.Persistence.Results.RunWindowChoices
  alias FavnOrchestrator.Persistence.Results.RunSubmission
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @max_assets 1_000
  @max_events 200
  @max_display_name_bytes 256
  @active_submission_statuses [:queued, :preparing, :admitting, :submitted]

  defmodule Header do
    @moduledoc "Exact-run state and aggregate values rendered above a run section."
    @enforce_keys [:run_id, :root_run_id, :status, :counts, :active?]
    defstruct [
      :run_id,
      :root_run_id,
      :parent_run_id,
      :rerun_of_run_id,
      :manifest_version_id,
      :status,
      :submit_kind,
      :trigger_type,
      :event_sequence,
      :started_at,
      :updated_at,
      :finished_at,
      :target_id,
      :target_label,
      :window_start_at,
      :window_end_at,
      :error_code,
      :error_message,
      :counts,
      :active?,
      :cancellation,
      :cancellable?,
      :retry_remaining?
    ]

    @type t :: %__MODULE__{}
  end

  defmodule Asset do
    @moduledoc """
    Lean Flow row for one asset attempt. Full diagnostics are not present.

    `stage` is dependency depth as recorded for the attempt, not execution
    order: two assets in the same stage may still run at different times. It is
    `nil` for an attempt that predates stage persistence.
    """
    @enforce_keys [:id, :run_id, :name, :asset_ref, :state]
    defstruct @enforce_keys ++ [:started_at, :finished_at, :stage]

    @type t :: %__MODULE__{}
  end

  defmodule Flow do
    @moduledoc "One exact run's header and at most 1,000 lean asset attempts."
    @enforce_keys [:header, :assets, :overflow?]
    defstruct @enforce_keys

    @type t :: %__MODULE__{header: Header.t(), assets: [Asset.t()], overflow?: boolean()}
  end

  @doc "Returns one exact run's Flow data under an authorized workspace."
  @spec flow(WorkspaceContext.t(), String.t()) :: {:ok, Flow.t()} | {:error, atom()}
  def flow(%WorkspaceContext{} = context, run_id) when is_binary(run_id) do
    case Persistence.stores().operator_reads.get_run_flow(%GetRunFlow{
           workspace_context: context,
           run_id: run_id,
           limit: @max_assets
         }) do
      {:ok, %RunFlowSnapshot{} = snapshot} ->
        with {:ok, scope} <- OperationCancellation.scope(context, run_id) do
          flow = from_snapshot(snapshot)
          {:ok, %{flow | header: with_cancellation(flow.header, scope)}}
        end

      {:error, %Error{kind: kind}} ->
        {:error, kind}
    end
  end

  @doc false
  @spec from_snapshot(RunFlowSnapshot.t()) :: Flow.t()
  def from_snapshot(%RunFlowSnapshot{} = snapshot) do
    assets = public_assets(snapshot.observed)

    %Flow{
      header: public_header(snapshot.header),
      assets: Enum.take(assets, @max_assets),
      overflow?: snapshot.overflow? or length(assets) > @max_assets
    }
  end

  @doc "Returns one exact run's lean header without loading Flow assets."
  @spec header(WorkspaceContext.t(), String.t()) :: {:ok, Header.t()} | {:error, atom()}
  def header(%WorkspaceContext{} = context, run_id) when is_binary(run_id) do
    case Persistence.stores().operator_reads.get_run_header(%GetRunHeader{
           workspace_context: context,
           run_id: run_id
         }) do
      {:ok, %RunViewHeader{} = header} ->
        with {:ok, scope} <- OperationCancellation.scope(context, run_id),
             do: {:ok, with_cancellation(public_header(header), scope)}

      {:error, %Error{kind: kind}} ->
        {:error, kind}
    end
  end

  @doc "Lists at most 1,000 lean window-run choices related to the selected run."
  @spec windows(WorkspaceContext.t(), String.t()) ::
          {:ok, RunWindowChoices.t()} | {:error, atom()}
  def windows(%WorkspaceContext{} = context, run_id) when is_binary(run_id) do
    case Persistence.stores().operator_reads.list_run_windows(%ListRunWindows{
           workspace_context: context,
           run_id: run_id,
           limit: @max_assets
         }) do
      {:ok, %RunWindowChoices{} = choices} -> {:ok, choices}
      {:error, %Error{kind: kind}} -> {:error, kind}
    end
  end

  @doc "Returns one exact observed asset attempt with complete stored detail."
  @spec asset_attempt(WorkspaceContext.t(), String.t(), String.t()) ::
          {:ok, RunAssetAttempt.t()} | {:error, atom()}
  def asset_attempt(%WorkspaceContext{} = context, run_id, asset_step_id)
      when is_binary(run_id) and is_binary(asset_step_id) do
    case Persistence.stores().operator_reads.get_run_asset_attempt(%GetRunAssetAttempt{
           workspace_context: context,
           run_id: run_id,
           asset_step_id: asset_step_id
         }) do
      {:ok, %RunAssetAttempt{} = attempt} -> {:ok, attempt}
      {:error, %Error{kind: kind}} -> {:error, kind}
    end
  end

  @doc "Returns one exact run's bounded event display rows without event payloads."
  @spec events(WorkspaceContext.t(), String.t()) :: {:ok, [map()]} | {:error, atom()}
  def events(%WorkspaceContext{} = context, run_id) when is_binary(run_id) do
    case Persistence.stores().operator_reads.list_run_event_summaries(%ListRunEventSummaries{
           workspace_context: context,
           run_id: run_id,
           limit: @max_events
         }) do
      {:ok, events} -> {:ok, Enum.map(events, &Map.from_struct/1)}
      {:error, %Error{kind: kind}} -> {:error, kind}
    end
  end

  @doc false
  @spec project_submission(RunSubmission.t()) :: map()
  def project_submission(%RunSubmission{} = submission) do
    %{
      run_id: submission.run_id,
      status: submission.status,
      status_label: submission_status_label(submission.status),
      status_tone: submission_status_tone(submission.status),
      active?: submission.status in @active_submission_statuses,
      target_kind: submission.target_kind,
      target_id: submission.target_id,
      attempt: submission.attempt,
      failure_kind: submission.failure_kind,
      enqueued_at: submission.enqueued_at,
      updated_at: submission.updated_at,
      terminal_at: submission.terminal_at,
      failure: submission_failure(submission)
    }
  end

  defp with_cancellation(header, scope),
    do: %{header | cancellation: scope, cancellable?: scope.cancellable?}

  defp public_header(header) do
    active? = header.status in [:pending, :running]

    header
    |> Map.from_struct()
    |> Map.merge(%{
      active?: active?,
      cancellable?: active?,
      retry_remaining?: not active? and header.counts.failed > 0
    })
    |> then(&struct(Header, &1))
  end

  defp submission_failure(%RunSubmission{
         status: :failed,
         error: error,
         failure_kind: failure_kind
       }) do
    values = error_strings(error)

    if Enum.any?(values, &String.contains?(&1, "physical_inspection_unavailable")) do
      %{
        code: "physical_inspection_unavailable",
        title: "Run preparation failed",
        message: "Favn could not inspect a physical relation required by this run.",
        remediation:
          "Check the runner diagnostics and correct the runner connection or driver configuration, then submit the run again.",
        failure_kind: failure_kind
      }
    else
      %{
        code: error_code(error),
        title: "Run request failed",
        message: error_message(error),
        remediation:
          "Review runner diagnostics, correct the reported configuration or connection problem, and submit the run again.",
        failure_kind: failure_kind
      }
    end
  end

  defp submission_failure(_submission), do: nil

  defp error_message(error),
    do: find_key(error, "message") || humanize_error(find_key(error, "reason"))

  defp error_code(error),
    do:
      find_key(error, "code") || find_key(error, "reason_code") || find_key(error, "type") ||
        "run_submission_failed"

  defp find_key(map, wanted) when is_map(map) do
    direct = Map.get(map, wanted) || Map.get(map, String.to_existing_atom(wanted))

    if is_binary(direct),
      do: direct,
      else: Enum.find_value(map, fn {_key, value} -> find_key(value, wanted) end)
  rescue
    ArgumentError -> Enum.find_value(map, fn {_key, value} -> find_key(value, wanted) end)
  end

  defp find_key(list, wanted) when is_list(list), do: Enum.find_value(list, &find_key(&1, wanted))
  defp find_key(_value, _wanted), do: nil

  defp error_strings(map) when is_map(map),
    do: Enum.flat_map(map, fn {key, value} -> [to_string(key) | error_strings(value)] end)

  defp error_strings(list) when is_list(list), do: Enum.flat_map(list, &error_strings/1)
  defp error_strings(value) when is_binary(value), do: [value]
  defp error_strings(value) when is_atom(value), do: [Atom.to_string(value)]
  defp error_strings(_value), do: []

  defp humanize_error(nil), do: "The request failed before run execution started."

  defp humanize_error(value) when is_binary(value) do
    value
    |> String.replace("_", " ")
    |> String.trim()
    |> String.capitalize()
  end

  defp submission_status_label(:queued), do: "Queued"
  defp submission_status_label(:preparing), do: "Preparing"
  defp submission_status_label(:admitting), do: "Starting"
  defp submission_status_label(:submitted), do: "Starting"
  defp submission_status_label(:failed), do: "Failed"
  defp submission_status_label(:cancelled), do: "Cancelled"
  defp submission_status_label(:superseded), do: "Superseded"

  defp submission_status_tone(status) when status in @active_submission_statuses, do: :info
  defp submission_status_tone(:failed), do: :error
  defp submission_status_tone(status) when status in [:cancelled, :superseded], do: :warning

  @doc false
  @spec public_assets([RunFlowCandidate.t()]) :: [Asset.t()]
  def public_assets(observed) do
    observed
    |> Enum.map(&public_asset/1)
    |> Enum.sort_by(&{&1.asset_ref, &1.id})
  end

  defp public_asset(%RunFlowCandidate{} = candidate) do
    %Asset{
      id: candidate.asset_step_id,
      run_id: candidate.run_id,
      name: display_name(candidate.asset_ref),
      asset_ref: candidate.asset_ref,
      state: candidate.status,
      started_at: candidate.started_at,
      finished_at: candidate.finished_at,
      stage: candidate.stage
    }
  end

  defp display_name(asset_ref) do
    asset_ref
    |> String.split(":", parts: 2)
    |> case do
      [module, "asset"] -> module |> String.replace_prefix("Elixir.", "") |> module_name()
      [_module, name] -> name
      [name] -> module_name(name)
    end
    |> truncate_utf8(@max_display_name_bytes)
  end

  defp module_name(value), do: value |> String.split(".") |> List.last()

  defp truncate_utf8(value, max_bytes) when byte_size(value) <= max_bytes, do: value

  defp truncate_utf8(value, max_bytes) do
    value
    |> String.graphemes()
    |> Enum.reduce_while("", fn grapheme, acc ->
      if byte_size(acc) + byte_size(grapheme) <= max_bytes,
        do: {:cont, acc <> grapheme},
        else: {:halt, acc}
    end)
  end
end
