defmodule FavnOrchestrator.OperatorRunActivity do
  @moduledoc """
  Browser-safe read model for a reserved run identity.

  A run submission exists before the run execution projection does. This model
  preserves that distinction so an accepted request never appears missing while
  it is queued, preparing, admitting, or failed before admission.
  """

  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.RunSubmission
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunReadModel
  alias FavnOrchestrator.RunSubmissions

  @active_submission_statuses [:queued, :preparing, :admitting, :submitted]

  @type activity ::
          %{kind: :run, detail: RunReadModel.operator_run_detail()}
          | %{kind: :submission, submission: map()}

  @doc "Returns admitted run detail or the durable pre-admission submission state."
  @spec get(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, activity()} | {:error, term()}
  def get(%WorkspaceContext{} = context, run_id, opts)
      when is_binary(run_id) and is_list(opts) do
    case RunReadModel.get_operator_run_detail(context, run_id, opts) do
      {:ok, detail} ->
        {:ok, %{kind: :run, detail: detail}}

      {:error, reason} when reason == :not_found ->
        get_submission(context, run_id)

      {:error, %Error{kind: :not_found}} ->
        get_submission(context, run_id)

      {:error, _reason} = error ->
        error
    end
  end

  @doc "Returns only the durable pre-admission state for a reserved run identity."
  @spec get_submission(WorkspaceContext.t(), String.t()) ::
          {:ok, activity()} | {:error, term()}
  def get_submission(%WorkspaceContext{} = context, run_id) when is_binary(run_id) do
    case RunSubmissions.get(context, run_id) do
      {:ok, %RunSubmission{} = submission} ->
        {:ok, %{kind: :submission, submission: project_submission(submission)}}

      {:error, %Error{kind: :not_found}} ->
        {:error, :not_found}

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  @spec project_submission(RunSubmission.t()) :: map()
  def project_submission(submission) do
    %{
      run_id: submission.run_id,
      status: submission.status,
      status_label: status_label(submission.status),
      status_tone: status_tone(submission.status),
      active?: submission.status in @active_submission_statuses,
      target_kind: submission.target_kind,
      target_id: submission.target_id,
      attempt: submission.attempt,
      failure_kind: submission.failure_kind,
      enqueued_at: submission.enqueued_at,
      updated_at: submission.updated_at,
      terminal_at: submission.terminal_at,
      failure: failure(submission)
    }
  end

  defp failure(%RunSubmission{status: :failed, error: error, failure_kind: failure_kind}) do
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

  defp failure(_submission), do: nil

  defp error_message(error),
    do: find_key(error, "message") || humanize_error(find_key(error, "reason"))

  defp error_code(error),
    do:
      find_key(error, "code") || find_key(error, "reason_code") || find_key(error, "type") ||
        "run_submission_failed"

  defp find_key(map, wanted) when is_map(map) do
    direct = Map.get(map, wanted) || Map.get(map, String.to_existing_atom(wanted))

    cond do
      is_binary(direct) -> direct
      true -> Enum.find_value(map, fn {_key, value} -> find_key(value, wanted) end)
    end
  rescue
    ArgumentError -> Enum.find_value(map, fn {_key, value} -> find_key(value, wanted) end)
  end

  defp find_key(list, wanted) when is_list(list),
    do: Enum.find_value(list, &find_key(&1, wanted))

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

  defp status_label(:queued), do: "Queued"
  defp status_label(:preparing), do: "Preparing"
  defp status_label(:admitting), do: "Starting"
  defp status_label(:submitted), do: "Starting"
  defp status_label(:failed), do: "Failed"
  defp status_label(:cancelled), do: "Cancelled"
  defp status_label(:superseded), do: "Superseded"

  defp status_tone(status) when status in [:queued, :preparing, :admitting, :submitted], do: :info
  defp status_tone(:failed), do: :error
  defp status_tone(status) when status in [:cancelled, :superseded], do: :warning
end
