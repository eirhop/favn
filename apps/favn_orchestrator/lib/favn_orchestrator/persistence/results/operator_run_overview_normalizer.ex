defmodule FavnOrchestrator.Persistence.Results.OperatorRunOverviewNormalizer do
  @moduledoc false

  alias FavnOrchestrator.Persistence.Results.AssetAttemptOverview
  alias FavnOrchestrator.Persistence.Results.BackfillWindow
  alias FavnOrchestrator.Persistence.Results.ExecutionGroupOverview
  alias FavnOrchestrator.Persistence.Results.OperatorRunOverview
  alias FavnOrchestrator.Persistence.Results.RunSummary

  @enforce_keys [
    :overview,
    :root_run,
    :runs,
    :requested_windows,
    :requested_windows_truncated?,
    :requested_window_counts,
    :attempts,
    :attempt_counts,
    :attempts_truncated?,
    :runs_truncated?,
    :target_refs
  ]
  defstruct @enforce_keys

  @type normalized_run :: %{
          required(:run_id) => String.t(),
          required(:root_run_id) => String.t() | nil,
          required(:parent_run_id) => String.t() | nil,
          required(:manifest_version_id) => String.t(),
          required(:runner_releases) => Favn.RunnerPool.releases(),
          required(:status) => FavnOrchestrator.RunState.status(),
          required(:submit_kind) => FavnOrchestrator.Persistence.RunEnum.submit_kind(),
          required(:trigger_type) => FavnOrchestrator.Persistence.RunEnum.trigger_type() | nil,
          required(:event_sequence) => pos_integer(),
          required(:inserted_at) => DateTime.t(),
          required(:updated_at) => DateTime.t(),
          required(:terminal_at) => DateTime.t() | nil,
          required(:rerun_of_run_id) => String.t() | nil
        }

  @type normalized_window :: %{
          required(:window_key) => String.t(),
          required(:window_start) => DateTime.t(),
          required(:window_end) => DateTime.t(),
          required(:status) => BackfillWindow.status(),
          required(:run_id) => String.t() | nil,
          required(:attempt_count) => non_neg_integer(),
          required(:last_error) => map() | nil,
          required(:payload) => map()
        }

  @type normalized_attempt :: %{
          required(:root_run_id) => String.t(),
          required(:run_id) => String.t(),
          required(:asset_step_id) => String.t(),
          required(:asset_ref) => String.t(),
          required(:window) => map() | nil,
          required(:status) => FavnOrchestrator.ExecutionStatus.known(),
          required(:stage) => non_neg_integer() | nil,
          required(:attempt_number) => pos_integer() | nil,
          required(:execution_pool) => String.t() | nil,
          required(:queue_reason) => String.t() | nil,
          required(:started_at) => DateTime.t() | nil,
          required(:finished_at) => DateTime.t() | nil,
          required(:duration_ms) => non_neg_integer() | nil,
          required(:error) => term(),
          required(:output_metadata) => map() | nil
        }

  @type normalized_t :: %__MODULE__{
          overview: %{
            status: :pending | :running | :succeeded | :failed,
            started_at: DateTime.t() | nil,
            finished_at: DateTime.t() | nil,
            updated_at: DateTime.t()
          },
          root_run: normalized_run(),
          runs: [normalized_run()],
          requested_windows: [normalized_window()],
          requested_windows_truncated?: boolean(),
          requested_window_counts: %{
            total: non_neg_integer(),
            completed: non_neg_integer(),
            failed: non_neg_integer()
          },
          attempts: [normalized_attempt()],
          attempt_counts: %{
            total: non_neg_integer(),
            completed: non_neg_integer(),
            succeeded: non_neg_integer(),
            skipped: non_neg_integer(),
            failed: non_neg_integer(),
            running: non_neg_integer(),
            queued: non_neg_integer(),
            planned: non_neg_integer(),
            effective_windows: non_neg_integer()
          },
          attempts_truncated?: boolean(),
          runs_truncated?: boolean(),
          target_refs: [String.t()]
        }

  @doc false
  @spec normalize_operator_overview(OperatorRunOverview.t()) ::
          {:ok, normalized_t()} | {:error, :invalid_operator_run_overview}
  def normalize_operator_overview(%OperatorRunOverview{} = overview) do
    with {:ok, normalized_overview} <- normalize_overview(overview.overview),
         {:ok, root_run} <- normalize_run(overview.root_run),
         {:ok, runs} <- normalize_runs(overview.runs),
         {:ok, requested_windows} <- normalize_windows(overview.requested_windows),
         {:ok, requested_window_counts} <-
           normalize_requested_window_counts(overview.requested_window_counts),
         {:ok, attempts} <- normalize_attempts(overview.attempts),
         {:ok, attempt_counts} <- normalize_attempt_counts(overview.attempt_counts),
         true <- is_boolean(overview.requested_windows_truncated?),
         true <- is_boolean(overview.attempts_truncated?),
         true <- is_boolean(overview.runs_truncated?),
         {:ok, target_refs} <- normalize_binary_list(overview.target_refs) do
      {:ok,
       %__MODULE__{
         overview: normalized_overview,
         root_run: root_run,
         runs: runs,
         requested_windows: requested_windows,
         requested_windows_truncated?: overview.requested_windows_truncated?,
         requested_window_counts: requested_window_counts,
         attempts: attempts,
         attempt_counts: attempt_counts,
         attempts_truncated?: overview.attempts_truncated?,
         runs_truncated?: overview.runs_truncated?,
         target_refs: target_refs
       }}
    else
      _ -> {:error, :invalid_operator_run_overview}
    end
  end

  defp normalize_overview(%ExecutionGroupOverview{
         status: status,
         started_at: started_at,
         finished_at: finished_at,
         updated_at: %DateTime{} = updated_at
       })
       when status in [:pending, :running, :succeeded, :failed] do
    with {:ok, started_at} <- optional_datetime(started_at),
         {:ok, finished_at} <- optional_datetime(finished_at) do
      {:ok,
       %{
         status: status,
         started_at: started_at,
         finished_at: finished_at,
         updated_at: updated_at
       }}
    end
  end

  defp normalize_overview(_overview), do: {:error, :invalid_operator_run_overview}

  defp normalize_run(%RunSummary{
         run_id: run_id,
         root_run_id: root_run_id,
         parent_run_id: parent_run_id,
         manifest_version_id: manifest_version_id,
         runner_releases: runner_releases,
         status: status,
         submit_kind: submit_kind,
         trigger_type: trigger_type,
         event_sequence: event_sequence,
         inserted_at: %DateTime{} = inserted_at,
         updated_at: %DateTime{} = updated_at,
         terminal_at: terminal_at,
         rerun_of_run_id: rerun_of_run_id
       }) do
    with {:ok, run_id} <- binary(run_id),
         {:ok, root_run_id} <- optional_binary(root_run_id),
         {:ok, parent_run_id} <- optional_binary(parent_run_id),
         {:ok, manifest_version_id} <- binary(manifest_version_id),
         {:ok, runner_releases} <- normalize_runner_releases(runner_releases),
         true <- status in [:pending, :running, :ok, :partial, :error, :cancelled, :timed_out],
         true <- submit_kind in [:manual, :rerun, :pipeline, :backfill_asset, :backfill_pipeline],
         true <-
           is_nil(trigger_type) or
             trigger_type in [
               :manual,
               :pipeline,
               :rerun,
               :retry,
               :backfill,
               :schedule,
               :resource_recovery
             ],
         true <- is_integer(event_sequence) and event_sequence > 0,
         {:ok, terminal_at} <- optional_datetime(terminal_at),
         {:ok, rerun_of_run_id} <- optional_binary(rerun_of_run_id) do
      {:ok,
       %{
         run_id: run_id,
         root_run_id: root_run_id,
         parent_run_id: parent_run_id,
         manifest_version_id: manifest_version_id,
         runner_releases: runner_releases,
         status: status,
         submit_kind: submit_kind,
         trigger_type: trigger_type,
         event_sequence: event_sequence,
         inserted_at: inserted_at,
         updated_at: updated_at,
         terminal_at: terminal_at,
         rerun_of_run_id: rerun_of_run_id
       }}
    else
      _ -> {:error, :invalid_operator_run_overview}
    end
  end

  defp normalize_run(_run), do: {:error, :invalid_operator_run_overview}

  defp normalize_window(%BackfillWindow{
         window_key: window_key,
         window_start: %DateTime{} = window_start,
         window_end: %DateTime{} = window_end,
         status: status,
         run_id: run_id,
         attempt_count: attempt_count,
         last_error: last_error,
         payload: payload
       })
       when status in [
              :planned,
              :ready,
              :claimed,
              :running,
              :succeeded,
              :failed,
              :cancelled
            ] do
    with {:ok, window_key} <- binary(window_key),
         {:ok, run_id} <- optional_binary(run_id),
         true <- is_integer(attempt_count) and attempt_count >= 0,
         true <- is_nil(last_error) or is_map(last_error),
         true <- is_map(payload) do
      {:ok,
       %{
         window_key: window_key,
         window_start: window_start,
         window_end: window_end,
         status: status,
         run_id: run_id,
         attempt_count: attempt_count,
         last_error: last_error,
         payload: payload
       }}
    else
      _ -> {:error, :invalid_operator_run_overview}
    end
  end

  defp normalize_window(_window), do: {:error, :invalid_operator_run_overview}

  defp normalize_attempt(%AssetAttemptOverview{
         root_run_id: root_run_id,
         run_id: run_id,
         asset_step_id: asset_step_id,
         asset_ref: asset_ref,
         window: window,
         status: status,
         stage: stage,
         attempt_number: attempt_number,
         execution_pool: execution_pool,
         queue_reason: queue_reason,
         started_at: started_at,
         finished_at: finished_at,
         duration_ms: duration_ms,
         error: error,
         output_metadata: output_metadata
       }) do
    with {:ok, root_run_id} <- binary(root_run_id),
         {:ok, run_id} <- binary(run_id),
         {:ok, asset_step_id} <- binary(asset_step_id),
         {:ok, asset_ref} <- binary(asset_ref),
         true <- is_nil(window) or is_map(window),
         true <-
           status in [
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
           ],
         true <- is_nil(stage) or (is_integer(stage) and stage >= 0),
         true <-
           is_nil(attempt_number) or (is_integer(attempt_number) and attempt_number > 0),
         true <- is_nil(execution_pool) or is_binary(execution_pool),
         true <- is_nil(queue_reason) or is_binary(queue_reason),
         {:ok, started_at} <- optional_datetime(started_at),
         {:ok, finished_at} <- optional_datetime(finished_at),
         true <-
           is_nil(duration_ms) or (is_integer(duration_ms) and duration_ms >= 0),
         true <- is_nil(output_metadata) or is_map(output_metadata) do
      {:ok,
       %{
         root_run_id: root_run_id,
         run_id: run_id,
         asset_step_id: asset_step_id,
         asset_ref: asset_ref,
         window: window,
         status: status,
         stage: stage,
         attempt_number: attempt_number,
         execution_pool: execution_pool,
         queue_reason: queue_reason,
         started_at: started_at,
         finished_at: finished_at,
         duration_ms: duration_ms,
         error: error,
         output_metadata: output_metadata
       }}
    else
      _ -> {:error, :invalid_operator_run_overview}
    end
  end

  @spec normalize_runs([RunSummary.t()]) ::
          {:ok, [normalized_run()]} | {:error, :invalid_operator_run_overview}
  defp normalize_runs([]), do: {:ok, []}

  defp normalize_runs([run | runs]) do
    with {:ok, normalized} <- normalize_run(run),
         {:ok, rest} <- normalize_runs(runs) do
      {:ok, [normalized | rest]}
    end
  end

  defp normalize_runs(_runs), do: {:error, :invalid_operator_run_overview}

  @spec normalize_windows([BackfillWindow.t()]) ::
          {:ok, [normalized_window()]} | {:error, :invalid_operator_run_overview}
  defp normalize_windows([]), do: {:ok, []}

  defp normalize_windows([window | windows]) do
    with {:ok, normalized} <- normalize_window(window),
         {:ok, rest} <- normalize_windows(windows) do
      {:ok, [normalized | rest]}
    end
  end

  defp normalize_windows(_windows), do: {:error, :invalid_operator_run_overview}

  @spec normalize_attempts([AssetAttemptOverview.t()]) ::
          {:ok, [normalized_attempt()]} | {:error, :invalid_operator_run_overview}
  defp normalize_attempts([]), do: {:ok, []}

  defp normalize_attempts([attempt | attempts]) do
    with {:ok, normalized} <- normalize_attempt(attempt),
         {:ok, rest} <- normalize_attempts(attempts) do
      {:ok, [normalized | rest]}
    end
  end

  defp normalize_attempts(_attempts), do: {:error, :invalid_operator_run_overview}

  defp normalize_requested_window_counts(%{total: total, completed: completed, failed: failed})
       when is_integer(total) and total >= 0 and is_integer(completed) and completed >= 0 and
              is_integer(failed) and failed >= 0,
       do: {:ok, %{total: total, completed: completed, failed: failed}}

  defp normalize_requested_window_counts(_counts), do: {:error, :invalid_operator_run_overview}

  defp normalize_attempt_counts(%{
         total: total,
         completed: completed,
         succeeded: succeeded,
         skipped: skipped,
         failed: failed,
         running: running,
         queued: queued,
         planned: planned,
         effective_windows: effective_windows
       })
       when is_integer(total) and total >= 0 and is_integer(completed) and completed >= 0 and
              is_integer(succeeded) and succeeded >= 0 and is_integer(skipped) and skipped >= 0 and
              is_integer(failed) and failed >= 0 and is_integer(running) and running >= 0 and
              is_integer(queued) and queued >= 0 and is_integer(planned) and planned >= 0 and
              is_integer(effective_windows) and effective_windows >= 0,
       do:
         {:ok,
          %{
            total: total,
            completed: completed,
            succeeded: succeeded,
            skipped: skipped,
            failed: failed,
            running: running,
            queued: queued,
            planned: planned,
            effective_windows: effective_windows
          }}

  defp normalize_attempt_counts(_counts), do: {:error, :invalid_operator_run_overview}

  defp normalize_runner_releases(releases) when is_map(releases) do
    releases
    |> Map.to_list()
    |> normalize_runner_release_pairs()
  end

  defp normalize_runner_releases(_releases), do: {:error, :invalid_operator_run_overview}

  defp normalize_runner_release_pairs([]), do: {:ok, %{}}

  defp normalize_runner_release_pairs([{pool, release_id} | pairs]) do
    with :ok <- Favn.RunnerPool.validate_runtime(pool),
         :ok <- Favn.RunnerRelease.validate_id(release_id),
         {:ok, rest} <- normalize_runner_release_pairs(pairs) do
      {:ok, Map.put(rest, pool, release_id)}
    else
      _ -> {:error, :invalid_operator_run_overview}
    end
  end

  defp normalize_binary_list([]), do: {:ok, []}

  defp normalize_binary_list([value | values]) do
    with {:ok, binary} <- binary(value),
         {:ok, rest} <- normalize_binary_list(values) do
      {:ok, [binary | rest]}
    end
  end

  defp normalize_binary_list(_values), do: {:error, :invalid_operator_run_overview}
  defp binary(value) when is_binary(value), do: {:ok, value}
  defp binary(_value), do: {:error, :invalid_operator_run_overview}
  defp optional_binary(nil), do: {:ok, nil}
  defp optional_binary(value), do: binary(value)
  defp optional_datetime(nil), do: {:ok, nil}
  defp optional_datetime(%DateTime{} = value), do: {:ok, value}
  defp optional_datetime(_value), do: {:error, :invalid_operator_run_overview}
end
