defmodule FavnOrchestrator.RunSubmission.Worker do
  @moduledoc false

  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Persistence.Commands.ClaimRunSubmissions
  alias FavnOrchestrator.Persistence.Commands.ClaimStaleRunSubmissions
  alias FavnOrchestrator.Persistence.Commands.RenewRunSubmissionClaim
  alias FavnOrchestrator.Persistence.Results.RunSubmission
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunSubmission.Processor

  @spec run(String.t(), keyword()) :: term()
  def run(workspace_id, opts) when is_binary(workspace_id) and is_list(opts) do
    lifecycle = Keyword.get(opts, :lifecycle, Lifecycle)

    Lifecycle.with_admission(
      fn -> claim_and_process(workspace_id, opts) end,
      lifecycle
    )
  end

  defp claim_and_process(workspace_id, opts) do
    context = SystemContext.workspace(workspace_id, :run_submission_worker)

    with {:ok, claimed} <- claim(context, opts) do
      case claimed do
        %RunSubmission{} = submission -> process_with_renewal(submission, opts)
        nil -> :empty
      end
    end
  end

  defp claim(context, opts) do
    store = Keyword.fetch!(opts, :store)
    now = now(opts)
    common = claim_fields(context, opts, now)

    stale =
      struct!(
        ClaimStaleRunSubmissions,
        Keyword.put(common, :command_id, command_id("claim-stale", opts, 0))
      )

    case store.claim_stale(stale) do
      {:ok, [%RunSubmission{} = submission]} ->
        {:ok, submission}

      {:ok, []} ->
        queued =
          struct!(
            ClaimRunSubmissions,
            Keyword.put(common, :command_id, command_id("claim", opts, 0))
          )

        case store.claim(queued) do
          {:ok, [%RunSubmission{} = submission]} -> {:ok, submission}
          {:ok, []} -> {:ok, nil}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp claim_fields(context, opts, occurred_at) do
    [
      workspace_context: context,
      owner_id: Keyword.fetch!(opts, :owner_id),
      lease_duration_ms: Keyword.fetch!(opts, :lease_duration_ms),
      occurred_at: occurred_at,
      limit: 1
    ]
  end

  defp process_with_renewal(submission, opts) do
    processor = Keyword.get(opts, :processor, Processor)
    parent = self()
    previous_trap_exit = Process.flag(:trap_exit, true)

    try do
      {pid, monitor} =
        :erlang.spawn_opt(
          fn ->
            send(
              parent,
              {:run_submission_processor_result, self(), processor.process(submission, opts)}
            )
          end,
          [:link, :monitor]
        )

      await_processing(pid, monitor, submission, opts, 1)
    after
      Process.flag(:trap_exit, previous_trap_exit)
    end
  end

  defp await_processing(pid, monitor, submission, opts, renewal_sequence) do
    receive do
      {:run_submission_processor_result, ^pid, result} ->
        Process.unlink(pid)
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        Process.unlink(pid)
        {:error, {:run_submission_processor_exit, reason}}

      {:EXIT, ^pid, :normal} ->
        await_processing(pid, monitor, submission, opts, renewal_sequence)

      {:EXIT, ^pid, reason} ->
        Process.demonitor(monitor, [:flush])
        {:error, {:run_submission_processor_exit, reason}}

      {:EXIT, _linked_process, :normal} ->
        await_processing(pid, monitor, submission, opts, renewal_sequence)

      {:EXIT, _linked_process, reason} ->
        stop_processor(pid, monitor)
        exit(reason)
    after
      Keyword.fetch!(opts, :renewal_interval_ms) ->
        case renew(submission, opts, renewal_sequence) do
          {:ok, %RunSubmission{cancellation_requested_at: requested_at} = renewed}
          when not is_nil(requested_at) and renewed.status in [:preparing, :admitting] ->
            stop_processor(pid, monitor)
            processor = Keyword.get(opts, :processor, Processor)
            processor.process(renewed, opts)

          {:ok, %RunSubmission{} = renewed} ->
            await_processing(pid, monitor, renewed, opts, renewal_sequence + 1)

          {:error, reason} ->
            stop_processor(pid, monitor)
            {:error, {:run_submission_claim_lost, reason}}
        end
    end
  end

  defp stop_processor(pid, monitor) do
    Process.exit(pid, :kill)

    receive do
      {:EXIT, ^pid, _reason} -> await_down(pid, monitor)
      {:DOWN, ^monitor, :process, ^pid, _reason} -> :ok
    after
      1_000 -> Process.demonitor(monitor, [:flush])
    end
  end

  defp await_down(pid, monitor) do
    receive do
      {:DOWN, ^monitor, :process, ^pid, _reason} -> :ok
    after
      1_000 -> Process.demonitor(monitor, [:flush])
    end
  end

  defp renew(submission, opts, renewal_sequence) do
    Keyword.fetch!(opts, :store).renew(%RenewRunSubmissionClaim{
      workspace_context: SystemContext.workspace(submission.workspace_id, :run_submission_worker),
      command_id: command_id("renew", opts, renewal_sequence),
      submission_id: submission.submission_id,
      owner_id: submission.claim_owner,
      claim_generation: submission.claim_generation,
      lease_duration_ms: Keyword.fetch!(opts, :lease_duration_ms),
      occurred_at: now(opts)
    })
  end

  defp command_id(operation, opts, sequence) do
    digest =
      {Keyword.fetch!(opts, :owner_id), operation, sequence}
      |> :erlang.term_to_binary([:deterministic])
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.url_encode64(padding: false)

    "run-submission-worker:#{operation}:#{digest}"
  end

  defp now(opts) do
    case Keyword.get(opts, :clock) do
      fun when is_function(fun, 0) -> fun.()
      _default -> DateTime.utc_now()
    end
  end
end
