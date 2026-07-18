defmodule FavnOrchestrator.Scheduler.PersistenceRuntime do
  @moduledoc """
  Multi-node PostgreSQL scheduler.

  Schedule cursors and occurrence intents are claimed with expiring database
  leases. Every deterministic occurrence is committed before run submission, so
  node crashes and retries cannot create duplicate scheduled runs.
  """

  use GenServer

  require Logger

  alias Favn.Window.Policy
  alias FavnOrchestrator.BoundedDispatcher
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ManifestIndexCache
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.ClaimDueSchedules
  alias FavnOrchestrator.Persistence.Commands.ClaimScheduleOccurrences
  alias FavnOrchestrator.Persistence.Commands.CommitScheduleEvaluation
  alias FavnOrchestrator.Persistence.Commands.CompleteScheduleOccurrence
  alias FavnOrchestrator.Persistence.Commands.ScheduleOccurrenceIntent
  alias FavnOrchestrator.Persistence.Results.ScheduleClaim
  alias FavnOrchestrator.Persistence.Results.ScheduleOccurrence
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.Persistence.TargetIdentity
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.Scheduler.Cron
  alias FavnOrchestrator.Scheduler.ManifestEntries
  alias FavnOrchestrator.Storage.JsonSafe

  @lease_duration_ms 30_000
  @default_tick_ms 15_000
  @max_batch 100
  @max_occurrences 500

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    name = Keyword.get(opts, :name, FavnOrchestrator.Scheduler.Runtime)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    workspace_ids = Keyword.get(opts, :workspace_ids, [])

    if workspace_ids == [] do
      {:stop, :scheduler_workspace_ids_required}
    else
      state = %{
        workspace_ids: workspace_ids,
        owner_id: owner_id(),
        tick_ms: Keyword.get(opts, :tick_ms, @default_tick_ms),
        auto_tick?: Keyword.get(opts, :auto_tick?, true),
        last_error: nil,
        last_tick_at: nil,
        claimed_schedules: 0,
        claimed_occurrences: 0
      }

      schedule_tick(state)
      {:ok, state}
    end
  end

  @impl true
  def handle_call(:tick, _from, state) do
    case tick(state) do
      {:ok, next} -> {:reply, :ok, next}
      {:error, reason, next} -> {:reply, {:error, reason}, next}
    end
  end

  def handle_call(:reload, _from, state), do: {:reply, :ok, state}
  def handle_call(:scheduled, _from, state), do: {:reply, [], state}
  def handle_call(:inspect_entries, _from, state), do: {:reply, [], state}

  def handle_call(:diagnostics, _from, state) do
    {:reply,
     {:ok,
      %{
        backend: :postgres,
        workspace_count: length(state.workspace_ids),
        owner_id: state.owner_id,
        last_tick_at: state.last_tick_at,
        last_error: state.last_error,
        claimed_schedules: state.claimed_schedules,
        claimed_occurrences: state.claimed_occurrences
      }}, state}
  end

  @impl true
  def handle_info(:tick, state) do
    next =
      case tick(state) do
        {:ok, next} ->
          next

        {:error, reason, next} ->
          Logger.error(
            "PostgreSQL scheduler tick failed reason=#{inspect(JsonSafe.error(reason))}"
          )

          next
      end

    schedule_tick(next)
    {:noreply, next}
  end

  defp tick(state) do
    result =
      Enum.reduce_while(state.workspace_ids, {:ok, state}, fn workspace_id, {:ok, acc} ->
        case tick_workspace(workspace_id, acc) do
          {:ok, next} -> {:cont, {:ok, next}}
          {:error, reason, next} -> {:halt, {:error, reason, next}}
        end
      end)

    case result do
      {:ok, next} ->
        {:ok, %{next | last_tick_at: DateTime.utc_now(), last_error: nil}}

      {:error, reason, next} ->
        {:error, reason,
         %{next | last_tick_at: DateTime.utc_now(), last_error: JsonSafe.error(reason)}}
    end
  end

  defp tick_workspace(workspace_id, state) do
    context = SystemContext.workspace(workspace_id, :scheduler)

    with {:ok, entries} <- workspace_entries(context),
         {:ok, claims} <- claim_schedules(context, state.owner_id),
         :ok <- evaluate_claims(context, entries, claims),
         {:ok, occurrences} <- claim_occurrences(context, state.owner_id),
         :ok <- dispatch_occurrences(context, entries, occurrences) do
      {:ok,
       %{
         state
         | claimed_schedules: state.claimed_schedules + length(claims),
           claimed_occurrences: state.claimed_occurrences + length(occurrences)
       }}
    else
      {:error, reason} -> {:error, {workspace_id, reason}, state}
    end
  end

  defp workspace_entries(context) do
    with {:ok, version} <- ManifestStore.get_active_manifest(context),
         {:ok, index} <- ManifestIndexCache.fetch(version),
         {:ok, entries} <- ManifestEntries.discover_all(version, index) do
      {:ok,
       Map.new(entries, fn entry ->
         key =
           {TargetIdentity.for_pipeline({entry.module, entry.id}), to_string(entry.schedule.name)}

         {key, entry}
       end)}
    end
  end

  defp claim_schedules(context, owner_id) do
    Persistence.stores().scheduler.claim_due_schedules(%ClaimDueSchedules{
      workspace_context: context,
      batch_id: unique_id("schedule-claim"),
      owner_id: owner_id,
      lease_duration_ms: @lease_duration_ms,
      limit: @max_batch
    })
  end

  defp evaluate_claims(context, entries, claims) do
    Enum.reduce_while(claims, :ok, fn claim, :ok ->
      case Map.fetch(entries, {claim.pipeline_target_id, claim.schedule_id}) do
        {:ok, entry} ->
          case commit_evaluation(context, entry, claim) do
            {:ok, _occurrences} -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, reason}}
          end

        :error ->
          {:halt, {:error, {:deployed_schedule_not_in_manifest, claim.pipeline_target_id}}}
      end
    end)
  end

  defp commit_evaluation(context, entry, %ScheduleClaim{} = claim) do
    now = DateTime.utc_now()
    cursor = reconcile_in_flight(context, claim.cursor || %{})
    {due_times, next_due_at, next_cursor} = evaluation(entry, claim, cursor, now)

    intents =
      Enum.map(due_times, fn due_at ->
        occurrence_key = occurrence_key(entry, due_at)

        %ScheduleOccurrenceIntent{
          occurrence_id: occurrence_id(context.workspace_id, occurrence_key),
          due_at: due_at,
          payload: %{
            "occurrence_key" => occurrence_key,
            "run_id" => scheduled_run_id(occurrence_key),
            "recovery" => if(DateTime.compare(due_at, now) == :lt, do: "missed", else: "on_time")
          }
        }
      end)

    Persistence.stores().scheduler.commit_evaluation(%CommitScheduleEvaluation{
      workspace_context: context,
      command_id:
        command_id("schedule-evaluate", claim.pipeline_target_id, claim.claim_generation),
      deployment_id: claim.deployment_id,
      pipeline_target_id: claim.pipeline_target_id,
      schedule_id: claim.schedule_id,
      owner_id: claim.owner_id,
      claim_generation: claim.claim_generation,
      expected_version: claim.version,
      next_due_at: next_due_at,
      cursor: next_cursor,
      occurrences: intents,
      occurred_at: now
    })
  end

  defp evaluation(entry, claim, cursor, now) do
    latest_due = Cron.latest_due(entry.schedule.cron, entry.schedule.timezone, now)
    active_run_id = cursor["in_flight_run_id"]
    queued_due = parse_datetime(cursor["queued_due_at"])

    cond do
      is_binary(active_run_id) and entry.schedule.overlap == :forbid ->
        {[], claim.next_due_at, cursor}

      is_binary(active_run_id) and entry.schedule.overlap == :queue_one ->
        queued = latest_due || queued_due || claim.next_due_at
        {[], now, Map.put(cursor, "queued_due_at", DateTime.to_iso8601(queued))}

      match?(%DateTime{}, queued_due) ->
        tracked_evaluation(entry, [queued_due], cursor, now)

      is_nil(latest_due) ->
        {[], DateTime.add(now, state_tick_floor_ms(), :millisecond), cursor}

      true ->
        due_times = selected_occurrences(entry, claim.next_due_at, latest_due)

        case entry.schedule.overlap do
          :allow -> allow_evaluation(entry, due_times, cursor, latest_due)
          :forbid -> tracked_evaluation(entry, Enum.take(due_times, 1), cursor, latest_due)
          :queue_one -> queue_one_evaluation(entry, due_times, cursor, latest_due, now)
        end
    end
  end

  defp allow_evaluation(entry, due_times, cursor, latest_due) do
    last = List.last(due_times) || latest_due
    {due_times, next_due(entry, last), clear_tracking(cursor)}
  end

  defp tracked_evaluation(entry, [], cursor, latest_due),
    do: {[], next_due(entry, latest_due), clear_tracking(cursor)}

  defp tracked_evaluation(entry, [due_at], cursor, _latest_due) do
    run_id = scheduled_run_id(occurrence_key(entry, due_at))

    next_cursor =
      cursor
      |> Map.put("in_flight_run_id", run_id)
      |> Map.put("queued_due_at", nil)

    {[due_at], next_due(entry, due_at), next_cursor}
  end

  defp queue_one_evaluation(entry, [], cursor, latest_due, _now),
    do: {[], next_due(entry, latest_due), clear_tracking(cursor)}

  defp queue_one_evaluation(entry, [due_at | rest], cursor, _latest_due, now) do
    run_id = scheduled_run_id(occurrence_key(entry, due_at))
    queued = List.last(rest)

    next_cursor =
      cursor
      |> Map.put("in_flight_run_id", run_id)
      |> Map.put("queued_due_at", if(queued, do: DateTime.to_iso8601(queued)))

    next_due_at = if queued, do: now, else: next_due(entry, due_at)
    {[due_at], next_due_at, next_cursor}
  end

  defp selected_occurrences(entry, next_due_at, latest_due) do
    from = DateTime.add(next_due_at, -1, :second)

    occurrences =
      Cron.occurrences_between(
        entry.schedule.cron,
        entry.schedule.timezone,
        from,
        latest_due,
        limit: @max_occurrences
      )

    case entry.schedule.missed do
      :all -> occurrences
      :one -> Enum.take(occurrences, 1)
      :skip -> Enum.take(occurrences, -1)
    end
  end

  defp next_due(entry, %DateTime{} = after_due) do
    Cron.next_due(entry.schedule.cron, entry.schedule.timezone, after_due) ||
      DateTime.add(after_due, state_tick_floor_ms(), :millisecond)
  end

  defp reconcile_in_flight(context, %{"in_flight_run_id" => run_id} = cursor)
       when is_binary(run_id) do
    case Runs.get(context, run_id) do
      {:ok, %{status: status}} when status in [:pending, :running] -> cursor
      {:ok, _terminal} -> Map.put(cursor, "in_flight_run_id", nil)
      {:error, %{kind: :not_found}} -> Map.put(cursor, "in_flight_run_id", nil)
      {:error, _unavailable} -> cursor
    end
  end

  defp reconcile_in_flight(_context, cursor), do: cursor

  defp claim_occurrences(context, owner_id) do
    Persistence.stores().scheduler.claim_occurrences(%ClaimScheduleOccurrences{
      workspace_context: context,
      batch_id: unique_id("occurrence-claim"),
      owner_id: owner_id,
      lease_duration_ms: @lease_duration_ms,
      limit: @max_batch
    })
  end

  defp dispatch_occurrences(context, entries, occurrences) do
    Enum.reduce_while(occurrences, :ok, fn occurrence, :ok ->
      case dispatch_occurrence(context, entries, occurrence) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp dispatch_occurrence(context, entries, %ScheduleOccurrence{} = occurrence) do
    with {:ok, entry} <-
           Map.fetch(entries, {occurrence.pipeline_target_id, occurrence.schedule_id}),
         {:ok, run_id} <- submit_occurrence(context, entry, occurrence) do
      complete_occurrence(context, occurrence, run_id, nil)
    else
      :error ->
        complete_occurrence(
          context,
          occurrence,
          nil,
          JsonSafe.error(:deployed_schedule_not_in_manifest)
        )

      {:error, reason} ->
        complete_occurrence(context, occurrence, nil, JsonSafe.error(reason))
    end
  end

  defp submit_occurrence(context, entry, occurrence) do
    occurrence_key = occurrence.payload["occurrence_key"]
    run_id = occurrence.payload["run_id"] || scheduled_run_id(occurrence_key)

    case Runs.get(context, run_id) do
      {:ok, run} ->
        if persisted_occurrence_key(run) == occurrence_key,
          do: {:ok, run_id},
          else: {:error, {:scheduled_run_id_conflict, run_id}}

      {:error, %{kind: :not_found}} ->
        with {:ok, anchor_window} <-
               maybe_anchor_window(entry.window, occurrence.due_at, entry.schedule.timezone) do
          BoundedDispatcher.run(fn ->
            RunManager.submit_pipeline_module_run(context, entry.module,
              run_id: run_id,
              manifest_version_id: entry.manifest_version_id,
              trigger: trigger(entry, occurrence),
              anchor_window: anchor_window
            )
          end)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp complete_occurrence(context, occurrence, run_id, error) do
    result =
      Persistence.stores().scheduler.complete_occurrence(%CompleteScheduleOccurrence{
        workspace_context: context,
        command_id:
          command_id("occurrence-complete", occurrence.occurrence_id, occurrence.claim_generation),
        occurrence_id: occurrence.occurrence_id,
        owner_id: occurrence.claim_owner,
        claim_generation: occurrence.claim_generation,
        run_id: run_id,
        error: error,
        occurred_at: DateTime.utc_now()
      })

    case result do
      {:ok, %ScheduleOccurrence{}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp trigger(entry, occurrence) do
    %{
      kind: :schedule,
      pipeline: %{module: entry.module, id: entry.id},
      schedule: %{
        id: entry.schedule.name,
        ref: entry.schedule.ref,
        cron: entry.schedule.cron,
        timezone: entry.schedule.timezone,
        overlap: entry.schedule.overlap,
        missed: entry.schedule.missed,
        active: entry.schedule.active
      },
      occurrence: %{
        due_at: occurrence.due_at,
        occurrence_key: occurrence.payload["occurrence_key"],
        recovery: recovery_atom(occurrence.payload["recovery"])
      },
      evaluated_at: DateTime.utc_now()
    }
  end

  defp maybe_anchor_window(nil, _due_at, _timezone), do: {:ok, nil}

  defp maybe_anchor_window(%Policy{} = policy, due_at, timezone) do
    with {:ok, policy} <- Policy.validate(policy) do
      Policy.resolve_scheduled(policy, due_at, timezone)
    end
  end

  defp occurrence_key(entry, due_at) do
    [
      "schedule",
      Atom.to_string(entry.module),
      to_string(entry.id),
      entry.schedule_fingerprint,
      DateTime.to_iso8601(due_at)
    ]
    |> Enum.join(":")
  end

  defp occurrence_id(workspace_id, key),
    do: "occurrence:" <> digest({workspace_id, key})

  defp scheduled_run_id(key), do: "run_schedule_" <> digest(key)

  defp persisted_occurrence_key(%{trigger: trigger}) when is_map(trigger) do
    occurrence = Map.get(trigger, :occurrence) || Map.get(trigger, "occurrence") || %{}
    Map.get(occurrence, :occurrence_key) || Map.get(occurrence, "occurrence_key")
  end

  defp persisted_occurrence_key(_run), do: nil

  defp clear_tracking(cursor),
    do: cursor |> Map.put("in_flight_run_id", nil) |> Map.put("queued_due_at", nil)

  defp parse_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _invalid -> nil
    end
  end

  defp parse_datetime(_value), do: nil

  defp recovery_atom("on_time"), do: :on_time
  defp recovery_atom(_value), do: :missed

  defp command_id(operation, identity, generation),
    do: "scheduler:#{operation}:" <> digest({identity, generation})

  defp unique_id(operation),
    do: "scheduler:#{operation}:" <> digest({node(), self(), :crypto.strong_rand_bytes(16)})

  defp digest(value) do
    :crypto.hash(:sha256, :erlang.term_to_binary(value))
    |> Base.url_encode64(padding: false)
  end

  defp owner_id do
    instance = System.get_env("FAVN_INSTANCE_ID", Atom.to_string(node()))
    "scheduler:#{String.slice(instance, 0, 96)}:#{digest({node(), self()})}"
  end

  defp schedule_tick(%{auto_tick?: true, tick_ms: tick_ms}),
    do: Process.send_after(self(), :tick, tick_ms)

  defp schedule_tick(_state), do: :ok

  defp state_tick_floor_ms, do: 1_000
end
