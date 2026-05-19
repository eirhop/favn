defmodule FavnOrchestrator.BackfillChildCoordinator do
  @moduledoc """
  Coordinates finite child-run admission for pipeline backfills.

  Backfill submission persists the parent run and all pending window rows before
  this coordinator is invoked. When an operator supplies
  `:backfill_child_concurrency`, the coordinator submits only that many child
  window runs at a time and advances the queue as backfill-window ledger rows
  become terminal. This keeps the public submit call non-blocking while applying
  admission before child pipeline runs start.
  """

  use GenServer

  alias Favn.Window.Anchor
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.Projector, as: BackfillProjector
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.TransitionWriter

  @poll_ms 100
  @terminal_window_statuses [:ok, :partial, :cancelled, :timed_out, :error]

  @type child_spec :: %{
          required(:window_key) => String.t(),
          required(:child_opts) => keyword()
        }

  @type submitter :: (keyword() -> {:ok, String.t()} | {:error, term()})
  @type failure_handler :: (String.t(), term() -> :ok | {:error, term()})

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, %{}, name: name)
  end

  @doc false
  @spec start_backfill(
          FavnOrchestrator.RunState.t(),
          module(),
          [child_spec()],
          keyword()
        ) :: :ok | {:error, term()}
  def start_backfill(parent, pipeline_module, child_specs, opts)
      when is_list(child_specs) and is_list(opts) do
    GenServer.call(__MODULE__, {:start_backfill, parent, pipeline_module, child_specs, opts})
  end

  @impl true
  def init(state) do
    {:ok, state, {:continue, :rehydrate_finite_backfills}}
  end

  @impl true
  def handle_continue(:rehydrate_finite_backfills, state) do
    {:noreply, rehydrate_finite_backfills(state)}
  end

  @impl true
  def handle_call(
        {:start_backfill, parent, pipeline_module, child_specs, opts},
        _from,
        state
      ) do
    limit = Keyword.fetch!(opts, :concurrency)
    submitter = Keyword.fetch!(opts, :submitter)
    failure_handler = Keyword.fetch!(opts, :failure_handler)

    entry = %{
      pipeline_module: pipeline_module,
      queue: :queue.from_list(child_specs),
      active: MapSet.new(),
      limit: limit,
      submitter: submitter,
      failure_handler: failure_handler
    }

    state = Map.put(state, parent.id, entry)

    case submit_available(parent.id, state) do
      {:ok, state} ->
        schedule_poll(parent.id)
        {:reply, :ok, state}

      {{:error, _reason} = error, state} ->
        {:reply, error, Map.delete(state, parent.id)}
    end
  end

  @impl true
  def handle_info({:poll_backfill, backfill_run_id}, state) do
    case Map.fetch(state, backfill_run_id) do
      {:ok, entry} ->
        state = refresh_active_windows(backfill_run_id, entry, state)

        case submit_available(backfill_run_id, state) do
          {:ok, state} ->
            if Map.has_key?(state, backfill_run_id), do: schedule_poll(backfill_run_id)
            {:noreply, state}

          {{:error, reason}, state} ->
            state = fail_backfill(backfill_run_id, reason, state)
            {:noreply, Map.delete(state, backfill_run_id)}
        end

      :error ->
        {:noreply, state}
    end
  end

  defp submit_available(backfill_run_id, state) do
    case Map.fetch(state, backfill_run_id) do
      {:ok, entry} ->
        available = max(entry.limit - MapSet.size(entry.active), 0)
        do_submit_available(backfill_run_id, entry, state, available)

      :error ->
        {:ok, state}
    end
  end

  defp do_submit_available(backfill_run_id, entry, state, 0) do
    {:ok, Map.put(state, backfill_run_id, entry)}
  end

  defp do_submit_available(backfill_run_id, entry, state, available) do
    case :queue.out(entry.queue) do
      {{:value, %{window_key: window_key, child_opts: child_opts}}, queue} ->
        case entry.submitter.(child_opts) do
          {:ok, _child_run_id} ->
            entry = %{entry | queue: queue, active: MapSet.put(entry.active, window_key)}
            do_submit_available(backfill_run_id, entry, state, available - 1)

          {:error, _reason} = error ->
            {error, Map.put(state, backfill_run_id, %{entry | queue: queue})}
        end

      {:empty, _queue} ->
        state =
          if MapSet.size(entry.active) == 0 do
            Map.delete(state, backfill_run_id)
          else
            Map.put(state, backfill_run_id, entry)
          end

        {:ok, state}
    end
  end

  defp refresh_active_windows(backfill_run_id, entry, state) do
    case BackfillProjector.list_all_backfill_windows(backfill_run_id: backfill_run_id) do
      {:ok, windows} ->
        terminal_keys =
          windows
          |> Enum.filter(&terminal_window?(entry.pipeline_module, &1))
          |> MapSet.new(& &1.window_key)

        entry = %{entry | active: MapSet.difference(entry.active, terminal_keys)}
        Map.put(state, backfill_run_id, entry)

      {:error, _reason} ->
        state
    end
  end

  defp terminal_window?(pipeline_module, %BackfillWindow{} = window) do
    window.pipeline_module == pipeline_module and window.status in @terminal_window_statuses
  end

  defp rehydrate_finite_backfills(state) do
    case Storage.list_runs() do
      {:ok, runs} ->
        runs
        |> Enum.filter(&finite_backfill_parent?/1)
        |> Enum.reduce(state, &rehydrate_finite_backfill/2)

      {:error, _reason} ->
        state
    end
  end

  defp finite_backfill_parent?(%RunState{submit_kind: :backfill_pipeline, metadata: metadata}) do
    case backfill_metadata(metadata) do
      %{backfill_child_concurrency: value} when is_integer(value) and value > 0 -> true
      %{"backfill_child_concurrency" => value} when is_integer(value) and value > 0 -> true
      _other -> false
    end
  end

  defp finite_backfill_parent?(%RunState{}), do: false

  defp backfill_metadata(metadata) when is_map(metadata) do
    Map.get(metadata, :backfill) || Map.get(metadata, "backfill") || %{}
  end

  defp backfill_metadata(_metadata), do: %{}

  defp rehydrate_finite_backfill(%RunState{} = parent, state) do
    with {:ok, windows} <- BackfillProjector.list_all_backfill_windows(backfill_run_id: parent.id),
         :ok <- project_terminal_child_windows(windows),
         {:ok, windows} <-
           BackfillProjector.list_all_backfill_windows(backfill_run_id: parent.id) do
      pending = Enum.filter(windows, &(&1.status == :pending))
      active = Enum.filter(windows, &(&1.status == :running))

      if pending == [] and active == [] do
        state
      else
        rehydrate_entry(parent, windows, pending, active, state)
      end
    else
      {:error, _reason} -> state
    end
  end

  defp project_terminal_child_windows(windows) do
    Enum.reduce_while(windows, :ok, fn window, :ok ->
      case project_terminal_child_window(window) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp project_terminal_child_window(%BackfillWindow{status: :running} = window) do
    case window.latest_attempt_run_id || window.child_run_id do
      run_id when is_binary(run_id) and run_id != "" ->
        with {:ok, run} <- Storage.get_run(run_id),
             {:ok, event_type} <- terminal_event_type(run.status) do
          BackfillProjector.project_transition(run, event_type, %{error: run.error})
        else
          {:error, :not_terminal} -> :ok
          {:error, :not_found} -> :ok
          {:error, _reason} = error -> error
        end

      _other ->
        :ok
    end
  end

  defp project_terminal_child_window(%BackfillWindow{}), do: :ok

  defp terminal_event_type(:ok), do: {:ok, :run_finished}
  defp terminal_event_type(:partial), do: {:ok, :run_failed}
  defp terminal_event_type(:error), do: {:ok, :run_failed}
  defp terminal_event_type(:cancelled), do: {:ok, :run_cancelled}
  defp terminal_event_type(:timed_out), do: {:ok, :run_timed_out}
  defp terminal_event_type(_status), do: {:error, :not_terminal}

  defp rehydrate_entry(%RunState{} = parent, windows, pending, active, state) do
    with {:ok, parent} <- resume_parent(parent, windows) do
      backfill = backfill_metadata(parent.metadata)
      pipeline_module = pipeline_module(parent, pending, active)

      child_specs =
        pending
        |> Enum.map(&child_spec(parent, &1, backfill))
        |> Enum.reject(&is_nil/1)

      entry = %{
        pipeline_module: pipeline_module,
        queue: :queue.from_list(child_specs),
        active: MapSet.new(active, & &1.window_key),
        limit: backfill_child_concurrency(backfill),
        submitter: fn child_opts ->
          RunManager.submit_pipeline_module_run(pipeline_module, child_opts)
        end,
        failure_handler: fn backfill_run_id, reason ->
          mark_pending_windows_failed(backfill_run_id, reason)
        end
      }

      state = Map.put(state, parent.id, entry)

      case submit_available(parent.id, state) do
        {:ok, state} ->
          if Map.has_key?(state, parent.id), do: schedule_poll(parent.id)
          state

        {{:error, reason}, state} ->
          state = fail_backfill(parent.id, reason, state)
          Map.delete(state, parent.id)
      end
    else
      {:error, _reason} -> state
    end
  end

  defp resume_parent(%RunState{} = parent, windows) do
    status = BackfillProjector.parent_status(windows)

    if parent.status == status and is_nil(parent.error) do
      {:ok, parent}
    else
      resumed =
        RunState.transition(parent,
          status: status,
          error: nil,
          result: %{status: status, backfill_windows: length(windows)}
        )

      case TransitionWriter.persist_transition(resumed, parent_event_type(status), %{
             status: status,
             window_counts: window_counts(windows),
             recovery: :finite_backfill_admission_rehydrated
           }) do
        :ok -> {:ok, resumed}
        {:error, _reason} = error -> error
      end
    end
  end

  defp pipeline_module(%RunState{} = parent, pending, active) do
    case pending ++ active do
      [%BackfillWindow{pipeline_module: pipeline_module} | _rest] -> pipeline_module
      [] -> Map.get(parent.trigger || %{}, :pipeline_module)
    end
  end

  defp child_spec(%RunState{} = parent, %BackfillWindow{} = window, backfill) do
    with {:ok, anchor} <-
           Anchor.new(window.window_kind, window.window_start_at, window.window_end_at,
             timezone: window.timezone
           ) do
      %{
        window_key: window.window_key,
        child_opts: child_opts(parent, window, anchor, backfill)
      }
    else
      {:error, _reason} -> nil
    end
  end

  defp child_opts(%RunState{} = parent, %BackfillWindow{} = window, anchor, backfill) do
    []
    |> maybe_put_opt(
      :pipeline_stage_concurrency,
      metadata_value(backfill, :pipeline_stage_concurrency)
    )
    |> maybe_put_refresh(backfill)
    |> Keyword.put(:timeout_ms, parent.timeout_ms)
    |> Keyword.put(:retry_backoff_ms, parent.retry_backoff_ms)
    |> Keyword.put(:max_attempts, parent.max_attempts)
    |> Keyword.put(:manifest_version_id, parent.manifest_version_id)
    |> Keyword.put(:anchor_window, anchor)
    |> Keyword.put(:parent_run_id, parent.id)
    |> Keyword.put(:root_run_id, parent.id)
    |> Keyword.put(:lineage_depth, 1)
    |> Keyword.put(:trigger, %{
      kind: :backfill,
      backfill_run_id: parent.id,
      window_key: window.window_key
    })
  end

  defp maybe_put_refresh(opts, backfill) do
    cond do
      not is_nil(metadata_value(backfill, :refresh_policy)) ->
        Keyword.put(opts, :refresh_policy, metadata_value(backfill, :refresh_policy))

      not is_nil(metadata_value(backfill, :refresh)) ->
        Keyword.put(opts, :refresh, metadata_value(backfill, :refresh))

      true ->
        Keyword.put(opts, :refresh, :missing)
    end
  end

  defp maybe_put_opt(opts, _key, nil), do: opts
  defp maybe_put_opt(opts, key, value), do: Keyword.put(opts, key, value)

  defp metadata_value(metadata, key) when is_map(metadata) do
    Map.get(metadata, key) || Map.get(metadata, Atom.to_string(key))
  end

  defp metadata_value(_metadata, _key), do: nil

  defp backfill_child_concurrency(backfill) do
    case metadata_value(backfill, :backfill_child_concurrency) do
      value when is_integer(value) and value > 0 -> value
    end
  end

  defp mark_pending_windows_failed(backfill_run_id, reason) do
    with {:ok, windows} <-
           BackfillProjector.list_all_backfill_windows(backfill_run_id: backfill_run_id),
         pending <- Enum.filter(windows, &(&1.status == :pending)),
         :ok <- put_failed_windows(pending, reason),
         {:ok, updated_windows} <-
           BackfillProjector.list_all_backfill_windows(backfill_run_id: backfill_run_id),
         {:ok, parent} <- Storage.get_run(backfill_run_id) do
      status = BackfillProjector.parent_status(updated_windows)

      parent
      |> RunState.transition(
        status: status,
        error: reason,
        result: %{status: status, backfill_windows: length(updated_windows)}
      )
      |> TransitionWriter.persist_transition(parent_event_type(status), %{
        status: status,
        error: reason,
        window_counts: window_counts(updated_windows)
      })
    end
  end

  defp put_failed_windows([], _reason), do: :ok

  defp put_failed_windows(windows, reason) do
    now = DateTime.utc_now()

    Enum.reduce_while(windows, :ok, fn %BackfillWindow{} = window, :ok ->
      updated = %{
        window
        | status: :error,
          last_error: reason,
          errors: window.errors ++ [reason],
          finished_at: window.finished_at || now,
          updated_at: now
      }

      case Storage.put_backfill_window(updated) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp parent_event_type(:ok), do: :backfill_finished
  defp parent_event_type(:partial), do: :backfill_partial
  defp parent_event_type(:cancelled), do: :backfill_cancelled
  defp parent_event_type(:timed_out), do: :backfill_timed_out
  defp parent_event_type(:error), do: :backfill_failed
  defp parent_event_type(:running), do: :backfill_progressed

  defp window_counts(windows) do
    Enum.reduce(windows, %{}, fn %BackfillWindow{status: status}, acc ->
      Map.update(acc, status, 1, &(&1 + 1))
    end)
  end

  defp fail_backfill(backfill_run_id, reason, state) do
    case Map.fetch(state, backfill_run_id) do
      {:ok, %{failure_handler: failure_handler}} ->
        _ = failure_handler.(backfill_run_id, reason)
        state

      :error ->
        state
    end
  end

  defp schedule_poll(backfill_run_id) do
    Process.send_after(self(), {:poll_backfill, backfill_run_id}, @poll_ms)
  end
end
