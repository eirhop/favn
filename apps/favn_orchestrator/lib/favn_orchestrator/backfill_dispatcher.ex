defmodule FavnOrchestrator.BackfillDispatcher do
  @moduledoc """
  Distributed dispatcher and terminal-state reconciler for backfill windows.

  Every orchestrator node may run this worker. PostgreSQL claims use
  `SKIP LOCKED`, expiring leases, and monotonically increasing fences. A stable
  child run identity makes recovery safe when a node crashes between run
  creation and recording the window's `running` transition.
  """

  use GenServer

  alias Favn.Retry.Policy
  alias Favn.Window.{Anchor, Selection}
  alias FavnOrchestrator.Backfills
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.ClaimBackfillWindows
  alias FavnOrchestrator.Persistence.Commands.TransitionBackfillWindow
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.Backfill
  alias FavnOrchestrator.Persistence.Results.BackfillWindow
  alias FavnOrchestrator.Persistence.Results.RunSubmission
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunSubmissions
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.RuntimeConfig

  @default_interval_ms 1_000
  @default_lease_ms 30_000
  @default_batch_size 100

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    state = %{
      workspace_ids:
        opts
        |> Keyword.get(
          :workspace_ids,
          RuntimeConfig.workspace_ids()
        )
        |> Enum.uniq(),
      owner_id: Keyword.get(opts, :owner_id, owner_id()),
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      lease_ms: Keyword.get(opts, :lease_duration_ms, @default_lease_ms),
      batch_size: Keyword.get(opts, :batch_size, @default_batch_size)
    }

    {:ok, state, {:continue, :dispatch}}
  end

  @impl true
  def handle_continue(:dispatch, state), do: dispatch(state)

  @impl true
  def handle_info(:dispatch, state), do: dispatch(state)

  defp dispatch(state) do
    _ =
      Lifecycle.with_admission(fn ->
        Enum.each(state.workspace_ids, &dispatch_workspace(&1, state))
      end)

    Process.send_after(self(), :dispatch, state.interval_ms)
    {:noreply, state}
  end

  defp dispatch_workspace(workspace_id, state) do
    context = SystemContext.workspace(workspace_id, :backfill_dispatcher)
    batch_id = command_id("claim", workspace_id <> ":" <> unique_identity())

    command = %ClaimBackfillWindows{
      workspace_context: context,
      batch_id: batch_id,
      owner_id: state.owner_id,
      lease_duration_ms: state.lease_ms,
      limit: state.batch_size
    }

    case store().claim_windows(command) do
      {:ok, windows} -> Enum.each(windows, &process_window(context, &1, state.owner_id))
      {:error, %Error{kind: :conflict}} -> :ok
      {:error, reason} -> emit_error(workspace_id, :claim, reason)
    end
  end

  defp process_window(context, %BackfillWindow{status: :claimed} = window, owner_id) do
    run_id = child_run_id(window)

    result =
      case Runs.get(context, run_id) do
        {:ok, %RunState{}} -> {:ok, run_id}
        {:error, %Error{kind: :not_found}} -> submit_child(context, window, run_id)
        {:error, reason} -> {:error, reason}
      end

    case result do
      {:ok, ^run_id} ->
        with {:ok, running} <- transition(context, window, owner_id, :running, run_id, nil) do
          reconcile_run(context, running, owner_id)
        end

      {:error, reason} ->
        case reserved_identity_after_submit_error(
               Runs.get(context, run_id),
               fn -> RunSubmissions.get(context, run_id) end
             ) do
          :reserved ->
            with {:ok, running} <- transition(context, window, owner_id, :running, run_id, nil) do
              reconcile_run(context, running, owner_id)
            end

          :missing ->
            case submission_error_disposition(reason, :missing) do
              :retry ->
                :ok

              :fail ->
                _ = transition(context, window, owner_id, :failed, nil, error_payload(reason))
                :ok
            end

          {:unknown, recovery_error} ->
            :retry = submission_error_disposition(reason, :unavailable)
            emit_error(context.workspace_id, :reconcile_submit_error, recovery_error)
        end
    end
  end

  defp process_window(context, %BackfillWindow{status: :running} = window, owner_id),
    do: reconcile_run(context, window, owner_id)

  defp process_window(_context, _window, _owner_id), do: :ok

  @doc false
  def reserved_identity_after_submit_error(run_result, submission_result)

  def reserved_identity_after_submit_error({:ok, %RunState{}}, _submission_result),
    do: :reserved

  def reserved_identity_after_submit_error(
        {:error, %Error{kind: :not_found}},
        submission_result
      ) do
    case resolve_submission_result(submission_result) do
      {:ok, %RunSubmission{}} -> :reserved
      {:error, %Error{kind: :not_found}} -> :missing
      {:error, reason} -> {:unknown, reason}
    end
  end

  def reserved_identity_after_submit_error({:error, reason}, _submission_result),
    do: {:unknown, reason}

  defp resolve_submission_result(fun) when is_function(fun, 0), do: fun.()
  defp resolve_submission_result(result), do: result

  @doc false
  @spec submission_error_disposition(term(), :missing | :unavailable) :: :retry | :fail
  def submission_error_disposition(_reason, :unavailable), do: :retry

  def submission_error_disposition(%Error{retryable?: true}, :missing), do: :retry
  def submission_error_disposition(_reason, :missing), do: :fail

  defp submit_child(context, window, run_id) do
    with {:ok, %Backfill{} = backfill} <- Backfills.get(context, window.backfill_id),
         {:ok, anchors} <- execution_anchors(backfill, window),
         {:ok, selection} <- Selection.backfill(anchors, hd(anchors).timezone),
         {:ok, opts} <- submission_options(backfill, window, run_id, selection),
         {:ok, ^run_id} <- submit_target(context, backfill, opts) do
      {:ok, run_id}
    end
  end

  defp submit_target(context, %Backfill{target_kind: :pipeline} = backfill, opts) do
    with {:ok, pipeline_ref} <- pipeline_ref(backfill.metadata) do
      RunSubmissions.enqueue_pipeline(context, pipeline_ref, opts)
    end
  end

  defp submit_target(context, %Backfill{target_kind: :asset} = backfill, opts) do
    with {:ok, asset_ref} <- asset_ref(backfill.metadata) do
      RunSubmissions.enqueue_asset(context, asset_ref, opts)
    end
  end

  defp submission_options(backfill, window, run_id, selection) do
    metadata = %{
      backfill_id: backfill.backfill_id,
      backfill_window_id: window.window_id,
      backfill_window_key: window.window_key,
      backfill_execution_group_id: field(window.payload, "execution_group_id"),
      backfill_root_run_id: backfill.root_run_id,
      operator_metadata: field(backfill.metadata, "operator_metadata", %{})
    }

    with {:ok, retry_policy} <- Policy.new(field(backfill.metadata, "retry_policy")),
         {:ok, refresh} <- decode_refresh(field(backfill.metadata, "refresh")),
         {:ok, required_generation} <-
           decode_required_generation(field(backfill.metadata, "required_generation")),
         {:ok, dependencies} <-
           decode_dependencies(field(backfill.metadata, "dependencies")) do
      {:ok,
       [
         run_id: run_id,
         submission_source: :backfill,
         manifest_version_id: backfill.manifest_version_id,
         window_selection: selection,
         combine_windows: field(backfill.metadata, "combine_windows", false),
         parent_run_id: backfill.root_run_id,
         root_run_id: backfill.root_run_id,
         lineage_depth: 1,
         metadata: metadata,
         retry_policy: retry_policy
       ]
       |> maybe_put(:timeout_ms, field(backfill.metadata, "timeout_ms"))
       |> maybe_put(:refresh, refresh)
       |> maybe_put(:required_generation, required_generation)
       |> maybe_put(:dependencies, dependencies)}
    end
  end

  defp reconcile_run(context, %BackfillWindow{run_id: run_id} = window, owner_id)
       when is_binary(run_id) do
    case Runs.get(context, run_id) do
      {:ok, %RunState{status: status}} when status in [:ok] ->
        transition(context, window, owner_id, :succeeded, run_id, nil)

      {:ok, %RunState{status: :cancelled}} ->
        transition(context, window, owner_id, :cancelled, run_id, nil)

      {:ok, %RunState{status: status, error: error}}
      when status in [:error, :partial, :timed_out] ->
        transition(context, window, owner_id, :failed, run_id, error_payload(error || status))

      {:ok, %RunState{}} ->
        :ok

      {:error, %Error{kind: :not_found}} ->
        reconcile_submission(context, window, owner_id, run_id)

      {:error, reason} ->
        emit_error(context.workspace_id, :reconcile, reason)
    end
  end

  defp reconcile_run(_context, _window, _owner_id), do: :ok

  defp reconcile_submission(context, window, owner_id, run_id) do
    case RunSubmissions.get(context, run_id) do
      {:ok, %RunSubmission{status: status}}
      when status in [:queued, :preparing, :admitting] ->
        :ok

      {:ok, %RunSubmission{status: :failed, error: error, failure_kind: failure_kind}} ->
        transition(
          context,
          window,
          owner_id,
          :failed,
          run_id,
          error || %{"failure_kind" => to_string(failure_kind)}
        )

      {:ok, %RunSubmission{status: :cancelled}} ->
        transition(context, window, owner_id, :cancelled, run_id, nil)

      {:ok, %RunSubmission{}} ->
        transition(context, window, owner_id, :failed, run_id, %{"reason" => "run_not_found"})

      {:error, %Error{kind: :not_found}} ->
        transition(context, window, owner_id, :failed, run_id, %{"reason" => "run_not_found"})

      {:error, reason} ->
        emit_error(context.workspace_id, :reconcile_submission, reason)
    end
  end

  defp transition(context, window, owner_id, status, run_id, error) do
    store().transition_window(%TransitionBackfillWindow{
      workspace_context: context,
      command_id:
        command_id(
          "window-#{status}",
          "#{window.backfill_id}:#{window.window_id}:#{window.fencing_token}:#{window.version}"
        ),
      backfill_id: window.backfill_id,
      window_id: window.window_id,
      owner_id: owner_id,
      fencing_token: window.fencing_token,
      expected_version: window.version,
      status: status,
      run_id: run_id,
      error: error,
      occurred_at: DateTime.utc_now()
    })
  end

  defp pipeline_ref(metadata) do
    with module when is_binary(module) <- field(metadata, "pipeline_module"),
         name when is_binary(name) <- field(metadata, "pipeline_name"),
         {:ok, module} <- existing_atom(module),
         {:ok, name} <- existing_atom(name) do
      {:ok, {module, name}}
    else
      _invalid -> {:error, :invalid_backfill_pipeline_identity}
    end
  end

  defp asset_ref(metadata) do
    with module when is_binary(module) <- field(metadata, "asset_module"),
         name when is_binary(name) <- field(metadata, "asset_name"),
         {:ok, module} <- existing_atom(module),
         {:ok, name} <- existing_atom(name) do
      {:ok, {module, name}}
    else
      _invalid -> {:error, :invalid_backfill_asset_identity}
    end
  end

  defp anchor(window) do
    with kind when is_binary(kind) <- field(window.payload, "kind"),
         {:ok, kind} <- known_kind(kind),
         timezone when is_binary(timezone) <- field(window.payload, "timezone") do
      Anchor.new(kind, window.window_start, window.window_end, timezone: timezone)
    else
      _invalid -> {:error, :invalid_backfill_window_payload}
    end
  end

  defp execution_anchors(backfill, window) do
    if field(backfill.metadata, "combine_windows", false) do
      with {:ok, first} <- anchor(window),
           {:ok, anchors} <-
             Anchor.expand_range(first.kind, backfill.range_start, backfill.range_end,
               timezone: first.timezone
             ),
           true <- anchors != [] do
        {:ok, anchors}
      else
        _invalid -> {:error, :invalid_combined_backfill_range}
      end
    else
      with {:ok, anchor} <- anchor(window), do: {:ok, [anchor]}
    end
  end

  defp known_kind("hour"), do: {:ok, :hour}
  defp known_kind("day"), do: {:ok, :day}
  defp known_kind("month"), do: {:ok, :month}
  defp known_kind("year"), do: {:ok, :year}
  defp known_kind(_kind), do: {:error, :invalid_window_kind}

  defp existing_atom(value) do
    {:ok, String.to_existing_atom(value)}
  rescue
    ArgumentError -> {:error, :unknown_atom}
  end

  defp decode_refresh(nil), do: {:ok, nil}
  defp decode_refresh("auto"), do: {:ok, :auto}
  defp decode_refresh("missing"), do: {:ok, :missing}
  defp decode_refresh("force"), do: {:ok, :force}

  defp decode_refresh(%{"mode" => "force_assets", "refs" => refs} = refresh)
       when is_list(refs) do
    case decode_refs(refs) do
      {:ok, refs} ->
        value =
          if Map.get(refresh, "include_upstream", false),
            do: {:force_assets, refs, include_upstream: true},
            else: {:force_assets, refs}

        {:ok, value}

      _invalid ->
        {:error, :invalid_backfill_refresh}
    end
  end

  defp decode_refresh(_other), do: {:error, :invalid_backfill_refresh}

  defp decode_refs(refs) do
    Enum.reduce_while(refs, {:ok, []}, fn ref, {:ok, acc} ->
      with module when is_binary(module) <- field(ref, "module"),
           name when is_binary(name) <- field(ref, "name"),
           {:ok, module} <- existing_atom(module),
           {:ok, name} <- existing_atom(name) do
        {:cont, {:ok, [{module, name} | acc]}}
      else
        _invalid -> {:halt, {:error, :invalid_ref}}
      end
    end)
    |> then(fn
      {:ok, decoded} -> {:ok, Enum.reverse(decoded)}
      error -> error
    end)
  end

  defp decode_dependencies(nil), do: {:ok, nil}
  defp decode_dependencies("all"), do: {:ok, :all}
  defp decode_dependencies("none"), do: {:ok, :none}
  defp decode_dependencies(_other), do: {:error, :invalid_backfill_dependencies}

  defp decode_required_generation(nil), do: {:ok, nil}

  defp decode_required_generation(generation) when is_map(generation) do
    target_id = field(generation, "target_id")
    evidence_generation_id = field(generation, "evidence_generation_id")
    target_generation_id = field(generation, "target_generation_id")

    if is_binary(target_id) and target_id != "" and is_binary(evidence_generation_id) and
         evidence_generation_id != "" and
         (is_nil(target_generation_id) or target_generation_id == evidence_generation_id) do
      {:ok,
       %{
         target_id: target_id,
         evidence_generation_id: evidence_generation_id,
         target_generation_id: target_generation_id
       }}
    else
      {:error, :invalid_required_generation}
    end
  end

  defp decode_required_generation(_other), do: {:error, :invalid_required_generation}

  defp child_run_id(window) do
    FavnOrchestrator.Persistence.BackfillPlan.child_run_id(
      window.backfill_id,
      window.window_id,
      window.payload
    )
  end

  @doc false
  @spec error_payload(term()) :: map()
  def error_payload({reason, details})
      when reason in [:operator_decision_required, :rebuild_required, :target_drift] and
             is_map(details) do
    %{
      "kind" => "admission",
      "type" => "backfill_admission",
      "message" => Atom.to_string(reason),
      "reason" => Atom.to_string(reason),
      "details" =>
        details
        |> Map.take([:target_id, :compatibility_status, :reason_code])
        |> Map.new(fn {key, value} -> {Atom.to_string(key), bounded_scalar(value)} end)
    }
  end

  def error_payload(reason) when is_atom(reason), do: %{"reason" => Atom.to_string(reason)}

  # A run or submission that already recorded an error stored it string-keyed and
  # JSON-safe. Inspecting it again would put a printed map where a reason belongs,
  # which no reader can group or unwrap: every window would carry its own blob and
  # the identical failure would look like N different ones. The submission path
  # already passes such a map through; this makes the run path agree.
  def error_payload(%{"reason" => reason} = error) when is_binary(reason), do: error
  def error_payload(%{"message" => message} = error) when is_binary(message), do: error

  def error_payload(reason) do
    %{"reason" => reason |> inspect(limit: 20, printable_limit: 1_000) |> String.slice(0, 2_000)}
  end

  defp bounded_scalar(value) when is_atom(value),
    do: value |> Atom.to_string() |> bounded_scalar()

  defp bounded_scalar(value) when is_binary(value), do: String.slice(value, 0, 1_024)
  defp bounded_scalar(_value), do: "unknown"

  defp emit_error(workspace_id, operation, reason) do
    :telemetry.execute(
      [:favn, :orchestrator, :backfill_dispatch, :error],
      %{count: 1},
      %{workspace_id: workspace_id, operation: operation, reason: error_kind(reason)}
    )
  end

  defp error_kind(%Error{kind: kind}), do: kind
  defp error_kind(_reason), do: :unknown

  defp field(map, key, default \\ nil) when is_map(map) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        value

      :error ->
        Enum.reduce_while(map, default, fn
          {candidate, value}, _acc when is_atom(candidate) ->
            if Atom.to_string(candidate) == key,
              do: {:halt, value},
              else: {:cont, default}

          _entry, _acc ->
            {:cont, default}
        end)
    end
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp command_id(prefix, identity) do
    hash = :crypto.hash(:sha256, identity) |> Base.url_encode64(padding: false)
    prefix <> ":" <> String.slice(hash, 0, 40)
  end

  defp unique_identity,
    do: Integer.to_string(System.unique_integer([:positive, :monotonic]))

  defp owner_id do
    instance = FavnOrchestrator.RuntimeConfig.instance_id() |> String.slice(0, 160)
    instance <> ":backfills:" <> String.slice(unique_identity(), 0, 40)
  end

  defp store, do: Persistence.stores().backfills
end
