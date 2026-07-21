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
  alias Favn.Window.Anchor
  alias FavnOrchestrator.Backfills
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.ClaimBackfillWindows
  alias FavnOrchestrator.Persistence.Commands.TransitionBackfillWindow
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.Backfill
  alias FavnOrchestrator.Persistence.Results.BackfillWindow
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.RunState

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
          Application.get_env(:favn_orchestrator, :workspace_ids, [])
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
    Enum.each(state.workspace_ids, &dispatch_workspace(&1, state))
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
        case Runs.get(context, run_id) do
          {:ok, %RunState{}} ->
            with {:ok, running} <- transition(context, window, owner_id, :running, run_id, nil) do
              reconcile_run(context, running, owner_id)
            end

          {:error, _missing_or_unavailable} ->
            _ = transition(context, window, owner_id, :failed, nil, error_payload(reason))
            :ok
        end
    end
  end

  defp process_window(context, %BackfillWindow{status: :running} = window, owner_id),
    do: reconcile_run(context, window, owner_id)

  defp process_window(_context, _window, _owner_id), do: :ok

  defp submit_child(context, window, run_id) do
    with {:ok, %Backfill{} = backfill} <- Backfills.get(context, window.backfill_id),
         {:ok, anchor} <- anchor(window),
         {:ok, opts} <- submission_options(backfill, window, run_id, anchor),
         {:ok, ^run_id} <- submit_target(context, backfill, opts) do
      {:ok, run_id}
    end
  end

  defp submit_target(context, %Backfill{target_kind: :pipeline} = backfill, opts) do
    with {:ok, pipeline_ref} <- pipeline_ref(backfill.metadata) do
      RunManager.submit_pipeline_ref_run(context, pipeline_ref, opts)
    end
  end

  defp submit_target(context, %Backfill{target_kind: :asset} = backfill, opts) do
    with {:ok, asset_ref} <- asset_ref(backfill.metadata) do
      RunManager.submit_asset_run(context, asset_ref, opts)
    end
  end

  defp submission_options(backfill, window, run_id, anchor) do
    metadata = %{
      backfill_id: backfill.backfill_id,
      backfill_window_id: window.window_id,
      backfill_window_key: window.window_key,
      backfill_root_run_id: backfill.root_run_id,
      operator_metadata: field(backfill.metadata, "operator_metadata", %{})
    }

    with {:ok, retry_policy} <- Policy.new(field(backfill.metadata, "retry_policy")),
         {:ok, refresh} <- decode_refresh(field(backfill.metadata, "refresh")),
         {:ok, dependencies} <-
           decode_dependencies(field(backfill.metadata, "dependencies")) do
      {:ok,
       [
         run_id: run_id,
         manifest_version_id: backfill.manifest_version_id,
         anchor_window: anchor,
         parent_run_id: backfill.root_run_id,
         root_run_id: backfill.root_run_id,
         lineage_depth: 1,
         metadata: metadata,
         retry_policy: retry_policy
       ]
       |> maybe_put(:timeout_ms, field(backfill.metadata, "timeout_ms"))
       |> maybe_put(:refresh, refresh)
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
        transition(context, window, owner_id, :failed, run_id, %{"reason" => "run_not_found"})

      {:error, reason} ->
        emit_error(context.workspace_id, :reconcile, reason)
    end
  end

  defp reconcile_run(_context, _window, _owner_id), do: :ok

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

  defp child_run_id(window),
    do: command_id("run-bfw", window.backfill_id <> ":" <> window.window_id)

  defp error_payload(reason) do
    %{"reason" => reason |> inspect(limit: 20, printable_limit: 1_000) |> String.slice(0, 2_000)}
  end

  defp emit_error(workspace_id, operation, reason) do
    :telemetry.execute(
      [:favn, :orchestrator, :backfill_dispatch, :error],
      %{count: 1},
      %{workspace_id: workspace_id, operation: operation, reason: error_kind(reason)}
    )
  end

  defp error_kind(%Error{kind: kind}), do: kind
  defp error_kind(_reason), do: :unknown

  defp field(map, key, default \\ nil) when is_map(map),
    do: Map.get(map, key, Map.get(map, String.to_existing_atom(key), default))

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
