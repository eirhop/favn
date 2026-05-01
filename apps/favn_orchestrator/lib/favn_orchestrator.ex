defmodule FavnOrchestrator do
  @moduledoc """
  Runtime orchestrator control-plane facade for manifest-pinned operations.

  `FavnOrchestrator` is the boundary used by runtime apps, operator tooling, and
  the thin `Favn` runtime helpers. It is not the stable authoring-time API that
  most application code should build against.
  """

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RunnerClient
  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias Favn.Window.Anchor
  alias Favn.Window.Policy
  alias FavnOrchestrator.Backfill.Repair, as: BackfillRepair
  alias FavnOrchestrator.BackfillManager
  alias FavnOrchestrator.Events
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.Scheduler.ManifestEntries
  alias FavnOrchestrator.Scheduler.Runtime, as: SchedulerRuntime
  alias FavnOrchestrator.SchedulerEntry
  alias FavnOrchestrator.Storage

  @type run_id :: String.t()
  @type manifest_summary :: %{
          required(:manifest_version_id) => String.t(),
          required(:content_hash) => String.t(),
          required(:asset_count) => non_neg_integer(),
          required(:pipeline_count) => non_neg_integer(),
          required(:schedule_count) => non_neg_integer()
        }

  @type manifest_target_option :: %{
          required(:target_id) => String.t(),
          required(:label) => String.t(),
          optional(:asset_ref) => String.t(),
          optional(:type) => String.t(),
          optional(:relation) => map() | nil,
          optional(:metadata) => map(),
          optional(:runtime_config) => map(),
          optional(:depends_on) => [String.t()],
          optional(:materialization) => map() | nil,
          optional(:window) => map() | nil
        }

  @type manifest_targets :: %{
          required(:manifest_version_id) => String.t(),
          required(:assets) => [manifest_target_option()],
          required(:pipelines) => [manifest_target_option()]
        }

  @doc """
  Registers one manifest version in orchestrator storage.
  """
  @spec register_manifest(Version.t()) :: :ok | {:error, term()}
  def register_manifest(%Version{} = version), do: ManifestStore.register_manifest(version)

  @doc """
  Returns one persisted manifest version.
  """
  @spec get_manifest(String.t()) :: {:ok, Version.t()} | {:error, term()}
  def get_manifest(manifest_version_id) when is_binary(manifest_version_id) do
    ManifestStore.get_manifest(manifest_version_id)
  end

  @doc """
  Lists persisted manifest versions.
  """
  @spec list_manifests() :: {:ok, [Version.t()]} | {:error, term()}
  def list_manifests, do: ManifestStore.list_manifests()

  @doc """
  Lists stable operator-facing manifest summaries.
  """
  @spec list_manifest_summaries() :: {:ok, [manifest_summary()]} | {:error, term()}
  def list_manifest_summaries do
    with {:ok, versions} <- list_manifests() do
      {:ok,
       versions
       |> Enum.map(&manifest_summary/1)
       |> Enum.sort_by(& &1.manifest_version_id)}
    end
  end

  @doc """
  Returns one stable operator-facing manifest summary.
  """
  @spec get_manifest_summary(String.t()) :: {:ok, manifest_summary()} | {:error, term()}
  def get_manifest_summary(manifest_version_id) when is_binary(manifest_version_id) do
    with {:ok, version} <- get_manifest(manifest_version_id) do
      {:ok, manifest_summary(version)}
    end
  end

  @doc """
  Returns manifest-scoped submit targets for one persisted manifest version.
  """
  @spec manifest_targets(String.t()) :: {:ok, manifest_targets()} | {:error, term()}
  def manifest_targets(manifest_version_id) when is_binary(manifest_version_id) do
    with {:ok, version} <- get_manifest(manifest_version_id) do
      {:ok,
       %{
         manifest_version_id: manifest_version_id,
         assets: manifest_asset_targets(version),
         pipelines: manifest_pipeline_targets(version)
       }}
    end
  end

  @doc """
  Returns submit targets for the currently active manifest version.
  """
  @spec active_manifest_targets() :: {:ok, manifest_targets()} | {:error, term()}
  def active_manifest_targets do
    with {:ok, manifest_version_id} <- active_manifest() do
      manifest_targets(manifest_version_id)
    end
  end

  @doc """
  Submits one asset run by manifest-scoped target id.
  """
  @spec submit_asset_run_for_manifest(String.t(), String.t(), keyword()) ::
          {:ok, run_id()} | {:error, term()}
  def submit_asset_run_for_manifest(manifest_version_id, target_id, opts \\ [])
      when is_binary(manifest_version_id) and is_binary(target_id) and is_list(opts) do
    with {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, asset_ref} <- resolve_asset_target_ref(version, target_id) do
      submit_asset_run(asset_ref, Keyword.put(opts, :manifest_version_id, manifest_version_id))
    end
  end

  @doc """
  Submits one pipeline run by manifest-scoped target id.
  """
  @spec submit_pipeline_run_for_manifest(String.t(), String.t(), keyword()) ::
          {:ok, run_id()} | {:error, term()}
  def submit_pipeline_run_for_manifest(manifest_version_id, target_id, opts \\ [])
      when is_binary(manifest_version_id) and is_binary(target_id) and is_list(opts) do
    with {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, pipeline_module} <- resolve_pipeline_target_module(version, target_id) do
      submit_pipeline_run(
        pipeline_module,
        Keyword.put(opts, :manifest_version_id, manifest_version_id)
      )
    end
  end

  @doc """
  Sets the active manifest version used by default for new runs.
  """
  @spec activate_manifest(String.t()) :: :ok | {:error, term()}
  def activate_manifest(manifest_version_id) when is_binary(manifest_version_id) do
    ManifestStore.set_active_manifest(manifest_version_id)
  end

  @doc """
  Returns the currently active manifest version id.
  """
  @spec active_manifest() :: {:ok, String.t()} | {:error, term()}
  def active_manifest, do: ManifestStore.get_active_manifest()

  @doc """
  Submits one asset run pinned to a manifest version.

  When `:manifest_version_id` is not provided, the active manifest version is used.
  """
  @spec submit_asset_run(Favn.Ref.t(), keyword()) :: {:ok, run_id()} | {:error, term()}
  def submit_asset_run(asset_ref, opts \\ [])

  def submit_asset_run({module, name} = asset_ref, opts)
      when is_atom(module) and is_atom(name) and is_list(opts) do
    RunManager.submit_asset_run(asset_ref, opts)
  end

  def submit_asset_run(_asset_ref, _opts), do: {:error, :invalid_target_ref}

  @doc """
  Submits one pipeline run from explicit target refs or a persisted manifest pipeline module.
  """
  @spec submit_pipeline_run([Favn.Ref.t()], keyword()) :: {:ok, run_id()} | {:error, term()}
  @spec submit_pipeline_run(module(), keyword()) :: {:ok, run_id()} | {:error, term()}
  def submit_pipeline_run(target_or_module, opts \\ [])

  def submit_pipeline_run(target_refs, opts) when is_list(target_refs) and is_list(opts) do
    RunManager.submit_pipeline_run(target_refs, opts)
  end

  def submit_pipeline_run(pipeline_module, opts)
      when is_atom(pipeline_module) and is_list(opts) do
    RunManager.submit_pipeline_module_run(pipeline_module, opts)
  end

  @doc """
  Inspects one manifest-owned asset relation through the configured runner boundary.
  """
  @spec inspect_manifest_asset(String.t(), String.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def inspect_manifest_asset(manifest_version_id, target_id, opts \\ [])
      when is_binary(manifest_version_id) and is_binary(target_id) and is_list(opts) do
    with {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, asset_ref} <- resolve_asset_target_ref(version, target_id),
         :ok <- validate_runner_client(configured_runner_client()),
         :ok <- configured_runner_client().register_manifest(version, configured_runner_opts()) do
      request = %RelationInspectionRequest{
        manifest_version_id: manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: asset_ref,
        sample_limit: Keyword.get(opts, :sample_limit, 20)
      }

      configured_runner_client().inspect_relation(request, configured_runner_opts())
    end
  end

  @doc """
  Submits a parent pipeline backfill run and child pipeline runs for each resolved anchor.

  This control-plane entrypoint is used by the private HTTP surface and internal
  callers. It resolves `:range_request` through `Favn.Backfill.RangeResolver`,
  persists a parent `:backfill_pipeline` run, creates one normalized backfill
  window row per anchor, and submits one child pipeline run per window.

  Required options:

  - `:range_request` - `Favn.Backfill.RangeRequest.t/0`, map, or keyword input.

  Common options:

  - `:manifest_version_id` - defaults to the active manifest.
  - `:coverage_baseline_id` - associates requested windows with a projected
    coverage baseline.
  - `:metadata` - user metadata merged into the parent run metadata.
  - `:max_attempts`, `:retry_backoff_ms`, and `:timeout_ms` - forwarded to child
    runs.
  """
  @spec submit_pipeline_backfill(module(), keyword()) :: {:ok, run_id()} | {:error, term()}
  def submit_pipeline_backfill(pipeline_module, opts \\ [])

  def submit_pipeline_backfill(pipeline_module, opts)
      when is_atom(pipeline_module) and is_list(opts) do
    BackfillManager.submit_pipeline_backfill(pipeline_module, opts)
  end

  def submit_pipeline_backfill(_pipeline_module, _opts), do: {:error, :invalid_pipeline_module}

  @doc """
  Submits one pipeline backfill by manifest-scoped target id.
  """
  @spec submit_pipeline_backfill_for_manifest(String.t(), String.t(), keyword()) ::
          {:ok, run_id()} | {:error, term()}
  def submit_pipeline_backfill_for_manifest(manifest_version_id, target_id, opts \\ [])
      when is_binary(manifest_version_id) and is_binary(target_id) and is_list(opts) do
    with {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, pipeline_module} <- resolve_pipeline_target_module(version, target_id) do
      submit_pipeline_backfill(
        pipeline_module,
        Keyword.put(opts, :manifest_version_id, manifest_version_id)
      )
    end
  end

  @doc """
  Lists normalized backfill-window ledger rows.
  """
  @spec list_backfill_windows(keyword()) ::
          {:ok, FavnOrchestrator.Page.t(FavnOrchestrator.Backfill.BackfillWindow.t())}
          | {:error, term()}
  def list_backfill_windows(filters \\ []) when is_list(filters) do
    Storage.list_backfill_windows(filters)
  end

  @doc """
  Lists projected coverage baselines.
  """
  @spec list_coverage_baselines(keyword()) ::
          {:ok, FavnOrchestrator.Page.t(FavnOrchestrator.Backfill.CoverageBaseline.t())}
          | {:error, term()}
  def list_coverage_baselines(filters \\ []) when is_list(filters) do
    Storage.list_coverage_baselines(filters)
  end

  @doc """
  Lists latest asset/window states.
  """
  @spec list_asset_window_states(keyword()) ::
          {:ok, FavnOrchestrator.Page.t(FavnOrchestrator.Backfill.AssetWindowState.t())}
          | {:error, term()}
  def list_asset_window_states(filters \\ []) when is_list(filters) do
    Storage.list_asset_window_states(filters)
  end

  @doc """
  Repairs derived operational-backfill read models from persisted run snapshots.

  The repair source is authoritative run state. By default this returns a dry-run
  report; pass `apply: true` to replace scoped derived read models. Repair never
  appends run events or rewrites run snapshots.
  """
  @spec repair_backfill_projections(keyword()) :: {:ok, map()} | {:error, term()}
  def repair_backfill_projections(opts \\ []) when is_list(opts) do
    BackfillRepair.repair(opts)
  end

  @doc """
  Reruns the latest attempt for one failed backfill window.
  """
  @spec rerun_backfill_window(String.t(), module(), String.t(), keyword()) ::
          {:ok, run_id()} | {:error, term()}
  def rerun_backfill_window(backfill_run_id, pipeline_module, window_key, opts \\ [])
      when is_binary(backfill_run_id) and is_atom(pipeline_module) and is_binary(window_key) and
             is_list(opts) do
    with {:ok, window} <-
           Storage.get_backfill_window(backfill_run_id, pipeline_module, window_key),
         :ok <- ensure_window_rerunnable(window),
         source_run_id when is_binary(source_run_id) <- window.latest_attempt_run_id,
         {:ok, anchor} <-
           Anchor.new(window.window_kind, window.window_start_at, window.window_end_at,
             timezone: window.timezone
           ) do
      rerun(
        source_run_id,
        opts
        |> Keyword.put(:anchor_window, anchor)
        |> Keyword.put(:parent_run_id, backfill_run_id)
        |> Keyword.put(:root_run_id, backfill_run_id)
        |> Keyword.put(:trigger, %{
          kind: :backfill,
          backfill_run_id: backfill_run_id,
          window_key: window_key,
          rerun: true
        })
        |> Keyword.update(:metadata, %{backfill_window_rerun: true}, fn metadata ->
          Map.merge(metadata, %{backfill_window_rerun: true})
        end)
      )
    else
      nil -> {:error, :backfill_window_has_no_attempt}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Requests cancellation for one run and forwards cancellation to the runner when work is in flight.
  """
  @spec cancel_run(run_id(), map()) :: :ok | {:error, term()}
  def cancel_run(run_id, reason \\ %{}) when is_binary(run_id) and is_map(reason) do
    RunManager.cancel_run(run_id, reason)
  end

  @doc """
  Submits a rerun pinned to the source run's manifest version.
  """
  @spec rerun(run_id(), keyword()) :: {:ok, run_id()} | {:error, term()}
  def rerun(source_run_id, opts \\ []) when is_binary(source_run_id) and is_list(opts) do
    RunManager.rerun(source_run_id, opts)
  end

  defp ensure_window_rerunnable(%FavnOrchestrator.Backfill.BackfillWindow{status: status})
       when status in [:error, :cancelled, :timed_out, :partial],
       do: :ok

  defp ensure_window_rerunnable(_window), do: {:error, :backfill_window_not_rerunnable}

  @doc """
  Returns one persisted run snapshot.
  """
  @spec get_run(run_id()) :: {:ok, Favn.Run.t()} | {:error, term()}
  def get_run(run_id) when is_binary(run_id) do
    case Storage.get_run(run_id) do
      {:ok, run_state} -> {:ok, Projector.project_run(run_state)}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Lists persisted run snapshots.
  """
  @spec list_runs(keyword()) :: {:ok, [Favn.Run.t()]} | {:error, term()}
  def list_runs(opts \\ []) when is_list(opts) do
    case Storage.list_runs(opts) do
      {:ok, runs} -> {:ok, Projector.project_runs(runs)}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Lists persisted run events for one run.
  """
  @spec list_run_events(run_id(), keyword()) :: {:ok, [RunEvent.t()]} | {:error, term()}
  def list_run_events(run_id, opts \\ []) when is_binary(run_id) and is_list(opts) do
    with {:ok, events} <- Storage.list_run_events(run_id),
         :ok <- validate_run_event_opts(opts) do
      {:ok,
       events
       |> Enum.map(&RunEvent.from_map/1)
       |> filter_run_events(opts)
       |> maybe_limit_run_events(opts)}
    end
  end

  @doc """
  Lists replayable events for one run stream after an optional persisted cursor sequence.
  """
  @spec list_run_stream_events(run_id(), keyword()) :: {:ok, [RunEvent.t()]} | {:error, term()}
  def list_run_stream_events(run_id, opts \\ []) when is_binary(run_id) and is_list(opts) do
    after_sequence = Keyword.get(opts, :after_sequence)
    limit = Keyword.get(opts, :limit, 200)

    with true <- is_integer(limit) and limit > 0,
         {:ok, events} <- list_run_events(run_id) do
      case after_sequence do
        nil ->
          {:ok, Enum.take(events, limit)}

        sequence when is_integer(sequence) and sequence >= 0 ->
          replay_after_sequence(events, sequence, limit)

        _ ->
          {:error, :cursor_invalid}
      end
    else
      false -> {:error, :cursor_invalid}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Subscribes the current process to one run-scoped live event stream.
  """
  @spec subscribe_run(run_id()) :: :ok | {:error, term()}
  def subscribe_run(run_id) when is_binary(run_id), do: Events.subscribe_run(run_id)
  def subscribe_run(_run_id), do: {:error, :invalid_run_id}

  @doc """
  Unsubscribes the current process from one run-scoped live event stream.
  """
  @spec unsubscribe_run(run_id()) :: :ok
  def unsubscribe_run(run_id) when is_binary(run_id), do: Events.unsubscribe_run(run_id)
  def unsubscribe_run(_run_id), do: :ok

  @doc """
  Subscribes the current process to the global runs live event stream.
  """
  @spec subscribe_runs() :: :ok | {:error, term()}
  def subscribe_runs, do: Events.subscribe_runs()

  @doc """
  Unsubscribes the current process from the global runs live event stream.
  """
  @spec unsubscribe_runs() :: :ok
  def unsubscribe_runs, do: Events.unsubscribe_runs()

  @doc """
  Reloads scheduler entries from the active manifest.
  """
  @spec reload_scheduler() :: :ok | {:error, term()}
  def reload_scheduler do
    SchedulerRuntime.reload()
  catch
    :exit, {:noproc, _} -> {:error, :scheduler_not_running}
  end

  @doc """
  Forces one scheduler evaluation tick.
  """
  @spec tick_scheduler() :: :ok | {:error, term()}
  def tick_scheduler do
    SchedulerRuntime.tick()
  catch
    :exit, {:noproc, _} -> {:error, :scheduler_not_running}
  end

  @doc """
  Lists scheduler runtime entries derived from the active manifest.
  """
  @spec scheduled_entries() :: [SchedulerEntry.t()] | {:error, term()}
  def scheduled_entries do
    SchedulerRuntime.inspect_entries()
  catch
    :exit, {:noproc, _} -> {:error, :scheduler_not_running}
  end

  @doc """
  Lists operator-facing schedule inspection entries.

  Falls back to active-manifest schedule descriptors when scheduler runtime is not running.
  """
  @spec list_schedule_entries() :: {:ok, [SchedulerEntry.t()]} | {:error, term()}
  def list_schedule_entries do
    case scheduled_entries() do
      entries when is_list(entries) -> {:ok, entries}
      {:error, :scheduler_not_running} -> list_schedule_entries_from_active_manifest()
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Returns one schedule inspection entry by remote schedule id.
  """
  @spec get_schedule_entry(String.t()) :: {:ok, SchedulerEntry.t()} | {:error, term()}
  def get_schedule_entry(schedule_id) when is_binary(schedule_id) do
    with {:ok, entries} <- list_schedule_entries() do
      case Enum.find(entries, &(schedule_entry_id(&1) == schedule_id)) do
        nil -> {:error, :schedule_not_found}
        %SchedulerEntry{} = entry -> {:ok, entry}
      end
    end
  end

  @doc """
  Returns the stable remote id for one schedule inspection entry.
  """
  @spec schedule_entry_id(SchedulerEntry.t()) :: String.t()
  def schedule_entry_id(%SchedulerEntry{} = entry) do
    pipeline_module = entry.pipeline_module || Favn.Unknown
    schedule_name = entry.schedule_id || :unknown

    "schedule:" <> Atom.to_string(pipeline_module) <> ":" <> Atom.to_string(schedule_name)
  end

  defp validate_run_event_opts(opts) do
    after_sequence = Keyword.get(opts, :after_sequence)
    limit = Keyword.get(opts, :limit)

    cond do
      not is_nil(after_sequence) and (not is_integer(after_sequence) or after_sequence < 0) ->
        {:error, :invalid_opts}

      not is_nil(limit) and (not is_integer(limit) or limit <= 0) ->
        {:error, :invalid_opts}

      true ->
        :ok
    end
  end

  defp filter_run_events(events, opts) do
    case Keyword.get(opts, :after_sequence) do
      sequence when is_integer(sequence) and sequence >= 0 ->
        Enum.filter(events, &(&1.sequence > sequence))

      _ ->
        events
    end
  end

  defp maybe_limit_run_events(events, opts) do
    case Keyword.get(opts, :limit) do
      limit when is_integer(limit) and limit > 0 -> Enum.take(events, limit)
      _ -> events
    end
  end

  defp replay_after_sequence(events, sequence, limit) do
    if sequence == 0 do
      {:ok, Enum.take(events, limit)}
    else
      if Enum.any?(events, &(&1.sequence == sequence)) do
        {:ok,
         events
         |> Enum.filter(&(&1.sequence > sequence))
         |> Enum.take(limit)}
      else
        {:error, :cursor_invalid}
      end
    end
  end

  defp list_schedule_entries_from_active_manifest do
    with {:ok, manifest_version_id} <- active_manifest(),
         {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         {:ok, runtime_entries} <- ManifestEntries.discover(version, index) do
      entries =
        runtime_entries
        |> Map.values()
        |> Enum.map(&SchedulerEntry.from_runtime/1)
        |> Enum.sort_by(&{inspect(&1.pipeline_module), inspect(&1.schedule_id)})

      {:ok, entries}
    end
  end

  defp manifest_summary(%Version{} = version) do
    manifest = version.manifest

    %{
      manifest_version_id: version.manifest_version_id,
      content_hash: version.content_hash,
      asset_count: list_count(manifest.assets),
      pipeline_count: list_count(manifest.pipelines),
      schedule_count: list_count(manifest.schedules)
    }
  end

  defp manifest_asset_targets(%Version{} = version) do
    version.manifest.assets
    |> List.wrap()
    |> Enum.map(fn asset ->
      target_ref = asset.ref

      %{
        target_id: target_id_for_asset(target_ref),
        label: inspect(target_ref),
        asset_ref: ref_to_string(target_ref),
        type: atom_name(asset.type),
        relation: relation_dto(asset.relation),
        metadata: normalize_map(asset.metadata),
        runtime_config: normalize_data(asset.runtime_config),
        depends_on: Enum.map(List.wrap(asset.depends_on), &ref_to_string/1),
        materialization: normalize_data(asset.materialization),
        window: normalize_data(asset.window)
      }
    end)
    |> Enum.sort_by(& &1.label)
  end

  defp manifest_pipeline_targets(%Version{} = version) do
    version.manifest.pipelines
    |> List.wrap()
    |> Enum.map(fn pipeline ->
      target_module = pipeline.module

      %{
        target_id: target_id_for_pipeline(target_module),
        label: inspect(target_module),
        window: window_policy_dto(pipeline.window)
      }
    end)
    |> Enum.sort_by(& &1.label)
  end

  defp window_policy_dto(nil), do: nil

  defp window_policy_dto(%Policy{} = policy) do
    %{
      kind: Atom.to_string(policy.kind),
      anchor: Atom.to_string(policy.anchor),
      timezone: policy.timezone,
      allow_full_load: policy.allow_full_load
    }
  end

  defp window_policy_dto(policy) do
    policy
    |> Policy.from_value!()
    |> window_policy_dto()
  end

  defp relation_dto(nil), do: nil

  defp relation_dto(relation) do
    case Favn.RelationRef.new!(relation) do
      ref ->
        %{
          connection: atom_name(ref.connection),
          catalog: ref.catalog,
          schema: ref.schema,
          name: ref.name
        }
    end
  rescue
    ArgumentError -> normalize_data(relation)
  end

  defp normalize_map(value) when is_map(value), do: value
  defp normalize_map(_value), do: %{}

  defp normalize_data(%Favn.RuntimeConfig.Ref{} = ref) do
    %{
      provider: atom_name(ref.provider),
      key: ref.key,
      secret: ref.secret?,
      required: ref.required?
    }
  end

  defp normalize_data(value) when is_map(value) do
    value
    |> Enum.map(fn {key, val} -> {to_string(key), normalize_data(val)} end)
    |> Map.new()
  end

  defp normalize_data(value) when is_list(value), do: Enum.map(value, &normalize_data/1)
  defp normalize_data({module, name}), do: ref_to_string({module, name})
  defp normalize_data(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp normalize_data(value) when is_atom(value), do: atom_name(value)
  defp normalize_data(value), do: value

  defp resolve_asset_target_ref(%Version{} = version, target_id) when is_binary(target_id) do
    case Enum.find(
           List.wrap(version.manifest.assets),
           &(target_id_for_asset(&1.ref) == target_id)
         ) do
      %{ref: target_ref} -> {:ok, target_ref}
      _ -> {:error, :invalid_asset_target}
    end
  end

  defp resolve_pipeline_target_module(%Version{} = version, target_id)
       when is_binary(target_id) do
    case Enum.find(
           List.wrap(version.manifest.pipelines),
           &(target_id_for_pipeline(&1.module) == target_id)
         ) do
      %{module: target_module} -> {:ok, target_module}
      _ -> {:error, :invalid_pipeline_target}
    end
  end

  defp target_id_for_asset({module, name}) when is_atom(module) and is_atom(name) do
    "asset:" <> Atom.to_string(module) <> ":" <> Atom.to_string(name)
  end

  defp target_id_for_pipeline(module) when is_atom(module) do
    "pipeline:" <> Atom.to_string(module)
  end

  defp configured_runner_client do
    Application.get_env(:favn_orchestrator, :runner_client, nil)
  end

  defp configured_runner_opts do
    Application.get_env(:favn_orchestrator, :runner_client_opts, [])
  end

  defp validate_runner_client(module) when is_atom(module) do
    callbacks = RunnerClient.behaviour_info(:callbacks)

    with {:module, ^module} <- Code.ensure_loaded(module),
         true <-
           Enum.all?(callbacks, fn {name, arity} -> function_exported?(module, name, arity) end) do
      :ok
    else
      _ -> {:error, :runner_client_not_available}
    end
  end

  defp validate_runner_client(_module), do: {:error, :runner_client_not_available}

  defp ref_to_string({module, name}) when is_atom(module) and is_atom(name) do
    Atom.to_string(module) <> ":" <> Atom.to_string(name)
  end

  defp ref_to_string(value), do: inspect(value)

  defp atom_name(nil), do: nil
  defp atom_name(value) when is_atom(value), do: Atom.to_string(value)
  defp atom_name(value), do: to_string(value)

  defp list_count(value) when is_list(value), do: length(value)
  defp list_count(_value), do: 0
end
