defmodule FavnOrchestrator do
  @moduledoc """
  Orchestrator control-plane facade for manifest-pinned run submission.
  """

  alias Favn.Manifest.Version
  alias FavnOrchestrator.Events
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.Scheduler.Runtime, as: SchedulerRuntime
  alias FavnOrchestrator.SchedulerEntry
  alias FavnOrchestrator.Storage

  @type run_id :: String.t()

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
end
