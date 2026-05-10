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
  alias Favn.Window.Request, as: WindowRequest
  alias Favn.Window.Spec, as: WindowSpec
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Backfill.Repair, as: BackfillRepair
  alias FavnOrchestrator.BackfillManager
  alias FavnOrchestrator.Diagnostics
  alias FavnOrchestrator.Events
  alias FavnOrchestrator.Freshness.Query, as: FreshnessQuery
  alias FavnOrchestrator.LogWriter
  alias FavnOrchestrator.Logs
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Page
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

  @type asset_catalogue_entry :: %{
          required(:target_id) => String.t(),
          required(:label) => String.t(),
          optional(:asset_ref) => String.t(),
          optional(:type) => String.t(),
          optional(:relation) => map() | nil,
          optional(:metadata) => map(),
          required(:status) => :healthy | :running | :failed | :unknown,
          required(:latest_run_id) => String.t() | nil,
          required(:latest_run_status) => atom() | nil,
          required(:latest_run_at) => DateTime.t() | nil
        }

  @type asset_timeline_window :: %{
          required(:id) => String.t(),
          required(:label) => String.t(),
          required(:date) => Date.t(),
          required(:range) => String.t(),
          required(:status) => :healthy | :running | :failed | :unknown,
          required(:latest_run_id) => String.t() | nil,
          required(:latest_run_status) => atom() | nil,
          required(:latest_run_at) => DateTime.t() | nil,
          required(:run_enabled?) => boolean(),
          required(:run_disabled_reason) => atom() | nil,
          required(:run_label) => String.t()
        }

  @type asset_detail :: %{
          required(:target_id) => String.t(),
          required(:manifest_version_id) => String.t(),
          required(:label) => String.t(),
          required(:name) => String.t(),
          required(:asset_ref) => String.t() | nil,
          required(:relation) => map() | nil,
          required(:type) => String.t() | nil,
          required(:status) => :healthy | :running | :failed | :unknown,
          required(:latest_run_id) => String.t() | nil,
          required(:latest_run_status) => atom() | nil,
          required(:latest_run_at) => DateTime.t() | nil,
          required(:window) => map() | nil,
          required(:timeline) => [asset_timeline_window()]
        }

  @doc """
  Returns redacted operator diagnostics for the orchestrator runtime.
  """
  @spec diagnostics(keyword()) :: map()
  def diagnostics(opts \\ []) when is_list(opts), do: Diagnostics.report(opts)

  @doc """
  Registers one manifest version in orchestrator storage.
  """
  @spec register_manifest(Version.t()) :: :ok | {:error, term()}
  def register_manifest(%Version{} = version), do: ManifestStore.register_manifest(version)

  @doc """
  Publishes one manifest version, returning the canonical stored version for duplicate content.
  """
  @spec publish_manifest(Version.t()) ::
          {:ok, :published | :already_published, Version.t()} | {:error, term()}
  def publish_manifest(%Version{} = version), do: ManifestStore.publish_manifest(version)

  @doc """
  Returns one persisted manifest version.
  """
  @spec get_manifest(String.t()) :: {:ok, Version.t()} | {:error, term()}
  def get_manifest(manifest_version_id) when is_binary(manifest_version_id) do
    ManifestStore.get_manifest(manifest_version_id)
  end

  @doc """
  Returns one persisted manifest version by content hash.
  """
  @spec get_manifest_by_content_hash(String.t()) :: {:ok, Version.t()} | {:error, term()}
  def get_manifest_by_content_hash(content_hash) when is_binary(content_hash) do
    ManifestStore.get_manifest_by_content_hash(content_hash)
  end

  @doc """
  Registers a persisted manifest version with the configured runner boundary.
  """
  @spec register_manifest_with_runner(String.t()) :: {:ok, map()} | {:error, term()}
  def register_manifest_with_runner(manifest_version_id) when is_binary(manifest_version_id) do
    runner_client = configured_runner_client()
    runner_opts = configured_runner_opts()

    with {:ok, version} <- get_manifest(manifest_version_id),
         :ok <- validate_runner_client(runner_client) do
      content_hash = version.content_hash

      case runner_client.register_manifest(version, runner_opts) do
        :ok ->
          {:ok, runner_manifest_registration(version, runner_client, :accepted)}

        {:error, {:manifest_version_conflict, ^manifest_version_id, ^content_hash, ^content_hash}} ->
          {:ok, runner_manifest_registration(version, runner_client, :already_registered)}

        {:error, {:manifest_version_conflict, ^manifest_version_id, _existing, _incoming}} ->
          {:error, :runner_manifest_conflict}

        {:error, reason} ->
          {:error, runner_registration_error(reason)}
      end
    end
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
  Returns operator-facing catalogue entries for the currently active manifest.

  Entries are manifest target metadata enriched with latest known freshness/run
  state. Missing runtime state is represented explicitly as `:unknown` with no
  latest run timestamp.
  """
  @spec active_asset_catalogue() :: {:ok, [asset_catalogue_entry()]} | {:error, term()}
  def active_asset_catalogue do
    with {:ok, manifest_version_id} <- active_manifest(),
         {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, freshness_states} <- catalogue_freshness_states(manifest_version_id),
         {:ok, runs} <- catalogue_runs(manifest_version_id) do
      {:ok, asset_catalogue_entries(version, freshness_states, runs)}
    end
  end

  @doc """
  Returns an operator-facing detail read model for one active asset target.

  The detail is a DTO built at the orchestrator boundary. It includes manifest
  target metadata, latest known freshness/run state, and a conservative 30-day
  daily timeline. Missing runtime evidence is represented as `:unknown`.
  """
  @spec active_asset_detail(String.t(), keyword()) :: {:ok, asset_detail()} | {:error, term()}
  def active_asset_detail(target_id, opts \\ []) when is_binary(target_id) and is_list(opts) do
    with {:ok, manifest_version_id} <- active_manifest(),
         {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, freshness_states} <- detail_freshness_states(manifest_version_id),
         {:ok, runs} <- catalogue_runs(manifest_version_id) do
      case asset_detail_entry(version, target_id, freshness_states, runs, opts) do
        nil -> {:error, :not_found}
        detail -> {:ok, detail}
      end
    end
  end

  @doc """
  Persists and publishes one trusted backend log entry.
  """
  @spec emit_log(term()) :: {:ok, [Favn.Log.Entry.t()]} | {:error, term()}
  def emit_log(entry), do: LogWriter.write(entry)

  @doc """
  Persists and publishes trusted backend log entries as one batch.
  """
  @spec emit_logs([term()]) :: {:ok, [Favn.Log.Entry.t()]} | {:error, term()}
  def emit_logs(entries) when is_list(entries), do: LogWriter.write(entries)

  @doc """
  Lists persisted backend logs matching the given filter.
  """
  @spec list_logs(term(), keyword()) :: {:ok, Page.t(Favn.Log.Entry.t())} | {:error, term()}
  def list_logs(filter \\ default_log_filter(), opts \\ []) when is_list(opts) do
    Storage.list_logs(filter, opts)
  end

  @doc """
  Replays persisted backend logs after an authoritative log cursor.
  """
  @spec replay_logs(term(), term(), keyword()) :: {:ok, [Favn.Log.Entry.t()]} | {:error, term()}
  def replay_logs(cursor, filter \\ default_log_filter(), opts \\ []) when is_list(opts) do
    Storage.replay_logs_after(cursor, filter, opts)
  end

  @doc """
  Subscribes the caller to live backend logs matching the given filter.
  """
  @spec subscribe_logs(term()) :: {:ok, term()} | {:error, term()}
  def subscribe_logs(filter \\ default_log_filter()), do: Logs.subscribe_logs(filter)

  @doc """
  Unsubscribes the caller from a prior backend log subscription or equivalent filter.
  """
  @spec unsubscribe_logs(term()) :: :ok
  def unsubscribe_logs(subscription_or_filter), do: Logs.unsubscribe_logs(subscription_or_filter)

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
  Submits a manifest-pinned asset run for one stable asset detail window id.
  """
  @spec submit_asset_window_run(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, run_id()} | {:error, term()}
  def submit_asset_window_run(manifest_version_id, target_id, window_id, opts \\ [])
      when is_binary(manifest_version_id) and is_binary(target_id) and is_binary(window_id) and
             is_list(opts) do
    with {:ok, version} <- get_manifest(manifest_version_id),
         {:ok, asset} <- resolve_asset_target(version, target_id),
         {:ok, window_request} <- window_request_from_id(window_id),
         {:ok, anchor_window} <- resolve_asset_window(asset, window_request) do
      submit_asset_run(
        asset.ref,
        opts
        |> Keyword.put(:manifest_version_id, manifest_version_id)
        |> Keyword.put(:anchor_window, anchor_window)
        |> put_window_run_metadata(window_id, anchor_window)
      )
    end
  end

  defp put_window_run_metadata(opts, window_id, %Anchor{} = anchor_window) do
    selected_window_metadata = window_run_metadata(window_id, anchor_window)

    case Keyword.fetch(opts, :metadata) do
      :error ->
        Keyword.put(opts, :metadata, selected_window_metadata)

      {:ok, metadata} when is_map(metadata) ->
        Keyword.put(opts, :metadata, Map.merge(metadata, selected_window_metadata))

      {:ok, _invalid_metadata} ->
        opts
    end
  end

  defp window_run_metadata(window_id, %Anchor{} = anchor_window) do
    %{
      selected_window: %{
        id: window_id,
        kind: anchor_window.kind,
        key: anchor_window.key,
        start_at: anchor_window.start_at,
        end_at: anchor_window.end_at,
        timezone: anchor_window.timezone
      }
    }
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

  Freshness-related options:

  - `:refresh` or `:refresh_policy` accepts `:auto`, `:force`, `:missing`,
    `{:force_assets, refs}`, `{:force_assets, refs, include_upstream: true}`, or
    equivalent maps. See `FavnOrchestrator.RefreshPolicy`.
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

  Freshness-related options:

  - `:refresh` or `:refresh_policy` accepts `:auto`, `:force`, `:missing`,
    `{:force_assets, refs}`, `{:force_assets, refs, include_upstream: true}`, or
    equivalent maps. See `FavnOrchestrator.RefreshPolicy`.

  Under `:auto`, manifest freshness policies decide which planned nodes run or
  skip. `:missing` skips nodes with prior successful freshness state. `:force`
  runs every planned node.
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
  - `:refresh` or `:refresh_policy` - forwarded to child pipeline runs. Defaults
    to `:missing` when neither option is provided.
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
  Returns one internal freshness state for an asset/freshness key.

  `freshness_key` must come from `Favn.Freshness.Key`, for example `"latest"`, a
  calendar key, or a window key. This is an orchestrator control-plane API, not a
  `favn_view` public endpoint.
  """
  @spec get_asset_freshness(Favn.Ref.t(), String.t()) ::
          {:ok, FavnOrchestrator.AssetFreshnessState.t()} | {:error, term()}
  def get_asset_freshness(asset_ref, freshness_key),
    do: FreshnessQuery.get_asset_freshness(asset_ref, freshness_key)

  @doc """
  Lists internal asset freshness states.

  Common filters are `:asset_ref_module`, `:asset_ref_name`, `:freshness_key`,
  `:status`, `:manifest_version_id`, `:limit`, and `:offset`.
  """
  @spec list_asset_freshness(keyword()) ::
          {:ok, FavnOrchestrator.Page.t(FavnOrchestrator.AssetFreshnessState.t())}
          | {:error, term()}
  def list_asset_freshness(filters \\ []) when is_list(filters),
    do: FreshnessQuery.list_asset_freshness(filters)

  @doc """
  Explains whether a stored freshness state is stale against current upstream versions.

  Options:

  - `:freshness_key` - downstream freshness key, defaults to `"latest"`.
  - `:upstream_node_keys` - concrete planned upstream node keys to compare.

  Returns `status: :fresh` with no reasons when stored input versions still match
  the current upstream freshness versions. Returns `status: :stale` with explicit
  stale reasons when an upstream version is missing or changed.
  """
  @spec explain_asset_staleness(Favn.Ref.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def explain_asset_staleness(asset_ref, opts \\ []) when is_list(opts),
    do: FreshnessQuery.explain_asset_staleness(asset_ref, opts)

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
  Lists persisted runs that have not reached a terminal status.
  """
  @spec list_in_flight_runs() :: {:ok, [Favn.Run.t()]} | {:error, term()}
  def list_in_flight_runs do
    with {:ok, pending} <- list_runs(status: :pending),
         {:ok, running} <- list_runs(status: :running) do
      {:ok, Enum.sort_by(pending ++ running, &run_updated_at_sort_key/1, :desc)}
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

  defp run_updated_at_sort_key(%{updated_at: %DateTime{} = updated_at}),
    do: DateTime.to_unix(updated_at, :microsecond)

  defp run_updated_at_sort_key(_run), do: 0

  @doc """
  Lists replayable events for the global runs stream after an optional persisted cursor.
  """
  @spec list_global_run_stream_events(keyword()) :: {:ok, [RunEvent.t()]} | {:error, term()}
  def list_global_run_stream_events(opts \\ []) when is_list(opts) do
    after_global_sequence = Keyword.get(opts, :after_global_sequence)
    limit = Keyword.get(opts, :limit, 200)

    with true <- is_integer(limit) and limit > 0,
         true <- is_nil(after_global_sequence) or is_integer(after_global_sequence),
         {:ok, events} <-
           Storage.list_global_run_events(
             after_global_sequence: after_global_sequence,
             limit: limit
           ) do
      {:ok, Enum.map(events, &RunEvent.from_map/1)}
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
    |> Enum.map(&manifest_asset_target/1)
    |> Enum.sort_by(& &1.label)
  end

  defp catalogue_freshness_states(manifest_version_id) do
    case list_asset_freshness(
           manifest_version_id: manifest_version_id,
           freshness_key: Favn.Freshness.Key.latest(),
           limit: Page.max_limit()
         ) do
      {:ok, page} -> {:ok, page.items}
      {:error, :asset_freshness_state_not_supported} -> {:ok, []}
      {:error, reason} -> {:error, reason}
    end
  end

  defp catalogue_runs(manifest_version_id) do
    list_runs(manifest_version_id: manifest_version_id)
  end

  defp detail_freshness_states(manifest_version_id) do
    case list_asset_freshness(manifest_version_id: manifest_version_id, limit: Page.max_limit()) do
      {:ok, page} -> {:ok, page.items}
      {:error, :asset_freshness_state_not_supported} -> {:ok, []}
      {:error, reason} -> {:error, reason}
    end
  end

  defp asset_catalogue_entries(%Version{} = version, freshness_states, runs) do
    freshness_by_ref = Map.new(freshness_states, &{freshness_ref_string(&1), &1})

    runs_by_ref =
      runs
      |> Enum.flat_map(&run_ref_entries/1)
      |> Enum.group_by(fn {ref_string, _run} -> ref_string end, fn {_ref_string, run} -> run end)
      |> Map.new(fn {ref_string, ref_runs} -> {ref_string, latest_run(ref_runs)} end)

    version.manifest.assets
    |> List.wrap()
    |> Enum.map(fn asset ->
      target = manifest_asset_target(asset)
      ref_string = ref_to_string(asset.ref)
      freshness = Map.get(freshness_by_ref, ref_string)
      run = Map.get(runs_by_ref, ref_string)

      target
      |> Map.put(:status, catalogue_status(freshness, run))
      |> Map.put(:latest_run_id, latest_run_id(freshness, run))
      |> Map.put(:latest_run_status, latest_run_status(freshness, run))
      |> Map.put(:latest_run_at, latest_run_at(freshness, run))
    end)
    |> Enum.sort_by(& &1.label)
  end

  defp asset_detail_entry(%Version{} = version, target_id, freshness_states, runs, opts) do
    version.manifest.assets
    |> List.wrap()
    |> Enum.find(&(manifest_asset_target(&1).target_id == target_id))
    |> case do
      nil ->
        nil

      asset ->
        target = manifest_asset_target(asset)
        ref_string = ref_to_string(asset.ref)
        latest_freshness = latest_freshness_for_ref(freshness_states, ref_string)
        latest_run = latest_run_for_ref(runs, ref_string)

        target
        |> Map.take([:target_id, :label, :asset_ref, :relation, :type, :window])
        |> Map.put(:manifest_version_id, version.manifest_version_id)
        |> Map.put(:name, asset_detail_name(target))
        |> Map.put(:status, catalogue_status(latest_freshness, latest_run))
        |> Map.put(:latest_run_id, latest_run_id(latest_freshness, latest_run))
        |> Map.put(:latest_run_status, latest_run_status(latest_freshness, latest_run))
        |> Map.put(:latest_run_at, latest_run_at(latest_freshness, latest_run))
        |> Map.put(
          :timeline,
          asset_detail_timeline(asset, latest_freshness, latest_run, freshness_states, opts)
        )
    end
  end

  defp latest_run_for_ref(runs, ref_string) do
    runs
    |> Enum.flat_map(&run_ref_entries/1)
    |> Enum.filter(fn {run_ref_string, _run} -> run_ref_string == ref_string end)
    |> Enum.map(fn {_run_ref_string, run} -> run end)
    |> latest_run()
  end

  defp latest_freshness_for_ref(freshness_states, ref_string) do
    Enum.find(freshness_states, fn state ->
      freshness_ref_string(state) == ref_string &&
        state.freshness_key == Favn.Freshness.Key.latest()
    end)
  end

  defp asset_detail_timeline(asset, latest_freshness, latest_run, freshness_states, opts) do
    selected_date = detail_timeline_selected_date(latest_freshness, latest_run, opts)
    window_states = asset_window_freshness_by_date(asset, freshness_states)
    latest_run_date = latest_run_at(latest_freshness, latest_run) |> detail_date_from_datetime()

    for offset <- 0..29 do
      date = Date.add(selected_date, offset - 29)
      date_iso = Date.to_iso8601(date)
      window_freshness = Map.get(window_states, date_iso)

      %{
        id: "window:day:#{date_iso}",
        label: Calendar.strftime(date, "%b %-d"),
        date: date,
        range: Calendar.strftime(date, "%b %-d, %Y"),
        status:
          timeline_status(window_freshness, latest_freshness, latest_run, date, latest_run_date),
        latest_run_id: latest_run_id(window_freshness, nil),
        latest_run_status: latest_run_status(window_freshness, nil),
        latest_run_at: latest_run_at(window_freshness, nil),
        run_label: "Run this window"
      }
      |> put_window_run_state(asset)
      |> maybe_put_latest_run(latest_freshness, latest_run, date, latest_run_date)
    end
  end

  defp put_window_run_state(window, %{window: nil}) do
    window
    |> Map.put(:run_enabled?, false)
    |> Map.put(:run_disabled_reason, :asset_has_no_window_policy)
  end

  defp put_window_run_state(%{id: window_id} = window, asset) do
    with {:ok, window_request} <- window_request_from_id(window_id),
         {:ok, _anchor_window} <- resolve_asset_window(asset, window_request) do
      window
      |> Map.put(:run_enabled?, true)
      |> Map.put(:run_disabled_reason, nil)
    else
      {:error, reason} ->
        window
        |> Map.put(:run_enabled?, false)
        |> Map.put(:run_disabled_reason, run_disabled_reason(reason))
    end
  end

  defp run_disabled_reason({:window_request_without_policy, _kind}),
    do: :asset_has_no_window_policy

  defp run_disabled_reason(_reason), do: :invalid_window

  defp detail_timeline_selected_date(latest_freshness, latest_run, opts) do
    case {opts[:today], latest_run_at(latest_freshness, latest_run)} do
      {%Date{} = date, _latest_run_at} -> date
      {_today, %DateTime{} = datetime} -> DateTime.to_date(datetime)
      _other -> Date.utc_today()
    end
  end

  defp asset_window_freshness_by_date(asset, freshness_states) do
    asset_ref_string = ref_to_string(asset.ref)

    freshness_states
    |> Enum.filter(&(freshness_ref_string(&1) == asset_ref_string))
    |> Enum.flat_map(fn state ->
      case window_date_from_freshness_key(state.freshness_key) do
        nil -> []
        date -> [{date, state}]
      end
    end)
    |> Map.new(fn {date, state} -> {date, state} end)
  end

  defp window_date_from_freshness_key("calendar:day:" <> rest) do
    rest
    |> String.split(":")
    |> List.last()
    |> case do
      <<_::binary-size(10)>> = date -> date
      _other -> nil
    end
  end

  defp window_date_from_freshness_key(_key), do: nil

  defp window_request_from_id("window:day:" <> date) do
    case WindowRequest.parse("day:#{date}") do
      {:ok, request} -> {:ok, request}
      {:error, reason} -> {:error, {:invalid_window_id, reason}}
    end
  end

  defp window_request_from_id(window_id), do: {:error, {:invalid_window_id, window_id}}

  defp resolve_asset_window(%{window: nil}, %WindowRequest{kind: kind}) do
    {:error, {:window_request_without_policy, kind}}
  end

  defp resolve_asset_window(%{window: %WindowSpec{} = spec}, %WindowRequest{kind: kind} = request) do
    if spec.kind == kind do
      WindowRequest.to_anchor(request, spec.timezone)
    else
      {:error, {:window_kind_mismatch, spec.kind, kind}}
    end
  end

  defp resolve_asset_window(asset, %WindowRequest{} = request) when is_atom(asset.window) do
    with {:ok, spec} <- WindowSpec.new(asset.window) do
      resolve_asset_window(%{asset | window: spec}, request)
    end
  end

  defp resolve_asset_window(%{window: %{} = window}, %WindowRequest{} = request) do
    case {Map.get(window, :kind) || Map.get(window, "kind"),
          Map.get(window, :timezone) || Map.get(window, "timezone")} do
      {kind, timezone} when not is_nil(kind) ->
        with {:ok, normalized_kind} <- Policy.normalize_kind(kind),
             {:ok, spec} <- WindowSpec.new(normalized_kind, timezone: timezone || "Etc/UTC") do
          resolve_asset_window(%{window: spec}, request)
        end

      _other ->
        {:error, :invalid_window_policy}
    end
  end

  defp detail_date_from_datetime(%DateTime{} = datetime), do: DateTime.to_date(datetime)
  defp detail_date_from_datetime(_datetime), do: nil

  defp timeline_status(
         %AssetFreshnessState{} = freshness,
         _latest_freshness,
         _latest_run,
         _date,
         _latest_run_date
       ) do
    catalogue_status(freshness, nil)
  end

  defp timeline_status(nil, latest_freshness, latest_run, date, date) do
    catalogue_status(latest_freshness, latest_run)
  end

  defp timeline_status(nil, _latest_freshness, _latest_run, _date, _latest_run_date), do: :unknown

  defp maybe_put_latest_run(window, latest_freshness, latest_run, date, date) do
    window
    |> Map.put_new(:latest_run_id, latest_run_id(latest_freshness, latest_run))
    |> Map.put_new(:latest_run_status, latest_run_status(latest_freshness, latest_run))
    |> Map.put_new(:latest_run_at, latest_run_at(latest_freshness, latest_run))
    |> put_latest_run_if_missing(:latest_run_id, latest_run_id(latest_freshness, latest_run))
    |> put_latest_run_if_missing(
      :latest_run_status,
      latest_run_status(latest_freshness, latest_run)
    )
    |> put_latest_run_if_missing(:latest_run_at, latest_run_at(latest_freshness, latest_run))
  end

  defp maybe_put_latest_run(window, _latest_freshness, _latest_run, _date, _latest_run_date),
    do: window

  defp put_latest_run_if_missing(window, key, value) do
    if is_nil(window[key]), do: Map.put(window, key, value), else: window
  end

  defp asset_detail_name(%{relation: relation, asset_ref: asset_ref, label: label}) do
    relation_name(relation) || asset_ref_name(asset_ref) || label
  end

  defp relation_name(%{name: name}) when is_binary(name), do: name
  defp relation_name(%{"name" => name}) when is_binary(name), do: name
  defp relation_name(_relation), do: nil

  defp asset_ref_name(asset_ref) when is_binary(asset_ref) do
    asset_ref
    |> String.split(":")
    |> List.last()
  end

  defp asset_ref_name(_asset_ref), do: nil

  defp manifest_asset_target(asset) do
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

  defp freshness_ref_string(%AssetFreshnessState{} = state) do
    ref_to_string({state.asset_ref_module, state.asset_ref_name})
  end

  defp run_ref_entries(run) do
    refs =
      [run.asset_ref | List.wrap(run.target_refs)] ++
        ((run.asset_results || %{})
         |> Map.keys()
         |> List.wrap())

    refs
    |> Enum.filter(&match?({_module, _name}, &1))
    |> Enum.uniq()
    |> Enum.map(&{ref_to_string(&1), run})
  end

  defp latest_run(runs) do
    Enum.max_by(
      runs,
      &DateTime.to_unix(run_time_sort_key(&1), :microsecond),
      &>=/2,
      fn -> nil end
    )
  end

  defp run_time_sort_key(run), do: run.finished_at || run.started_at || DateTime.from_unix!(0)

  defp catalogue_status(%AssetFreshnessState{} = freshness, _run) do
    case freshness.latest_attempt_status || freshness.status do
      status when status in [:ok, :skipped_fresh] -> :healthy
      :running -> :running
      status when status in [:error, :cancelled, :timed_out, :blocked] -> :failed
      _other -> :unknown
    end
  end

  defp catalogue_status(nil, run), do: run_status(run)

  defp run_status(nil), do: :unknown
  defp run_status(%{status: status}) when status in [:pending, :running], do: :running
  defp run_status(%{status: :ok}), do: :healthy

  defp run_status(%{status: status}) when status in [:partial, :error, :cancelled, :timed_out],
    do: :failed

  defp run_status(_run), do: :unknown

  defp latest_run_id(%AssetFreshnessState{latest_attempt_run_id: id}, _run) when is_binary(id),
    do: id

  defp latest_run_id(%AssetFreshnessState{latest_success_run_id: id}, _run) when is_binary(id),
    do: id

  defp latest_run_id(_freshness, %{id: id}) when is_binary(id), do: id
  defp latest_run_id(_freshness, _run), do: nil

  defp latest_run_status(%AssetFreshnessState{latest_attempt_status: status}, _run)
       when not is_nil(status),
       do: status

  defp latest_run_status(%AssetFreshnessState{status: status}, _run) when not is_nil(status),
    do: status

  defp latest_run_status(_freshness, %{status: status}), do: status
  defp latest_run_status(_freshness, _run), do: nil

  defp latest_run_at(%AssetFreshnessState{latest_attempt_at: at}, _run) when not is_nil(at),
    do: at

  defp latest_run_at(%AssetFreshnessState{latest_success_at: at}, _run) when not is_nil(at),
    do: at

  defp latest_run_at(_freshness, run) when not is_nil(run), do: run.finished_at || run.started_at
  defp latest_run_at(_freshness, _run), do: nil

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

  defp normalize_data(%Favn.Window.Spec{} = spec) do
    %{
      kind: atom_name(spec.kind),
      lookback: spec.lookback,
      refresh_from: normalize_data(spec.refresh_from),
      required: spec.required,
      timezone: spec.timezone
    }
  end

  defp normalize_data(%DateTime{} = value), do: DateTime.to_iso8601(value)

  defp normalize_data(value) when is_struct(value) do
    value
    |> Map.from_struct()
    |> normalize_data()
  end

  defp normalize_data(value) when is_map(value) do
    value
    |> Enum.map(fn {key, val} -> {to_string(key), normalize_data(val)} end)
    |> Map.new()
  end

  defp normalize_data(value) when is_list(value), do: Enum.map(value, &normalize_data/1)
  defp normalize_data({module, name}), do: ref_to_string({module, name})
  defp normalize_data(value) when is_atom(value), do: atom_name(value)
  defp normalize_data(value), do: value

  defp resolve_asset_target_ref(%Version{} = version, target_id) when is_binary(target_id) do
    with {:ok, asset} <- resolve_asset_target(version, target_id), do: {:ok, asset.ref}
  end

  defp resolve_asset_target(%Version{} = version, target_id) when is_binary(target_id) do
    case Enum.find(
           List.wrap(version.manifest.assets),
           &(target_id_for_asset(&1.ref) == target_id)
         ) do
      %{ref: _target_ref} = asset -> {:ok, asset}
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

  defp default_log_filter do
    case Code.ensure_loaded(Favn.Log.Filter) do
      {:module, Favn.Log.Filter} -> struct(Favn.Log.Filter)
      _other -> %{}
    end
  end

  defp validate_runner_client(module) when is_atom(module) do
    callbacks =
      RunnerClient.behaviour_info(:callbacks) -- RunnerClient.behaviour_info(:optional_callbacks)

    with {:module, ^module} <- Code.ensure_loaded(module),
         true <-
           Enum.all?(callbacks, fn {name, arity} -> function_exported?(module, name, arity) end) do
      :ok
    else
      _ -> {:error, :runner_client_not_available}
    end
  end

  defp validate_runner_client(_module), do: {:error, :runner_client_not_available}

  defp runner_manifest_registration(%Version{} = version, runner_client, status) do
    %{
      manifest_version_id: version.manifest_version_id,
      content_hash: version.content_hash,
      runner_client: atom_name(runner_client),
      status: atom_name(status)
    }
  end

  defp runner_registration_error(:runner_client_not_available), do: :runner_client_not_available
  defp runner_registration_error({:runner_node_unreachable, _node}), do: :runner_unavailable
  defp runner_registration_error({:runner_node_ignored, _node}), do: :runner_unavailable

  defp runner_registration_error({:runner_function_undefined, _module, _function, _arity}),
    do: :runner_client_not_available

  defp runner_registration_error({:runner_dispatch_failed, _details}), do: :runner_unavailable

  defp runner_registration_error({:manifest_version_conflict, _id, _existing, _incoming}),
    do: :runner_manifest_conflict

  defp runner_registration_error(_reason), do: :runner_unavailable

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
