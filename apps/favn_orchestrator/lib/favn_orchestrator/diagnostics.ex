defmodule FavnOrchestrator.Diagnostics do
  @moduledoc """
  Operator-facing diagnostics for the single-node orchestrator runtime.
  """

  alias Favn.Contracts.RunnerClient
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Scheduler.Runtime, as: SchedulerRuntime
  alias FavnOrchestrator.Storage

  @default_recent_limit 5
  @in_flight_statuses [:pending, :running]
  @failed_statuses [:error, :timed_out, :partial]

  @type check :: %{
          required(:check) => atom(),
          required(:status) => :ok | :warning | :error,
          required(:summary) => String.t(),
          required(:details) => map(),
          optional(:reason) => term()
        }

  @doc """
  Returns redacted, stable operator diagnostics.
  """
  @spec report(keyword()) :: %{
          status: :ok | :degraded,
          generated_at: DateTime.t(),
          checks: [check()]
        }
  def report(opts \\ []) when is_list(opts) do
    recent_limit = Keyword.get(opts, :recent_limit, @default_recent_limit)

    checks = [
      safe_check(:storage_readiness, &storage_check/0),
      safe_check(:active_manifest, &active_manifest_check/0),
      safe_check(:scheduler, &scheduler_check/0),
      safe_check(:runner, &runner_check/0),
      safe_check(:in_flight_runs, &in_flight_runs_check/0),
      safe_check(:recent_failed_runs, fn -> recent_failed_runs_check(recent_limit) end)
    ]

    status = if Enum.all?(checks, &(&1.status == :ok)), do: :ok, else: :degraded
    report = %{status: status, generated_at: DateTime.utc_now(), checks: checks}

    OperationalEvents.emit(:diagnostics_report_generated, %{check_count: length(checks)}, %{
      status: status,
      failing_checks: Enum.map(Enum.reject(checks, &(&1.status == :ok)), & &1.check)
    })

    report
  end

  defp storage_check do
    case Storage.diagnostics() do
      {:ok, %{ready?: false} = details} ->
        error(:storage_readiness, "Storage is not ready", details, Map.get(details, :status))

      {:ok, details} ->
        ok(:storage_readiness, "Storage is ready", details)

      {:error, reason} ->
        error(:storage_readiness, "Storage diagnostics failed", %{}, normalize_error(reason))
    end
  end

  defp active_manifest_check do
    with {:ok, manifest_version_id} <- ManifestStore.get_active_manifest(),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id) do
      ok(:active_manifest, "Active manifest is set", %{
        manifest_version_id: version.manifest_version_id,
        content_hash: version.content_hash,
        asset_count: length(version.manifest.assets),
        pipeline_count: length(version.manifest.pipelines),
        schedule_count: length(version.manifest.schedules)
      })
    else
      {:error, :active_manifest_not_set} ->
        warning(:active_manifest, "Active manifest is not set", %{}, :active_manifest_not_set)

      {:error, reason} ->
        error(:active_manifest, "Active manifest cannot be loaded", %{}, normalize_error(reason))
    end
  end

  defp scheduler_check do
    scheduler_opts = Application.get_env(:favn_orchestrator, :scheduler, [])
    enabled? = Keyword.get(scheduler_opts, :enabled, false)
    name = Keyword.get(scheduler_opts, :name, SchedulerRuntime)

    cond do
      not enabled? ->
        ok(:scheduler, "Scheduler is disabled", %{enabled: false, running?: false})

      Process.whereis(name) == nil ->
        error(
          :scheduler,
          "Scheduler is enabled but not running",
          %{enabled: true, running?: false},
          :not_running
        )

      true ->
        case SchedulerRuntime.diagnostics(name) do
          {:ok, details} ->
            ok(:scheduler, "Scheduler is running", Map.put(details, :enabled, true))

          {:error, reason} ->
            error(:scheduler, "Scheduler diagnostics failed", %{enabled: true}, reason)
        end
    end
  end

  defp runner_check do
    module = Application.get_env(:favn_orchestrator, :runner_client, nil)
    opts = Application.get_env(:favn_orchestrator, :runner_client_opts, [])

    case validate_runner_client(module) do
      :ok ->
        details = %{client: module_name(module)}

        if function_exported?(module, :diagnostics, 1) do
          case module.diagnostics(opts) do
            {:ok, runner_details} ->
              ok(:runner, "Runner is available", Map.merge(details, runner_details))

            {:error, reason} ->
              error(:runner, "Runner is unavailable", details, normalize_error(reason))
          end
        else
          ok(
            :runner,
            "Runner client is configured",
            Map.put(details, :availability_probe, :not_supported)
          )
        end

      {:error, reason} ->
        error(:runner, "Runner client is unavailable", %{}, reason)
    end
  end

  defp in_flight_runs_check do
    case list_runs_by_status(@in_flight_statuses, 50) do
      {:ok, runs} ->
        ok(:in_flight_runs, "In-flight run summary is available", %{
          count: length(runs),
          runs: Enum.map(runs, &run_summary/1)
        })

      {:error, reason} ->
        error(:in_flight_runs, "In-flight runs could not be listed", %{}, normalize_error(reason))
    end
  end

  defp recent_failed_runs_check(limit) do
    case list_runs_by_status(@failed_statuses, limit) do
      {:ok, runs} ->
        ok(:recent_failed_runs, "Recent failed run summary is available", %{
          count: length(runs),
          limit: limit,
          runs: Enum.map(runs, &failed_run_summary/1)
        })

      {:error, reason} ->
        error(
          :recent_failed_runs,
          "Recent failed runs could not be listed",
          %{},
          normalize_error(reason)
        )
    end
  end

  defp list_runs_by_status(statuses, limit) when is_list(statuses) and is_integer(limit) do
    statuses
    |> Enum.reduce_while({:ok, []}, fn status, {:ok, acc} ->
      case Storage.list_runs(status: status, limit: limit) do
        {:ok, runs} -> {:cont, {:ok, runs ++ acc}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, runs} ->
        {:ok, runs |> Enum.sort_by(&run_sort_key/1, :desc) |> Enum.take(limit)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp run_summary(%RunState{} = run) do
    %{
      run_id: run.id,
      status: run.status,
      manifest_version_id: run.manifest_version_id,
      submit_kind: run.submit_kind,
      updated_at: run.updated_at,
      runner_execution_id: run.runner_execution_id
    }
  end

  defp run_sort_key(%RunState{updated_at: %DateTime{} = updated_at}),
    do: DateTime.to_unix(updated_at, :microsecond)

  defp run_sort_key(%RunState{}), do: 0

  defp failed_run_summary(%RunState{} = run) do
    run
    |> run_summary()
    |> Map.put(:error_summary, error_summary(run.error))
  end

  defp error_summary(nil), do: nil
  defp error_summary(reason) when is_atom(reason), do: %{kind: :atom, reason: reason}

  defp error_summary({reason, _details}) when is_atom(reason), do: %{kind: :tuple, reason: reason}

  defp error_summary(%{__struct__: module}) when is_atom(module),
    do: %{kind: :struct, module: module_name(module)}

  defp error_summary(reason) when is_map(reason),
    do: %{kind: :map, keys: reason |> Map.keys() |> Enum.map(&to_string/1) |> Enum.sort()}

  defp error_summary(reason),
    do: %{kind: :term, type: reason |> :erlang.term_to_binary() |> byte_size()}

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

  defp safe_check(check, fun) when is_atom(check) and is_function(fun, 0) do
    fun.()
  rescue
    exception ->
      error(check, "Check raised", %{}, %{
        kind: :raised,
        exception: module_name(exception.__struct__)
      })
  catch
    :exit, reason ->
      error(check, "Check exited", %{}, %{
        kind: :exited,
        reason: Redaction.redact_untrusted(reason)
      })

    kind, reason ->
      error(check, "Check failed", %{}, %{kind: kind, reason: Redaction.redact_untrusted(reason)})
  end

  defp ok(check, summary, details), do: build(check, :ok, summary, details, nil)

  defp warning(check, summary, details, reason),
    do: build(check, :warning, summary, details, reason)

  defp error(check, summary, details, reason), do: build(check, :error, summary, details, reason)

  defp build(check, status, summary, details, nil) do
    %{check: check, status: status, summary: summary, details: Redaction.redact(details)}
  end

  defp build(check, status, summary, details, reason) do
    check
    |> build(status, summary, details, nil)
    |> Map.put(:reason, Redaction.redact(reason))
  end

  defp normalize_error({:raised, %{__exception__: true, __struct__: module}}),
    do: %{kind: :raised, exception: module_name(module)}

  defp normalize_error({:thrown, reason}),
    do: %{kind: :thrown, reason: Redaction.redact_untrusted(reason)}

  defp normalize_error({:exited, reason}),
    do: %{kind: :exited, reason: Redaction.redact_untrusted(reason)}

  defp normalize_error(reason), do: reason

  defp module_name(nil), do: nil
  defp module_name(module) when is_atom(module), do: Atom.to_string(module)
end
