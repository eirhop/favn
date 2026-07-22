defmodule Favn.Dev.OrchestratorClient do
  @moduledoc false

  alias Favn.Dev.HttpClient
  alias Favn.Dev.ExecutionPackageBatches
  alias Favn.Manifest.Publication
  alias Favn.Manifest.Serializer

  @manifest_publication_timeout_ms 60_000

  @type session_context :: %{required(String.t()) => String.t()}

  @spec begin_runner_replacement(String.t(), String.t(), String.t()) ::
          {:ok, String.t()} | {:error, term()}
  def begin_runner_replacement(base_url, service_token, maintenance_token)
      when is_binary(base_url) and is_binary(service_token) and is_binary(maintenance_token) do
    url = base_url <> "/api/orchestrator/v1/maintenance/runner-replacement"
    context = %{"maintenance_token" => maintenance_token}

    case request_post(:begin_runner_replacement, url, service_token, %{}, context) do
      {:ok, %{"data" => %{"maintenance_token" => ^maintenance_token}}} ->
        {:ok, maintenance_token}

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, operation_error(:begin_runner_replacement, :post, url, :invalid_response)}
    end
  end

  @spec runner_replacement_status(String.t(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def runner_replacement_status(base_url, service_token)
      when is_binary(base_url) and is_binary(service_token) do
    url = base_url <> "/api/orchestrator/v1/maintenance/runner-replacement"

    case request_get(:runner_replacement_status, url, service_token) do
      {:ok, %{"data" => status}} when is_map(status) ->
        {:ok, status}

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, operation_error(:runner_replacement_status, :get, url, :invalid_response)}
    end
  end

  @spec verify_replacement_runner(String.t(), String.t(), String.t(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def verify_replacement_runner(base_url, service_token, maintenance_token, runner_release_id)
      when is_binary(base_url) and is_binary(service_token) and is_binary(maintenance_token) and
             is_binary(runner_release_id) do
    url = base_url <> "/api/orchestrator/v1/maintenance/runner-replacement/verify-runner"
    context = %{"maintenance_token" => maintenance_token}

    case request_post(
           :verify_replacement_runner,
           url,
           service_token,
           %{runner_release_id: runner_release_id},
           context
         ) do
      {:ok, %{"data" => %{"runner_release_id" => ^runner_release_id} = verified}} ->
        {:ok, verified}

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, operation_error(:verify_replacement_runner, :post, url, :invalid_response)}
    end
  end

  @spec finish_runner_replacement(String.t(), String.t(), String.t()) ::
          :ok | {:error, term()}
  def finish_runner_replacement(base_url, service_token, maintenance_token)
      when is_binary(base_url) and is_binary(service_token) and is_binary(maintenance_token) do
    url = base_url <> "/api/orchestrator/v1/maintenance/runner-replacement"
    context = %{"maintenance_token" => maintenance_token}

    case request(:finish_runner_replacement, :delete, url, service_token, nil, context, nil) do
      {:ok, %{"data" => %{"status" => "accepting"}}} ->
        :ok

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, operation_error(:finish_runner_replacement, :delete, url, :invalid_response)}
    end
  end

  @spec publish_manifest(String.t(), String.t(), Publication.t(), session_context() | nil) ::
          {:ok, map()} | {:error, term()}
  def publish_manifest(
        base_url,
        service_token,
        %Publication{} = publication,
        session_context \\ nil
      )
      when is_binary(base_url) and is_binary(service_token) do
    with {:ok, missing, batch_limits} <-
           missing_execution_packages(base_url, service_token, publication, session_context),
         :ok <-
           upload_execution_packages(
             base_url,
             service_token,
             publication,
             missing,
             batch_limits,
             session_context
           ) do
      publish_manifest_index(base_url, service_token, publication, session_context)
    end
  end

  @spec verify_service_token(String.t(), String.t()) :: :ok | {:error, term()}
  def verify_service_token(base_url, service_token)
      when is_binary(base_url) and is_binary(service_token) do
    url = base_url <> "/api/orchestrator/v1/bootstrap/service-token"

    case request_get(:verify_service_token, url, service_token) do
      {:ok, %{"data" => %{"status" => "ok"}}} ->
        :ok

      {:ok, %{"data" => %{"verified" => true}}} ->
        :ok

      {:ok, %{"data" => %{"authenticated" => true}}} ->
        :ok

      {:ok, _decoded} ->
        {:error, operation_error(:verify_service_token, :get, url, :invalid_response)}

      {:error, _reason} = error ->
        error
    end
  end

  @spec activate_manifest(String.t(), String.t(), String.t(), session_context()) ::
          {:ok, map()} | {:error, term()}
  def activate_manifest(base_url, service_token, manifest_version_id, session_context)
      when is_binary(base_url) and is_binary(service_token) and is_binary(manifest_version_id) and
             is_map(session_context) do
    input = %{manifest_version_id: manifest_version_id}

    request_post(
      :activate_manifest,
      base_url <> "/api/orchestrator/v1/manifests/#{manifest_version_id}/activate",
      service_token,
      %{
        selection: %{
          common_assets: "all",
          common_pipelines: "all",
          workspace_assets: [],
          workspace_pipelines: []
        },
        configuration: %{}
      },
      session_context,
      activation_idempotency_key(session_context, input, [])
    )
  end

  @spec activate_manifest_service(String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def activate_manifest_service(
        base_url,
        service_token,
        manifest_version_id,
        workspace_id,
        opts \\ []
      )
      when is_binary(base_url) and is_binary(service_token) and is_binary(manifest_version_id) and
             is_binary(workspace_id) and workspace_id != "" and is_list(opts) do
    context =
      %{"workspace_id" => workspace_id}
      |> maybe_put_maintenance_token(Keyword.get(opts, :maintenance_token))

    input = %{
      manifest_version_id: manifest_version_id,
      workspace_id: workspace_id
    }

    request_post(
      :activate_manifest,
      base_url <> "/api/orchestrator/v1/manifests/#{URI.encode(manifest_version_id)}/activate",
      service_token,
      %{
        selection: %{
          common_assets: "all",
          common_pipelines: "all",
          workspace_assets: [],
          workspace_pipelines: []
        },
        configuration: %{}
      },
      context,
      activation_idempotency_key(context, input, opts)
    )
  end

  @spec register_runner(String.t(), String.t(), session_context(), map()) ::
          {:ok, map()} | {:error, term()}
  def register_runner(base_url, service_token, session_context, payload)
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_map(payload) do
    manifest_version_id =
      Map.get(payload, :manifest_version_id) || Map.get(payload, "manifest_version_id")

    if is_binary(manifest_version_id) and manifest_version_id != "" do
      request_post(
        :register_runner,
        base_url <>
          "/api/orchestrator/v1/manifests/#{URI.encode(manifest_version_id)}/runner/register",
        service_token,
        %{},
        session_context
      )
    else
      {:error, operation_error(:register_runner, :post, base_url, :missing_manifest_version_id)}
    end
  end

  @spec bootstrap_active_manifest(String.t(), String.t(), session_context()) ::
          {:ok, map()} | {:error, term()}
  def bootstrap_active_manifest(base_url, service_token, session_context)
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) do
    url = base_url <> "/api/orchestrator/v1/bootstrap/active-manifest"

    case request_get(:bootstrap_active_manifest, url, service_token, session_context) do
      {:ok, %{"data" => data}} when is_map(data) ->
        {:ok, data}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, operation_error(:bootstrap_active_manifest, :get, url, :invalid_response)}
    end
  end

  @spec cancel_run(String.t(), String.t(), String.t(), session_context()) ::
          {:ok, map()} | {:error, term()}
  def cancel_run(base_url, service_token, run_id, session_context)
      when is_binary(base_url) and is_binary(service_token) and is_binary(run_id) and
             is_map(session_context) do
    url = base_url <> "/api/orchestrator/v1/runs/#{URI.encode(run_id)}/cancel"

    case request_post(
           :cancel_run,
           url,
           service_token,
           %{},
           session_context,
           idempotency_key(:cancel_run, session_context, %{run_id: run_id})
         ) do
      {:ok, %{"data" => data}} when is_map(data) ->
        {:ok, data}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, operation_error(:cancel_run, :post, url, :invalid_response)}
    end
  end

  @spec password_login(String.t(), String.t(), String.t(), String.t(), String.t()) ::
          {:ok, session_context()} | {:error, term()}
  def password_login(base_url, service_token, workspace_id, username, password)
      when is_binary(base_url) and is_binary(service_token) and is_binary(workspace_id) and
             workspace_id != "" and is_binary(username) and is_binary(password) do
    url = base_url <> "/api/orchestrator/v1/auth/password/sessions"

    with {:ok,
          %{
            "data" => %{
              "session" => %{"id" => session_id},
              "session_token" => session_token,
              "actor" => %{"id" => actor_id}
            }
          }} <-
           request_post(
             :password_login,
             url,
             service_token,
             %{username: username, password: password},
             %{"workspace_id" => workspace_id}
           ),
         true <- is_binary(session_id) and session_id != "",
         true <- is_binary(actor_id) and actor_id != "",
         true <- is_binary(session_token) and session_token != "" do
      {:ok,
       %{
         "workspace_id" => workspace_id,
         "actor_id" => actor_id,
         "session_id" => session_id,
         "session_token" => session_token
       }}
    else
      false -> {:error, operation_error(:password_login, :post, url, :invalid_response)}
      {:error, _reason} = error -> error
      _other -> {:error, operation_error(:password_login, :post, url, :invalid_response)}
    end
  end

  @spec active_manifest(String.t(), String.t(), session_context()) ::
          {:ok, map()} | {:error, term()}
  def active_manifest(base_url, service_token, session_context)
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) do
    url = base_url <> "/api/orchestrator/v1/manifests/active"

    case request_get(:active_manifest, url, service_token, session_context) do
      {:ok, %{"data" => data}} when is_map(data) ->
        {:ok, data}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, operation_error(:active_manifest, :get, url, :invalid_response)}
    end
  end

  @spec plan_rebuild(String.t(), String.t(), session_context(), String.t(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def plan_rebuild(base_url, service_token, session_context, target_id, reason)
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_binary(target_id) and is_binary(reason) do
    url = base_url <> "/api/orchestrator/v1/rebuilds/plan"
    payload = %{target_id: target_id, reason: reason}

    case request_post(
           :plan_rebuild,
           url,
           service_token,
           payload,
           session_context,
           fresh_idempotency_key()
         ) do
      {:ok, %{"data" => %{"plan" => plan}}} when is_map(plan) -> {:ok, plan}
      {:error, _reason} = error -> error
      _other -> {:error, operation_error(:plan_rebuild, :post, url, :invalid_response)}
    end
  end

  @spec start_rebuild(String.t(), String.t(), session_context(), String.t(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def start_rebuild(base_url, service_token, session_context, plan_id, plan_hash)
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_binary(plan_id) and is_binary(plan_hash) do
    url = base_url <> "/api/orchestrator/v1/rebuilds"
    payload = %{plan_id: plan_id, plan_hash: plan_hash, approved: true}

    rebuild_mutation(
      :start_rebuild,
      url,
      service_token,
      session_context,
      payload
    )
  end

  @spec get_rebuild(String.t(), String.t(), session_context(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def get_rebuild(base_url, service_token, session_context, operation_id)
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_binary(operation_id) do
    url = base_url <> "/api/orchestrator/v1/rebuilds/#{URI.encode(operation_id)}"

    case request_get(:get_rebuild, url, service_token, session_context) do
      {:ok, %{"data" => %{"rebuild" => rebuild}}} when is_map(rebuild) -> {:ok, rebuild}
      {:error, _reason} = error -> error
      _other -> {:error, operation_error(:get_rebuild, :get, url, :invalid_response)}
    end
  end

  @spec cancel_rebuild(String.t(), String.t(), session_context(), String.t(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def cancel_rebuild(base_url, service_token, session_context, operation_id, reason)
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_binary(operation_id) and is_binary(reason) do
    url = base_url <> "/api/orchestrator/v1/rebuilds/#{URI.encode(operation_id)}/cancel"

    rebuild_mutation(
      :cancel_rebuild,
      url,
      service_token,
      session_context,
      %{reason: reason}
    )
  end

  @spec retry_rebuild(String.t(), String.t(), session_context(), String.t(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def retry_rebuild(base_url, service_token, session_context, operation_id, plan_hash)
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_binary(operation_id) and is_binary(plan_hash) do
    url = base_url <> "/api/orchestrator/v1/rebuilds/#{URI.encode(operation_id)}/retry"

    rebuild_mutation(
      :retry_rebuild,
      url,
      service_token,
      session_context,
      %{plan_hash: plan_hash}
    )
  end

  @spec reconcile_rebuild(String.t(), String.t(), session_context(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def reconcile_rebuild(base_url, service_token, session_context, operation_id)
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_binary(operation_id) do
    url = base_url <> "/api/orchestrator/v1/rebuilds/#{URI.encode(operation_id)}/reconcile"
    rebuild_mutation(:reconcile_rebuild, url, service_token, session_context, %{})
  end

  defp rebuild_mutation(operation, url, service_token, session_context, payload) do
    idempotency_key =
      if operation in [:retry_rebuild, :reconcile_rebuild],
        do: fresh_idempotency_key(),
        else: idempotency_key(operation, session_context, payload)

    case request_post(
           operation,
           url,
           service_token,
           payload,
           session_context,
           idempotency_key
         ) do
      {:ok, %{"data" => %{"rebuild" => rebuild}}} when is_map(rebuild) -> {:ok, rebuild}
      {:error, _reason} = error -> error
      _other -> {:error, operation_error(operation, :post, url, :invalid_response)}
    end
  end

  @spec submit_run(String.t(), String.t(), session_context(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def submit_run(base_url, service_token, session_context, payload, opts \\ [])
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_map(payload) and is_list(opts) do
    url = base_url <> "/api/orchestrator/v1/runs"

    idempotency_key =
      Keyword.get(opts, :idempotency_key)
      |> submit_run_idempotency_key(session_context, payload)

    case request_post(
           :submit_run,
           url,
           service_token,
           payload,
           session_context,
           idempotency_key
         ) do
      {:ok, %{"data" => %{"run" => run}}} when is_map(run) ->
        {:ok, run}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, operation_error(:submit_run, :post, url, :invalid_response)}
    end
  end

  @spec submit_backfill(String.t(), String.t(), session_context(), map()) ::
          {:ok, map()} | {:error, term()}
  def submit_backfill(base_url, service_token, session_context, payload)
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_map(payload) do
    url = base_url <> "/api/orchestrator/v1/backfills"

    case request_post(
           :submit_backfill,
           url,
           service_token,
           payload,
           session_context,
           idempotency_key(:submit_backfill, session_context, payload)
         ) do
      {:ok, %{"data" => %{"run" => run}}} when is_map(run) ->
        {:ok, run}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, operation_error(:submit_backfill, :post, url, :invalid_response)}
    end
  end

  @spec plan_backfill(String.t(), String.t(), session_context(), map()) ::
          {:ok, map()} | {:error, term()}
  def plan_backfill(base_url, service_token, session_context, payload)
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_map(payload) do
    url = base_url <> "/api/orchestrator/v1/backfills/plan"

    case request_post(:plan_backfill, url, service_token, payload, session_context) do
      {:ok, %{"data" => %{"plan" => plan}}} when is_map(plan) ->
        {:ok, plan}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, operation_error(:plan_backfill, :post, url, :invalid_response)}
    end
  end

  @spec plan_missing_coverage_backfill(
          String.t(),
          String.t(),
          session_context(),
          String.t(),
          keyword()
        ) :: {:ok, map()} | {:error, term()}
  def plan_missing_coverage_backfill(
        base_url,
        service_token,
        session_context,
        target_id,
        opts \\ []
      )
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_binary(target_id) and is_list(opts) do
    url =
      base_url <>
        "/api/orchestrator/v1/coverage/assets/#{URI.encode(target_id)}/backfill/plan"

    payload =
      opts
      |> Keyword.take([:cursor, :limit])
      |> Map.new()

    case request_post(
           :plan_missing_coverage_backfill,
           url,
           service_token,
           payload,
           session_context
         ) do
      {:ok, %{"data" => %{"plan" => plan}}} when is_map(plan) ->
        {:ok, plan}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, operation_error(:plan_missing_coverage_backfill, :post, url, :invalid_response)}
    end
  end

  @spec submit_missing_coverage_backfill(
          String.t(),
          String.t(),
          session_context(),
          String.t(),
          map()
        ) :: {:ok, String.t()} | {:error, term()}
  def submit_missing_coverage_backfill(
        base_url,
        service_token,
        session_context,
        target_id,
        plan
      )
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_binary(target_id) and is_map(plan) do
    url =
      base_url <> "/api/orchestrator/v1/coverage/assets/#{URI.encode(target_id)}/backfill"

    payload = %{plan: plan}

    case request_post(
           :submit_missing_coverage_backfill,
           url,
           service_token,
           payload,
           session_context,
           idempotency_key(:submit_missing_coverage_backfill, session_context, payload)
         ) do
      {:ok, %{"data" => %{"run_id" => run_id}}} when is_binary(run_id) ->
        {:ok, run_id}

      {:error, _reason} = error ->
        error

      _other ->
        {:error,
         operation_error(:submit_missing_coverage_backfill, :post, url, :invalid_response)}
    end
  end

  @spec list_backfill_windows(String.t(), String.t(), session_context(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def list_backfill_windows(
        base_url,
        service_token,
        session_context,
        backfill_run_id,
        filters \\ []
      )
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_binary(backfill_run_id) and is_list(filters) do
    url =
      base_url <>
        "/api/orchestrator/v1/backfills/#{URI.encode(backfill_run_id)}/windows" <>
        query_string(filters)

    request_page(:list_backfill_windows, url, service_token, session_context)
  end

  @spec rerun_backfill_window(
          String.t(),
          String.t(),
          session_context(),
          String.t(),
          String.t(),
          map()
        ) :: {:ok, map()} | {:error, term()}
  def rerun_backfill_window(
        base_url,
        service_token,
        session_context,
        backfill_run_id,
        window_key,
        payload \\ %{}
      )
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_binary(backfill_run_id) and is_binary(window_key) and is_map(payload) do
    url =
      base_url <>
        "/api/orchestrator/v1/backfills/#{URI.encode(backfill_run_id)}/windows/rerun"

    case request_post(
           :rerun_backfill_window,
           url,
           service_token,
           Map.put(payload, :window_key, window_key),
           session_context,
           idempotency_key(
             :rerun_backfill_window,
             session_context,
             Map.merge(payload, %{backfill_run_id: backfill_run_id, window_key: window_key})
           )
         ) do
      {:ok, %{"data" => %{"run" => run}}} when is_map(run) ->
        {:ok, run}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, operation_error(:rerun_backfill_window, :post, url, :invalid_response)}
    end
  end

  @spec list_coverage_baselines(String.t(), String.t(), session_context(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def list_coverage_baselines(base_url, service_token, session_context, filters \\ [])
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_list(filters) do
    url = base_url <> "/api/orchestrator/v1/backfills/coverage-baselines" <> query_string(filters)

    request_page(:list_coverage_baselines, url, service_token, session_context)
  end

  @spec list_asset_window_states(String.t(), String.t(), session_context(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def list_asset_window_states(base_url, service_token, session_context, filters \\ [])
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_list(filters) do
    url = base_url <> "/api/orchestrator/v1/assets/window-states" <> query_string(filters)

    request_page(:list_asset_window_states, url, service_token, session_context)
  end

  @spec repair_backfill_projections(String.t(), String.t(), session_context(), map()) ::
          {:ok, map()} | {:error, term()}
  def repair_backfill_projections(base_url, service_token, session_context, payload)
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_map(payload) do
    url = base_url <> "/api/orchestrator/v1/backfills/projections/repair"

    case request_post(:repair_backfill_projections, url, service_token, payload, session_context) do
      {:ok, %{"data" => %{"repair" => repair}}} when is_map(repair) ->
        {:ok, repair}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, operation_error(:repair_backfill_projections, :post, url, :invalid_response)}
    end
  end

  @spec list_runs(String.t(), String.t(), session_context(), keyword()) ::
          {:ok, [map()]} | {:error, term()}
  def list_runs(base_url, service_token, session_context, filters \\ [])
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_list(filters) do
    url = base_url <> "/api/orchestrator/v1/runs" <> query_string(filters)

    case request_get(:list_runs, url, service_token, session_context) do
      {:ok, %{"data" => %{"items" => runs}}} when is_list(runs) ->
        {:ok, runs}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, operation_error(:list_runs, :get, url, :invalid_response)}
    end
  end

  @spec get_run(String.t(), String.t(), session_context(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def get_run(base_url, service_token, session_context, run_id)
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_binary(run_id) do
    url = base_url <> "/api/orchestrator/v1/runs/#{run_id}"

    case request_get(:get_run, url, service_token, session_context) do
      {:ok, %{"data" => %{"run" => run}}} when is_map(run) ->
        {:ok, run}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, operation_error(:get_run, :get, url, :invalid_response)}
    end
  end

  @spec list_run_events(String.t(), String.t(), session_context(), String.t(), keyword()) ::
          {:ok, [map()]} | {:error, term()}
  def list_run_events(base_url, service_token, session_context, run_id, filters \\ [])
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_binary(run_id) and is_list(filters) do
    url =
      base_url <>
        "/api/orchestrator/v1/runs/#{URI.encode(run_id)}/events" <> query_string(filters)

    case request_get(:list_run_events, url, service_token, session_context) do
      {:ok, %{"data" => %{"items" => events}}} when is_list(events) ->
        {:ok, events}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, operation_error(:list_run_events, :get, url, :invalid_response)}
    end
  end

  @spec in_flight_runs(String.t(), String.t(), session_context()) ::
          {:ok, [String.t()]} | {:error, term()}
  def in_flight_runs(base_url, service_token, session_context)
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) do
    url = base_url <> "/api/orchestrator/v1/runs/in-flight"

    with {:ok, %{"data" => %{"run_ids" => run_ids}}} <-
           request_get(:list_in_flight_runs, url, service_token, session_context),
         true <- is_list(run_ids) do
      {:ok, Enum.filter(run_ids, &is_binary/1)}
    else
      false -> {:error, operation_error(:list_in_flight_runs, :get, url, :invalid_response)}
      {:error, _reason} = error -> error
      _other -> {:error, operation_error(:list_in_flight_runs, :get, url, :invalid_response)}
    end
  end

  @spec diagnostics(String.t(), String.t(), session_context() | nil) ::
          {:ok, map()} | {:error, term()}
  def diagnostics(base_url, service_token, session_context \\ nil)
      when is_binary(base_url) and is_binary(service_token) do
    url = base_url <> "/api/orchestrator/v1/diagnostics"

    case request_get(:diagnostics, url, service_token, session_context) do
      {:ok, %{"data" => diagnostics}} when is_map(diagnostics) ->
        {:ok, diagnostics}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, operation_error(:diagnostics, :get, url, :invalid_response)}
    end
  end

  @spec health(String.t()) :: :ok | {:error, term()}
  def health(base_url) when is_binary(base_url) do
    url = base_url <> "/api/orchestrator/v1/health"

    case HttpClient.request(:get, url, [], nil, connect_timeout_ms: 1_000, timeout_ms: 2_000) do
      {:ok, %{"data" => %{"status" => "ok"}}} ->
        :ok

      {:ok, %{"status" => "ok"}} ->
        :ok

      {:ok, decoded} ->
        {:error, operation_error(:health_check, :get, url, {:invalid_response, decoded})}

      {:error, reason} ->
        {:error, operation_error(:health_check, :get, url, reason)}
    end
  end

  defp request_post(
         operation,
         url,
         service_token,
         payload,
         session_context,
         idempotency_key \\ nil
       ) do
    body = JSON.encode!(payload)

    request(operation, :post, url, service_token, body, session_context, idempotency_key)
  end

  defp request_get(operation, url, service_token, session_context \\ nil) do
    request(operation, :get, url, service_token, nil, session_context, nil)
  end

  defp request_page(operation, url, service_token, session_context) do
    case request_get(operation, url, service_token, session_context) do
      {:ok, %{"data" => %{"items" => items, "pagination" => pagination}}}
      when is_list(items) and is_map(pagination) ->
        {:ok, %{"items" => items, "pagination" => pagination}}

      {:ok, %{"data" => %{"items" => items}}} when is_list(items) ->
        {:ok, %{"items" => items, "pagination" => %{}}}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, operation_error(operation, :get, url, :invalid_response)}
    end
  end

  defp query_string([]), do: ""

  defp query_string(filters) when is_list(filters) do
    params =
      filters
      |> Enum.reject(fn {_key, value} -> is_nil(value) or value == "" end)
      |> Enum.map(fn {key, value} -> {Atom.to_string(key), query_value(value)} end)

    case URI.encode_query(params) do
      "" -> ""
      query -> "?" <> query
    end
  end

  defp query_value(value) when is_atom(value), do: Atom.to_string(value)
  defp query_value(value), do: to_string(value)

  defp request(
         operation,
         method,
         url,
         service_token,
         body,
         session_context,
         idempotency_key,
         extra_headers \\ [],
         request_opts \\ []
       ) do
    headers =
      [
        {"accept", "application/json"}
      ]
      |> add_authorization_header(service_token)
      |> add_session_headers(session_context)
      |> add_workspace_header(session_context)
      |> add_maintenance_header(session_context)
      |> add_idempotency_header(idempotency_key)
      |> Kernel.++(extra_headers)

    case HttpClient.request(method, url, headers, body, request_opts) do
      {:ok, decoded} -> {:ok, decoded}
      {:error, reason} -> {:error, operation_error(operation, method, url, reason)}
    end
  end

  defp add_authorization_header(headers, token) when is_binary(token) and token != "" do
    headers ++ [{"authorization", "Bearer #{token}"}]
  end

  defp add_authorization_header(headers, _token), do: headers

  defp add_session_headers(headers, %{"actor_id" => actor_id, "session_token" => session_token})
       when is_binary(actor_id) and actor_id != "" and is_binary(session_token) and
              session_token != "" do
    headers ++ [{"x-favn-actor-id", actor_id}, {"x-favn-session-token", session_token}]
  end

  defp add_session_headers(headers, %{"local_dev_context" => "trusted"}) do
    headers ++ [{"x-favn-local-dev-context", "trusted"}]
  end

  defp add_session_headers(headers, _session_context), do: headers

  defp add_workspace_header(headers, %{"workspace_id" => workspace_id})
       when is_binary(workspace_id) and workspace_id != "" do
    headers ++ [{"x-favn-workspace-id", workspace_id}]
  end

  defp add_workspace_header(headers, _session_context), do: headers

  defp add_maintenance_header(headers, %{"maintenance_token" => token})
       when is_binary(token) and token != "" do
    headers ++ [{"x-favn-maintenance-token", token}]
  end

  defp add_maintenance_header(headers, _session_context), do: headers

  defp add_idempotency_header(headers, key) when is_binary(key) and key != "" do
    headers ++ [{"idempotency-key", key}]
  end

  defp add_idempotency_header(headers, _key), do: headers

  defp maybe_put_maintenance_token(context, token) when is_binary(token) and token != "",
    do: Map.put(context, "maintenance_token", token)

  defp maybe_put_maintenance_token(context, _token), do: context

  defp submit_run_idempotency_key(key, _session_context, _payload)
       when is_binary(key) and key != "" do
    key
  end

  defp submit_run_idempotency_key(_key, _session_context, _payload), do: fresh_idempotency_key()

  defp activation_idempotency_key(context, input, opts) do
    case Keyword.get(opts, :idempotency_key) do
      key when is_binary(key) and key != "" -> key
      _missing -> default_activation_idempotency_key(context, input)
    end
  end

  defp default_activation_idempotency_key(context, input) do
    case Map.get(context, "maintenance_token") do
      token when is_binary(token) and token != "" ->
        idempotency_key(:activate_manifest, %{}, Map.put(input, :command_token, token))

      _missing ->
        fresh_idempotency_key()
    end
  end

  defp fresh_idempotency_key do
    "favn-local-" <>
      (:crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false))
  end

  defp idempotency_key(operation, session_context, input) when is_atom(operation) do
    fingerprint =
      %{operation: operation, session: idempotency_session_context(session_context), input: input}
      |> canonicalize()
      |> JSON.encode!()

    digest = :crypto.hash(:sha256, fingerprint)

    "favn-local-" <> Base.url_encode64(digest, padding: false)
  end

  defp idempotency_session_context(%{} = session_context) do
    session_context
    |> Map.take(["actor_id"])
    |> Enum.reject(fn {_key, value} -> not is_binary(value) or value == "" end)
    |> Map.new()
  end

  defp idempotency_session_context(_session_context), do: %{}

  defp canonicalize(nil), do: %{"__type__" => "null"}

  defp canonicalize(value) when is_boolean(value),
    do: %{"__type__" => "boolean", "value" => value}

  defp canonicalize(value) when is_binary(value), do: %{"__type__" => "string", "value" => value}

  defp canonicalize(value) when is_integer(value),
    do: %{"__type__" => "integer", "value" => value}

  defp canonicalize(value) when is_float(value), do: %{"__type__" => "float", "value" => value}

  defp canonicalize(value) when is_map(value) do
    entries =
      value
      |> Enum.map(fn {key, val} -> [to_string(key), canonicalize(val)] end)
      |> Enum.sort_by(fn [key, _val] -> key end)

    %{"__type__" => "map", "entries" => entries}
  end

  defp canonicalize(value) when is_list(value) do
    %{"__type__" => "list", "items" => Enum.map(value, &canonicalize/1)}
  end

  defp canonicalize(value) when is_atom(value),
    do: %{"__type__" => "atom", "value" => Atom.to_string(value)}

  defp operation_error(operation, method, url, reason) do
    %{operation: operation, method: method, url: sanitized_url(url), reason: reason}
  end

  defp sanitized_url(url) do
    case URI.parse(url) do
      %URI{scheme: scheme, host: host} = uri
      when scheme in ["http", "https"] and is_binary(host) and host != "" ->
        uri
        |> Map.put(:userinfo, nil)
        |> Map.put(:query, nil)
        |> Map.put(:fragment, nil)
        |> URI.to_string()

      _invalid ->
        :redacted
    end
  end

  defp missing_execution_packages(base_url, service_token, publication, session_context) do
    hashes = Publication.required_package_hashes(publication.version)

    case gzip_post(
           :missing_execution_packages,
           base_url <> "/api/orchestrator/v1/execution-packages/missing",
           service_token,
           %{hashes: hashes},
           session_context
         ) do
      {:ok,
       %{
         "data" => %{
           "missing" => missing,
           "publication_limits" => publication_limits
         }
       }}
      when is_list(missing) ->
        with {:ok, batch_limits} <- execution_package_batch_limits(publication_limits) do
          {:ok, missing, batch_limits}
        end

      {:ok, _response} ->
        {:error, :invalid_missing_execution_packages_response}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp upload_execution_packages(
         base_url,
         service_token,
         publication,
         missing,
         batch_limits,
         session_context
       ) do
    packages_by_hash = Publication.packages_by_hash(publication)

    with {:ok, packages} <- select_missing_packages(missing, packages_by_hash),
         {:ok, batches} <- ExecutionPackageBatches.build(packages, batch_limits) do
      batches
      |> Enum.reduce_while(:ok, fn batch, :ok ->
        payload = %{packages: batch}

        case gzip_post(
               :upload_execution_packages,
               base_url <> "/api/orchestrator/v1/execution-packages",
               service_token,
               payload,
               session_context
             ) do
          {:ok, _response} -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  defp execution_package_batch_limits(%{
         "max_packages" => max_packages,
         "compressed_limit_bytes" => compressed_limit_bytes,
         "decompressed_limit_bytes" => decompressed_limit_bytes
       })
       when is_integer(max_packages) and max_packages > 0 and
              is_integer(compressed_limit_bytes) and compressed_limit_bytes > 0 and
              is_integer(decompressed_limit_bytes) and decompressed_limit_bytes > 0 do
    {:ok,
     [
       max_count: max_packages,
       max_compressed_bytes: compressed_limit_bytes,
       max_decompressed_bytes: decompressed_limit_bytes
     ]}
  end

  defp execution_package_batch_limits(_limits),
    do: {:error, :invalid_execution_package_publication_limits}

  defp select_missing_packages(missing, packages_by_hash) do
    missing
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, []}, fn hash, {:ok, packages} ->
      case Map.fetch(packages_by_hash, hash) do
        {:ok, package} -> {:cont, {:ok, [package | packages]}}
        :error -> {:halt, {:error, {:unexpected_missing_execution_package_hash, hash}}}
      end
    end)
    |> case do
      {:ok, packages} -> {:ok, Enum.reverse(packages)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp publish_manifest_index(base_url, service_token, publication, session_context) do
    version = publication.version

    gzip_post(
      :publish_manifest,
      base_url <> "/api/orchestrator/v1/manifests",
      service_token,
      %{
        manifest_version_id: version.manifest_version_id,
        content_hash: version.content_hash,
        schema_version: version.schema_version,
        runner_contract_version: version.runner_contract_version,
        required_runner_release_id: version.required_runner_release_id,
        serialization_format: version.serialization_format,
        manifest: canonical_json_value(version.manifest)
      },
      session_context
    )
  end

  defp gzip_post(operation, url, service_token, payload, session_context) do
    body = payload |> JSON.encode!() |> :zlib.gzip()

    request(
      operation,
      :post,
      url,
      service_token,
      body,
      session_context,
      nil,
      [{"content-encoding", "gzip"}],
      timeout_ms: @manifest_publication_timeout_ms
    )
  end

  defp canonical_json_value(value) do
    value
    |> Serializer.encode_manifest!()
    |> JSON.decode!()
  end
end
