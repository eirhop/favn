defmodule Favn.Dev.OrchestratorClient do
  @moduledoc false

  alias Favn.Dev.LocalHttpClient
  alias Favn.Manifest.Serializer

  @type session_context :: %{required(String.t()) => String.t()}

  @spec publish_manifest(String.t(), String.t(), map()) ::
          {:ok, map()} | {:error, term()}
  def publish_manifest(base_url, service_token, payload)
      when is_binary(base_url) and is_binary(service_token) and is_map(payload) do
    request_post(
      :publish_manifest,
      base_url <> "/api/orchestrator/v1/manifests",
      service_token,
      normalize_publish_payload(payload)
    )
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

  @spec activate_manifest(String.t(), String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def activate_manifest(base_url, service_token, manifest_version_id)
      when is_binary(base_url) and is_binary(service_token) and is_binary(manifest_version_id) do
    request_post(
      :activate_manifest,
      base_url <> "/api/orchestrator/v1/manifests/#{manifest_version_id}/activate",
      service_token,
      %{},
      nil,
      idempotency_key(:activate_manifest, nil, %{manifest_version_id: manifest_version_id})
    )
  end

  @spec register_runner(String.t(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def register_runner(base_url, service_token, payload)
      when is_binary(base_url) and is_binary(service_token) and is_map(payload) do
    manifest_version_id =
      Map.get(payload, :manifest_version_id) || Map.get(payload, "manifest_version_id")

    if is_binary(manifest_version_id) and manifest_version_id != "" do
      request_post(
        :register_runner,
        base_url <>
          "/api/orchestrator/v1/manifests/#{URI.encode(manifest_version_id)}/runner/register",
        service_token,
        %{}
      )
    else
      {:error, operation_error(:register_runner, :post, base_url, :missing_manifest_version_id)}
    end
  end

  @spec bootstrap_active_manifest(String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def bootstrap_active_manifest(base_url, service_token)
      when is_binary(base_url) and is_binary(service_token) do
    url = base_url <> "/api/orchestrator/v1/bootstrap/active-manifest"

    case request_get(:bootstrap_active_manifest, url, service_token) do
      {:ok, %{"data" => data}} when is_map(data) ->
        {:ok, data}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, operation_error(:bootstrap_active_manifest, :get, url, :invalid_response)}
    end
  end

  @spec cancel_run(String.t(), String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def cancel_run(base_url, service_token, run_id)
      when is_binary(base_url) and is_binary(service_token) and is_binary(run_id) do
    request_post(
      :cancel_run,
      base_url <> "/api/orchestrator/v1/runs/#{run_id}/cancel",
      service_token,
      %{},
      nil,
      idempotency_key(:cancel_run, nil, %{run_id: run_id})
    )
  end

  @spec password_login(String.t(), String.t(), String.t(), String.t()) ::
          {:ok, session_context()} | {:error, term()}
  def password_login(base_url, service_token, username, password)
      when is_binary(base_url) and is_binary(service_token) and is_binary(username) and
             is_binary(password) do
    url = base_url <> "/api/orchestrator/v1/auth/password/sessions"

    with {:ok,
          %{
            "data" => %{
              "session" => %{"id" => session_id},
              "session_token" => session_token,
              "actor" => %{"id" => actor_id}
            }
          }} <-
           request_post(:password_login, url, service_token, %{
             username: username,
             password: password
           }),
          true <- is_binary(session_id) and session_id != "",
          true <- is_binary(actor_id) and actor_id != "",
          true <- is_binary(session_token) and session_token != "" do
      {:ok, %{"actor_id" => actor_id, "session_id" => session_id, "session_token" => session_token}}
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

  @spec submit_run(String.t(), String.t(), session_context(), map()) ::
          {:ok, map()} | {:error, term()}
  def submit_run(base_url, service_token, session_context, payload)
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_map(payload) do
    url = base_url <> "/api/orchestrator/v1/runs"

    case request_post(
           :submit_run,
           url,
           service_token,
           payload,
           session_context,
           idempotency_key(:submit_run, session_context, payload)
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

  @spec rerun_backfill_window(String.t(), String.t(), session_context(), String.t(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def rerun_backfill_window(base_url, service_token, session_context, backfill_run_id, window_key)
      when is_binary(base_url) and is_binary(service_token) and is_map(session_context) and
             is_binary(backfill_run_id) and is_binary(window_key) do
    url =
      base_url <>
        "/api/orchestrator/v1/backfills/#{URI.encode(backfill_run_id)}/windows/rerun"

    case request_post(
           :rerun_backfill_window,
           url,
           service_token,
           %{window_key: window_key},
           session_context,
           idempotency_key(:rerun_backfill_window, session_context, %{
             backfill_run_id: backfill_run_id,
             window_key: window_key
           })
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

  @spec in_flight_runs(String.t(), String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def in_flight_runs(base_url, service_token)
      when is_binary(base_url) and is_binary(service_token) do
    url = base_url <> "/api/orchestrator/v1/runs/in-flight"

    with {:ok, %{"data" => %{"run_ids" => run_ids}}} <-
           request_get(:list_in_flight_runs, url, service_token),
         true <- is_list(run_ids) do
      {:ok, Enum.filter(run_ids, &is_binary/1)}
    else
      false -> {:error, operation_error(:list_in_flight_runs, :get, url, :invalid_response)}
      {:error, _reason} = error -> error
      _other -> {:error, operation_error(:list_in_flight_runs, :get, url, :invalid_response)}
    end
  end

  @spec diagnostics(String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def diagnostics(base_url, service_token)
      when is_binary(base_url) and is_binary(service_token) do
    url = base_url <> "/api/orchestrator/v1/diagnostics"

    case request_get(:diagnostics, url, service_token) do
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

    case LocalHttpClient.request(:get, url, [], nil, connect_timeout_ms: 1_000, timeout_ms: 2_000) do
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
         session_context \\ nil,
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

  defp request(operation, method, url, service_token, body, session_context, idempotency_key) do
    headers =
      [
        {"accept", "application/json"},
        {"authorization", "Bearer #{service_token}"}
      ]
      |> add_session_headers(session_context)
      |> add_idempotency_header(idempotency_key)

    case LocalHttpClient.request(method, url, headers, body) do
      {:ok, decoded} -> {:ok, decoded}
      {:error, reason} -> {:error, operation_error(operation, method, url, reason)}
    end
  end

  defp add_session_headers(headers, %{"actor_id" => actor_id, "session_token" => session_token})
       when is_binary(actor_id) and actor_id != "" and is_binary(session_token) and
              session_token != "" do
    headers ++ [{"x-favn-actor-id", actor_id}, {"x-favn-session-token", session_token}]
  end

  defp add_session_headers(headers, _session_context), do: headers

  defp add_idempotency_header(headers, key) when is_binary(key) and key != "" do
    headers ++ [{"idempotency-key", key}]
  end

  defp add_idempotency_header(headers, _key), do: headers

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
    |> Map.take(["actor_id", "session_id"])
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
    %{operation: operation, method: method, url: url, reason: reason}
  end

  defp normalize_publish_payload(payload) when is_map(payload) do
    manifest = Map.get(payload, :manifest) || Map.get(payload, "manifest")

    if manifest do
      manifest_payload =
        manifest
        |> Serializer.encode_manifest!()
        |> JSON.decode!()

      payload
      |> Map.put(:manifest, manifest_payload)
      |> Map.delete("manifest")
    else
      payload
    end
  end
end
