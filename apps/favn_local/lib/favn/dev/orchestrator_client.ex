defmodule Favn.Dev.OrchestratorClient do
  @moduledoc false

  alias Favn.Dev.LocalHttpClient
  alias Favn.Manifest.Serializer

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

  @spec activate_manifest(String.t(), String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def activate_manifest(base_url, service_token, manifest_version_id)
      when is_binary(base_url) and is_binary(service_token) and is_binary(manifest_version_id) do
    request_post(
      :activate_manifest,
      base_url <> "/api/orchestrator/v1/manifests/#{manifest_version_id}/activate",
      service_token,
      %{}
    )
  end

  @spec cancel_run(String.t(), String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def cancel_run(base_url, service_token, run_id)
      when is_binary(base_url) and is_binary(service_token) and is_binary(run_id) do
    request_post(
      :cancel_run,
      base_url <> "/api/orchestrator/v1/runs/#{run_id}/cancel",
      service_token,
      %{}
    )
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

  @spec health(String.t()) :: :ok | {:error, term()}
  def health(base_url) when is_binary(base_url) do
    url = base_url <> "/api/orchestrator/v1/health"

    case LocalHttpClient.request(:get, url, [], nil, connect_timeout_ms: 1_000, timeout_ms: 2_000) do
      {:ok, %{"data" => %{"status" => "ok"}}} -> :ok
      {:ok, %{"status" => "ok"}} -> :ok
      {:ok, decoded} -> {:error, operation_error(:health_check, :get, url, {:invalid_response, decoded})}
      {:error, reason} -> {:error, operation_error(:health_check, :get, url, reason)}
    end
  end

  defp request_post(operation, url, service_token, payload) do
    body = JSON.encode!(payload)

    request(operation, :post, url, service_token, body)
  end

  defp request_get(operation, url, service_token) do
    request(operation, :get, url, service_token, nil)
  end

  defp request(operation, method, url, service_token, body) do
    headers = [
      {"authorization", "Bearer #{service_token}"},
      {"content-type", "application/json"}
    ]

    case LocalHttpClient.request(method, url, headers, body) do
      {:ok, decoded} -> {:ok, decoded}
      {:error, reason} -> {:error, operation_error(operation, method, url, reason)}
    end
  end

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
