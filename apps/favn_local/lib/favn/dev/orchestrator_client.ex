defmodule Favn.Dev.OrchestratorClient do
  @moduledoc false

  alias Favn.Manifest.Serializer

  @spec publish_manifest(String.t(), String.t(), map()) ::
          {:ok, map()} | {:error, term()}
  def publish_manifest(base_url, service_token, payload)
      when is_binary(base_url) and is_binary(service_token) and is_map(payload) do
    request_post(
      base_url <> "/api/orchestrator/v1/manifests",
      service_token,
      normalize_publish_payload(payload)
    )
  end

  @spec activate_manifest(String.t(), String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def activate_manifest(base_url, service_token, manifest_version_id)
      when is_binary(base_url) and is_binary(service_token) and is_binary(manifest_version_id) do
    request_post(
      base_url <> "/api/orchestrator/v1/manifests/#{manifest_version_id}/activate",
      service_token,
      %{}
    )
  end

  @spec cancel_run(String.t(), String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def cancel_run(base_url, service_token, run_id)
      when is_binary(base_url) and is_binary(service_token) and is_binary(run_id) do
    request_post(base_url <> "/api/orchestrator/v1/runs/#{run_id}/cancel", service_token, %{})
  end

  @spec in_flight_runs(String.t(), String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def in_flight_runs(base_url, service_token)
      when is_binary(base_url) and is_binary(service_token) do
    with {:ok, %{"data" => %{"run_ids" => run_ids}}} <-
           request_get(base_url <> "/api/orchestrator/v1/runs/in-flight", service_token),
         true <- is_list(run_ids) do
      {:ok, Enum.filter(run_ids, &is_binary/1)}
    else
      false -> {:error, :invalid_response}
      {:error, _reason} = error -> error
      _other -> {:error, :invalid_response}
    end
  end

  defp request_post(url, service_token, payload) do
    body = JSON.encode!(payload)

    args = [
      "-sS",
      "-X",
      "POST",
      "-H",
      "authorization: Bearer #{service_token}",
      "-H",
      "content-type: application/json",
      "-d",
      body,
      "-w",
      "\n%{http_code}",
      url
    ]

    request(args)
  end

  defp request_get(url, service_token) do
    args = [
      "-sS",
      "-X",
      "GET",
      "-H",
      "authorization: Bearer #{service_token}",
      "-w",
      "\n%{http_code}",
      url
    ]

    request(args)
  end

  defp request(args) do
    case System.cmd("curl", args, stderr_to_stdout: true) do
      {output, 0} ->
        decode_response(output)

      {output, status} ->
        {:error, {:http_failed, status, output}}
    end
  end

  defp decode_response(output) when is_binary(output) do
    case String.split(output, "\n", trim: true) do
      [] ->
        {:error, :empty_response}

      lines ->
        status = lines |> List.last() |> String.trim()
        body = lines |> Enum.drop(-1) |> Enum.join("\n")

        with {code, ""} <- Integer.parse(status),
             {:ok, decoded} <- JSON.decode(body) do
          if code >= 200 and code < 300 do
            {:ok, decoded}
          else
            {:error, {:http_error, code, decoded}}
          end
        else
          _ -> {:error, {:invalid_response, output}}
        end
    end
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
