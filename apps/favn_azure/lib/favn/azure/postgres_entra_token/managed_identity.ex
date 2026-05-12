defmodule Favn.Azure.PostgresEntraToken.ManagedIdentity do
  @moduledoc """
  Managed identity token provider for Azure Database for PostgreSQL Entra auth.

  `endpoint: :auto` selects Azure App Service managed identity when both
  `IDENTITY_ENDPOINT` and `IDENTITY_HEADER` are present; otherwise it falls back
  to the Azure Instance Metadata Service endpoint.
  """

  alias Favn.Azure.{Token, TokenError}

  @behaviour Favn.Azure.PostgresEntraTokenProvider

  @resource "https://ossrdbms-aad.database.windows.net"
  @imds_endpoint "http://169.254.169.254/metadata/identity/oauth2/token"
  @imds_api_version "2018-02-01"
  @azure_app_service_api_version "2019-08-01"
  @retry_statuses [404, 429, 500, 502, 503, 504]
  @retry_delays [100, 200, 400]

  @impl true
  def fetch_token(auth, opts) do
    with {:ok, request} <- build_request(auth, opts) do
      request_with_retry(request, opts, @retry_delays)
    end
  end

  defp build_request(auth, opts) do
    env = Keyword.get(opts, :env, &System.get_env/1)
    endpoint = Keyword.get(auth, :endpoint, :auto)

    case resolve_endpoint(endpoint, env) do
      {:ok, :imds, url, headers} ->
        {:ok, request(url, headers, auth, @imds_api_version)}

      {:ok, :azure_app_service, url, headers} ->
        {:ok, request(url, headers, auth, @azure_app_service_api_version)}

      {:error, %TokenError{} = error} ->
        {:error, error}
    end
  end

  defp resolve_endpoint(:auto, env) do
    case {env.("IDENTITY_ENDPOINT"), env.("IDENTITY_HEADER")} do
      {endpoint, header}
      when is_binary(endpoint) and endpoint != "" and is_binary(header) and header != "" ->
        {:ok, :azure_app_service, endpoint, [{~c"X-IDENTITY-HEADER", to_charlist(header)}]}

      _missing ->
        {:ok, :imds, @imds_endpoint, [{~c"Metadata", ~c"true"}]}
    end
  end

  defp resolve_endpoint(:imds, _env), do: {:ok, :imds, @imds_endpoint, [{~c"Metadata", ~c"true"}]}

  defp resolve_endpoint(:azure_app_service, env) do
    case {env.("IDENTITY_ENDPOINT"), env.("IDENTITY_HEADER")} do
      {endpoint, header}
      when is_binary(endpoint) and endpoint != "" and is_binary(header) and header != "" ->
        {:ok, :azure_app_service, endpoint, [{~c"X-IDENTITY-HEADER", to_charlist(header)}]}

      _missing ->
        {:error,
         %TokenError{
           type: :invalid_config,
           message: "Azure App Service managed identity endpoint is not configured",
           details: %{reason: :missing_identity_endpoint}
         }}
    end
  end

  defp resolve_endpoint(endpoint, _env) do
    {:error,
     %TokenError{
       type: :invalid_config,
       message: "invalid managed identity endpoint",
       details: %{endpoint: inspect(endpoint)}
     }}
  end

  defp request(url, headers, auth, api_version) do
    query =
      [api_version: api_version, resource: @resource]
      |> maybe_put(:client_id, Keyword.get(auth, :client_id))
      |> URI.encode_query()

    separator = if String.contains?(url, "?"), do: "&", else: "?"
    {String.to_charlist(url <> separator <> query), headers}
  end

  defp maybe_put(query, _key, nil), do: query
  defp maybe_put(query, key, value), do: Keyword.put(query, key, value)

  defp request_with_retry(request, opts, delays) do
    case http_request(request, opts) do
      {:ok, token} ->
        {:ok, token}

      {:error, %TokenError{retryable?: true} = error} when delays != [] ->
        retry(request, opts, delays, error)

      {:error, %TokenError{} = error} ->
        {:error, error}
    end
  end

  defp retry(request, opts, [delay | rest], _error) do
    sleeper = Keyword.get(opts, :sleeper, &Process.sleep/1)
    sleeper.(delay)
    request_with_retry(request, opts, rest)
  end

  defp http_request({url, headers}, opts) do
    http_client = Keyword.get(opts, :http_client, &:httpc.request/4)

    case http_client.(:get, {url, headers}, [{:timeout, 5_000}], body_format: :binary) do
      {:ok, {{_version, status, _reason}, _headers, body}} when status in 200..299 ->
        parse_token(body)

      {:ok, {{_version, status, _reason}, _headers, _body}} ->
        {:error, http_error(status)}

      {:error, :timeout} ->
        {:error, timeout_error()}

      {:error, reason} ->
        {:error,
         %TokenError{
           type: :connection_error,
           message: "managed identity token request failed",
           retryable?: false,
           details: %{reason: inspect(reason)}
         }}
    end
  end

  defp parse_token(json) do
    with {:ok, decoded} <- Jason.decode(json),
         token when is_binary(token) and token != "" <- decoded["access_token"] do
      {:ok, %Token{access_token: token, expires_on: decoded["expires_on"]}}
    else
      _other ->
        {:error,
         %TokenError{
           type: :execution_error,
           message: "managed identity returned an invalid token response",
           details: %{reason: :invalid_token_response}
         }}
    end
  end

  defp http_error(status) do
    %TokenError{
      type: if(status in @retry_statuses, do: :connection_error, else: :authentication_error),
      message: "managed identity token request failed",
      retryable?: status in @retry_statuses,
      details: %{status: status}
    }
  end

  defp timeout_error do
    %TokenError{
      type: :connection_error,
      message: "managed identity token request timed out",
      retryable?: true,
      details: %{reason: :timeout}
    }
  end
end
