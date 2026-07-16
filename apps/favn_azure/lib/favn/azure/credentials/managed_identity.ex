defmodule Favn.Azure.Credentials.ManagedIdentity do
  @moduledoc """
  Managed-identity source for runner-local Azure credentials.

  `endpoint: :auto` selects Azure App Service managed identity when both
  `IDENTITY_ENDPOINT` and `IDENTITY_HEADER` are present; otherwise it uses the
  Azure Instance Metadata Service endpoint.
  """

  alias Favn.Azure.Credentials.Request
  alias Favn.Azure.{Token, TokenError}

  @behaviour Favn.Azure.CredentialProvider

  @imds_endpoint "http://169.254.169.254/metadata/identity/oauth2/token"
  @imds_api_version "2018-02-01"
  @azure_app_service_api_version "2019-08-01"
  @retry_statuses [404, 429, 500, 502, 503, 504]
  @retry_delays [100, 200, 400]
  @max_response_bytes 1_048_576

  @impl true
  def fetch_token(%Request{} = request, opts) do
    with {:ok, http_request} <- build_request(request, opts) do
      request_with_retry(http_request, opts, @retry_delays)
    end
  end

  defp build_request(request, opts) do
    env = Keyword.get(opts, :env, &System.get_env/1)

    case resolve_endpoint(request.endpoint, env) do
      {:ok, :imds, url, headers} ->
        {:ok, request(url, headers, request, @imds_api_version)}

      {:ok, :azure_app_service, url, headers} ->
        {:ok, request(url, headers, request, @azure_app_service_api_version)}

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

  defp request(url, headers, request, api_version) do
    query =
      [{:"api-version", api_version}, {:resource, request.resource}]
      |> maybe_put(:client_id, request.client_id)
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

      {:error, _reason} ->
        {:error,
         %TokenError{
           type: :connection_error,
           message: "managed identity token request failed",
           retryable?: false,
           details: %{reason: :request_failed}
         }}
    end
  end

  defp parse_token(json) do
    with true <- is_binary(json) and byte_size(json) <= @max_response_bytes,
         {:ok, decoded} <- Jason.decode(json),
         token when is_binary(token) and token != "" <- decoded["access_token"],
         {:ok, token} <- Token.new(token, decoded["expires_on"]) do
      {:ok, token}
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
