defmodule Favn.Azure.Credentials.AzureCLI do
  @moduledoc "Azure CLI source for runner-local Azure credentials."

  alias Favn.Azure.Credentials.Request
  alias Favn.Azure.{Token, TokenError}

  @behaviour Favn.Azure.CredentialProvider

  @max_response_bytes 1_048_576

  @impl true
  def fetch_token(%Request{resource: resource}, opts) do
    cmd = Keyword.get(opts, :system_cmd, &System.cmd/3)

    case cmd.("az", ["account", "get-access-token", "--resource", resource, "--output", "json"],
           stderr_to_stdout: false
         ) do
      {json, 0} -> parse_token(json)
      {_output, status} -> {:error, cli_error(status)}
    end
  rescue
    _error ->
      {:error,
       %TokenError{
         type: :connection_error,
         message: "Azure CLI token acquisition failed",
         retryable?: false,
         details: %{reason: :command_unavailable}
       }}
  end

  defp parse_token(json) do
    with true <- is_binary(json) and byte_size(json) <= @max_response_bytes,
         {:ok, decoded} <- Jason.decode(json),
         token when is_binary(token) and token != "" <- decoded["accessToken"],
         expiry when not is_nil(expiry) <- decoded["expires_on"] || decoded["expiresOn"],
         {:ok, token} <- Token.new(token, expiry) do
      {:ok, token}
    else
      _other -> invalid_response()
    end
  end

  defp invalid_response do
    {:error,
     %TokenError{
       type: :execution_error,
       message: "Azure CLI returned an invalid token response",
       details: %{reason: :invalid_token_response}
     }}
  end

  defp cli_error(status) do
    %TokenError{
      type: :authentication_error,
      message: "Azure CLI token acquisition failed",
      retryable?: false,
      details: %{status: status}
    }
  end
end
