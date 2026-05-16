defmodule Favn.Azure.PostgresEntraToken.AzureCLI do
  @moduledoc """
  Azure CLI token provider for Azure Database for PostgreSQL Entra auth.
  """

  alias Favn.Azure.{Token, TokenError}

  @behaviour Favn.Azure.PostgresEntraTokenProvider

  @resource "https://ossrdbms-aad.database.windows.net"

  @impl true
  def fetch_token(_auth, opts) do
    cmd = Keyword.get(opts, :system_cmd, &System.cmd/3)

    case cmd.("az", ["account", "get-access-token", "--resource", @resource, "--output", "json"],
           stderr_to_stdout: false
         ) do
      {json, 0} -> parse_token(json)
      {_output, status} -> {:error, cli_error(status)}
    end
  rescue
    error in ErlangError ->
      {:error,
       %TokenError{
         type: :connection_error,
         message: "Azure CLI token acquisition failed",
         retryable?: false,
         details: %{reason: inspect(error.original)}
       }}
  end

  defp parse_token(json) do
    with {:ok, decoded} <- Jason.decode(json),
         token when is_binary(token) and token != "" <- decoded["accessToken"] do
      {:ok, %Token{access_token: token, expires_on: decoded["expiresOn"]}}
    else
      _other ->
        {:error,
         %TokenError{
           type: :execution_error,
           message: "Azure CLI returned an invalid token response",
           details: %{reason: :invalid_token_response}
         }}
    end
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
