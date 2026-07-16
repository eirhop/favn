defmodule Favn.Azure.PostgresEntraToken do
  @moduledoc """
  Fetches Microsoft Entra access tokens for Azure Database for PostgreSQL.

  This compatibility facade delegates to the shared runner-local credential
  cache. New integrations may call `Favn.Azure.Credentials` directly.
  """

  alias Favn.Azure.Credentials
  alias Favn.Azure.{Token, TokenError}

  @resource "https://ossrdbms-aad.database.windows.net"

  @type auth :: keyword() | map()
  @type opts :: keyword()

  @spec fetch_token(auth(), opts()) :: {:ok, Token.t()} | {:error, TokenError.t()}
  def fetch_token(auth, opts \\ []) do
    with {:ok, auth} <- normalize_auth(auth) do
      {credential_opts, provider_opts} = Keyword.split(opts, [:cache, :timeout])

      auth
      |> Keyword.merge(credential_opts)
      |> Keyword.put(:provider_options, provider_opts)
      |> then(&Credentials.fetch_token(@resource, &1))
    end
  end

  defp normalize_auth(auth) when is_map(auth), do: normalize_auth(Map.to_list(auth))

  defp normalize_auth(auth) when is_list(auth) do
    if Keyword.keyword?(auth), do: {:ok, auth}, else: invalid_auth_config()
  end

  defp normalize_auth(_auth), do: invalid_auth_config()

  defp invalid_auth_config do
    {:error,
     %TokenError{
       type: :invalid_config,
       message: "invalid Azure PostgreSQL Entra auth config",
       details: %{reason: :invalid_auth_config}
     }}
  end
end
