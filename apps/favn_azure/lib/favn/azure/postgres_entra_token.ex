defmodule Favn.Azure.PostgresEntraToken do
  @moduledoc """
  Fetches Microsoft Entra access tokens for Azure Database for PostgreSQL.

  The raw HTTP and Azure CLI providers request the PostgreSQL resource exactly as
  `https://ossrdbms-aad.database.windows.net`. Returned tokens are runtime-only
  values for immediate connection bootstrap use and are not cached.
  """

  alias Favn.Azure.PostgresEntraToken.{AzureCLI, ManagedIdentity}
  alias Favn.Azure.{Token, TokenError}

  @type auth :: keyword() | map()
  @type opts :: keyword()

  @spec fetch_token(auth(), opts()) :: {:ok, Token.t()} | {:error, TokenError.t()}
  def fetch_token(auth, opts \\ []) do
    with {:ok, auth} <- normalize_auth(auth) do
      provider = Keyword.get(opts, :provider_module) || provider_module(auth)
      provider.fetch_token(auth, opts)
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

  defp provider_module(auth) do
    case Keyword.get(auth, :provider) do
      :managed_identity -> ManagedIdentity
      :azure_cli -> AzureCLI
      _other -> UnsupportedProvider
    end
  end

  defmodule UnsupportedProvider do
    @moduledoc false

    alias Favn.Azure.TokenError

    @behaviour Favn.Azure.PostgresEntraTokenProvider

    @impl true
    def fetch_token(auth, _opts) do
      {:error,
       %TokenError{
         type: :invalid_config,
         message: "unsupported Azure PostgreSQL Entra token provider",
         details: %{provider: inspect(Keyword.get(auth, :provider))}
       }}
    end
  end
end
