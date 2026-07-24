defmodule FavnOrchestrator.Auth.Session do
  @moduledoc false

  alias FavnOrchestrator.RuntimeConfig

  @default_ttl_seconds 43_200
  @max_ttl_seconds 2_592_000
  @providers ["password_local", "trusted_local_dev"]
  @token_pattern ~r/\A[A-Za-z0-9_-]{43}\z/

  @spec issue(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def issue(actor_id, opts) when is_binary(actor_id) and is_list(opts) do
    with :ok <- validate_opts(opts),
         {:ok, ttl_seconds} <- ttl_seconds(opts),
         {:ok, provider} <- provider(opts) do
      token = raw_token()
      now = DateTime.utc_now()

      {:ok,
       %{
         id: "ses_" <> random_id(),
         actor_id: actor_id,
         provider: provider,
         issued_at: now,
         expires_at: DateTime.add(now, ttl_seconds, :second),
         revoked_at: nil,
         token_hash: token_hash(token),
         token: token
       }}
    end
  end

  def issue(_actor_id, _opts), do: {:error, :invalid_session_options}

  @spec active?(map()) :: :ok | {:error, term()}
  def active?(%{revoked_at: revoked_at, expires_at: %DateTime{} = expires_at}) do
    cond do
      not is_nil(revoked_at) -> {:error, :session_revoked}
      DateTime.compare(expires_at, DateTime.utc_now()) != :gt -> {:error, :session_expired}
      true -> :ok
    end
  end

  def active?(_session), do: {:error, :invalid_session}

  @spec valid_token?(term()) :: boolean()
  def valid_token?(token) when is_binary(token), do: Regex.match?(@token_pattern, token)
  def valid_token?(_token), do: false

  @spec token_hash(String.t()) :: String.t()
  def token_hash(token) do
    :sha256
    |> :crypto.hash(token)
    |> Base.url_encode64(padding: false)
  end

  @spec raw_token() :: String.t()
  def raw_token do
    32
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

  @spec random_id() :: String.t()
  def random_id do
    10
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

  defp validate_opts(opts) do
    if Keyword.keyword?(opts) and Keyword.keys(opts) -- [:provider, :ttl_seconds] == [] do
      :ok
    else
      {:error, :invalid_session_options}
    end
  end

  defp ttl_seconds(opts) do
    default = configured_default_ttl()

    case Keyword.get(opts, :ttl_seconds, default) do
      ttl when is_integer(ttl) and ttl >= 1 and ttl <= @max_ttl_seconds -> {:ok, ttl}
      _ttl -> {:error, :invalid_session_ttl}
    end
  end

  defp provider(opts) do
    case Keyword.get(opts, :provider, "password_local") do
      provider when provider in @providers -> {:ok, provider}
      _provider -> {:error, :invalid_session_provider}
    end
  end

  defp configured_default_ttl do
    case RuntimeConfig.auth_session_ttl_seconds() do
      ttl when is_integer(ttl) and ttl >= 1 and ttl <= @max_ttl_seconds -> ttl
      _invalid -> @default_ttl_seconds
    end
  end
end
