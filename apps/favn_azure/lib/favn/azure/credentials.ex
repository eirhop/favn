defmodule Favn.Azure.Credentials do
  @moduledoc """
  Fetches and reuses short-lived Azure access tokens inside a Favn runner.

  Add `{Favn.Azure.RunnerPlugin, []}` to `config :favn, :runner_plugins` to use
  the runner-local cache. When that plugin is configured, a temporarily missing
  cache fails closed instead of silently bypassing its bounds. Calls in an
  application without the plugin use an owned, timeout-bounded direct fetch;
  `cache: false` selects that path explicitly.

  ## Consumer code

      {:ok, token} =
        Favn.Azure.Credentials.fetch_token(
          "https://vault.azure.net",
          provider: "managed_identity"
        )

      headers = [{"authorization", "Bearer " <> token.access_token}]

  Use `fetch_access_token/2` when only the string is needed. The cache performs
  single-flight refresh: concurrent callers for the same request share one
  provider call. A still-valid cached token is used if a refresh fails. Direct
  and cached calls are both bounded by finite timeouts.

  ## DuckDB session scripts

      params: [
        azure_token:
          Favn.Azure.Credentials.token_ref(
            "https://storage.azure.com/",
            provider: "managed_identity"
          )
      ]

  The ref contains no token. DuckDB resolves it immediately before planning a
  physical session, treats the result as secret, and includes only a hash in
  pool identity.

  For DuckLake metadata in Azure Database for PostgreSQL, request its Entra
  audience and pass the ref as the `PASSWORD` parameter of a native DuckDB
  PostgreSQL secret:

      Favn.Azure.Credentials.token_ref(
        "https://ossrdbms-aad.database.windows.net",
        provider: "managed_identity"
      )

  Built-in provider names are the strings `"cli"` and `"managed_identity"`.
  This lets an environment value be passed unchanged both to Favn and to a
  native DuckDB Azure `CHAIN` parameter. A custom module implementing
  `Favn.Azure.CredentialProvider` may be passed instead when extending the
  credential source in Elixir.

  Cache state is runner-local and disposable. It is not a credential store and
  is never a durability mechanism.
  """

  @behaviour Favn.RuntimeValue.Provider

  alias Favn.Azure.Credentials.{Cache, Request, Source}
  alias Favn.Azure.{Token, TokenError}

  @typedoc "Canonical built-in provider string: `\"cli\"` or `\"managed_identity\"`."
  @type built_in_provider :: Request.built_in_provider()

  @typedoc "A built-in provider name or a custom credential-provider module."
  @type provider :: Request.provider()

  @default_cache Cache
  @default_call_timeout 12_000
  @max_call_timeout 60_000
  @max_provider_options_bytes 65_536

  @doc """
  Fetches a normalized Azure token, using the runner cache when available.

  The `:provider` option accepts `"cli"`, `"managed_identity"`, or a custom
  module implementing `Favn.Azure.CredentialProvider`.
  """
  @spec fetch_token(String.t() | Request.t(), keyword() | map()) ::
          {:ok, Token.t()} | {:error, TokenError.t()}
  def fetch_token(resource_or_request, opts \\ [])

  def fetch_token(resource, opts) when is_binary(resource) do
    with {:ok, opts} <- normalize_opts(opts),
         {request_opts, fetch_opts} <- Keyword.split(opts, [:provider, :client_id, :endpoint]),
         {:ok, request} <- Request.new(resource, request_opts) do
      fetch_token(request, fetch_opts)
    end
  end

  def fetch_token(%Request{} = request, opts) do
    with {:ok, opts} <- normalize_opts(opts),
         :ok <- validate_fetch_opts(opts) do
      cache = Keyword.get(opts, :cache, :default)
      provider_opts = Keyword.get(opts, :provider_options, [])
      timeout = Keyword.get(opts, :timeout, @default_call_timeout)

      case cache_server(cache) do
        :direct -> fetch_direct(request, provider_opts, timeout)
        {:ok, server} -> Cache.fetch(server, request, provider_opts, timeout)
        {:error, error} -> {:error, error}
      end
    end
  end

  def fetch_token(_request, _opts), do: invalid_options()

  @doc """
  Fetches only the access-token string.

  Provider selection follows `fetch_token/2`.
  """
  @spec fetch_access_token(String.t() | Request.t(), keyword() | map()) ::
          {:ok, String.t()} | {:error, TokenError.t()}
  def fetch_access_token(resource_or_request, opts \\ []) do
    with {:ok, %Token{access_token: access_token}} <- fetch_token(resource_or_request, opts) do
      {:ok, access_token}
    end
  end

  @doc """
  Builds a secret runtime-value ref for a DuckDB session-script parameter.

  The `:provider` option accepts `"cli"`, `"managed_identity"`, or a custom
  module implementing `Favn.Azure.CredentialProvider`. Raises
  `Favn.Azure.TokenError` when the credential request is invalid.
  """
  @spec token_ref(String.t(), keyword() | map()) :: Favn.RuntimeValue.Ref.t()
  def token_ref(resource, opts \\ []) do
    request = Request.new!(resource, opts)
    Favn.RuntimeValue.new(__MODULE__, request_to_map(request), secret?: true)
  end

  @impl Favn.RuntimeValue.Provider
  def fetch_runtime_value(%Request{} = request), do: fetch_access_token(request)

  def fetch_runtime_value(request) when is_map(request) do
    resource = Map.get(request, :resource, Map.get(request, "resource"))
    request_opts = Map.drop(request, [:resource, "resource"])

    with {:ok, request} <- Request.new(resource, request_opts) do
      fetch_access_token(request)
    end
  end

  def fetch_runtime_value(_request) do
    {:error,
     %TokenError{
       type: :invalid_config,
       message: "invalid Azure credential runtime value",
       details: %{reason: :invalid_request}
     }}
  end

  defp cache_server(false), do: :direct

  defp cache_server(:default) do
    cond do
      Process.whereis(@default_cache) -> {:ok, @default_cache}
      default_cache_configured?() -> {:error, cache_unavailable_error()}
      true -> :direct
    end
  end

  defp cache_server(server) when is_atom(server) or is_pid(server) do
    if alive?(server) do
      {:ok, server}
    else
      {:error, cache_unavailable_error()}
    end
  end

  defp cache_server(_server), do: {:error, elem(invalid_options(), 1)}

  defp alive?(pid) when is_pid(pid), do: Process.alive?(pid)
  defp alive?(name), do: not is_nil(Process.whereis(name))

  defp normalize_opts(opts) when is_map(opts), do: normalize_opts(Map.to_list(opts))

  defp normalize_opts(opts) when is_list(opts) do
    allowed = [:provider, :client_id, :endpoint, :cache, :timeout, :provider_options]

    if Keyword.keyword?(opts) and Keyword.keys(opts) -- allowed == [] do
      {:ok, opts}
    else
      invalid_options()
    end
  end

  defp normalize_opts(_opts), do: invalid_options()

  defp validate_fetch_opts(opts) do
    timeout = Keyword.get(opts, :timeout, @default_call_timeout)
    provider_options = Keyword.get(opts, :provider_options, [])

    if Keyword.keys(opts) -- [:cache, :timeout, :provider_options] == [] and
         is_integer(timeout) and timeout > 0 and timeout <= @max_call_timeout and
         is_list(provider_options) and Keyword.keyword?(provider_options) and
         bounded_term?(provider_options, @max_provider_options_bytes) do
      :ok
    else
      invalid_options()
    end
  end

  defp bounded_term?(term, limit) do
    :erlang.external_size(term) <= limit
  rescue
    _error -> false
  end

  defp fetch_direct(request, provider_opts, timeout) do
    caller = self()
    tag = make_ref()

    {provider, monitor} =
      spawn_monitor(fn ->
        send(caller, {tag, Source.fetch_token(request, provider_opts)})
      end)

    receive do
      {^tag, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^provider, _reason} ->
        {:error, direct_provider_error(:provider_exited)}
    after
      timeout ->
        Process.exit(provider, :kill)
        Process.demonitor(monitor, [:flush])
        {:error, direct_provider_error(:provider_timeout)}
    end
  end

  defp default_cache_configured? do
    :favn
    |> Application.get_env(:runner_plugins, [])
    |> List.wrap()
    |> Enum.any?(fn
      Favn.Azure.RunnerPlugin -> true
      {Favn.Azure.RunnerPlugin, _opts} -> true
      _other -> false
    end)
  end

  defp cache_unavailable_error do
    %TokenError{
      type: :connection_error,
      message: "Azure credential cache is unavailable",
      retryable?: true,
      details: %{reason: :cache_unavailable}
    }
  end

  defp direct_provider_error(reason) do
    %TokenError{
      type: :connection_error,
      message: "Azure credential provider did not complete",
      retryable?: true,
      details: %{reason: reason}
    }
  end

  defp invalid_options do
    {:error,
     %TokenError{
       type: :invalid_config,
       message: "invalid Azure credential options",
       details: %{reason: :invalid_options}
     }}
  end

  defp request_to_map(%Request{} = request) do
    %{
      resource: request.resource,
      provider: request.provider,
      client_id: request.client_id,
      endpoint: request.endpoint
    }
  end
end
