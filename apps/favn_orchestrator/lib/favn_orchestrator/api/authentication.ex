defmodule FavnOrchestrator.API.Authentication do
  @moduledoc """
  Authenticates private API service credentials and forwarded actor sessions.

  The trusted local-development context is deliberately narrow: it requires an
  explicit header, local-dev runtime configuration, a loopback API bind, and a
  loopback peer. Only after those checks does it obtain the runtime's synthetic
  local administrator context.
  """

  import Plug.Conn, only: [get_req_header: 2]

  alias FavnOrchestrator.API.Config
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.Auth.Store

  @type role :: :viewer | :operator | :admin

  @doc "Validates the request's service bearer token or trusted local context."
  @spec ensure_service(Plug.Conn.t()) :: :ok | {:error, :service_unauthorized}
  def ensure_service(conn) do
    if local_dev_allowed?(conn) do
      :ok
    else
      case authenticate_service(conn) do
        {:ok, _identity} -> :ok
        {:error, :service_unauthorized} = error -> error
      end
    end
  end

  @doc "Returns the active persisted actor/session when it grants `required_role`."
  @spec actor_context(Plug.Conn.t(), role()) ::
          {:ok, Auth.session(), Auth.actor()} | {:error, :forbidden | :unauthenticated | term()}
  def actor_context(conn, required_role) when required_role in [:viewer, :operator, :admin] do
    case header(conn, "x-favn-session-token") do
      token when is_binary(token) and token != "" ->
        forwarded_actor_context(header(conn, "x-favn-actor-id"), token, required_role)

      _other ->
        local_actor_context(conn, required_role)
    end
  end

  @doc "Returns the authenticated service identity without exposing token material."
  @spec service_identity(Plug.Conn.t()) :: String.t() | nil
  def service_identity(conn) do
    if local_dev_allowed?(conn) do
      "local-dev-cli"
    else
      case authenticate_service(conn) do
        {:ok, identity} -> identity
        {:error, :service_unauthorized} -> nil
      end
    end
  end

  @doc "Returns the normalized network peer identity used for login throttling."
  @spec remote_identity(Plug.Conn.t()) :: String.t()
  def remote_identity(%Plug.Conn{remote_ip: remote_ip}) do
    case :inet.ntoa(remote_ip) do
      address when is_list(address) -> List.to_string(address)
      _invalid -> "unknown"
    end
  rescue
    _error -> "unknown"
  end

  @doc "Returns redacted service-token diagnostics for an authenticated request."
  @spec service_token_diagnostics(Plug.Conn.t()) :: map()
  def service_token_diagnostics(conn) do
    identity = service_identity(conn)

    %{
      authenticated: not is_nil(identity),
      service_identity: identity,
      service_tokens: %{
        configured_count: ServiceTokens.configured_count(configured_tokens()),
        redacted: true
      }
    }
  end

  defp forwarded_actor_context(actor_id, token, required_role) do
    case Auth.actor_from_forwarded_context(actor_id, token) do
      {:ok, session, actor} -> authorize_role(session, actor, required_role)
      {:error, _reason} -> {:error, :unauthenticated}
    end
  end

  defp local_actor_context(conn, required_role) do
    cond do
      local_dev_allowed?(conn) ->
        with {:ok, session, actor} <-
               Store.trusted_local_dev_context("local-dev-cli", "Local Dev CLI", [:admin]) do
          authorize_role(session, actor, required_role)
        end

      local_dev_requested?(conn) ->
        {:error, :forbidden}

      true ->
        {:error, :unauthenticated}
    end
  end

  defp authorize_role(session, actor, required_role) do
    if Auth.has_role?(actor, required_role),
      do: {:ok, session, actor},
      else: {:error, :forbidden}
  end

  defp authenticate_service(conn) do
    ServiceTokens.authenticate(bearer_token(conn), configured_tokens())
  end

  defp bearer_token(conn) do
    case get_req_header(conn, "authorization") do
      ["Bearer " <> token] -> token
      _other -> nil
    end
  end

  defp configured_tokens, do: ServiceTokens.configured_tokens()

  defp local_dev_requested?(conn), do: header(conn, "x-favn-local-dev-context") == "trusted"

  defp local_dev_allowed?(conn) do
    local_dev_requested?(conn) and Config.local_dev_trusted_context_allowed?() and
      loopback_peer?(conn.remote_ip)
  end

  defp loopback_peer?({127, _b, _c, _d}), do: true
  defp loopback_peer?({0, 0, 0, 0, 0, 0, 0, 1}), do: true
  defp loopback_peer?(_remote_ip), do: false

  defp header(conn, key) do
    case get_req_header(conn, key) do
      [value] -> value
      _other -> nil
    end
  end
end
