defmodule FavnOrchestrator.API.Authentication do
  @moduledoc """
  Authenticates private API service credentials and forwarded actor sessions.
  """

  import Plug.Conn, only: [get_req_header: 2]

  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.ManifestDeployerTokens
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.ManifestDeploymentContext
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Persistence.PlatformContext

  @type role :: :viewer | :operator | :admin
  @type service_principal :: %{
          required(:id) => String.t(),
          required(:principal_kind) => :service,
          required(:principal_id) => String.t(),
          required(:service_identity) => String.t()
        }

  @doc "Validates the request's service bearer token."
  @spec ensure_service(Plug.Conn.t()) :: :ok | {:error, :service_unauthorized}
  def ensure_service(conn) do
    case authenticate_service(conn) do
      {:ok, _identity} -> :ok
      {:error, :service_unauthorized} = error -> error
    end
  end

  @doc "Returns the active persisted actor/session when it grants `required_role`."
  @spec actor_context(Plug.Conn.t(), role()) ::
          {:ok, Auth.session(), Auth.actor()} | {:error, :forbidden | :unauthenticated | term()}
  def actor_context(conn, required_role) when required_role in [:viewer, :operator, :admin] do
    case workspace_context(conn, required_role) do
      {:ok, session, actor, _context} -> {:ok, session, actor}
      {:error, _reason} = error -> error
    end
  end

  @doc "Returns the active actor and its database-authorized workspace context."
  @spec workspace_context(Plug.Conn.t(), role()) ::
          {:ok, Auth.session(), Auth.actor(), WorkspaceContext.t()}
          | {:error, :forbidden | :unauthenticated | term()}
  def workspace_context(conn, required_role)
      when required_role in [:viewer, :operator, :admin] do
    with {:ok, workspace_id} <- workspace_id(conn),
         {:ok, auth_context} <-
           WorkspaceContext.new(workspace_id, "auth:session", [:customer_reader]),
         {:ok, session, actor} <- persisted_actor_context(conn, auth_context),
         {:ok, context} <- authorized_context(workspace_id, session, actor, required_role) do
      {:ok, session, actor, context}
    else
      {:error, :invalid_context} -> {:error, :unauthenticated}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Returns actor workspace authority when actor headers are present, otherwise
  falls back to an authenticated platform-operator service identity.

  A forbidden or invalid actor session never falls back to service authority.
  This keeps explicit actor authorization authoritative while allowing trusted
  service-only clients such as the local CLI to use one consistent workspace
  contract.
  """
  @spec workspace_or_service_context(Plug.Conn.t(), role()) ::
          {:ok, Auth.session() | service_principal(), Auth.actor() | service_principal(),
           WorkspaceContext.t()}
          | {:error, :forbidden | :service_unauthorized | :unauthenticated | term()}
  def workspace_or_service_context(conn, required_role)
      when required_role in [:viewer, :operator, :admin] do
    if forwarded_actor_headers_present?(conn) do
      workspace_context(conn, required_role)
    else
      service_workspace_context(conn)
    end
  end

  @doc "Builds workspace authority for a platform-operator service request."
  @spec service_workspace_context(Plug.Conn.t()) ::
          {:ok, service_principal(), service_principal(), WorkspaceContext.t()}
          | {:error, :service_unauthorized | :forbidden | :unauthenticated}
  def service_workspace_context(conn) do
    with {:ok, workspace_id} <- workspace_id(conn),
         {:ok, principal} <- authenticate_platform_service(conn),
         true <- :platform_operator in principal.platform_roles,
         identity = principal.service_identity,
         actor_id = "service:" <> identity,
         session_id = "api-service:" <> identity,
         {:ok, context} <-
           WorkspaceContext.new(workspace_id, actor_id, [:workspace_admin],
             request_id: session_id
           ) do
      principal = %{
        id: actor_id,
        principal_kind: :service,
        principal_id: actor_id,
        service_identity: identity
      }

      {:ok, Map.put(principal, :id, session_id), principal, context}
    else
      false -> {:error, :forbidden}
      {:error, :invalid_context} -> {:error, :forbidden}
      {:error, _reason} = error -> error
    end
  end

  @doc false
  def command_principal(
        %{principal_kind: :service, principal_id: principal_id} = session,
        %{principal_kind: :service, principal_id: principal_id} = actor
      ) do
    %{
      kind: :service,
      id: principal_id,
      actor_id: nil,
      session_id: nil,
      service_identity: actor.service_identity || session.service_identity
    }
  end

  def command_principal(%{id: session_id}, %{id: actor_id}) do
    %{
      kind: :actor,
      id: actor_id,
      actor_id: actor_id,
      session_id: session_id,
      service_identity: nil
    }
  end

  @doc "Returns the one explicit workspace selected by the request."
  @spec workspace_id(Plug.Conn.t()) :: {:ok, String.t()} | {:error, :unauthenticated}
  def workspace_id(conn) do
    case get_req_header(conn, "x-favn-workspace-id") do
      [workspace_id] when workspace_id != "" and byte_size(workspace_id) <= 255 ->
        {:ok, workspace_id}

      _missing_or_ambiguous ->
        {:error, :unauthenticated}
    end
  end

  @doc "Authenticates a workspace-scoped manifest deployer or platform operator."
  @spec manifest_deployment_context(Plug.Conn.t()) ::
          {:ok, ManifestDeploymentContext.t()}
          | {:error, :service_unauthorized | :forbidden | :unauthenticated}
  def manifest_deployment_context(conn) do
    with {:ok, workspace_id} <- workspace_id(conn),
         {:ok, identity} <- manifest_deployer_identity(conn, workspace_id),
         request_id when is_binary(request_id) and request_id != "" <-
           conn.assigns[:request_id] || header(conn, "x-request-id"),
         {:ok, context} <- ManifestDeploymentContext.new(identity, workspace_id, request_id) do
      {:ok, context}
    else
      {:error, :workspace_forbidden} -> {:error, :forbidden}
      {:error, :invalid_context} -> {:error, :forbidden}
      {:error, _reason} = error -> error
      _missing -> {:error, :unauthenticated}
    end
  end

  @doc "Builds explicit platform authority from an authenticated internal service."
  @spec platform_context(Plug.Conn.t(), :platform_reader | :platform_operator) ::
          {:ok, PlatformContext.t()} | {:error, :service_unauthorized | :forbidden}
  def platform_context(conn, role) when role in [:platform_reader, :platform_operator] do
    with {:ok, principal} <- authenticate_platform_service(conn),
         true <- role in principal.platform_roles,
         identity <- principal.service_identity,
         {:ok, context} <-
           PlatformContext.new(
             "service:" <> identity,
             "api-service:" <> identity,
             [role]
           ) do
      {:ok, context}
    else
      false -> {:error, :forbidden}
      {:error, :invalid_context} -> {:error, :forbidden}
      {:error, _reason} = error -> error
    end
  end

  @doc "Builds read-only capacity authority from a separately scoped service token."
  @spec capacity_context(Plug.Conn.t()) ::
          {:ok, PlatformContext.t(), String.t()} | {:error, :service_unauthorized | :forbidden}
  def capacity_context(conn) do
    with {:ok, principal} <- authenticate_platform_service(conn),
         true <- :capacity_reader in principal.platform_roles,
         identity <- principal.service_identity,
         {:ok, context} <-
           PlatformContext.new(
             "capacity:" <> identity,
             "capacity-api:" <> identity,
             [:platform_reader]
           ) do
      {:ok, context, identity}
    else
      false -> {:error, :forbidden}
      {:error, :invalid_context} -> {:error, :forbidden}
      {:error, _reason} = error -> error
    end
  end

  defp persisted_actor_context(conn, context) do
    case header(conn, "x-favn-session-token") do
      token when is_binary(token) and token != "" ->
        case apply(Auth, :actor_from_forwarded_context, [
               context,
               header(conn, "x-favn-actor-id"),
               token
             ]) do
          {:ok, session, actor} -> {:ok, session, actor}
          {:error, _reason} -> {:error, :unauthenticated}
        end

      _missing ->
        {:error, :unauthenticated}
    end
  end

  defp authorized_context(workspace_id, session, actor, required_role) do
    if Auth.has_role?(actor, required_role) do
      WorkspaceContext.new(
        workspace_id,
        actor.id,
        persistence_roles(actor.roles),
        request_id: session.id
      )
    else
      {:error, :forbidden}
    end
  end

  defp persistence_roles(roles) do
    roles
    |> Enum.map(fn
      :viewer -> :customer_reader
      :operator -> :customer_operator
      :admin -> :workspace_admin
    end)
    |> Enum.uniq()
  end

  @doc "Returns the authenticated service identity without exposing token material."
  @spec service_identity(Plug.Conn.t()) :: String.t() | nil
  def service_identity(conn) do
    case authenticate_service(conn) do
      {:ok, principal} -> principal.service_identity
      {:error, :service_unauthorized} -> nil
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

  defp authenticate_service(conn) do
    ServiceTokens.authenticate(bearer_token(conn), configured_tokens())
  end

  defp authenticate_platform_service(conn) do
    authenticate_service(conn)
  end

  defp manifest_deployer_identity(conn, workspace_id) do
    token = bearer_token(conn)

    case ManifestDeployerTokens.authenticate(
           token,
           workspace_id,
           ManifestDeployerTokens.configured_tokens()
         ) do
      {:ok, identity} ->
        {:ok, identity}

      {:error, :workspace_forbidden} = error ->
        error

      {:error, :service_unauthorized} ->
        with {:ok, principal} <- ServiceTokens.authenticate(token, configured_tokens()),
             true <- :platform_operator in principal.platform_roles do
          {:ok, principal.service_identity}
        else
          false -> {:error, :forbidden}
          {:error, _reason} -> {:error, :service_unauthorized}
        end
    end
  end

  defp bearer_token(conn) do
    case get_req_header(conn, "authorization") do
      ["Bearer " <> token] -> token
      _other -> nil
    end
  end

  defp configured_tokens, do: ServiceTokens.configured_tokens()

  defp header(conn, key) do
    case get_req_header(conn, key) do
      [value] -> value
      _other -> nil
    end
  end

  defp forwarded_actor_headers_present?(conn) do
    get_req_header(conn, "x-favn-actor-id") != [] or
      get_req_header(conn, "x-favn-session-token") != []
  end
end
