defmodule FavnView.Auth.Scope do
  @moduledoc """
  Sanitized operator authentication scope for browser and LiveView assigns.

  The encrypted Phoenix session stores the raw opaque session token, while this
  reconstructed scope deliberately excludes tokens, token hashes, passwords, and
  service credential material.
  """

  @type actor :: %{
          required(:id) => String.t(),
          required(:username) => String.t() | nil,
          required(:display_name) => String.t() | nil,
          required(:roles) => [atom()]
        }

  @type session :: %{
          required(:id) => String.t(),
          required(:actor_id) => String.t(),
          required(:provider) => String.t() | nil,
          required(:issued_at) => DateTime.t() | nil,
          required(:expires_at) => DateTime.t() | nil,
          required(:revoked_at) => DateTime.t() | nil
        }

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          operator_context: FavnOrchestrator.OperatorContext.t(),
          actor: actor(),
          session: session(),
          workspace_configuration: FavnOrchestrator.WorkspaceConfiguration.t() | nil,
          roles: [atom()]
        }

  defstruct [
    :workspace_id,
    :operator_context,
    :actor,
    :session,
    workspace_configuration: %FavnOrchestrator.WorkspaceConfiguration{
      workspace_id: nil,
      deployment_id: nil,
      default_timezone: "Etc/UTC",
      default_timezone_source: :no_active_deployment
    },
    roles: []
  ]

  @doc """
  Builds a browser-safe scope from orchestrator-owned actor and session data.
  """
  @spec new(
          String.t(),
          FavnOrchestrator.operator_actor(),
          FavnOrchestrator.operator_session()
        ) :: t()
  def new(workspace_id, actor, session)
      when is_binary(workspace_id) and workspace_id != "" and is_map(actor) and is_map(session) do
    roles = sanitize_roles(actor.roles)
    {:ok, operator_context} = FavnOrchestrator.operator_context(workspace_id, actor, session)

    %__MODULE__{
      workspace_id: workspace_id,
      operator_context: operator_context,
      actor: %{
        id: actor.id,
        username: actor.username,
        display_name: actor.display_name,
        roles: roles
      },
      session: %{
        id: session.id,
        actor_id: session.actor_id,
        provider: session.provider,
        issued_at: session.issued_at,
        expires_at: session.expires_at,
        revoked_at: session.revoked_at
      },
      workspace_configuration:
        FavnOrchestrator.WorkspaceConfiguration.without_active_deployment(workspace_id),
      roles: roles
    }
  end

  @doc "Attaches the active deployment's browser-safe workspace configuration."
  @spec put_workspace_configuration(t(), FavnOrchestrator.WorkspaceConfiguration.t()) ::
          {:ok, t()} | {:error, :workspace_configuration_scope_mismatch}
  def put_workspace_configuration(
        %__MODULE__{workspace_id: workspace_id} = scope,
        %FavnOrchestrator.WorkspaceConfiguration{workspace_id: workspace_id} = configuration
      ) do
    {:ok, %{scope | workspace_configuration: configuration}}
  end

  def put_workspace_configuration(%__MODULE__{}, %FavnOrchestrator.WorkspaceConfiguration{}),
    do: {:error, :workspace_configuration_scope_mismatch}

  @doc """
  Returns whether the scope has at least the required operator role.
  """
  @spec has_role?(t() | nil, :viewer | :operator | :admin) :: boolean()
  def has_role?(%__MODULE__{actor: actor}, role) when role in [:viewer, :operator, :admin] do
    FavnOrchestrator.operator_has_role?(actor, role)
  end

  def has_role?(_scope, _role), do: false

  defp sanitize_roles(roles) when is_list(roles) do
    roles
    |> Enum.map(&sanitize_role/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp sanitize_roles(_roles), do: []

  defp sanitize_role(role) when role in [:viewer, :operator, :admin], do: role

  defp sanitize_role(role) when is_binary(role) do
    case role do
      "viewer" -> :viewer
      "operator" -> :operator
      "admin" -> :admin
      _other -> nil
    end
  end

  defp sanitize_role(_role), do: nil
end
