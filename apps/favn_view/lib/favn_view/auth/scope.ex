defmodule FavnView.Auth.Scope do
  @moduledoc """
  Sanitized operator authentication scope for browser and LiveView assigns.

  The Phoenix session stores only a random browser session id. This scope is
  reconstructed through the public orchestrator facade and deliberately excludes
  raw tokens, token hashes, passwords, and service credential material.
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

  @type t :: %__MODULE__{actor: actor(), session: session(), roles: [atom()]}

  defstruct [:actor, :session, roles: []]

  @doc """
  Builds a browser-safe scope from orchestrator-owned actor and session data.
  """
  @spec new(map(), map()) :: t()
  def new(actor, session) when is_map(actor) and is_map(session) do
    roles = sanitize_roles(Map.get(actor, :roles) || Map.get(actor, "roles") || [])

    %__MODULE__{
      actor: %{
        id: Map.get(actor, :id) || Map.fetch!(actor, "id"),
        username: Map.get(actor, :username) || Map.get(actor, "username"),
        display_name: Map.get(actor, :display_name) || Map.get(actor, "display_name"),
        roles: roles
      },
      session: %{
        id: Map.get(session, :id) || Map.fetch!(session, "id"),
        actor_id: Map.get(session, :actor_id) || Map.get(session, "actor_id"),
        provider: Map.get(session, :provider) || Map.get(session, "provider"),
        issued_at: Map.get(session, :issued_at) || Map.get(session, "issued_at"),
        expires_at: Map.get(session, :expires_at) || Map.get(session, "expires_at"),
        revoked_at: Map.get(session, :revoked_at) || Map.get(session, "revoked_at")
      },
      roles: roles
    }
  end

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
