defmodule FavnOrchestrator.Persistence.RunSubmissionAuthority do
  @moduledoc """
  Redacted authority snapshot attached to durable run-submission intent.

  The snapshot is derived exclusively from an already-validated workspace
  context. It is audit evidence, not a reusable authorization credential.
  """

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_id, :principal_id, :roles]
  defstruct [:workspace_id, :principal_id, :request_id, roles: []]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          principal_id: String.t(),
          roles: [WorkspaceContext.role()],
          request_id: String.t() | nil
        }

  @doc "Builds the only authority representation accepted for durable submissions."
  @spec from_context(WorkspaceContext.t()) :: t()
  def from_context(%WorkspaceContext{} = context) do
    %__MODULE__{
      workspace_id: context.workspace_id,
      principal_id: context.principal_id,
      roles: Enum.sort(context.roles),
      request_id: context.request_id
    }
  end

  @doc false
  @spec dump(t()) :: map()
  def dump(%__MODULE__{} = snapshot) do
    %{
      "workspace_id" => snapshot.workspace_id,
      "principal_id" => snapshot.principal_id,
      "roles" => Enum.map(snapshot.roles, &Atom.to_string/1),
      "request_id" => snapshot.request_id
    }
  end

  @doc false
  @spec load!(map()) :: t()
  def load!(
        %{
          "workspace_id" => workspace_id,
          "principal_id" => principal_id,
          "roles" => roles,
          "request_id" => request_id
        } = value
      )
      when map_size(value) == 4 and is_binary(workspace_id) and is_binary(principal_id) and
             is_list(roles) and (is_nil(request_id) or is_binary(request_id)) do
    %__MODULE__{
      workspace_id: workspace_id,
      principal_id: principal_id,
      roles: roles |> Enum.map(&role!/1) |> Enum.sort(),
      request_id: request_id
    }
  end

  def load!(_value), do: raise(ArgumentError, "invalid run-submission authority snapshot")

  defp role!("customer_reader"), do: :customer_reader
  defp role!("customer_operator"), do: :customer_operator
  defp role!("workspace_admin"), do: :workspace_admin
  defp role!("platform_operator"), do: :platform_operator
  defp role!(_role), do: raise(ArgumentError, "invalid run-submission authority role")
end
