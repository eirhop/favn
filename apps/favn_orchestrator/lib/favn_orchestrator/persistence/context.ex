defmodule FavnOrchestrator.Persistence.WorkspaceContext do
  @moduledoc """
  Validated authority for one customer workspace.

  Persistence commands never accept a bare workspace identifier. A public
  orchestrator use case resolves authentication and authorization into this
  context before calling a store.
  """

  @enforce_keys [:workspace_id, :principal_id, :roles]
  defstruct [:workspace_id, :principal_id, :request_id, roles: []]

  @type role :: :customer_reader | :customer_operator | :workspace_admin | :platform_operator
  @type t :: %__MODULE__{
          workspace_id: String.t(),
          principal_id: String.t(),
          roles: [role()],
          request_id: String.t() | nil
        }

  @doc "Builds a workspace context from an already-authorized principal."
  @spec new(String.t(), String.t(), [role()], keyword()) ::
          {:ok, t()} | {:error, :invalid_context}
  def new(workspace_id, principal_id, roles, opts \\ [])

  def new(workspace_id, principal_id, roles, opts)
      when is_binary(workspace_id) and is_binary(principal_id) and is_list(roles) do
    context = %__MODULE__{
      workspace_id: workspace_id,
      principal_id: principal_id,
      roles: Enum.uniq(roles),
      request_id: Keyword.get(opts, :request_id)
    }

    if valid?(context) do
      {:ok, context}
    else
      {:error, :invalid_context}
    end
  end

  def new(_workspace_id, _principal_id, _roles, _opts), do: {:error, :invalid_context}

  @doc "Returns whether a context is structurally safe for persistence use."
  @spec valid?(term()) :: boolean()
  def valid?(%__MODULE__{} = context) do
    valid_id?(context.workspace_id) and valid_id?(context.principal_id) and
      context.roles != [] and Enum.all?(context.roles, &valid_role?/1) and
      (is_nil(context.request_id) or valid_id?(context.request_id))
  end

  def valid?(_context), do: false

  defp valid_role?(role),
    do: role in [:customer_reader, :customer_operator, :workspace_admin, :platform_operator]

  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255
end

defmodule FavnOrchestrator.Persistence.PlatformContext do
  @moduledoc """
  Explicit consultant authority for platform-global reads and mutations.

  Workspace-scoped mutations also require a `WorkspaceContext`; destructive
  deployment authority is therefore explicit at both the platform and tenant
  boundaries.
  """

  @enforce_keys [:principal_id, :grant_id, :roles]
  defstruct [:principal_id, :grant_id, :request_id, roles: []]

  @type role :: :platform_reader | :platform_operator | :platform_admin
  @type t :: %__MODULE__{
          principal_id: String.t(),
          grant_id: String.t(),
          roles: [role()],
          request_id: String.t() | nil
        }

  @doc "Builds a platform context from an already-validated platform grant."
  @spec new(String.t(), String.t(), [role()], keyword()) ::
          {:ok, t()} | {:error, :invalid_context}
  def new(principal_id, grant_id, roles, opts \\ [])

  def new(principal_id, grant_id, roles, opts)
      when is_binary(principal_id) and is_binary(grant_id) and is_list(roles) do
    context = %__MODULE__{
      principal_id: principal_id,
      grant_id: grant_id,
      roles: Enum.uniq(roles),
      request_id: Keyword.get(opts, :request_id)
    }

    if valid?(context) do
      {:ok, context}
    else
      {:error, :invalid_context}
    end
  end

  def new(_principal_id, _grant_id, _roles, _opts), do: {:error, :invalid_context}

  @doc "Returns whether a platform context is structurally safe for persistence use."
  @spec valid?(term()) :: boolean()
  def valid?(%__MODULE__{} = context) do
    valid_id?(context.principal_id) and valid_id?(context.grant_id) and context.roles != [] and
      Enum.all?(context.roles, &valid_role?/1) and
      (is_nil(context.request_id) or valid_id?(context.request_id))
  end

  def valid?(_context), do: false

  defp valid_role?(role), do: role in [:platform_reader, :platform_operator, :platform_admin]

  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255
end
