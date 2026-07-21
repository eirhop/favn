defmodule FavnOrchestrator.Persistence.SystemContext do
  @moduledoc false

  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.RuntimeConfig

  @spec workspace(String.t(), atom(), keyword()) :: WorkspaceContext.t()
  def workspace(workspace_id, purpose, opts \\ [])
      when is_binary(workspace_id) and is_atom(purpose) and is_list(opts) do
    instance_id = RuntimeConfig.instance_id()
    principal = "favn:#{String.slice(instance_id, 0, 96)}:#{purpose}"

    {:ok, context} =
      WorkspaceContext.new(workspace_id, principal, [:customer_operator],
        request_id: Keyword.get(opts, :request_id)
      )

    context
  end

  @spec platform(atom(), keyword()) :: PlatformContext.t()
  def platform(purpose, opts \\ []) when is_atom(purpose) and is_list(opts) do
    instance_id = RuntimeConfig.instance_id()
    principal = "favn:#{String.slice(instance_id, 0, 96)}:#{purpose}"

    {:ok, context} =
      PlatformContext.new(principal, "system:#{purpose}", [:platform_reader],
        request_id: Keyword.get(opts, :request_id)
      )

    context
  end
end
