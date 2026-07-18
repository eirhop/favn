defmodule FavnOrchestrator.OperatorContext do
  @moduledoc """
  Browser-safe identity hints for one workspace-scoped operator session.

  This value carries no bearer token and grants no authority by itself. Every
  orchestrator use case revalidates the persisted session, actor, membership,
  and required role before constructing an internal persistence context.
  """

  @enforce_keys [:workspace_id, :actor_id, :session_id]
  defstruct [:workspace_id, :actor_id, :session_id]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          actor_id: String.t(),
          session_id: String.t()
        }

  @doc "Builds non-authoritative operator identity hints from sanitized DTOs."
  @spec new(String.t(), map(), map()) :: {:ok, t()} | {:error, :invalid_operator_context}
  def new(workspace_id, actor, session) when is_map(actor) and is_map(session) do
    context = %__MODULE__{
      workspace_id: workspace_id,
      actor_id: field(actor, :id),
      session_id: field(session, :id)
    }

    if valid_id?(context.workspace_id) and valid_id?(context.actor_id) and
         valid_id?(context.session_id),
       do: {:ok, context},
       else: {:error, :invalid_operator_context}
  end

  def new(_workspace_id, _actor, _session), do: {:error, :invalid_operator_context}

  defp field(map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255
end
