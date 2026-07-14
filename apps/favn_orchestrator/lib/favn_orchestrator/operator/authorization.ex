defmodule FavnOrchestrator.Operator.Authorization do
  @moduledoc """
  Rehydrates and authorizes same-BEAM operator context.

  Caller-supplied actor and session maps are identity hints only. Authorization
  always reloads both records from orchestrator storage and returns the
  persisted actor so command code never relies on caller-supplied roles or
  actor attributes.
  """

  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Storage

  @type context :: %{
          optional(:actor) => map(),
          optional(:session) => map(),
          optional(String.t()) => term()
        }

  @type role :: :viewer | :operator | :admin

  @doc """
  Returns the persisted actor when the context identifies a live session with
  the required role.
  """
  @spec authorize(context(), role()) ::
          {:ok, Auth.actor()} | {:error, :unauthenticated | :forbidden}
  def authorize(context, required_role)
      when is_map(context) and required_role in [:viewer, :operator, :admin] do
    with {:ok, actor_id} <- context_id(context, :actor),
         {:ok, session_id} <- context_id(context, :session),
         {:ok, actor, session} <- load_context(actor_id, session_id),
         :ok <- validate_session(actor, session, DateTime.utc_now()) do
      if Auth.has_role?(actor, required_role), do: {:ok, actor}, else: {:error, :forbidden}
    else
      _error -> {:error, :unauthenticated}
    end
  end

  def authorize(_context, required_role) when required_role in [:viewer, :operator, :admin],
    do: {:error, :unauthenticated}

  defp context_id(context, key) do
    case Map.get(context, key) || Map.get(context, Atom.to_string(key)) do
      %{id: id} when is_binary(id) and id != "" -> {:ok, id}
      %{"id" => id} when is_binary(id) and id != "" -> {:ok, id}
      _context_value -> {:error, :missing_context}
    end
  end

  defp load_context(actor_id, session_id) do
    with {:ok, session} <- Storage.get_auth_session(session_id),
         true <- field(session, :actor_id) == actor_id,
         {:ok, actor} <- Storage.get_auth_actor(actor_id) do
      {:ok, actor, session}
    end
  end

  defp validate_session(actor, session, now) do
    expires_at = field(session, :expires_at)

    if field(actor, :status) in [:active, "active"] and
         field(actor, :id) == field(session, :actor_id) and
         is_nil(field(session, :revoked_at)) and
         match?(%DateTime{}, expires_at) and
         DateTime.compare(expires_at, now) == :gt do
      :ok
    else
      {:error, :invalid_session}
    end
  end

  defp field(value, key) when is_map(value),
    do: Map.get(value, key) || Map.get(value, Atom.to_string(key))
end
