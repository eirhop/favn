defmodule FavnOrchestrator.Persistence.CapacityIdentity do
  @moduledoc """
  Stable identities shared by deployment capacity configuration and admission.

  Human-readable scope kind/key columns remain queryable; the primary identity
  is a bounded digest so tenant, pool, pipeline, and run identifiers cannot make
  foreign keys exceed PostgreSQL identifier limits.
  """

  @type kind :: :workspace | :pool | :pipeline | :run

  @doc "Returns the bounded durable identity for one workspace capacity scope."
  @spec scope_id(String.t(), kind(), String.t()) :: String.t()
  def scope_id(workspace_id, kind, key)
      when is_binary(workspace_id) and kind in [:workspace, :pool, :pipeline, :run] and
             is_binary(key) do
    digest =
      :crypto.hash(:sha256, :erlang.term_to_binary({workspace_id, kind, key}))
      |> Base.url_encode64(padding: false)

    "capacity:#{kind}:#{digest}"
  end
end
