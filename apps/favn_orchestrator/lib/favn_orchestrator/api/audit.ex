defmodule FavnOrchestrator.API.Audit do
  @moduledoc """
  Writes API audit evidence without changing an already completed command result.

  Audit persistence failures are logged. They must not report a successful
  mutation as failed, because retrying that mutation could be unsafe.
  """

  require Logger

  alias FavnOrchestrator.Auth

  @doc "Writes an audit entry and logs any storage failure."
  @spec put_best_effort(map()) :: :ok
  def put_best_effort(entry) when is_map(entry) do
    case Auth.put_audit(entry) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("auth audit write failed: #{inspect(reason)}")
        :ok
    end
  end

  @doc "Writes an entry only when `enabled?` is true."
  @spec put_if(boolean(), map()) :: :ok
  def put_if(true, entry), do: put_best_effort(entry)
  def put_if(false, _entry), do: :ok
end
