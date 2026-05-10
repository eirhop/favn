defmodule FavnOrchestrator.LogWriter do
  @moduledoc """
  Persists trusted backend logs and broadcasts them only after durable write.
  """

  alias FavnOrchestrator.Logs
  alias FavnOrchestrator.Storage

  @spec write(term() | [term()]) :: {:ok, [term()]} | {:error, term()}
  def write(entries) when is_list(entries) do
    with {:ok, persisted_entries} <- Storage.persist_log_entries(entries) do
      Enum.each(persisted_entries, &Logs.broadcast_log_entry/1)
      {:ok, persisted_entries}
    end
  end

  def write(entry), do: write([entry])
end
