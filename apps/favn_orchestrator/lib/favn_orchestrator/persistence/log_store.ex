defmodule FavnOrchestrator.Persistence.LogStore do
  @moduledoc "Persistence contract for redacted, bounded operational logs."

  alias FavnOrchestrator.Persistence.Commands.AppendLogBatch
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.PageLogs
  alias FavnOrchestrator.Persistence.Results.LogPage
  alias FavnOrchestrator.Persistence.Results.LogEntry

  @callback append_batch(AppendLogBatch.t()) :: {:ok, [LogEntry.t()]} | {:error, Error.t()}
  @callback page(PageLogs.t()) :: {:ok, LogPage.t()} | {:error, Error.t()}
end
