defmodule FavnOrchestrator.Persistence.LogStore do
  @moduledoc "Persistence contract for redacted, bounded operational logs."

  alias FavnOrchestrator.Persistence.Commands.AppendLogBatch
  alias FavnOrchestrator.Persistence.Commands.PurgeLogs
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.PageLogs
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.LogEntry
  alias FavnOrchestrator.Persistence.Results.PurgeResult

  @callback append_batch(AppendLogBatch.t()) :: {:ok, [LogEntry.t()]} | {:error, Error.t()}
  @callback page(PageLogs.t()) :: {:ok, CursorPage.t(LogEntry.t())} | {:error, Error.t()}
  @callback purge(PurgeLogs.t()) :: {:ok, PurgeResult.t()} | {:error, Error.t()}
end
