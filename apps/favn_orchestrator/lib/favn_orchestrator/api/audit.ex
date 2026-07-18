defmodule FavnOrchestrator.API.Audit do
  @moduledoc """
  Writes API audit evidence without changing an already completed command result.

  Audit persistence failures are logged. They must not report a successful
  mutation as failed, because retrying that mutation could be unsafe.
  """

  require Logger

  alias FavnOrchestrator.Identity
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Redaction

  @doc "Persists redacted audit evidence without changing an already-committed response."
  @spec put_best_effort(WorkspaceContext.t() | PlatformContext.t(), map()) :: :ok
  def put_best_effort(context, entry) when is_map(entry) do
    case Identity.record_audit(context, entry) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "api audit persistence failed: " <>
            inspect(Redaction.redact_operational_bounded(reason))
        )

        :ok
    end
  end
end
