defmodule FavnOrchestrator.Operator.Audit do
  @moduledoc false

  require Logger

  alias FavnOrchestrator.Identity
  alias FavnOrchestrator.Redaction

  @spec put_best_effort(struct(), map()) :: :ok
  def put_best_effort(context, entry) when is_map(entry) do
    case Identity.record_audit(context, entry) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "operator audit persistence failed: " <>
            inspect(Redaction.redact_operational_bounded(reason))
        )

        :ok
    end
  end
end
