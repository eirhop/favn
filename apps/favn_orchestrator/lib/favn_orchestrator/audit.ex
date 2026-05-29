defmodule FavnOrchestrator.Audit do
  @moduledoc """
  Orchestrator-owned audit helpers for operator control-plane commands.

  The current same-BEAM operator facades use this module for accepted commands:
  actor/session authorization, request parsing, manifest lookup, target
  resolution, and option normalization happen before an audit event is inserted.
  Rejected, forbidden, unauthenticated, and validation-failed attempt auditing is
  a separate security-event concern.
  """

  alias FavnOrchestrator.Audit.Event
  alias FavnOrchestrator.Audit.Redactor
  alias FavnOrchestrator.Operator.Context

  @doc "Builds a redacted audit event for an operator command."
  @spec operator_command_event(Context.t(), map()) :: {:ok, Event.t()} | {:error, term()}
  def operator_command_event(%Context{} = context, attrs) when is_map(attrs) do
    attrs
    |> Map.merge(%{
      actor_id: context.actor_id,
      session_id: context.session_id,
      browser_session_id: context.browser_session_id,
      source: context.source,
      request_context: context.request_context,
      payload: Redactor.redact_payload(Map.get(attrs, :payload, %{})),
      metadata: Redactor.redact_payload(Map.get(attrs, :metadata, %{}))
    })
    |> Event.new()
  end
end
