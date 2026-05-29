defmodule FavnOrchestrator.Audit.Store do
  @moduledoc """
  Storage boundary for durable audit events.
  """

  alias FavnOrchestrator.Audit.Event
  alias FavnOrchestrator.Storage

  @doc "Persists a durable audit event before accepting a control-plane mutation."
  @spec put_event(Event.t()) :: :ok | {:error, term()}
  def put_event(%Event{} = event), do: Storage.put_audit_event(event)

  @doc "Updates the outcome/resource fields for a previously inserted audit event."
  @spec update_event_result(String.t(), map()) :: :ok | {:error, term()}
  def update_event_result(event_id, attrs) when is_binary(event_id) and is_map(attrs) do
    Storage.update_audit_event_result(event_id, attrs)
  end

  @doc "Lists a bounded page of audit events."
  @spec list_events(keyword()) ::
          {:ok, FavnOrchestrator.CursorPage.t(Event.t())} | {:error, term()}
  def list_events(opts \\ []) when is_list(opts), do: Storage.list_audit_events(opts)
end
