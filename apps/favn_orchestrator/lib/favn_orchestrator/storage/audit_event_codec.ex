defmodule FavnOrchestrator.Storage.AuditEventCodec do
  @moduledoc false

  alias FavnOrchestrator.Audit.Event

  @format "favn.audit.event.storage.v1"
  @schema_version 1

  @doc "Encodes an audit event to the durable storage DTO."
  @spec encode(Event.t() | map()) :: {:ok, String.t()} | {:error, term()}
  def encode(%Event{} = event), do: encode(Map.from_struct(event))

  def encode(event) when is_map(event) do
    with {:ok, event} <- Event.new(event) do
      dto = %{
        "format" => @format,
        "schema_version" => @schema_version,
        "id" => event.id,
        "occurred_at" => DateTime.to_iso8601(event.occurred_at),
        "action" => event.action,
        "outcome" => Atom.to_string(event.outcome),
        "actor_id" => event.actor_id,
        "session_id" => event.session_id,
        "browser_session_id" => event.browser_session_id,
        "source" => Atom.to_string(event.source),
        "manifest_version_id" => event.manifest_version_id,
        "target_type" => optional_atom_string(event.target_type),
        "target_id" => event.target_id,
        "target_ref" => event.target_ref,
        "resource_type" => optional_atom_string(event.resource_type),
        "resource_id" => event.resource_id,
        "payload" => event.payload || %{},
        "request_context" => event.request_context || %{},
        "idempotency" => event.idempotency,
        "failure_class" => event.failure_class,
        "service_identity" => event.service_identity,
        "metadata" => event.metadata || %{}
      }

      {:ok, Jason.encode!(dto)}
    end
  rescue
    error -> {:error, {:audit_event_encode_failed, error}}
  end

  def encode(value), do: {:error, {:invalid_audit_event, value}}

  @doc "Decodes a durable storage DTO into an audit event."
  @spec decode(String.t()) :: {:ok, Event.t()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    with {:ok, %{"format" => @format, "schema_version" => @schema_version} = dto} <-
           Jason.decode(payload),
         {:ok, occurred_at} <- datetime_from_dto(Map.get(dto, "occurred_at")) do
      Event.new(%{
        id: Map.get(dto, "id"),
        schema_version: Map.get(dto, "schema_version"),
        occurred_at: occurred_at,
        action: Map.get(dto, "action"),
        outcome: Map.get(dto, "outcome"),
        actor_id: Map.get(dto, "actor_id"),
        session_id: Map.get(dto, "session_id"),
        browser_session_id: Map.get(dto, "browser_session_id"),
        source: Map.get(dto, "source"),
        manifest_version_id: Map.get(dto, "manifest_version_id"),
        target_type: Map.get(dto, "target_type"),
        target_id: Map.get(dto, "target_id"),
        target_ref: Map.get(dto, "target_ref"),
        resource_type: Map.get(dto, "resource_type"),
        resource_id: Map.get(dto, "resource_id"),
        payload: Map.get(dto, "payload", %{}),
        request_context: Map.get(dto, "request_context", %{}),
        idempotency: Map.get(dto, "idempotency"),
        failure_class: Map.get(dto, "failure_class"),
        service_identity: Map.get(dto, "service_identity"),
        metadata: Map.get(dto, "metadata", %{})
      })
    else
      {:ok, %{"format" => @format, "schema_version" => version}} ->
        {:error, {:unsupported_audit_event_schema_version, version}}

      {:ok, other} ->
        {:error, {:invalid_audit_event_dto, other}}

      {:error, %Jason.DecodeError{} = reason} ->
        {:error, {:invalid_audit_event_json, reason}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def decode(value), do: {:error, {:invalid_audit_event_payload, value}}

  defp optional_atom_string(nil), do: nil
  defp optional_atom_string(value) when is_atom(value), do: Atom.to_string(value)

  defp datetime_from_dto(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      _other -> {:error, {:invalid_audit_event_field, :occurred_at, value}}
    end
  end

  defp datetime_from_dto(value), do: {:error, {:invalid_audit_event_field, :occurred_at, value}}
end
