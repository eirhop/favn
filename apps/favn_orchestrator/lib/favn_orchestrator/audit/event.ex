defmodule FavnOrchestrator.Audit.Event do
  @moduledoc """
  Durable orchestrator audit event for operator control-plane commands.

  The current same-BEAM operator command facades persist events only after the
  command is authorized, parsed, and resolved to a manifest target. The broader
  outcome enum is reserved for future callers that deliberately audit rejected
  security or validation attempts.
  """

  @schema_version 1
  @outcomes [:accepted, :replayed, :rejected, :forbidden, :unauthenticated, :validation_failed]
  @sources [:live_view, :http_api, :cli, :system]
  @target_types [:asset, :pipeline]
  @resource_types [:run, :backfill]

  @enforce_keys [:id, :schema_version, :occurred_at, :action, :outcome, :source]
  defstruct [
    :id,
    :schema_version,
    :occurred_at,
    :action,
    :outcome,
    :actor_id,
    :session_id,
    :browser_session_id,
    :source,
    :manifest_version_id,
    :target_type,
    :target_id,
    :target_ref,
    :resource_type,
    :resource_id,
    :payload,
    :request_context,
    :idempotency,
    :failure_class,
    :service_identity,
    :metadata
  ]

  @type outcome ::
          :accepted | :replayed | :rejected | :forbidden | :unauthenticated | :validation_failed
  @type source :: :live_view | :http_api | :cli | :system
  @type target_type :: :asset | :pipeline
  @type resource_type :: :run | :backfill

  @type t :: %__MODULE__{
          id: String.t(),
          schema_version: 1,
          occurred_at: DateTime.t(),
          action: String.t(),
          outcome: outcome(),
          actor_id: String.t() | nil,
          session_id: String.t() | nil,
          browser_session_id: String.t() | nil,
          source: source(),
          manifest_version_id: String.t() | nil,
          target_type: target_type() | nil,
          target_id: String.t() | nil,
          target_ref: String.t() | nil,
          resource_type: resource_type() | nil,
          resource_id: String.t() | nil,
          payload: map(),
          request_context: map(),
          idempotency: map() | nil,
          failure_class: String.t() | nil,
          service_identity: String.t() | nil,
          metadata: map()
        }

  @doc "Builds and validates an audit event."
  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) do
    event = %__MODULE__{
      id: field(attrs, :id) || new_id(),
      schema_version: field(attrs, :schema_version) || @schema_version,
      occurred_at: field(attrs, :occurred_at) || DateTime.utc_now(),
      action: field(attrs, :action),
      outcome: normalize_atom(field(attrs, :outcome)),
      actor_id: optional_binary(field(attrs, :actor_id)),
      session_id: optional_binary(field(attrs, :session_id)),
      browser_session_id: optional_binary(field(attrs, :browser_session_id)),
      source: normalize_atom(field(attrs, :source)),
      manifest_version_id: optional_binary(field(attrs, :manifest_version_id)),
      target_type: normalize_atom(field(attrs, :target_type)),
      target_id: optional_binary(field(attrs, :target_id)),
      target_ref: optional_binary(field(attrs, :target_ref)),
      resource_type: normalize_atom(field(attrs, :resource_type)),
      resource_id: optional_binary(field(attrs, :resource_id)),
      payload: field(attrs, :payload) || %{},
      request_context: field(attrs, :request_context) || %{},
      idempotency: field(attrs, :idempotency),
      failure_class: optional_binary(field(attrs, :failure_class)),
      service_identity: optional_binary(field(attrs, :service_identity)),
      metadata: field(attrs, :metadata) || %{}
    }

    validate(event)
  end

  def new(value), do: {:error, {:invalid_audit_event, value}}

  @doc "Returns the event schema version."
  @spec schema_version() :: 1
  def schema_version, do: @schema_version

  @doc "Merges result fields into an event and validates the result."
  @spec put_result(t(), map()) :: {:ok, t()} | {:error, term()}
  def put_result(%__MODULE__{} = event, attrs) when is_map(attrs) do
    event
    |> Map.merge(%{
      outcome: normalize_atom(field(attrs, :outcome) || event.outcome),
      resource_type: normalize_atom(field(attrs, :resource_type) || event.resource_type),
      resource_id: optional_binary(field(attrs, :resource_id)) || event.resource_id,
      failure_class: optional_binary(field(attrs, :failure_class)) || event.failure_class,
      metadata: Map.merge(event.metadata || %{}, field(attrs, :metadata) || %{})
    })
    |> validate()
  end

  defp validate(%__MODULE__{} = event) do
    cond do
      not non_empty_binary?(event.id) ->
        {:error, {:invalid_audit_event_field, :id, event.id}}

      event.schema_version != @schema_version ->
        {:error, {:unsupported_audit_event_schema_version, event.schema_version}}

      not match?(%DateTime{}, event.occurred_at) ->
        {:error, {:invalid_audit_event_field, :occurred_at, event.occurred_at}}

      not non_empty_binary?(event.action) ->
        {:error, {:invalid_audit_event_field, :action, event.action}}

      event.outcome not in @outcomes ->
        {:error, {:invalid_audit_event_field, :outcome, event.outcome}}

      event.source not in @sources ->
        {:error, {:invalid_audit_event_field, :source, event.source}}

      not is_nil(event.target_type) and event.target_type not in @target_types ->
        {:error, {:invalid_audit_event_field, :target_type, event.target_type}}

      not is_nil(event.resource_type) and event.resource_type not in @resource_types ->
        {:error, {:invalid_audit_event_field, :resource_type, event.resource_type}}

      not is_map(event.payload) ->
        {:error, {:invalid_audit_event_field, :payload, event.payload}}

      not is_map(event.request_context) ->
        {:error, {:invalid_audit_event_field, :request_context, event.request_context}}

      not is_map(event.metadata) ->
        {:error, {:invalid_audit_event_field, :metadata, event.metadata}}

      true ->
        {:ok, event}
    end
  end

  defp field(map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  defp normalize_atom(value) when is_atom(value), do: value

  defp normalize_atom(value) when is_binary(value) do
    value
    |> String.downcase()
    |> String.to_existing_atom()
  rescue
    ArgumentError -> value
  end

  defp normalize_atom(value), do: value

  defp optional_binary(value) when is_binary(value) and value != "", do: value
  defp optional_binary(_value), do: nil

  defp non_empty_binary?(value), do: is_binary(value) and value != ""

  defp new_id do
    "aud_" <> Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
  end
end
