defmodule FavnOrchestrator.Operator.Context do
  @moduledoc """
  Browser-safe operator command context accepted by orchestrator facades.

  The context carries only stable identifiers and explicitly allow-listed request
  metadata. Command facades still reload persisted actor and session state before
  authorizing any mutation.
  """

  alias FavnOrchestrator.Audit.Redactor

  @sources [:live_view, :http_api, :cli, :system]

  @enforce_keys [:actor_id, :session_id, :source]
  defstruct [:actor_id, :session_id, :browser_session_id, :source, request_context: %{}]

  @type source :: :live_view | :http_api | :cli | :system

  @type t :: %__MODULE__{
          actor_id: String.t(),
          session_id: String.t(),
          browser_session_id: String.t() | nil,
          source: source(),
          request_context: map()
        }

  @doc """
  Builds an operator context from persisted-safe actor and session DTOs.
  """
  @spec from_actor_session(map(), map(), keyword()) :: {:ok, t()} | {:error, term()}
  def from_actor_session(actor, session, opts \\ []) when is_map(actor) and is_map(session) do
    attrs = %{
      actor_id: id(actor),
      session_id: id(session),
      browser_session_id: Keyword.get(opts, :browser_session_id),
      source: Keyword.get(opts, :source, :live_view),
      request_context: Keyword.get(opts, :request_context, %{})
    }

    new(attrs)
  end

  @doc """
  Normalizes a context struct or legacy `%{actor: actor, session: session}` map.
  """
  @spec normalize(t() | map()) :: {:ok, t()} | {:error, term()}
  def normalize(%__MODULE__{} = context), do: new(Map.from_struct(context))

  def normalize(%{} = context) do
    cond do
      Map.has_key?(context, :actor) or Map.has_key?(context, "actor") ->
        actor = Map.get(context, :actor) || Map.get(context, "actor")
        session = Map.get(context, :session) || Map.get(context, "session")

        opts = [
          browser_session_id:
            Map.get(context, :browser_session_id) || Map.get(context, "browser_session_id"),
          source: Map.get(context, :source) || Map.get(context, "source") || :live_view,
          request_context:
            Map.get(context, :request_context) || Map.get(context, "request_context") || %{}
        ]

        if is_map(actor) and is_map(session) do
          from_actor_session(actor, session, opts)
        else
          {:error, :missing_context}
        end

      true ->
        new(context)
    end
  end

  def normalize(_context), do: {:error, :missing_context}

  @doc """
  Builds a context from explicit safe attributes.
  """
  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) do
    actor_id = field(attrs, :actor_id)
    session_id = field(attrs, :session_id)
    source = normalize_source(field(attrs, :source) || :live_view)
    browser_session_id = optional_binary(field(attrs, :browser_session_id))
    request_context = Redactor.redact_request_context(field(attrs, :request_context) || %{})

    cond do
      not non_empty_binary?(actor_id) ->
        {:error, :missing_context}

      not non_empty_binary?(session_id) ->
        {:error, :missing_context}

      source not in @sources ->
        {:error, {:invalid_operator_context_source, field(attrs, :source)}}

      not is_map(request_context) ->
        {:error, :invalid_operator_request_context}

      true ->
        {:ok,
         %__MODULE__{
           actor_id: actor_id,
           session_id: session_id,
           browser_session_id: browser_session_id,
           source: source,
           request_context: request_context
         }}
    end
  end

  def new(_attrs), do: {:error, :missing_context}

  defp id(%{id: id}), do: id
  defp id(%{"id" => id}), do: id
  defp id(_value), do: nil

  defp field(map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  defp normalize_source(source) when source in @sources, do: source
  defp normalize_source(source) when is_binary(source), do: source_from_string(source)
  defp normalize_source(source), do: source

  defp source_from_string("live_view"), do: :live_view
  defp source_from_string("http_api"), do: :http_api
  defp source_from_string("cli"), do: :cli
  defp source_from_string("system"), do: :system
  defp source_from_string(source), do: source

  defp optional_binary(value) when is_binary(value) and value != "", do: value
  defp optional_binary(_value), do: nil

  defp non_empty_binary?(value), do: is_binary(value) and value != ""
end
