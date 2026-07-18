defmodule FavnOrchestrator.API.SSE do
  @moduledoc """
  Delivers authenticated orchestrator run events over Server-Sent Events.

  The router owns authentication and cursor parsing. This module owns replay,
  subscription lifecycle, heartbeat delivery, and line-safe SSE encoding.
  """

  import Plug.Conn,
    only: [
      chunk: 2,
      put_resp_content_type: 2,
      put_resp_header: 3,
      send_chunked: 2,
      send_resp: 3
    ]

  require Logger

  alias FavnOrchestrator.API.DTO
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.RunEvents.EventType
  alias FavnOrchestrator.Events
  alias FavnOrchestrator.Persistence.Error, as: PersistenceError
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Runs

  @retry_ms 3_000
  @heartbeat_ms 15_000
  @replay_limit 200

  @type field_name :: :event | :id
  @type stream :: {:global, non_neg_integer() | nil} | {:run, String.t(), non_neg_integer()}

  @doc "Opens a workspace-scoped PostgreSQL-backed SSE stream."
  @spec stream(Plug.Conn.t(), WorkspaceContext.t(), stream()) :: Plug.Conn.t()
  def stream(conn, %WorkspaceContext{} = context, stream) do
    if test_conn?(conn),
      do: test_persistence_stream(conn, context, stream),
      else: live_persistence_stream(conn, context, stream)
  end

  @doc """
  Encodes one SSE control field value or rejects it when it is not line-safe.
  """
  @spec field(field_name(), atom() | String.t()) :: {:ok, String.t()} | {:error, term()}
  def field(name, value) when name in [:event, :id] do
    value = stringify(value)

    if EventType.line_safe?(value) do
      {:ok, value}
    else
      {:error, {:invalid_sse_field, name, value}}
    end
  end

  def field(name, value), do: {:error, {:invalid_sse_field, name, value}}

  @doc false
  @spec delivery_error_action(term()) :: :close | :continue
  def delivery_error_action(:missing_global_sequence), do: :close
  def delivery_error_action(_reason), do: :continue

  # sobelow_skip ["XSS.SendResp"]
  defp test_persistence_stream(conn, context, stream) do
    with {:ok, page} <- fetch_persistence_page(context, stream, initial_cursor(stream)),
         {:ok, replay_body, cursor} <- encode_replay(stream, page.items) do
      body =
        "retry: #{@retry_ms}\n\n" <>
          replay_body <> ready_body(stream_name(stream), cursor) <> ": heartbeat\n\n"

      conn |> put_sse_headers() |> send_resp(200, body)
    else
      {:error, :cursor_invalid} ->
        Response.error(conn, 410, "cursor_expired", "Cursor is invalid or no longer replayable")

      {:error, reason} ->
        Logger.error("sse.persistence_replay failed: #{inspect(reason)}")
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  defp live_persistence_stream(conn, context, stream) do
    with :ok <- Events.subscribe_persistence_publications(),
         {:ok, page} <- fetch_persistence_page(context, stream, initial_cursor(stream)) do
      conn = conn |> put_sse_headers() |> send_chunked(200)
      heartbeat_ref = Process.send_after(self(), :sse_heartbeat, @heartbeat_ms)

      try do
        with {:ok, conn} <- chunk(conn, "retry: #{@retry_ms}\n\n"),
             {:ok, conn, cursor} <- chunk_replay(conn, stream, page.items),
             {:ok, conn} <- chunk(conn, ready_body(stream_name(stream), cursor)) do
          if page.has_more?, do: send(self(), :favn_persistence_published)
          persistence_loop(conn, context, stream, cursor, heartbeat_ref)
        else
          {:error, _reason} -> conn
        end
      after
        Process.cancel_timer(heartbeat_ref)
        Events.unsubscribe_persistence_publications()
      end
    else
      {:error, :cursor_invalid} ->
        Events.unsubscribe_persistence_publications()
        Response.error(conn, 410, "cursor_expired", "Cursor is invalid or no longer replayable")

      {:error, reason} ->
        Events.unsubscribe_persistence_publications()
        Logger.error("sse.persistence_open failed: #{inspect(reason)}")
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  defp persistence_loop(conn, context, stream, cursor, heartbeat_ref) do
    receive do
      :favn_persistence_published ->
        deliver_persistence_page(conn, context, stream, cursor, heartbeat_ref)

      :sse_heartbeat ->
        deliver_persistence_page(conn, context, stream, cursor, heartbeat_ref, true)
    end
  end

  defp deliver_persistence_page(
         conn,
         context,
         stream,
         cursor,
         heartbeat_ref,
         heartbeat? \\ false
       ) do
    with {:ok, page} <- fetch_persistence_page(context, stream, cursor),
         {:ok, conn, next_cursor} <- chunk_replay(conn, stream, page.items, cursor),
         {:ok, conn} <- maybe_heartbeat(conn, heartbeat?) do
      next_ref = reschedule_heartbeat(heartbeat_ref, heartbeat?)
      if page.has_more?, do: send(self(), :favn_persistence_published)
      persistence_loop(conn, context, stream, next_cursor || cursor, next_ref)
    else
      {:error, :cursor_invalid} ->
        Process.cancel_timer(heartbeat_ref)
        conn

      {:error, reason} ->
        Logger.error("sse.persistence_delivery failed: #{inspect(reason)}")
        persistence_loop(conn, context, stream, cursor, heartbeat_ref)
    end
  end

  defp fetch_persistence_page(context, {:run, run_id, initial_sequence}, cursor) do
    sequence = if is_binary(cursor), do: run_sequence(cursor), else: initial_sequence

    case Runs.page_events(context, run_id,
           after_sequence: sequence,
           limit: @replay_limit
         ) do
      {:ok, page} -> {:ok, page}
      {:error, %PersistenceError{kind: :invalid}} -> {:error, :cursor_invalid}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_persistence_page(context, {:global, initial_publication_id}, cursor) do
    publication_id =
      if is_binary(cursor), do: global_sequence(cursor), else: initial_publication_id

    case Runs.page_published_events(context,
           after_publication_id: publication_id,
           limit: @replay_limit
         ) do
      {:ok, page} -> {:ok, page}
      {:error, %PersistenceError{kind: :invalid}} -> {:error, :cursor_invalid}
      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_heartbeat(conn, false), do: {:ok, conn}
  defp maybe_heartbeat(conn, true), do: chunk(conn, ": heartbeat\n\n")

  defp reschedule_heartbeat(heartbeat_ref, false), do: heartbeat_ref

  defp reschedule_heartbeat(heartbeat_ref, true) do
    Process.cancel_timer(heartbeat_ref)
    Process.send_after(self(), :sse_heartbeat, @heartbeat_ms)
  end

  defp encode_replay(stream, events) do
    Enum.reduce_while(events, {:ok, "", initial_cursor(stream)}, fn event, {:ok, body, _cursor} ->
      cursor = event_cursor(stream, event)

      case event_body(event, stream, cursor) do
        {:ok, event_body} -> {:cont, {:ok, body <> event_body, cursor}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp chunk_replay(conn, stream, events) do
    chunk_replay(conn, stream, events, initial_cursor(stream))
  end

  defp chunk_replay(conn, stream, events, initial_cursor) do
    Enum.reduce_while(events, {:ok, conn, initial_cursor}, fn event, {:ok, conn, _cursor} ->
      cursor = event_cursor(stream, event)

      with {:ok, body} <- event_body(event, stream, cursor),
           {:ok, conn} <- chunk(conn, body) do
        {:cont, {:ok, conn, cursor}}
      else
        {:error, reason} ->
          Logger.error("sse.run_event delivery failed: #{inspect(reason)}")
          {:halt, {:error, reason}}
      end
    end)
  end

  defp event_body(event, stream, cursor) do
    with {:ok, event_name} <- field(:event, event.event_type),
         {:ok, cursor} <- field(:id, cursor),
         {:ok, payload} <- Jason.encode(event_payload(event, stream, cursor, event_name)) do
      {:ok, "id: #{cursor}\nevent: #{event_name}\ndata: #{payload}\n\n"}
    end
  end

  defp ready_body(stream, cursor) do
    payload =
      Jason.encode!(%{
        schema_version: 1,
        stream: stream,
        event_type: "stream.ready",
        cursor: cursor,
        occurred_at: DateTime.utc_now()
      })

    "event: stream.ready\ndata: #{payload}\n\n"
  end

  defp event_payload(event, stream, cursor, event_name) do
    %{
      schema_version: 1,
      event_id: cursor,
      stream: stream_name(stream),
      event_type: event_name,
      run_id: event.run_id,
      status: normalize_name(event.status),
      occurred_at: normalize_datetime(event.occurred_at),
      sequence: event.sequence,
      global_sequence: event.global_sequence,
      cursor: cursor,
      summary: "Run #{event.run_id} #{event_name}",
      details: %{
        entity: normalize_name(event.entity),
        manifest_version_id: event.manifest_version_id,
        asset_ref: normalize_ref(event.asset_ref),
        stage: event.stage
      }
    }
  end

  defp put_sse_headers(conn) do
    conn
    |> put_resp_content_type("text/event-stream")
    |> put_resp_header("cache-control", "no-cache, no-transform")
    |> put_resp_header("x-accel-buffering", "no")
  end

  defp test_conn?(conn), do: match?({Plug.Adapters.Test.Conn, _}, conn.adapter)

  defp stream_name({:run, run_id, _sequence}), do: "run:" <> run_id
  defp stream_name({:global, _sequence}), do: "runs"

  defp initial_cursor({:run, _run_id, 0}), do: nil

  defp initial_cursor({:run, run_id, sequence}),
    do: "run:" <> run_id <> ":" <> Integer.to_string(sequence)

  defp initial_cursor({:global, nil}), do: nil
  defp initial_cursor({:global, sequence}), do: "global:" <> Integer.to_string(sequence)

  defp event_cursor({:run, _run_id, _sequence}, event),
    do: "run:" <> event.run_id <> ":" <> Integer.to_string(event.sequence)

  defp event_cursor({:global, _sequence}, event),
    do: "global:" <> Integer.to_string(event.global_sequence)

  defp run_sequence("run:" <> cursor) do
    cursor |> String.split(":") |> List.last() |> positive_int(0)
  end

  defp run_sequence(_cursor), do: 0

  defp global_sequence("global:" <> sequence), do: positive_int(sequence, 0)
  defp global_sequence(_cursor), do: 0

  defp positive_int(value, default) do
    case Integer.parse(to_string(value)) do
      {int, ""} when int > 0 -> int
      _ -> default
    end
  end

  defp normalize_name(nil), do: nil
  defp normalize_name(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_name(value), do: DTO.normalize(value)

  defp normalize_ref(nil), do: nil

  defp normalize_ref({module, name}) when is_atom(module) and is_atom(name),
    do: Atom.to_string(module) <> ":" <> Atom.to_string(name)

  defp normalize_ref(value), do: DTO.normalize(value)

  defp normalize_datetime(nil), do: nil
  defp normalize_datetime(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp normalize_datetime(value), do: DTO.normalize(value)

  defp stringify(value) when is_atom(value) and not is_nil(value), do: Atom.to_string(value)
  defp stringify(value) when is_binary(value), do: value
  defp stringify(_value), do: nil
end
