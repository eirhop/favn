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

  alias FavnOrchestrator
  alias FavnOrchestrator.API.DTO
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunEvents.EventType

  @retry_ms 3_000
  @heartbeat_ms 15_000
  @replay_limit 200

  @type field_name :: :event | :id
  @type stream :: {:global, non_neg_integer() | nil} | {:run, String.t(), non_neg_integer()}

  @doc """
  Opens an SSE response for a previously validated stream cursor.

  `Plug.Adapters.Test.Conn` receives the finite replay plus one ready event so
  focused request tests do not enter the live receive loop.
  """
  @spec stream(Plug.Conn.t(), stream()) :: Plug.Conn.t()
  def stream(conn, stream) do
    if test_conn?(conn), do: test_stream(conn, stream), else: live_stream(conn, stream)
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
  defp test_stream(conn, stream) do
    with {:ok, replay_events} <- fetch_replay_events(stream),
         {:ok, replay_body, cursor} <- encode_replay(stream, replay_events) do
      body =
        "retry: #{@retry_ms}\n\n" <>
          replay_body <> ready_body(stream_name(stream), cursor) <> ": heartbeat\n\n"

      conn = put_sse_headers(conn)
      send_resp(conn, 200, body)
    else
      {:error, :cursor_invalid} ->
        Response.error(conn, 410, "cursor_expired", "Cursor is invalid or no longer replayable")

      {:error, reason} ->
        Logger.error("sse.replay failed: #{inspect(reason)}")
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  defp live_stream(conn, stream) do
    with :ok <- subscribe(stream),
         {:ok, replay_events} <- fetch_replay_events(stream) do
      conn = conn |> put_sse_headers() |> send_chunked(200)
      heartbeat_ref = Process.send_after(self(), :sse_heartbeat, @heartbeat_ms)

      try do
        with {:ok, conn} <- chunk(conn, "retry: #{@retry_ms}\n\n"),
             {:ok, conn, cursor} <- chunk_replay(conn, stream, replay_events),
             {:ok, conn} <- chunk(conn, ready_body(stream_name(stream), cursor)) do
          live_loop(conn, stream, cursor, heartbeat_ref)
        else
          {:error, _reason} -> conn
        end
      after
        Process.cancel_timer(heartbeat_ref)
        unsubscribe(stream)
      end
    else
      {:error, :cursor_invalid} ->
        unsubscribe(stream)
        Response.error(conn, 410, "cursor_expired", "Cursor is invalid or no longer replayable")

      {:error, reason} ->
        unsubscribe(stream)
        Logger.error("sse.open failed: #{inspect(reason)}")
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  defp fetch_replay_events({:run, run_id, sequence}) do
    run_id
    |> FavnOrchestrator.list_run_stream_events(
      after_sequence: sequence,
      limit: @replay_limit + 1
    )
    |> reject_incomplete_replay()
  end

  defp fetch_replay_events({:global, nil}) do
    FavnOrchestrator.list_global_run_stream_events(
      after_global_sequence: nil,
      limit: @replay_limit
    )
  end

  defp fetch_replay_events({:global, global_sequence}) do
    [after_global_sequence: global_sequence, limit: @replay_limit + 1]
    |> FavnOrchestrator.list_global_run_stream_events()
    |> reject_incomplete_replay()
  end

  defp reject_incomplete_replay({:ok, events}) when length(events) > @replay_limit,
    do: {:error, :cursor_invalid}

  defp reject_incomplete_replay(result), do: result

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
    Enum.reduce_while(events, {:ok, conn, initial_cursor(stream)}, fn event,
                                                                      {:ok, conn, _cursor} ->
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

  defp live_loop(conn, stream, cursor, heartbeat_ref) do
    receive do
      {:favn_run_event, %RunEvent{} = event} ->
        deliver_live_event(conn, stream, cursor, heartbeat_ref, event)

      :sse_heartbeat ->
        next_ref = Process.send_after(self(), :sse_heartbeat, @heartbeat_ms)
        Process.cancel_timer(heartbeat_ref)

        case chunk(conn, ": heartbeat\n\n") do
          {:ok, conn} -> live_loop(conn, stream, cursor, next_ref)
          {:error, _reason} -> conn
        end
    end
  end

  defp deliver_live_event(conn, stream, cursor, heartbeat_ref, event) do
    with {:ok, %RunEvent{} = event} <- hydrate_live_event(stream, event, cursor),
         next_cursor = event_cursor(stream, event),
         {:ok, body} <- event_body(event, stream, next_cursor),
         {:ok, conn} <- chunk(conn, body) do
      live_loop(conn, stream, next_cursor, heartbeat_ref)
    else
      {:ok, nil} ->
        live_loop(conn, stream, cursor, heartbeat_ref)

      {:error, reason} ->
        Logger.error("sse.run_event delivery failed: #{inspect(reason)}")

        case delivery_error_action(reason) do
          :close ->
            Process.cancel_timer(heartbeat_ref)
            conn

          :continue ->
            live_loop(conn, stream, cursor, heartbeat_ref)
        end
    end
  end

  defp hydrate_live_event(
         {:run, run_id, _initial_sequence},
         %RunEvent{run_id: run_id} = event,
         cursor
       ) do
    if event.sequence > run_sequence(cursor), do: {:ok, event}, else: {:ok, nil}
  end

  defp hydrate_live_event({:run, _run_id, _initial_sequence}, _event, _cursor), do: {:ok, nil}

  defp hydrate_live_event(
         {:global, _initial_sequence},
         %RunEvent{global_sequence: sequence} = event,
         cursor
       )
       when is_integer(sequence) and sequence > 0 do
    if sequence > global_sequence(cursor), do: {:ok, event}, else: {:ok, nil}
  end

  defp hydrate_live_event({:global, _initial_sequence}, %RunEvent{}, _cursor),
    do: {:error, :missing_global_sequence}

  defp subscribe({:run, run_id, _sequence}), do: FavnOrchestrator.subscribe_run(run_id)
  defp subscribe({:global, _sequence}), do: FavnOrchestrator.subscribe_runs()

  defp unsubscribe({:run, run_id, _sequence}), do: FavnOrchestrator.unsubscribe_run(run_id)
  defp unsubscribe({:global, _sequence}), do: FavnOrchestrator.unsubscribe_runs()

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
