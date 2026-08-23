defmodule FavnOrchestrator.OperatorRunPages do
  @moduledoc "Bounded, screen-specific Window runs and Events read models."

  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.PageRunEventSummaries
  alias FavnOrchestrator.Persistence.Queries.PageRunWindows
  alias FavnOrchestrator.Persistence.Results.RunSummaryPage
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @limit 50
  @cursor_version 1

  defmodule Page do
    @moduledoc "One bounded summary page."
    @enforce_keys [:items, :total, :has_more?]
    defstruct [:items, :total, :has_more?, :next_cursor, projection_cursor: 0]
    @type t :: %__MODULE__{}
  end

  @doc "Returns at most 50 lean window-run summaries."
  @spec windows(WorkspaceContext.t(), String.t(), keyword()) :: {:ok, Page.t()} | {:error, atom()}
  def windows(%WorkspaceContext{} = context, run_id, opts)
      when is_binary(run_id) and is_list(opts) do
    with {:ok, limit} <- page_limit(opts),
         {:ok, after_cursor} <- decode_window_cursor(opts[:after], context, run_id),
         {:ok, %RunSummaryPage{} = page} <-
           Persistence.stores().operator_reads.page_run_windows(%PageRunWindows{
             workspace_context: context,
             run_id: run_id,
             after: after_cursor,
             limit: limit
           }) do
      {:ok, public_page(page, &encode_window_cursor(&1, context, run_id))}
    else
      {:error, %Error{kind: kind}} -> {:error, kind}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Returns at most 50 payload-free event summaries."
  @spec events(WorkspaceContext.t(), String.t(), keyword()) :: {:ok, Page.t()} | {:error, atom()}
  def events(%WorkspaceContext{} = context, run_id, opts)
      when is_binary(run_id) and is_list(opts) do
    with {:ok, limit} <- page_limit(opts),
         {:ok, after_event_id} <- decode_event_cursor(opts[:after], context, run_id),
         {:ok, %RunSummaryPage{} = page} <-
           Persistence.stores().operator_reads.page_run_event_summaries(%PageRunEventSummaries{
             workspace_context: context,
             run_id: run_id,
             after_event_id: after_event_id,
             limit: limit
           }) do
      {:ok, public_page(page, &encode_event_cursor(&1, context, run_id))}
    else
      {:error, %Error{kind: kind}} -> {:error, kind}
      {:error, reason} -> {:error, reason}
    end
  end

  defp public_page(page, encoder) do
    %Page{
      items: Enum.map(page.items, &Map.from_struct/1),
      total: page.total,
      has_more?: page.has_more?,
      next_cursor: page.next_cursor && encoder.(page.next_cursor),
      projection_cursor: page.projection_cursor
    }
  end

  defp page_limit(opts) do
    case Keyword.get(opts, :limit, @limit) do
      value when is_integer(value) and value in 1..@limit -> {:ok, value}
      _invalid -> {:error, :invalid_limit}
    end
  end

  defp encode_window_cursor(cursor, context, run_id),
    do:
      encode_cursor(%{
        "v" => @cursor_version,
        "run" => run_id,
        "fingerprint" => cursor_fingerprint(context.workspace_id, run_id),
        "start" => DateTime.to_iso8601(cursor.window_start_at),
        "id" => cursor.window_id
      })

  defp encode_event_cursor(event_id, context, run_id),
    do:
      encode_cursor(%{
        "v" => @cursor_version,
        "run" => run_id,
        "fingerprint" => cursor_fingerprint(context.workspace_id, run_id),
        "event_id" => event_id
      })

  defp encode_cursor(value), do: value |> Jason.encode!() |> Base.url_encode64(padding: false)

  defp decode_window_cursor(nil, _context, _run_id), do: {:ok, nil}

  defp decode_window_cursor(cursor, context, run_id) do
    with {:ok,
          %{
            "v" => @cursor_version,
            "run" => ^run_id,
            "fingerprint" => fingerprint,
            "start" => start,
            "id" => id
          }} <- decode_cursor(cursor),
         true <- fingerprint == cursor_fingerprint(context.workspace_id, run_id),
         {:ok, datetime, 0} <- DateTime.from_iso8601(start),
         true <- is_binary(id) and id != "" do
      {:ok, %{window_start_at: datetime, window_id: id}}
    else
      _invalid -> {:error, :invalid_cursor}
    end
  end

  defp decode_event_cursor(nil, _context, _run_id), do: {:ok, nil}

  defp decode_event_cursor(cursor, context, run_id) do
    with {:ok,
          %{
            "v" => @cursor_version,
            "run" => ^run_id,
            "fingerprint" => fingerprint,
            "event_id" => event_id
          }} <- decode_cursor(cursor),
         true <- fingerprint == cursor_fingerprint(context.workspace_id, run_id),
         true <- is_integer(event_id) and event_id > 0 do
      {:ok, event_id}
    else
      _invalid -> {:error, :invalid_cursor}
    end
  end

  defp decode_cursor(cursor) when is_binary(cursor) do
    with {:ok, json} <- Base.url_decode64(cursor, padding: false), do: Jason.decode(json)
  end

  defp decode_cursor(_cursor), do: {:error, :invalid_cursor}

  defp cursor_fingerprint(workspace_id, run_id) do
    :crypto.hash(:sha256, [workspace_id, 0, run_id])
    |> Base.url_encode64(padding: false)
  end
end
