defmodule FavnView.LogsLiveSupportTest do
  use ExUnit.Case, async: false

  alias Favn.Log.Entry
  alias FavnView.LogsLiveSupport
  alias Phoenix.LiveView.Socket

  setup do
    keys = [:list_logs_fun, :replay_logs_fun, :log_subscribe_fun]
    previous = Map.new(keys, &{&1, Application.get_env(:favn_view, &1)})

    Application.put_env(:favn_view, :log_subscribe_fun, fn _, _ ->
      send(self(), :subscribed)
      {:ok, nil}
    end)

    on_exit(fn ->
      Enum.each(previous, fn {key, value} ->
        if is_nil(value),
          do: Application.delete_env(:favn_view, key),
          else: Application.put_env(:favn_view, key, value)
      end)
    end)

    :ok
  end

  test "subscribes before an empty snapshot and replays from its independent watermark" do
    Application.put_env(:favn_view, :list_logs_fun, fn _, _, _ ->
      assert_received :subscribed
      {:ok, %{items: [], replay_cursor: cursor(12)}}
    end)

    Application.put_env(:favn_view, :replay_logs_fun, fn _, cursor, _, _ ->
      assert cursor == cursor(12)
      {:ok, %{items: [entry(13)], replay_cursor: cursor(15), has_more?: false}}
    end)

    socket = mount()
    assert Enum.map(socket.assigns.logs, & &1.id) == ["event-13"]
    assert socket.assigns.next_cursor == cursor(15)
    assert socket.assigns.live?
  end

  test "bounded replay schedules another mailbox turn and trimming does not change progress" do
    Application.put_env(:favn_view, :list_logs_fun, fn _, _, _ ->
      {:ok, %{items: Enum.map(1..500, &entry/1), replay_cursor: cursor(500)}}
    end)

    Application.put_env(:favn_view, :replay_logs_fun, fn _, cursor, _, _ ->
      case cursor.publication_id do
        500 ->
          {:ok,
           %{items: Enum.map(501..700, &entry/1), replay_cursor: cursor(700), has_more?: true}}

        700 ->
          {:ok, %{items: [entry(700), entry(701)], replay_cursor: cursor(900), has_more?: false}}

        900 ->
          {:ok, %{items: [], replay_cursor: cursor(950), has_more?: false}}
      end
    end)

    socket = mount()
    assert_received :favn_logs_available
    socket = LogsLiveSupport.wakeup(socket)
    assert length(socket.assigns.logs) == 200
    assert List.last(socket.assigns.logs).id == "event-701"
    assert socket.assigns.next_cursor == cursor(900)
    socket = LogsLiveSupport.wakeup(socket)
    assert socket.assigns.next_cursor == cursor(950)
    assert List.last(socket.assigns.logs).id == "event-701"
  end

  test "revoked authorization or transient failure retains the last successful cursor" do
    Application.put_env(:favn_view, :list_logs_fun, fn _, _, _ ->
      {:ok, %{items: [entry(10)], replay_cursor: cursor(20)}}
    end)

    Application.put_env(:favn_view, :replay_logs_fun, fn _, _, _, _ -> {:error, :unauthorized} end)

    socket = mount()
    assert socket.assigns.next_cursor == cursor(20)
    assert socket.assigns.stream_warning == "Unable to refresh logs."
    assert List.last(socket.assigns.logs).id == "event-10"
  end

  test "successful polling preserves the unavailable subscription warning" do
    Application.put_env(:favn_view, :log_subscribe_fun, fn _, _ -> {:error, :unavailable} end)

    Application.put_env(:favn_view, :list_logs_fun, fn _, _, _ ->
      {:ok, %{items: [], replay_cursor: cursor(0)}}
    end)

    Application.put_env(:favn_view, :replay_logs_fun, fn _, _, _, _ ->
      {:ok, %{items: [], replay_cursor: cursor(1), has_more?: false}}
    end)

    socket = mount()
    refute socket.assigns.live?
    assert socket.assigns.stream_warning =~ "live streaming is unavailable"

    Application.put_env(:favn_view, :replay_logs_fun, fn _, _, _, _ -> {:error, :unavailable} end)
    socket = LogsLiveSupport.poll(socket)
    assert socket.assigns.stream_warning == "Unable to refresh logs."

    Application.put_env(:favn_view, :replay_logs_fun, fn _, _, _, _ ->
      {:ok, %{items: [], replay_cursor: cursor(2), has_more?: false}}
    end)

    socket = LogsLiveSupport.poll(socket)
    assert socket.assigns.stream_warning =~ "live streaming is unavailable"
    assert socket.assigns.next_cursor == cursor(2)
  end

  test "a missed notification is recovered by the periodic poll" do
    Application.put_env(:favn_view, :list_logs_fun, fn _, _, _ ->
      {:ok, %{items: [], replay_cursor: cursor(0)}}
    end)

    Application.put_env(:favn_view, :replay_logs_fun, fn _, _, _, _ ->
      {:ok, %{items: [], replay_cursor: cursor(1), has_more?: false}}
    end)

    socket = mount()

    Application.put_env(:favn_view, :replay_logs_fun, fn _, after_cursor, _, _ ->
      assert after_cursor == cursor(1)
      {:ok, %{items: [entry(2)], replay_cursor: cursor(3), has_more?: false}}
    end)

    assert [%{id: "event-2"}] = LogsLiveSupport.poll(socket).assigns.logs
  end

  test "backend filter changes replace the snapshot and text search keeps its cursor" do
    Application.put_env(:favn_view, :list_logs_fun, fn _, filter, _ ->
      case filter.levels do
        [] -> {:ok, %{items: [], replay_cursor: cursor(10)}}
        [:error] -> {:ok, %{items: [%{entry(2) | level: :error}], replay_cursor: cursor(20)}}
      end
    end)

    Application.put_env(:favn_view, :replay_logs_fun, fn _, cursor, _, _ ->
      {:ok, %{items: [], replay_cursor: cursor, has_more?: false}}
    end)

    socket = mount()
    socket = LogsLiveSupport.handle_filter(socket, %{"filters" => %{"level" => "error"}})
    assert socket.assigns.next_cursor == cursor(20)
    assert [%{id: "event-2"}] = socket.assigns.logs

    Application.put_env(:favn_view, :list_logs_fun, fn _, _, _ ->
      flunk("text search should stay local")
    end)

    socket =
      LogsLiveSupport.handle_filter(socket, %{
        "filters" => %{"level" => "error", "search" => "missing"}
      })

    assert socket.assigns.visible_logs == []
    assert socket.assigns.next_cursor == cursor(20)
  end

  defp mount do
    socket = %Socket{
      transport_pid: self(),
      assigns: %{__changed__: %{}, current_scope: "Etc/UTC"}
    }

    LogsLiveSupport.mount_logs(socket, %{filter: %{}, scope: :global, operator_context: :operator})
  end

  defp cursor(id), do: %{publication_id: id, batch_offset: 999}

  defp entry(id),
    do: %Entry{
      id: "event-#{id}",
      global_sequence: (id - 1) * 1_000 + 1,
      message: "message",
      occurred_at: ~U[2026-09-15 10:00:00Z]
    }
end
