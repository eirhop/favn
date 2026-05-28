defmodule FavnView.RunEventRefresh do
  @moduledoc false

  import Phoenix.Component, only: [assign: 3]

  alias FavnView.LiveRefresh

  @type run_id :: String.t()
  @type sequence_map :: %{optional(run_id()) => non_neg_integer()}

  @spec init(Phoenix.LiveView.Socket.t(), [atom()]) :: Phoenix.LiveView.Socket.t()
  def init(socket, timer_keys) when is_list(timer_keys) do
    socket
    |> LiveRefresh.init(timer_keys)
    |> assign(:run_event_subscriptions, MapSet.new())
    |> assign(:run_event_sequences, %{})
    |> assign(:pending_run_event_sequences, %{})
    |> assign(:run_events_live?, false)
  end

  @spec sync_subscriptions(Phoenix.LiveView.Socket.t(), [run_id()], sequence_map(), keyword()) ::
          Phoenix.LiveView.Socket.t()
  def sync_subscriptions(socket, run_ids, sequence_by_run_id, opts) do
    socket = merge_sequences(socket, sequence_by_run_id)

    if Phoenix.LiveView.connected?(socket) do
      wanted = run_ids |> clean_run_ids() |> MapSet.new()
      current = Map.get(socket.assigns, :run_event_subscriptions, MapSet.new())

      {socket, subscribed} =
        wanted
        |> MapSet.difference(current)
        |> Enum.reduce({socket, current}, fn run_id, {acc_socket, acc_subscribed} ->
          case subscribe_fun(opts).(run_id) do
            :ok ->
              next_socket = replay_gap(acc_socket, run_id, opts)
              {next_socket, MapSet.put(acc_subscribed, run_id)}

            {:error, _reason} ->
              {acc_socket, acc_subscribed}
          end
        end)

      current
      |> MapSet.difference(wanted)
      |> Enum.each(&unsubscribe_fun(opts).(&1))

      socket
      |> assign(:run_event_subscriptions, MapSet.intersection(subscribed, wanted))
      |> assign(:run_events_live?, MapSet.size(MapSet.intersection(subscribed, wanted)) > 0)
    else
      socket
    end
  end

  @spec handle_event(Phoenix.LiveView.Socket.t(), map(), keyword()) :: Phoenix.LiveView.Socket.t()
  def handle_event(socket, %{run_id: run_id, sequence: sequence} = event, opts)
      when is_binary(run_id) and is_integer(sequence) do
    subscriptions = Map.get(socket.assigns, :run_event_subscriptions, MapSet.new())
    latest_sequence = socket.assigns.run_event_sequences |> Map.get(run_id, 0)

    if MapSet.member?(subscriptions, run_id) and sequence > latest_sequence do
      socket
      |> put_pending_sequence(event)
      |> schedule_refresh(opts)
    else
      socket
    end
  end

  def handle_event(socket, _event, _opts), do: socket

  @spec mark_refreshed(Phoenix.LiveView.Socket.t(), sequence_map()) :: Phoenix.LiveView.Socket.t()
  def mark_refreshed(socket, sequence_by_run_id) when is_map(sequence_by_run_id) do
    socket
    |> merge_sequences(sequence_by_run_id)
    |> assign(:pending_run_event_sequences, %{})
  end

  @spec unsubscribe_all(Phoenix.LiveView.Socket.t(), (run_id() -> term())) :: :ok
  def unsubscribe_all(socket, unsubscribe_fun) when is_function(unsubscribe_fun, 1) do
    socket.assigns
    |> Map.get(:run_event_subscriptions, MapSet.new())
    |> Enum.each(unsubscribe_fun)

    :ok
  end

  defp replay_gap(socket, run_id, opts) do
    after_sequence = socket.assigns.run_event_sequences |> Map.get(run_id, 0)

    case list_events_fun(opts).(run_id, after_sequence: after_sequence, limit: 200) do
      {:ok, []} ->
        socket

      {:ok, events} ->
        events
        |> Enum.reduce(socket, &put_pending_sequence(&2, &1))
        |> schedule_refresh(opts)

      {:error, _reason} ->
        schedule_refresh(socket, opts)
    end
  end

  defp put_pending_sequence(socket, %{run_id: run_id, sequence: sequence})
       when is_binary(run_id) and is_integer(sequence) do
    pending = Map.get(socket.assigns, :pending_run_event_sequences, %{})
    current = Map.get(pending, run_id, 0)

    assign(socket, :pending_run_event_sequences, Map.put(pending, run_id, max(sequence, current)))
  end

  defp put_pending_sequence(socket, _event), do: socket

  defp merge_sequences(socket, sequence_by_run_id) when is_map(sequence_by_run_id) do
    sequences = Map.get(socket.assigns, :run_event_sequences, %{})
    pending = Map.get(socket.assigns, :pending_run_event_sequences, %{})

    merged =
      [sequences, pending, sequence_by_run_id]
      |> Enum.reduce(%{}, fn values, acc ->
        Enum.reduce(values, acc, fn {run_id, sequence}, next ->
          if is_binary(run_id) and is_integer(sequence) do
            Map.update(next, run_id, sequence, &max(&1, sequence))
          else
            next
          end
        end)
      end)

    assign(socket, :run_event_sequences, merged)
  end

  defp schedule_refresh(socket, opts) do
    LiveRefresh.schedule_once(
      socket,
      Keyword.fetch!(opts, :refresh_key),
      Keyword.fetch!(opts, :refresh_message),
      Keyword.fetch!(opts, :coalesce_ms)
    )
  end

  defp clean_run_ids(run_ids) do
    run_ids
    |> List.wrap()
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> Enum.uniq()
  end

  defp subscribe_fun(opts), do: Keyword.fetch!(opts, :subscribe_fun)
  defp unsubscribe_fun(opts), do: Keyword.fetch!(opts, :unsubscribe_fun)
  defp list_events_fun(opts), do: Keyword.fetch!(opts, :list_events_fun)
end
