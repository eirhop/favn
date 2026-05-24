defmodule FavnView.LiveRefresh do
  @moduledoc false

  import Phoenix.Component, only: [assign: 3]

  @type assign_key :: atom()
  @type message :: atom()
  @type token :: reference()

  @spec init(Phoenix.LiveView.Socket.t(), [assign_key()]) :: Phoenix.LiveView.Socket.t()
  def init(socket, keys) when is_list(keys) do
    Enum.reduce(keys, socket, &assign(&2, &1, nil))
  end

  @spec schedule_once(Phoenix.LiveView.Socket.t(), assign_key(), message(), non_neg_integer()) ::
          Phoenix.LiveView.Socket.t()
  def schedule_once(socket, key, message, delay_ms) do
    if Map.get(socket.assigns, key) do
      socket
    else
      token = make_ref()
      Process.send_after(self(), {message, token}, delay_ms)
      assign(socket, key, token)
    end
  end

  @spec take(Phoenix.LiveView.Socket.t(), assign_key(), token()) ::
          {:ok, Phoenix.LiveView.Socket.t()} | {:stale, Phoenix.LiveView.Socket.t()}
  def take(socket, key, token) do
    if Map.get(socket.assigns, key) == token do
      {:ok, assign(socket, key, nil)}
    else
      {:stale, socket}
    end
  end
end
