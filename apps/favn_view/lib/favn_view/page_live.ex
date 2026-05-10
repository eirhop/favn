defmodule FavnView.PageLive do
  @moduledoc false

  use FavnView, :live_view

  @impl true
  def mount(_params, _session, socket) do
    {:ok, redirect(socket, to: ~p"/assets")}
  end
end
