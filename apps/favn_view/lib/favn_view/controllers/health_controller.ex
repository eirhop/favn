defmodule FavnView.HealthController do
  use FavnView, :controller

  alias FavnView.Readiness

  def live(conn, _params) do
    json(conn, %{data: Readiness.normalize(Readiness.liveness())})
  end

  def ready(conn, _params) do
    readiness = Readiness.readiness()
    status = if readiness.status == :ready, do: 200, else: 503

    conn
    |> put_status(status)
    |> json(%{data: Readiness.normalize(readiness)})
  end
end
