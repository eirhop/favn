defmodule FavnSQLRuntime.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [Favn.SQL.Admission.Limiter, Favn.SQL.SessionPool]
    Supervisor.start_link(children, strategy: :one_for_one, name: FavnSQLRuntime.Supervisor)
  end
end
