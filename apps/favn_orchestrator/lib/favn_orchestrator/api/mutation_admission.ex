defmodule FavnOrchestrator.API.MutationAdmission do
  @moduledoc """
  Rejects new private-API mutations after the control plane begins draining.

  Health, diagnostics, streams, and other read-only requests remain available
  until the listener itself stops.
  """

  @behaviour Plug

  import Plug.Conn, only: [get_req_header: 2, halt: 1, register_before_send: 2]

  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.Lifecycle

  @safe_methods ["GET", "HEAD", "OPTIONS"]

  @impl true
  def init(opts), do: opts

  @impl true
  def call(%Plug.Conn{method: method} = conn, _opts) when method in @safe_methods, do: conn

  def call(conn, opts) do
    lifecycle = Keyword.get(opts, :lifecycle, Lifecycle)

    case acquire(conn, lifecycle) do
      {:ok, permit} ->
        register_before_send(conn, fn conn ->
          :ok = Lifecycle.release_admission(permit, lifecycle)
          conn
        end)

      {:error, reason} ->
        {code, message} = error(reason)

        conn
        |> Response.error(503, code, message, %{}, true)
        |> halt()
    end
  end

  defp acquire(conn, lifecycle) do
    case get_req_header(conn, "x-favn-maintenance-token") do
      [token] when token != "" -> Lifecycle.acquire_maintenance_admission(token, lifecycle)
      _missing_or_ambiguous -> Lifecycle.acquire_admission(lifecycle)
    end
  end

  defp error(:runtime_starting),
    do: {"runtime_starting", "Control plane is not accepting mutations yet"}

  defp error(:runtime_maintenance),
    do: {"runtime_maintenance", "Control plane is in a bounded maintenance window"}

  defp error(_reason),
    do: {"runtime_draining", "Control plane is draining and is not accepting mutations"}
end
