defmodule FavnOrchestrator.API.MutationAdmission do
  @moduledoc """
  Rejects new private-API mutations after the control plane begins draining.

  Health, diagnostics, streams, and other read-only requests remain available
  until the listener itself stops.
  """

  @behaviour Plug

  import Plug.Conn, only: [halt: 1, register_before_send: 2]

  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.Lifecycle

  @safe_methods ["GET", "HEAD", "OPTIONS"]

  @impl true
  def init(opts), do: opts

  @impl true
  def call(%Plug.Conn{method: method} = conn, _opts) when method in @safe_methods, do: conn

  def call(conn, opts) do
    lifecycle = Keyword.get(opts, :lifecycle, Lifecycle)

    case Lifecycle.acquire_admission(lifecycle) do
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

  defp error(:runtime_starting),
    do: {"runtime_starting", "Control plane is not accepting mutations yet"}

  defp error(_reason),
    do: {"runtime_draining", "Control plane is draining and is not accepting mutations"}
end
