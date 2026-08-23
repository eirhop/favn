defmodule FavnView.LiveViewSerializer do
  @moduledoc false

  @behaviour Phoenix.Socket.Serializer

  alias FavnView.RunDetailTelemetry
  alias Phoenix.Socket.V2.JSONSerializer

  @impl true
  def fastlane!(message), do: JSONSerializer.fastlane!(message)

  @impl true
  def encode!(message) do
    result = JSONSerializer.encode!(message)
    {:socket_push, _opcode, encoded} = result
    :ok = RunDetailTelemetry.finish_render(IO.iodata_length(encoded))
    result
  end

  @impl true
  def decode!(message, opts), do: JSONSerializer.decode!(message, opts)
end
