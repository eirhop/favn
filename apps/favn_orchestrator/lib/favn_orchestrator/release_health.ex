defmodule FavnOrchestrator.ReleaseHealth do
  @moduledoc """
  Container-local readiness probe for the control-plane release.

  The probe opens one bounded HTTP connection to the validated View bind
  address and accepts only a 200 response from the public readiness endpoint.
  The wildcard bind maps to loopback for probing. The check does not start
  applications, log configuration, or require an HTTP client executable in the
  final image.
  """

  @default_port 4000
  @default_timeout_ms 3_000

  @type error :: :invalid_host | :invalid_port | :connect_failed | :request_failed | :not_ready

  @doc "Checks the configured View readiness endpoint from inside the release container."
  @spec run(map()) :: :ok | {:error, error()}
  def run(env \\ System.get_env()) when is_map(env) do
    with {:ok, address, host} <- bind_address(Map.get(env, "FAVN_VIEW_BIND_HOST", "0.0.0.0")),
         {:ok, port} <- port(Map.get(env, "FAVN_VIEW_PORT", Integer.to_string(@default_port))),
         {:ok, socket} <- connect(address, port),
         :ok <- request(socket, host),
         :ok <- response(socket) do
      :ok
    end
  end

  @doc "Runs the readiness check and raises a bounded error for release scripts."
  @spec run!() :: :ok
  def run! do
    case run() do
      :ok -> :ok
      {:error, reason} -> raise "control-plane readiness check failed: #{reason}"
    end
  end

  defp port(value) when is_binary(value) do
    case Integer.parse(value) do
      {port, ""} when port in 1..65_535 -> {:ok, port}
      _invalid -> {:error, :invalid_port}
    end
  end

  defp port(_value), do: {:error, :invalid_port}

  defp bind_address(value) when is_binary(value) do
    case :inet.parse_ipv4_address(String.to_charlist(value)) do
      {:ok, {0, 0, 0, 0}} -> {:ok, {127, 0, 0, 1}, "127.0.0.1"}
      {:ok, {_a, _b, _c, _d} = address} -> {:ok, address, value}
      _invalid -> {:error, :invalid_host}
    end
  end

  defp bind_address(_value), do: {:error, :invalid_host}

  defp connect(address, port) do
    case :gen_tcp.connect(
           address,
           port,
           [
             :binary,
             active: false,
             packet: :line,
             send_timeout: @default_timeout_ms
           ],
           @default_timeout_ms
         ) do
      {:ok, socket} -> {:ok, socket}
      {:error, _reason} -> {:error, :connect_failed}
    end
  end

  defp request(socket, host) do
    case :gen_tcp.send(
           socket,
           "GET /health/ready HTTP/1.1\r\nHost: #{host}\r\nConnection: close\r\n\r\n"
         ) do
      :ok -> :ok
      {:error, _reason} -> close(socket, :request_failed)
    end
  end

  defp response(socket) do
    result =
      case :gen_tcp.recv(socket, 0, @default_timeout_ms) do
        {:ok, "HTTP/1.1 200 " <> _rest} -> :ok
        {:ok, "HTTP/1.0 200 " <> _rest} -> :ok
        {:ok, _other} -> {:error, :not_ready}
        {:error, _reason} -> {:error, :not_ready}
      end

    _ = :gen_tcp.close(socket)
    result
  end

  defp close(socket, reason) do
    _ = :gen_tcp.close(socket)
    {:error, reason}
  end
end
